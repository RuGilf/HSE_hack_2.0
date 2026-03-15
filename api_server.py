"""
FastAPI сервер для расширения VkusVill Ratings.
Отдаёт оценки продуктов из БД по названию.

Запуск: uvicorn api_server:app --reload --host 0.0.0.0 --port 8000
"""

import sqlite3
import unicodedata
import re
from pathlib import Path

from contextlib import asynccontextmanager

from fastapi import Body, FastAPI
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Предзагрузка кэша при старте сервера."""
    _load_ratings_cache()
    yield


app = FastAPI(title="VkusVill Ratings API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
)

DB_DIR = Path(__file__).parent / "vkusvill_data"

# Кэш: загружаем все продукты в память при старте (≈13k строк)
_ratings_exact: dict[str, dict] = {}  # norm_name -> {score, pros, cons} для O(1)
_ratings_fuzzy: list[tuple[str, int, str, str]] = []  # (norm_name, score, pros, cons) для подстрок
_ratings_by_url: dict[str, dict] = {}  # url -> {score, pros, cons}
_cache_loaded = False


def normalize_for_match(s: str) -> str:
    """Нормализация для нечёткого сопоставления."""
    s = unicodedata.normalize("NFKC", (s or "").strip().lower())
    s = re.sub(r"\s+", " ", s)
    return s


def find_db_with_scores() -> Path | None:
    """Ищет БД с оценками. Приоритет: products_p1.db, затем p1_worker, phase1, final."""
    candidates = [DB_DIR / "products_p1.db"]
    candidates.extend(DB_DIR.glob("products_p1_worker_*.db"))
    candidates.extend([
        DB_DIR / "products_phase1.db",
        DB_DIR / "products_final.db",
    ])
    for p in candidates:
        if p.exists():
            return p
    return None


def _load_ratings_cache() -> None:
    """Загружает все продукты из БД в память. Один раз при первом запросе."""
    global _ratings_exact, _ratings_fuzzy, _ratings_by_url, _cache_loaded
    if _cache_loaded:
        return
    db_path = find_db_with_scores()
    if not db_path:
        _cache_loaded = True
        return
    try:
        con = sqlite3.connect(str(db_path), timeout=15)
        cur = con.cursor()
        rows = []
        try:
            cur.execute(
                "SELECT url, name, score, pros, cons FROM products WHERE name IS NOT NULL AND status = 'scored'"
            )
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            rows = []
        if not rows:
            try:
                cur.execute(
                    "SELECT url, name, score_p2, pros, cons FROM final_products WHERE name IS NOT NULL"
                )
                rows = cur.fetchall()
            except sqlite3.OperationalError:
                rows = []
        con.close()
        for row in rows:
            url = (row[0] or "").strip()
            name = row[1]
            n = normalize_for_match(name)
            if not n:
                continue
            data = {
                "score": row[2] or 0,
                "pros": (row[3] or "").strip() or "—",
                "cons": (row[4] or "").strip() or "—",
            }
            if n not in _ratings_exact:
                _ratings_exact[n] = data
            _ratings_fuzzy.append((n, data["score"], data["pros"], data["cons"]))
            if url:
                _ratings_by_url[url] = data
    except Exception:
        pass
    _cache_loaded = True


def get_rating_from_db(name: str) -> dict | None:
    """Ищет продукт по названию в in-memory кэше. O(1) для точного совпадения."""
    _load_ratings_cache()
    norm_query = normalize_for_match(name)
    if not norm_query:
        return None
    if norm_query in _ratings_exact:
        return _ratings_exact[norm_query]
    for n, score, pros, cons in _ratings_fuzzy:
        if norm_query in n or n in norm_query:
            return {"score": score, "pros": pros, "cons": cons}
    return None


def get_ratings_batch(names: list[str]) -> dict[str, dict]:
    """Возвращает {name: {score, pros, cons}}. Один проход по кэшу для всех имён."""
    _load_ratings_cache()
    empty = {"score": None, "pros": "", "cons": ""}
    queries = [(n, normalize_for_match(n)) for x in names if (n := (x or "").strip())]
    result = {name: empty.copy() for name, _ in queries}
    # Точные совпадения O(1)
    for name, norm in queries:
        if norm and norm in _ratings_exact:
            result[name] = _ratings_exact[norm]
    # Нечёткий поиск для оставшихся
    pending = {name: norm for name, norm in queries if norm and result[name]["score"] is None}
    if pending:
        for n, score, pros, cons in _ratings_fuzzy:
            for name, norm_query in list(pending.items()):
                if norm_query in n or n in norm_query:
                    result[name] = {"score": score, "pros": pros, "cons": cons}
                    del pending[name]
                    if not pending:
                        break
            if not pending:
                break
    return result


@app.get("/get_rating")
def get_rating(name: str = ""):
    """
    GET /get_rating?name=Бананы
    Ответ: { "score": 77, "pros": "...", "cons": "..." }
    """
    if not name.strip():
        return {"score": None, "pros": "", "cons": ""}

    data = get_rating_from_db(name)
    if data:
        return data
    return {"score": None, "pros": "", "cons": ""}


@app.post("/get_ratings_batch")
def get_ratings_batch_endpoint(names: list[str] = Body(...)):
    """
    POST /get_ratings_batch
    Body: ["Бананы", "Молоко", ...]
    Ответ: { "ratings": { "Бананы": {score, pros, cons}, ... } }
    """
    if not names:
        return {"ratings": {}}
    ratings = get_ratings_batch(names)
    return {"ratings": ratings}


def _get_recommendations(url: str) -> list[dict]:
    """Рекомендации по URL товара."""
    path = DB_DIR / "recommendations.db"
    if not path.exists():
        return []
    try:
        con = sqlite3.connect(str(path), timeout=5)
        cur = con.execute(
            "SELECT target_url, target_name, target_score, reason, score_diff, price_diff_pct, "
            "target_pros, target_cons, target_price, target_weight "
            "FROM recommendations WHERE source_url = ? ORDER BY rank LIMIT 10",
            (url.strip(),),
        )
        rows = cur.fetchall()
        con.close()
        return [
            {
                "url": r[0],
                "name": r[1],
                "score": r[2],
                "reason": r[3] or "",
                "score_diff": r[4],
                "price_diff_pct": r[5],
                "pros": (r[6] or "").strip() or "—",
                "cons": (r[7] or "").strip() or "—",
                "price": r[8],
                "weight": r[9] or "",
            }
            for r in rows
        ]
    except Exception:
        return []


def _get_cluster_info(url: str) -> dict | None:
    """Информация о кластере по URL товара."""
    path = DB_DIR / "recommendations.db"
    if not path.exists():
        return None
    try:
        con = sqlite3.connect(str(path), timeout=5)
        cur = con.execute(
            "SELECT cluster_id, cluster_name, is_budget FROM product_recommendations WHERE url = ?",
            (url.strip(),),
        )
        row = cur.fetchone()
        con.close()
        if row and row[0] is not None and row[0] >= 0:
            return {"cluster_id": row[0], "cluster_name": row[1] or "", "is_budget": bool(row[2])}
    except Exception:
        pass
    return None


def _get_cluster_tops(cluster_id: int, limit: int = 5) -> list[dict]:
    """Топ товаров в кластере по score."""
    path = DB_DIR / "clusters.db"
    if not path.exists() or cluster_id < 0:
        return []
    try:
        con = sqlite3.connect(str(path), timeout=5)
        cur = con.execute(
            "SELECT t.url, t.name, t.score, t.value_score, p.price, p.weight "
            "FROM cluster_tops t "
            "LEFT JOIN cluster_products p ON t.url = p.url "
            "WHERE t.cluster_id = ? AND t.top_type = 'score' ORDER BY t.rank LIMIT ?",
            (cluster_id, limit),
        )
        rows = cur.fetchall()
        con.close()
        return [
            {"url": r[0], "name": r[1], "score": r[2], "value_score": r[3], "price": r[4], "weight": r[5] or ""}
            for r in rows
        ]
    except Exception:
        return []


def _get_badges_batch(urls: list[str]) -> dict[str, dict]:
    """Плашки «Лучший товар» и «Выгодный» по URL из cluster_tops (rank=1)."""
    path = DB_DIR / "clusters.db"
    if not path.exists() or not urls:
        return {u: {"is_best": False, "is_value": False} for u in urls}
    urls = [u.strip() for u in urls if u and "vkusvill.ru/goods/" in u]
    if not urls:
        return {}
    result = {u: {"is_best": False, "is_value": False} for u in urls}
    try:
        con = sqlite3.connect(str(path), timeout=5)
        placeholders = ",".join("?" * len(urls))
        cur = con.execute(
            f"SELECT url, top_type FROM cluster_tops WHERE rank = 1 AND url IN ({placeholders})",
            urls,
        )
        for url, top_type in cur.fetchall():
            if url in result:
                if top_type == "score":
                    result[url]["is_best"] = True
                elif top_type == "value":
                    result[url]["is_value"] = True
        con.close()
    except Exception:
        pass
    return result


@app.post("/get_badges_batch")
def get_badges_batch(urls: list[str] = Body(...)):
    """
    POST /get_badges_batch
    Body: ["https://vkusvill.ru/goods/...", ...]
    Ответ: { "badges": { url: { is_best, is_value }, ... } }
    """
    badges = _get_badges_batch(urls or [])
    return {"badges": badges}


@app.get("/get_recommendations")
def get_recommendations(url: str = ""):
    """
    GET /get_recommendations?url=https://vkusvill.ru/goods/...
    Ответ: { "recommendations": [{url, name, score, reason, score_diff, price_diff_pct, pros, cons}, ...] }
    """
    if not url.strip():
        return {"recommendations": []}
    return {"recommendations": _get_recommendations(url)}


@app.get("/get_product_extended")
def get_product_extended(url: str = "", name: str = ""):
    """
    GET /get_product_extended?url=...&name=...
    Полная информация: рейтинг + кластер + рекомендации.
    """
    _load_ratings_cache()
    result = {"rating": None, "cluster": None, "recommendations": [], "cluster_tops": [], "badges": None}
    url = url.strip()
    name = name.strip()
    if url:
        result["recommendations"] = _get_recommendations(url)
        cluster = _get_cluster_info(url)
        result["cluster"] = cluster
        if cluster and cluster.get("cluster_id", -1) >= 0:
            result["cluster_tops"] = _get_cluster_tops(cluster["cluster_id"], 5)
        if not result["rating"]:
            result["rating"] = _ratings_by_url.get(url)
        badges = _get_badges_batch([url])
        result["badges"] = badges.get(url, {"is_best": False, "is_value": False})
    if name and not result["rating"]:
        result["rating"] = get_rating_from_db(name)
    return result


@app.get("/health")
def health():
    return {"status": "ok", "db": str(find_db_with_scores())}


@app.get("/debug_sample")
def debug_sample():
    """Проверка: возвращает первый продукт с pros/cons из БД."""
    db_path = find_db_with_scores()
    if not db_path:
        return {"error": "no db", "db_dir": str(DB_DIR)}
    try:
        con = sqlite3.connect(str(db_path), timeout=5)
        cur = con.execute(
            "SELECT name, score, pros, cons FROM products WHERE status='scored' LIMIT 1"
        )
        row = cur.fetchone()
        con.close()
        if row:
            return {"name": row[0], "score": row[1], "pros": row[2], "cons": row[3], "db": str(db_path)}
        return {"error": "no scored rows"}
    except Exception as e:
        return {"error": str(e), "db": str(db_path)}
