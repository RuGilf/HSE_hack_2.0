"""
VkusVill Scorer v4 — Phase 1 (scoring) + Phase 2 (TrueSkill refinement)
════════════════════════════════════════════════════════════════════════

pip install cerebras-cloud-sdk aiosqlite trueskill

Идея Phase 2:
  LLM плохо ставит абсолютные оценки (разброс ±10), но хорошо ранжирует
  5 продуктов. TrueSkill превращает ранги в точный глобальный рейтинг.
  Каждый запрос = 5 продуктов → C(5,2)=10 пар → σ быстро сходится.

Математика (15k, 2 ключа, 7h):
  2 × 900 req/h × 7h = 12 600 матчей × 5 прод = 63 000 появлений
  63 000 / 15 000 = 4.2 на продукт × 4 пар/матч = ~17 сравнений
  σ: 10.0 → ~3.5 (конвергенция)

Схема:
  Phase 1: products.json → p1_worker_N.db → [p1merge] → products_phase1.db
  Phase 2: products_phase1.db → p2_worker_N.db → [p2merge] → products_final.db

Команды:
  python scorer_v4.py solo                               # одна машина

  # Две машины — Phase 1:
  PC1: python scorer_v4.py p1score --worker 0 --total 2 --key csk-AAA
  PC2: python scorer_v4.py p1score --worker 1 --total 2 --key csk-BBB
       python scorer_v4.py p1merge \\
           vkusvill_data/products_p1_worker_0.db \\
           vkusvill_data/products_p1_worker_1.db

  # Phase 2 (оба ПК читают products_phase1.db):
  PC1: python scorer_v4.py p2score --worker 0 --total 2 --key csk-AAA
  PC2: python scorer_v4.py p2score --worker 1 --total 2 --key csk-BBB
       python scorer_v4.py p2merge \\
           vkusvill_data/products_p2_worker_0.db \\
           vkusvill_data/products_p2_worker_1.db
"""

import json
import asyncio
import time
import re
import os
import hashlib
import argparse
import sqlite3
import random
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import aiosqlite
from cerebras.cloud.sdk import AsyncCerebras
import trueskill as ts_lib

# ════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════

CEREBRAS_API_KEY = os.environ.get("CEREBRAS_API_KEY", "csk-md8nrnefvcyt5542e26vtkytyvw2n86k28fdey6nm28yp8r3")
MODEL            = "qwen-3-235b-a22b-instruct-2507"
TOP_P            = 0.8
MAX_CONCURRENT   = 5
MAX_RETRIES      = 5
RETRY_BASE       = 2.0

# Phase 1 — абсолютный скоринг
TEMP_P1   = 0.7
MAXTOK_P1 = 8192
BSIZE_P1  = 5

# Phase 2 — ранжирование для TrueSkill
TEMP_P2   = 0.30   # ниже температура → стабильнее ранги
MAXTOK_P2 = 2048   # запас для внутреннего thinking Qwen
BSIZE_P2  = 5      # продуктов в одном матче

# TrueSkill параметры
TS_SIGMA_INIT = 10.0   # начальная неопределённость (шум Phase 1 ≈ ±10)
TS_BETA       = 4.0    # шум исполнения (надёжность LLM-ранжирования)
TS_TAU        = 0.15   # динамика: мало → быстрая стабилизация
TS_DRAW       = 0.05   # вероятность ничьей
TS_SIGMA_GOAL = 3.5    # стоп когда avg σ < этого

P2_MAX_HOURS  = 7.0   # жёсткий лимит по времени
P2_BATCH_SZ   = 0     # 0 = полный раунд (все продукты по разу)

INPUT_FILE    = Path("vkusvill_data") / "products.json"
DB_DIR        = Path("vkusvill_data")
WATCH_SEC     = 60

LABELS = list("ABCDE")


# ════════════════════════════════════════════════════════
# SHARED UTILITIES
# ════════════════════════════════════════════════════════

def shard_of(url: str, total: int) -> int:
    return int(hashlib.md5(url.encode()).hexdigest(), 16) % total


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_json_array(text: str) -> list:
    """Вытащить JSON-массив из текста, срезав теги думалки и markdown."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = re.sub(r"```(?:json)?", "", text).replace("```", "")
    s, e = text.find("["), text.rfind("]")
    if s == -1 or e <= s:
        raise json.JSONDecodeError("No array found", text, 0)
    return json.loads(text[s : e + 1])


def read_products_json(path: Path) -> list[dict]:
    """Читает JSON с продуктами, переживает незаконченные записи."""
    try:
        raw = path.read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError) as exc:
        print(f"  ⚠️  {path}: {exc}")
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Попытка восстановить частично записанный файл
    raw = raw.rstrip().rstrip(",")
    lb = raw.rfind("}")
    if lb > 0:
        try:
            return json.loads(raw[:lb + 1] + "]")
        except json.JSONDecodeError:
            pass
    print(f"  ⚠️  Не удалось разобрать {path}")
    return []


class RateLimiter:
    """Ограничитель по RPM и RPH с async-ожиданием."""

    def __init__(self, rpm: int = 28, rph: int = 880):
        self.rpm, self.rph = rpm, rph
        self._ts: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                self._ts = [t for t in self._ts if now - t < 3600]
                in_min  = sum(1 for t in self._ts if now - t < 60)
                in_hour = len(self._ts)
                if in_min < self.rpm and in_hour < self.rph:
                    self._ts.append(now)
                    return
                if in_min >= self.rpm:
                    oldest = min(t for t in self._ts if now - t < 60)
                    wait   = 60.0 - (now - oldest) + 0.3
                    reason = f"RPM {in_min}/{self.rpm}"
                else:
                    wait   = 3600.0 - (now - self._ts[0]) + 0.3
                    reason = f"RPH {in_hour}/{self.rph}"
            print(f"  ⏳ {reason} → ждём {wait:.1f}s")
            await asyncio.sleep(wait)


# ════════════════════════════════════════════════════════
# PHASE 1 — ABSOLUTE SCORING  (0–100 per product)
# ════════════════════════════════════════════════════════

P1_SYSTEM = """Ты эксперт-нутрициолог. Тебе дают товары из магазина ВкусВилл.

Дай оценку от 0 до 100 по полезности. Дай ответ в json.
В нем должно быть:
— название товара
— плюсы и минусы кратко, НЕЗАВИСИМО ОТ ДРУГИХ ТОВАРОВ. Через ; с новой строчки.
— и финальная оценка.

Оцениваем товары для среднестатистического человека относительно ВСЕХ продуктов МИРА.

Якоря (правильные оценки, ориентируйся на них):
[
  {"name":"Филе форели на пару с овощами","score":94,"pros":"Белок, Омега-3, овощи, на пару","cons":"Раф. масло, соль"},
  {"name":"Яйцо куриное С0","score":93,"pros":"Эталонный белок, витамины, 0% углеводов","cons":"Холестерин, нужна термообработка"},
  {"name":"Гречка по-купечески","score":92,"pros":"Баланс БЖУ, сложные углеводы, клетчатка","cons":"Натрий, раф. масло"},
  {"name":"Бананы","score":77,"pros":"Калий, магний, быстрая энергия","cons":"Много сахара, высокий ГИ"},
  {"name":"Сосиски ВкусВилл","score":48,"pros":"Мясной состав, без нитрита натрия","cons":"Переработанное мясо, жир, соль"},
  {"name":"Кексы-мини шоколадные","score":35,"pros":"Без маргарина","cons":"Сахар, белая мука, 460 ккал/100г"},
  {"name":"Напиток Вкус-Кола 1л","score":15,"pros":"Натуральные экстракты, без фосфорной кислоты","cons":"Жидкий сахар, пустые калории"}
]

ВАЖНО:
1. Оценивай КАЖДЫЙ продукт НЕЗАВИСИМО от остальных в запросе.
2. Ответ — ТОЛЬКО JSON-массив. Без markdown-обёрток, без пояснений.
3. Формат: [{"name":"…","url":"…","score":0,"pros":"…","cons":"…","price":0,"weight":"…"}]"""


def p1_user_msg(batch: list[dict]) -> str:
    return (f"/no_think\nТовары:\n\n"
            f"{json.dumps(batch, ensure_ascii=False, indent=2)}\n\n"
            f"Оцени каждый от 0 до 100. Ответ — JSON-массив.")


P1_DDL = """
CREATE TABLE IF NOT EXISTS products (
    url            TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    weight         TEXT,
    price          REAL,
    nutrition_json TEXT,
    composition    TEXT,
    scraped_at     TEXT,
    score          INTEGER,
    pros           TEXT,
    cons           TEXT,
    scored_at      TEXT,
    status         TEXT DEFAULT 'pending'
);
CREATE INDEX IF NOT EXISTS idx_p1_status ON products(status);
"""
P1_UPSERT = """
INSERT INTO products (url,name,weight,price,nutrition_json,composition,scraped_at,status)
VALUES (?,?,?,?,?,?,?,'pending')
ON CONFLICT(url) DO UPDATE SET
    name=excluded.name, weight=excluded.weight, price=excluded.price,
    nutrition_json=excluded.nutrition_json, composition=excluded.composition,
    scraped_at=excluded.scraped_at
WHERE products.status != 'scored';
"""
P1_SCORE_SQL = ("UPDATE products SET score=?,pros=?,cons=?,price=?,weight=?,"
                "scored_at=?,status='scored' WHERE url=?;")
P1_ERROR_SQL = "UPDATE products SET status='error',scored_at=? WHERE url=?;"


async def p1_init(db):
    await db.executescript(P1_DDL)
    await db.commit()


async def p1_import(db, products, worker_id, total) -> int:
    added = 0
    for p in products:
        if shard_of(p["url"], total) != worker_id:
            continue
        cur = await db.execute("SELECT status FROM products WHERE url=?", (p["url"],))
        row = await cur.fetchone()
        if row and row[0] == "scored":
            continue
        await db.execute(P1_UPSERT, (
            p["url"], p["name"], p.get("weight"), p.get("price"),
            json.dumps(p.get("nutrition"), ensure_ascii=False) if p.get("nutrition") else None,
            p.get("composition"), p.get("scraped_at"),
        ))
        added += 1
    await db.commit()
    return added


async def p1_pending(db) -> list[dict]:
    cur = await db.execute(
        "SELECT url,name,weight,price,nutrition_json,composition "
        "FROM products WHERE status IN ('pending','error') ORDER BY rowid")
    return [{"url": r[0], "name": r[1], "weight": r[2], "price": r[3],
             "nutrition": json.loads(r[4]) if r[4] else None, "composition": r[5]}
            for r in await cur.fetchall()]


async def p1_save(db, results: list[dict]):
    t = now_iso()
    for r in results:
        if r.get("score", -1) >= 0:
            await db.execute(P1_SCORE_SQL, (r["score"], r.get("pros", ""), r.get("cons", ""),
                                            r.get("price"), r.get("weight"), t, r["url"]))
        else:
            await db.execute(P1_ERROR_SQL, (t, r["url"]))
    await db.commit()


async def p1_stats(db) -> dict:
    cur = await db.execute("SELECT status,COUNT(*) FROM products GROUP BY status")
    return {r[0]: r[1] for r in await cur.fetchall()}


async def p1_export(db, path: Path) -> int:
    cur = await db.execute(
        "SELECT url,name,score,pros,cons,price,weight,nutrition_json,composition,scored_at "
        "FROM products WHERE status='scored' ORDER BY score DESC")
    data = [{"url": r[0], "name": r[1], "score": r[2], "pros": r[3], "cons": r[4],
             "price": r[5], "weight": r[6],
             "nutrition": json.loads(r[7]) if r[7] else None,
             "composition": r[8], "scored_at": r[9]} for r in await cur.fetchall()]
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return len(data)


async def p1_process_batch(client, db, lock, batch, idx, lim, sem):
    async with sem:
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            await lim.acquire()
            try:
                t0 = time.monotonic()
                resp = await client.chat.completions.create(
                    model=MODEL, temperature=TEMP_P1, top_p=TOP_P,
                    max_completion_tokens=MAXTOK_P1,
                    messages=[{"role": "system", "content": P1_SYSTEM},
                               {"role": "user",   "content": p1_user_msg(batch)}])
                results = extract_json_array(resp.choices[0].message.content or "")
                for r in results:
                    r["score"] = max(0, min(100, int(r.get("score", 0))))
                async with lock:
                    await p1_save(db, results)
                print(f"  ✅ p1 batch {idx:>4} | {len(results)} items | {time.monotonic()-t0:.1f}s")
                return results
            except json.JSONDecodeError as e:
                last_err = e
                print(f"  ⚠️  p1 batch {idx} att {attempt}: bad JSON")
            except Exception as e:
                last_err = e
                print(f"  ⚠️  p1 batch {idx} att {attempt}: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES:
                d = RETRY_BASE * 2 ** (attempt - 1)
                if last_err and ("rate" in str(last_err).lower() or "429" in str(last_err)):
                    d = max(d, 60.0)
                await asyncio.sleep(d)

        errors = [{"name": p.get("name", "?"), "url": p["url"], "score": -1,
                   "pros": "", "cons": "ERROR", "price": p.get("price"),
                   "weight": p.get("weight")} for p in batch]
        async with lock:
            await p1_save(db, errors)
        return errors


async def run_p1_worker(worker_id: int, total: int, inp: Path):
    db_path = DB_DIR / f"products_p1_worker_{worker_id}.db"
    js_path = DB_DIR / f"products_p1_worker_{worker_id}.json"
    print(f"\n{'═'*55}\n  PHASE 1 | WORKER {worker_id}/{total} | {db_path}\n{'═'*55}\n")

    if not CEREBRAS_API_KEY:
        print("❌ export CEREBRAS_API_KEY=csk-...")
        return

    DB_DIR.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await p1_init(db)

    stats = await p1_stats(db)
    if stats.get("scored", 0):
        print(f"  📂 RESUME: {stats['scored']} оценённых | {stats}")

    client  = AsyncCerebras(api_key=CEREBRAS_API_KEY)
    lim     = RateLimiter()
    sem     = asyncio.Semaphore(MAX_CONCURRENT)
    lock    = asyncio.Lock()
    no_new  = 0

    while True:
        products = read_products_json(inp)
        if not products:
            await asyncio.sleep(WATCH_SEC)
            continue

        new = await p1_import(db, products, worker_id, total)
        stats = await p1_stats(db)
        print(f"\n  📦 JSON: {len(products)} | новых: {new} | {stats}")

        todo = await p1_pending(db)
        if not todo:
            n = await p1_export(db, js_path)
            print(f"  💾 {n} → {js_path}")
            no_new += 1
            if no_new >= 3:
                print("  🏁 3 раунда без новых. Завершено.")
                break
            await asyncio.sleep(WATCH_SEC)
            continue
        no_new = 0

        batches = [todo[i:i + BSIZE_P1] for i in range(0, len(todo), BSIZE_P1)]
        print(f"  🚀 {len(batches)} батчей × {BSIZE_P1}")
        tasks = [asyncio.create_task(p1_process_batch(client, db, lock, b, i, lim, sem))
                 for i, b in enumerate(batches)]
        for fut in asyncio.as_completed(tasks):
            await fut

        n = await p1_export(db, js_path)
        print(f"  💾 Экспорт: {n} → {js_path}")

    await db.close()


def merge_p1(dbs: list[str], out_path: str):
    """Объединяет несколько Phase 1 баз в одну."""
    out = sqlite3.connect(out_path)
    out.executescript(P1_DDL)

    for p in dbs:
        if not Path(p).exists():
            print(f"  ⚠️  {p} не найден")
            continue
        src = sqlite3.connect(p)
        rows = src.execute(
            "SELECT url,name,weight,price,nutrition_json,composition,"
            "scraped_at,score,pros,cons,scored_at,status FROM products"
        ).fetchall()
        for r in rows:
            out.execute("INSERT OR REPLACE INTO products VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", r)
        print(f"  📥 {p}: {len(rows)} строк")
        src.close()

    out.commit()
    stats = dict(out.execute("SELECT status,COUNT(*) FROM products GROUP BY status").fetchall())
    print(f"  📊 MERGED: {stats}")

    jp = out_path.replace(".db", ".json")
    rows = out.execute(
        "SELECT url,name,score,pros,cons,price,weight,nutrition_json,composition,scored_at "
        "FROM products WHERE status='scored' ORDER BY score DESC"
    ).fetchall()
    data = [{"url": r[0], "name": r[1], "score": r[2], "pros": r[3], "cons": r[4],
             "price": r[5], "weight": r[6],
             "nutrition": json.loads(r[7]) if r[7] else None,
             "composition": r[8], "scored_at": r[9]} for r in rows]
    Path(jp).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  💾 {out_path}\n  📄 {jp} ({len(data)} оценённых товаров)")
    out.close()


# ════════════════════════════════════════════════════════
# PHASE 2 — TRUESKILL REFINEMENT  (ranked matchups of 5)
# ════════════════════════════════════════════════════════
#
# Алгоритм:
#   1. Инициализируем mu из Phase 1 score, sigma = TS_SIGMA_INIT (≈ шум P1)
#   2. Swiss-system: сортируем по mu + gauss(0, sigma), группируем по 5
#      → похожие по рейтингу продукты дерутся между собой
#   3. LLM ранжирует 5 продуктов: лучший → ["C","A","E","B","D"]
#   4. TrueSkill обновляет mu/sigma для всех 10 пар в матче
#   5. При мерже: replay всех сравнений хронологически → финальный рейтинг
#
# Разные воркеры используют разный random seed → разные матчи → больше покрытие.
# ════════════════════════════════════════════════════════

P2_SYSTEM = """Ты эксперт-нутрициолог. Ранжируй 5 продуктов от САМОГО ПОЛЕЗНОГО к НАИМЕНЕЕ ПОЛЕЗНОМУ.

Критерии оценки:
  • Натуральность состава (чем меньше обработки и добавок, тем лучше)
  • Баланс БЖУ (полноценный белок, сложные углеводы, здоровые жиры)
  • Степень переработки (цельные продукты > минимально обработанные > ультраобработанные)
  • Добавки, сахар, трансжиры, насыщенные жиры (чем меньше, тем лучше)
  • Калорийность относительно питательной ценности

Ориентиры по шкале полезности (от лучшего к худшему):
  Форель на пару с овощами (94) > Яйцо куриное (93) > Гречка по-купечески (92) >
  Бананы (77) > Сосиски (48) > Шоколадные кексы (35) > Газировка с сахаром (15)

Ответ — ТОЛЬКО JSON-массив из 5 букв [A–E], от лучшего к худшему.
Пример: ["C","A","E","B","D"]
Без пояснений, без markdown, без дополнительного текста."""

P2_DDL = """
CREATE TABLE IF NOT EXISTS ts_ratings (
    url         TEXT PRIMARY KEY,
    mu          REAL    NOT NULL,
    sigma       REAL    NOT NULL,
    comparisons INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT
);
CREATE TABLE IF NOT EXISTS ts_comparisons (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    matchup_id   TEXT    NOT NULL,
    worker_id    INTEGER NOT NULL,
    urls_json    TEXT    NOT NULL,
    ranking_json TEXT    NOT NULL,
    created_at   TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_p2_time ON ts_comparisons(created_at);
"""


def make_ts_env() -> ts_lib.TrueSkill:
    """Создаёт TrueSkill окружение с нашими параметрами."""
    return ts_lib.TrueSkill(
        mu=50.0,
        sigma=TS_SIGMA_INIT,
        beta=TS_BETA,
        tau=TS_TAU,
        draw_probability=TS_DRAW,
    )


def ts_rate(env: ts_lib.TrueSkill, ratings: dict, ranked_urls: list[str]):
    """
    Обновляет рейтинги in-place.
    ranked_urls: список URL от лучшего к худшему.
    """
    valid = [u for u in ranked_urls if u in ratings]
    if len(valid) < 2:
        return
    teams   = [[ratings[u]] for u in valid]
    updated = env.rate(teams, ranks=list(range(len(valid))))
    for i, u in enumerate(valid):
        ratings[u] = updated[i][0]


def generate_matchups(ratings: dict, rng: random.Random, n: int = 0) -> list[list[str]]:
    """
    Swiss-system матчи: сортируем по mu + gauss(0, sigma), берём последовательные пятёрки.
    Шум пропорционален sigma → неопределённые продукты "прыгают" дальше по списку.
    n=0 → полный раунд (все продукты ровно по разу).
    """
    urls = list(ratings.keys())
    sorted_urls = sorted(urls, key=lambda u: ratings[u].mu + rng.gauss(0, ratings[u].sigma))

    matchups = [sorted_urls[i:i + BSIZE_P2]
                for i in range(0, len(sorted_urls) - BSIZE_P2 + 1, BSIZE_P2)]

    if n > 0:
        rng.shuffle(matchups)
        return matchups[:n]
    return matchups


def fmt_product_p2(label: str, p: dict) -> str:
    """Форматирует продукт для prompt Phase 2 — компактно, но информативно."""
    nut = p.get("nutrition") or {}
    nut_str = (f"Ккал:{nut.get('Ккал', '?')} "
               f"Б:{nut.get('Белки, г', '?')}г "
               f"Ж:{nut.get('Жиры, г', '?')}г "
               f"У:{nut.get('Углеводы, г', '?')}г")
    comp = (p.get("composition") or "—")[:180]
    price  = p.get("price", "?")
    weight = p.get("weight", "?")
    return (f"{label}: {p['name']} ({weight}, {price}₽)\n"
            f"   {nut_str}\n"
            f"   {comp}")


def p2_user_msg(group: list[dict]) -> str:
    lbls  = LABELS[:len(group)]
    parts = [fmt_product_p2(lbls[i], p) for i, p in enumerate(group)]
    return ("Продукты для ранжирования:\n\n" + "\n\n".join(parts) +
            "\n\nОтсортируй от самого полезного к наименее полезному. "
            "Только JSON-массив букв.")


def parse_ranking(raw: str, labels: list[str]) -> list[str]:
    """Парсит ответ LLM, возвращает буквы в порядке ранжирования."""
    arr = [str(x).upper().strip() for x in extract_json_array(raw)]
    if set(arr) != set(labels) or len(arr) != len(labels):
        raise ValueError(f"Неверные метки: {arr}, ожидалось {labels}")
    return arr


async def p2_init(db):
    await db.executescript(P2_DDL)
    await db.commit()


async def p2_load_ratings(db) -> dict:
    """Загружает сохранённые рейтинги: {url: (mu, sigma, comparisons)}"""
    cur = await db.execute("SELECT url,mu,sigma,comparisons FROM ts_ratings")
    return {r[0]: (r[1], r[2], r[3]) for r in await cur.fetchall()}


async def p2_save_ratings(db, ratings: dict, counts: dict):
    t = now_iso()
    for url, r in ratings.items():
        await db.execute(
            "INSERT OR REPLACE INTO ts_ratings (url,mu,sigma,comparisons,updated_at) "
            "VALUES (?,?,?,?,?)",
            (url, r.mu, r.sigma, counts.get(url, 0), t))
    await db.commit()


async def p2_save_comparison(db, mid: str, wid: int, urls: list, ranking: list):
    await db.execute(
        "INSERT INTO ts_comparisons "
        "(matchup_id,worker_id,urls_json,ranking_json,created_at) VALUES (?,?,?,?,?)",
        (mid, wid, json.dumps(urls), json.dumps(ranking), now_iso()))
    await db.commit()


async def p2_compare(client, db, lock, env, ratings, counts,
                     group: list[dict], worker_id: int, lim, sem):
    """Один матч: отправляет группу в LLM, обновляет TrueSkill."""
    urls   = [p["url"] for p in group]
    labels = LABELS[:len(group)]
    mid    = uuid.uuid4().hex[:8]

    async with sem:
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            await lim.acquire()
            try:
                t0 = time.monotonic()
                resp = await client.chat.completions.create(
                    model=MODEL, temperature=TEMP_P2, top_p=TOP_P,
                    max_completion_tokens=MAXTOK_P2,
                    messages=[{"role": "system", "content": P2_SYSTEM},
                               {"role": "user",   "content": p2_user_msg(group)}])

                ranked_labels = parse_ranking(resp.choices[0].message.content or "", labels)
                label_to_url  = dict(zip(labels, urls))
                ranked_urls   = [label_to_url[l] for l in ranked_labels]

                async with lock:
                    ts_rate(env, ratings, ranked_urls)
                    for u in urls:
                        counts[u] = counts.get(u, 0) + 1
                    await p2_save_comparison(db, mid, worker_id, urls, ranked_urls)

                # Лог: показываем реальный порядок ранжирования
                name_order = " > ".join(
                    group[labels.index(l)]["name"][:13] for l in ranked_labels)
                print(f"  ✅ p2 {mid} | {name_order} | {time.monotonic()-t0:.1f}s")
                return True

            except (json.JSONDecodeError, ValueError) as e:
                last_err = e
                print(f"  ⚠️  p2 {mid} att {attempt}: {e}")
            except Exception as e:
                last_err = e
                print(f"  ⚠️  p2 {mid} att {attempt}: {type(e).__name__}: {e}")

            if attempt < MAX_RETRIES:
                d = RETRY_BASE * 2 ** (attempt - 1)
                if last_err and ("rate" in str(last_err).lower() or "429" in str(last_err)):
                    d = max(d, 60.0)
                await asyncio.sleep(d)

    print(f"  ❌ p2 {mid} FAILED after {MAX_RETRIES} попыток")
    return False


def p2_build_output(ratings: dict, counts: dict, url2p: dict) -> list[dict]:
    """Строит отсортированный список товаров с TrueSkill рейтингами."""
    sorted_urls = sorted(ratings, key=lambda u: ratings[u].mu, reverse=True)
    items = []
    for rank, url in enumerate(sorted_urls, 1):
        r = ratings[url]
        p = url2p.get(url, {})
        items.append({
            "rank":        rank,
            "url":         url,
            "name":        p.get("name", ""),
            "score_p2":    max(0, min(100, round(r.mu))),   # финальный score
            "mu":          round(r.mu, 2),
            "sigma":       round(r.sigma, 2),
            "score_p1":    p.get("score_p1"),               # исходный score
            "comparisons": counts.get(url, 0),
            "pros":        p.get("pros", ""),
            "cons":        p.get("cons", ""),
            "price":       p.get("price"),
            "weight":      p.get("weight"),
            "nutrition":   p.get("nutrition"),
            "composition": p.get("composition"),
        })
    return items


def p2_export_sync(ratings: dict, counts: dict, url2p: dict, path: Path) -> int:
    """Атомарный экспорт в JSON."""
    items = p2_build_output(ratings, counts, url2p)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return len(items)


def load_p1_products(p1_path: str) -> list[dict]:
    """Загружает все оценённые товары из Phase 1 БД."""
    con = sqlite3.connect(p1_path)
    rows = con.execute(
        "SELECT url,name,weight,price,nutrition_json,composition,score,pros,cons "
        "FROM products WHERE status='scored'"
    ).fetchall()
    con.close()
    return [{"url": r[0], "name": r[1], "weight": r[2], "price": r[3],
             "nutrition": json.loads(r[4]) if r[4] else None,
             "composition": r[5], "score_p1": r[6], "pros": r[7], "cons": r[8]}
            for r in rows]


async def run_p2_worker(worker_id: int, total: int, p1_db: str):
    db_path = DB_DIR / f"products_p2_worker_{worker_id}.db"
    js_path = DB_DIR / f"products_p2_worker_{worker_id}.json"

    print(f"\n{'═'*55}")
    print(f"  PHASE 2 | WORKER {worker_id}/{total}")
    print(f"  Phase 1: {p1_db}")
    print(f"  Phase 2 DB: {db_path}")
    print(f"  TrueSkill: β={TS_BETA} τ={TS_TAU} σ₀={TS_SIGMA_INIT} σ*={TS_SIGMA_GOAL}")
    print(f"  Лимит: {P2_MAX_HOURS}h | матч: {BSIZE_P2} продуктов")
    print(f"{'═'*55}\n")

    if not CEREBRAS_API_KEY:
        print("❌ export CEREBRAS_API_KEY=csk-...")
        return

    # Загружаем Phase 1
    products = load_p1_products(p1_db)
    if not products:
        print("❌ Пустая Phase 1 БД. Сначала запустите Phase 1.")
        return
    print(f"  📦 Загружено {len(products)} продуктов из Phase 1")
    url2p = {p["url"]: p for p in products}

    DB_DIR.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await p2_init(db)

    env = make_ts_env()

    # Инициализируем или возобновляем рейтинги
    saved  = await p2_load_ratings(db)
    ratings: dict[str, ts_lib.Rating] = {}
    counts: dict[str, int] = {}

    if saved:
        print(f"  📂 RESUME: {len(saved)} рейтингов в БД")
        for url, (mu, sigma, n) in saved.items():
            ratings[url] = env.create_rating(mu=mu, sigma=sigma)
            counts[url]  = n
        # Добавляем новые товары, которых нет в сохранённых рейтингах
        new_added = 0
        for p in products:
            if p["url"] not in ratings:
                s = float(p.get("score_p1") or 50)
                ratings[p["url"]] = env.create_rating(mu=s, sigma=TS_SIGMA_INIT)
                counts[p["url"]]  = 0
                new_added += 1
        if new_added:
            print(f"  ➕ {new_added} новых продуктов добавлено")
    else:
        print("  🆕 Инициализируем рейтинги из Phase 1 scores")
        for p in products:
            s = float(p.get("score_p1") or 50)
            ratings[p["url"]] = env.create_rating(mu=s, sigma=TS_SIGMA_INIT)
            counts[p["url"]]  = 0

    client  = AsyncCerebras(api_key=CEREBRAS_API_KEY)
    lim     = RateLimiter()
    sem     = asyncio.Semaphore(MAX_CONCURRENT)
    lock    = asyncio.Lock()

    # Каждый воркер использует разный seed → разные матчи
    rng     = random.Random(worker_id * 31337 + 1)
    t_start = time.monotonic()
    batch_n = 0
    total_ok = 0

    print(f"\n  ▶️  Старт. Продуктов: {len(ratings)} | σ_goal={TS_SIGMA_GOAL}\n")

    while True:
        # Статистика конвергенции
        sigmas  = [r.sigma for r in ratings.values()]
        avg_s   = sum(sigmas) / len(sigmas)
        max_s   = max(sigmas)
        elapsed = (time.monotonic() - t_start) / 3600
        total_c = sum(counts.values())

        print(f"\n  ══ Батч {batch_n} | σ avg={avg_s:.2f} max={max_s:.2f} | "
              f"матчей={total_ok} | {elapsed:.2f}h/{P2_MAX_HOURS}h")

        if avg_s <= TS_SIGMA_GOAL:
            print(f"  🎯 Конвергенция! avg σ={avg_s:.2f} ≤ {TS_SIGMA_GOAL}")
            break
        if elapsed >= P2_MAX_HOURS:
            print(f"  ⏰ Лимит времени {P2_MAX_HOURS}h")
            break

        # Генерируем матчи
        matchups = generate_matchups(ratings, rng, n=P2_BATCH_SZ)
        print(f"  🎲 Матчей в батче: {len(matchups)} "
              f"({'полный раунд' if P2_BATCH_SZ == 0 else f'из {P2_BATCH_SZ}'})")

        tasks = []
        for grp_urls in matchups:
            grp = [url2p[u] for u in grp_urls if u in url2p]
            if len(grp) < 2:
                continue
            tasks.append(asyncio.create_task(
                p2_compare(client, db, lock, env, ratings, counts,
                           grp, worker_id, lim, sem)))

        ok = 0
        for fut in asyncio.as_completed(tasks):
            if await fut:
                ok += 1
        total_ok += ok
        print(f"  ✅ Батч {batch_n}: {ok}/{len(tasks)} успешно")

        # Сохраняем рейтинги и экспортируем
        async with lock:
            await p2_save_ratings(db, ratings, counts)
        n = p2_export_sync(ratings, counts, url2p, js_path)
        print(f"  💾 {n} → {js_path}")

        batch_n += 1

    # Финал
    async with lock:
        await p2_save_ratings(db, ratings, counts)
    n = p2_export_sync(ratings, counts, url2p, js_path)

    sigmas = [r.sigma for r in ratings.values()]
    print(f"\n  ✅ PHASE 2 DONE")
    print(f"  📊 σ avg={sum(sigmas)/len(sigmas):.2f} | матчей всего: {total_ok}")
    print(f"  📄 {js_path} ({n} товаров)")
    await db.close()


def merge_p2(p2_dbs: list[str], p1_db: str, out_path: str):
    """
    Мержит Phase 2: replay всех сравнений хронологически через TrueSkill.
    Математически корректно — порядок replay несущественен при достаточном количестве матчей.
    """
    print(f"\n  🔀 Мерж Phase 2: {len(p2_dbs)} воркеров")

    # Базовые данные из Phase 1
    products = load_p1_products(p1_db)
    url2p    = {p["url"]: p for p in products}
    print(f"  📦 Phase 1: {len(products)} продуктов")

    # Собираем все сравнения
    all_comps: list[tuple] = []
    for p in p2_dbs:
        if not Path(p).exists():
            print(f"  ⚠️  {p} не найден")
            continue
        con  = sqlite3.connect(p)
        rows = con.execute(
            "SELECT urls_json,ranking_json,created_at "
            "FROM ts_comparisons ORDER BY created_at"
        ).fetchall()
        all_comps.extend((r[2], json.loads(r[0]), json.loads(r[1])) for r in rows)
        print(f"  📥 {p}: {len(rows)} сравнений")
        con.close()

    # Сортируем хронологически (перемешиваем воркеров)
    all_comps.sort(key=lambda x: x[0])
    print(f"  🗂  Итого сравнений: {len(all_comps)}")

    # Инициализируем из Phase 1 и делаем replay
    env     = make_ts_env()
    ratings = {}
    counts  = {}
    for p in products:
        s = float(p.get("score_p1") or 50)
        ratings[p["url"]] = env.create_rating(mu=s, sigma=TS_SIGMA_INIT)
        counts[p["url"]]  = 0

    for _, _, ranked_urls in all_comps:
        valid = [u for u in ranked_urls if u in ratings]
        if len(valid) < 2:
            continue
        ts_rate(env, ratings, valid)
        for u in valid:
            counts[u] = counts.get(u, 0) + 1

    # Статистика
    sigmas = [r.sigma for r in ratings.values()]
    avg_s  = sum(sigmas) / len(sigmas) if sigmas else 0
    print(f"\n  📊 После мержа: σ avg={avg_s:.2f} max={max(sigmas):.2f}")
    print(f"  📊 Сравнений на продукт: avg={sum(counts.values())/len(counts):.1f}")

    # Строим финальный список
    items = p2_build_output(ratings, counts, url2p)

    # SQLite
    out_db = sqlite3.connect(out_path)
    out_db.executescript("""
        CREATE TABLE IF NOT EXISTS final_products (
            rank        INTEGER,
            url         TEXT PRIMARY KEY,
            name        TEXT,
            score_p2    INTEGER,
            mu          REAL,
            sigma       REAL,
            score_p1    INTEGER,
            comparisons INTEGER,
            pros        TEXT,
            cons        TEXT,
            price       REAL,
            weight      TEXT,
            nutrition_json TEXT,
            composition TEXT
        );
    """)
    for it in items:
        out_db.execute(
            "INSERT OR REPLACE INTO final_products VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                it["rank"], it["url"], it["name"],
                it["score_p2"], it["mu"], it["sigma"],
                it["score_p1"], it["comparisons"],
                it["pros"], it["cons"],
                it["price"], it["weight"],
                json.dumps(it["nutrition"], ensure_ascii=False) if it.get("nutrition") else None,
                it["composition"],
            ))
    out_db.commit()
    out_db.close()

    # JSON
    jp = out_path.replace(".db", ".json")
    Path(jp).write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  💾 {out_path}")
    print(f"  📄 {jp} ({len(items)} товаров)")

    # Топ и дно
    print("\n  🏆 Топ-10 (score_p2 | σ | score_p1):")
    for it in items[:10]:
        d = it["score_p2"] - (it["score_p1"] or it["score_p2"])
        arrow = f"↑{d:+d}" if d != 0 else "  ─"
        print(f"     {it['rank']:>4}. [{it['score_p2']:>3} σ={it['sigma']:.1f} p1={it['score_p1']} {arrow}] "
              f"{it['name']}")
    print("  💀 Последние-10:")
    for it in items[-10:]:
        d = it["score_p2"] - (it["score_p1"] or it["score_p2"])
        arrow = f"↑{d:+d}" if d != 0 else "  ─"
        print(f"     {it['rank']:>4}. [{it['score_p2']:>3} σ={it['sigma']:.1f} p1={it['score_p1']} {arrow}] "
              f"{it['name']}")


# ════════════════════════════════════════════════════════
# SOLO — обе фазы на одной машине последовательно
# ════════════════════════════════════════════════════════

async def run_solo():
    print("\n  🖥  SOLO MODE: Phase 1 → merge → Phase 2 → final merge\n")

    # Phase 1
    await run_p1_worker(0, 1, INPUT_FILE)

    # Merge P1 (один воркер = просто копируем)
    p1w = str(DB_DIR / "products_p1_worker_0.db")
    p1m = str(DB_DIR / "products_phase1.db")
    merge_p1([p1w], p1m)

    # Phase 2
    await run_p2_worker(0, 1, p1m)

    # Merge P2
    p2w = str(DB_DIR / "products_p2_worker_0.db")
    pfinal = str(DB_DIR / "products_final.db")
    merge_p2([p2w], p1m, pfinal)

    print(f"\n  ✅ Всё готово: {pfinal.replace('.db', '.json')}")


# ════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════

def main():
    global CEREBRAS_API_KEY, INPUT_FILE

    ap = argparse.ArgumentParser(
        description="VkusVill Scorer v4 — Phase 1 (scoring) + Phase 2 (TrueSkill)")
    sub = ap.add_subparsers(dest="cmd")

    sub.add_parser("solo", help="Одна машина: Phase 1 + Phase 2")

    p = sub.add_parser("p1score", help="Phase 1 воркер (initial scoring)")
    p.add_argument("--worker", type=int, required=True, help="Номер воркера (0-based)")
    p.add_argument("--total",  type=int, required=True, help="Всего воркеров")
    p.add_argument("--input",  type=str, default=str(INPUT_FILE))
    p.add_argument("--key",    type=str, default="", help="Cerebras API key")

    p = sub.add_parser("p1merge", help="Мерж Phase 1 баз данных")
    p.add_argument("databases", nargs="+", help="Пути к .db файлам воркеров")
    p.add_argument("--output", default=str(DB_DIR / "products_phase1.db"))

    p = sub.add_parser("p2score", help="Phase 2 воркер (TrueSkill refinement)")
    p.add_argument("--worker", type=int, required=True)
    p.add_argument("--total",  type=int, required=True)
    p.add_argument("--p1db",   type=str, default=str(DB_DIR / "products_phase1.db"),
                   help="Путь к merged Phase 1 БД")
    p.add_argument("--key",    type=str, default="")

    p = sub.add_parser("p2merge", help="Мерж Phase 2 баз данных (replay TrueSkill)")
    p.add_argument("databases", nargs="+")
    p.add_argument("--p1db",   type=str, default=str(DB_DIR / "products_phase1.db"))
    p.add_argument("--output", default=str(DB_DIR / "products_final.db"))

    args = ap.parse_args()

    if not args.cmd:
        ap.print_help()
        print("""
═══════════════════════════════════════════════════════════
  ПРИМЕРЫ
═══════════════════════════════════════════════════════════

  # Одна машина (всё сразу):
  python scorer_v4.py solo

  # Две машины:

  # ── Phase 1 ──────────────────────────────────────────
  PC1: python scorer_v4.py p1score --worker 0 --total 2 --key csk-AAA
  PC2: python scorer_v4.py p1score --worker 1 --total 2 --key csk-BBB
  # (ждём завершения, копируем .db с обоих ПК)
       python scorer_v4.py p1merge \\
           vkusvill_data/products_p1_worker_0.db \\
           vkusvill_data/products_p1_worker_1.db

  # ── Phase 2 (читают одну products_phase1.db) ─────────
  PC1: python scorer_v4.py p2score --worker 0 --total 2 --key csk-AAA
  PC2: python scorer_v4.py p2score --worker 1 --total 2 --key csk-BBB
  # (ждём завершения / утро, копируем .db с обоих ПК)
       python scorer_v4.py p2merge \\
           vkusvill_data/products_p2_worker_0.db \\
           vkusvill_data/products_p2_worker_1.db

  # ── Три ключа (3й ключ = ещё одна машина) ────────────
  PC3: python scorer_v4.py p2score --worker 2 --total 3 --key csk-CCC
  # (при мерже добавить products_p2_worker_2.db)

═══════════════════════════════════════════════════════════
  ВЫХОДНЫЕ ФАЙЛЫ
═══════════════════════════════════════════════════════════
  products_final.db    ← SQLite (таблица final_products)
  products_final.json  ← JSON (sorted by score_p2 DESC)

  Поля в JSON:
    rank, url, name
    score_p2  — финальный рейтинг после TrueSkill (0-100)
    mu        — точное значение TrueSkill mu
    sigma     — остаточная неопределённость (ниже = точнее)
    score_p1  — исходный рейтинг после Phase 1
    comparisons — кол-во матчей для этого продукта
    pros, cons, price, weight, nutrition, composition

═══════════════════════════════════════════════════════════
  МАТЕМАТИКА (15k товаров, 2 ключа, 7h)
═══════════════════════════════════════════════════════════
  Бюджет:  2 × 900 req/h × 7h = 12 600 матчей
  Матч:    5 продуктов → C(5,2) = 10 пар данных TrueSkill
  Матчей:  12 600 × 5 = 63 000 появлений / 15 000 прод = 4.2
  Пар:     4.2 × 4 = ~17 сравнений на продукт
  σ:       10.0 → ~3.5 (конвергенция ~60% достигнута overnight)

  С 3 ключами: 18 900 матчей → ~25 сравнений → σ ≈ 2.5
═══════════════════════════════════════════════════════════""")
        return

    if hasattr(args, "key") and args.key:
        CEREBRAS_API_KEY = args.key

    if args.cmd == "solo":
        asyncio.run(run_solo())

    elif args.cmd == "p1score":
        INPUT_FILE = Path(args.input)
        asyncio.run(run_p1_worker(args.worker, args.total, INPUT_FILE))

    elif args.cmd == "p1merge":
        merge_p1(args.databases, args.output)

    elif args.cmd == "p2score":
        asyncio.run(run_p2_worker(args.worker, args.total, args.p1db))

    elif args.cmd == "p2merge":
        merge_p2(args.databases, args.p1db, args.output)


if __name__ == "__main__":
    main()