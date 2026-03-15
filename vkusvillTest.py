#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import random
import re
import sys
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import Optional

import nodriver as uc

BASE_URL = "https://vkusvill.ru"
CATALOG_START_URL = "https://vkusvill.ru/goods/"

OUTPUT_DIR = Path("vkusvill_data")
OUTPUT_DIR.mkdir(exist_ok=True)
DEBUG_DIR = OUTPUT_DIR / "debug"
DEBUG_DIR.mkdir(exist_ok=True)

PRODUCTS_FILE = OUTPUT_DIR / "products.json"
PROGRESS_FILE = OUTPUT_DIR / "progress.json"
ERRORS_FILE = OUTPUT_DIR / "errors.json"
LOG_FILE = OUTPUT_DIR / "scraper.log"

HEADLESS = False
PAGE_WAIT_SEC = 2
CATEGORY_DELAY = (0.5, 1.5)  # Пауза между категориями
PRODUCT_DELAY = (0.3, 0.7)   # Пауза между товарами
MAX_PAGES_PER_CATEGORY = 100
SAVE_EVERY = 1
FIX_MISSING_WEIGHTS = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("vkusvill")


def normalize_url(url: str) -> str:
    if url.startswith("/"):
        return BASE_URL + url
    return url

def safe_name(text: str) -> str:
    return re.sub(r"[^\w\-]+", "_", text)[:80]

def set_page_param(url: str, page_num: int) -> str:
    # Во ВкусВилл пагинация идет через параметр PAGEN_1
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs["PAGEN_1"] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment,
    ))


class VkusVillScraper:
    def __init__(self):
        self.browser = None
        self.products = {}
        self.seen_urls = set()
        self.errors = []
        self.save_counter = 0
        self._load_progress()

    def _load_progress(self):
        if PRODUCTS_FILE.exists():
            try:
                data = json.loads(PRODUCTS_FILE.read_text(encoding="utf-8"))
                for item in data:
                    url = item.get("url")
                    if url:
                        self.products[url] = item
                        self.seen_urls.add(url)
                log.info(f"Загружен прогресс: {len(self.products)} товаров")
            except Exception as e:
                log.warning(f"Не удалось загрузить products.json: {e}")

        if ERRORS_FILE.exists():
            try:
                self.errors = json.loads(ERRORS_FILE.read_text(encoding="utf-8"))
            except Exception:
                self.errors = []

    def _save(self, force=False):
        self.save_counter += 1
        if not force and self.save_counter % SAVE_EVERY != 0:
            return

        PRODUCTS_FILE.write_text(
            json.dumps(list(self.products.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        PROGRESS_FILE.write_text(
            json.dumps(
                {
                    "total_products": len(self.products),
                    "total_errors": len(self.errors),
                    "updated_at": datetime.now().isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        ERRORS_FILE.write_text(
            json.dumps(self.errors, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    async def start_browser(self):
        self.browser = await uc.start(
            headless=HEADLESS,
            browser_args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        log.info("Браузер запущен")

    async def stop_browser(self):
        if self.browser:
            try:
                await self.browser.stop()
            except Exception:
                pass

    async def ensure_site_access(self):
        log.info("Открываю главную, чтобы пройти проверки...")
        page = await self.browser.get(BASE_URL)
        await asyncio.sleep(5)
        
        html = await page.get_content()
        if "cloudflare" in html.lower() or "qrator" in html.lower():
            log.warning("Антибот активен. Дождитесь нормальной загрузки в браузере.")
            input("Нажмите Enter, когда сайт загрузится...")
        
        log.info("Доступ к сайту подтвержден")

    async def get_category_links(self) -> list[str]:
        log.info("Собираю ссылки на категории...")
        page = await self.browser.get(CATALOG_START_URL)
        await asyncio.sleep(PAGE_WAIT_SEC)

        categories_json = await page.evaluate("""
            (() => {
                let result = [];
                // Берем ссылки из бокового меню каталога
                let elements = document.querySelectorAll('a.VVCatalog2020Menu__Link');
                for (let el of elements) {
                    let href = el.getAttribute('href');
                    // Оставляем только те, что ведут в каталог /goods/
                    if (href && href.includes('/goods/')) {
                        result.push(href);
                    }
                }
                return JSON.stringify(result);
            })()
        """)

        categories = set()
        if categories_json:
            for href in json.loads(categories_json):
                categories.add(normalize_url(href))

        category_list = sorted(list(categories))
        log.info(f"Найдено {len(category_list)} категорий.")
        return category_list

    async def smooth_scroll(self, page, scrolls=4, amount=800, sleep_time=0.3):
            """Быстрый скролл для активации ленивой загрузки (Lazy Load)"""
            for _ in range(scrolls):
                try:
                    await page.scroll_down(amount)
                    await asyncio.sleep(sleep_time)
                except Exception:
                    pass

    async def collect_links_from_current_page(self, page) -> list[str]:
        # Скроллим быстро (5 раз по 0.3 сек = всего 1.5 сек на страницу)
        await self.smooth_scroll(page, scrolls=5, amount=800, sleep_time=0.3)
        
        links_json = await page.evaluate("""
            (() => {
                let result = [];
                let elements = document.querySelectorAll('a.ProductCard__link, a.js-product-detail-link');
                for (let el of elements) {
                    let href = el.getAttribute('href');
                    if (href && href.includes('/goods/') && href.includes('.html')) {
                        result.push(href);
                    }
                }
                return JSON.stringify(result);
            })()
        """)

        out = set()
        if links_json:
            for href in json.loads(links_json):
                out.add(normalize_url(href))

        return sorted(list(out)) 

    async def collect_category_product_links(self, category_url: str) -> list[str]:
        all_links = set()

        first_page = await self.browser.get(category_url)
        
        try:
            # Ждем появления карточек товаров
            await first_page.select('.ProductCard', timeout=10)
        except Exception:
            await asyncio.sleep(PAGE_WAIT_SEC)

        # Парсим первую страницу
        page1_links = await self.collect_links_from_current_page(first_page)
        if page1_links:
            all_links.update(page1_links)
            log.info(f"Страница 1: найдено {len(page1_links)} ссылок")
        else:
            log.warning("На первой странице товары не найдены. Возможно, пустая категория.")
            return []

        base_url = first_page.url

        # Идем по пагинации
        for page_num in range(2, MAX_PAGES_PER_CATEGORY + 1):
            page_url = set_page_param(base_url, page_num)
            log.info(f"Проверяю страницу {page_num}: {page_url}")

            page = await self.browser.get(page_url)
            
            try:
                await page.select('.ProductCard', timeout=10)
            except Exception:
                await asyncio.sleep(PAGE_WAIT_SEC)

            # Проверяем, не перекинул ли нас сайт обратно на первую страницу (частое поведение при выходе за пределы пагинации)
            current_url = page.url
            if "PAGEN_1" not in current_url and page_num > 1:
                log.info("Достигнут конец категории (редирект на первую страницу).")
                break

            links = await self.collect_links_from_current_page(page)
            new_links = set(links) - all_links

            log.info(f"Страница {page_num}: найдено {len(links)} ссылок, новых {len(new_links)}")

            if not links or not new_links:
                log.info("Новых товаров нет, перехожу к следующей категории.")
                break

            all_links.update(new_links)

        return sorted(list(all_links))

    async def scrape_product(self, product_url: str) -> Optional[dict]:
            try:
                page = await self.browser.get(product_url)
            except Exception as e:
                log.error(f"Ошибка загрузки URL {product_url}: {e}")
                return None

            # МГНОВЕННОЕ ОЖИДАНИЕ: проверяем наличие h1 каждые 0.2 секунды (до 3 сек максимум)
            h1_found = False
            for _ in range(15):
                try:
                    has_h1 = await page.evaluate("!!document.querySelector('.Product__title')")
                    if has_h1:
                        h1_found = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.2) # Очень короткая пауза

            if not h1_found:
                log.warning(f"Пропуск: Товар не найден или не прогрузился ({product_url})")
                return None

            # Очень быстрый скролл, чтобы прогрузились табы с составом
            await self.smooth_scroll(page, scrolls=2, amount=800, sleep_time=0.2)

            # Выполняем JS
            product_json = await page.evaluate("""
                (() => {
                    const result = {
                        name: null,
                        weight: null,
                        price: null,
                        nutrition: null,
                        composition: null
                    };

                    // 1. Название (сразу меняем неразрывные пробелы на обычные)
                    const h1 = document.querySelector('.Product__title');
                    if (h1) {
                        result.name = h1.textContent.replace(/&nbsp;|\\u00A0/g, ' ').trim();
                    }

                    // 2. Вес/Объем
                    const weightEl = document.querySelector('.ProductCard__weight, .Product__headWeightQ');
                    if (weightEl && weightEl.textContent.trim().length > 0) {
                        // Берем из отдельного HTML элемента
                        result.weight = weightEl.textContent.replace(/&nbsp;|\\u00A0/g, ' ').trim();
                    } else if (result.name) {
                        // Если элемента нет, ищем в конце названия (учитывая варианты с запятой и без)
                        const weightMatch = result.name.match(/(?:,)?\\s*([\\d\\.,]+\\s*(?:г|кг|мл|л|шт\\.?))$/i);
                        if (weightMatch) {
                            result.weight = weightMatch[1].trim();
                        }
                    }

                    // 3. Цена
                    const priceMeta = document.querySelector('meta[itemprop="price"]');
                    if (priceMeta) {
                        result.price = parseFloat(priceMeta.getAttribute('content'));
                    } else {
                        const priceEl = document.querySelector('.Price');
                        if (priceEl) {
                            let text = priceEl.textContent.replace('руб', '').replace(/\\s/g, '').trim();
                            result.price = parseFloat(text);
                        }
                    }

                    // 4. КБЖУ (У некоторых товаров, вроде черного кофе, этого блока нет - это нормально)
                    const nutrition = {};
                    document.querySelectorAll('.VV23_DetailProdPageAccordion__EnergyItem').forEach(item => {
                        const val = item.querySelector('.VV23_DetailProdPageAccordion__EnergyValue');
                        const desc = item.querySelector('.VV23_DetailProdPageAccordion__EnergyDesc');
                        if (val && desc) {
                            nutrition[desc.textContent.trim()] = val.textContent.trim();
                        }
                    });
                    if (Object.keys(nutrition).length > 0) result.nutrition = nutrition;

                    // 5. Состав
                    const compEl = document.querySelector('.Product__composition ._sostav');
                    if (compEl) {
                        result.composition = compEl.textContent.replace(/&nbsp;|\\u00A0/g, ' ').trim();
                    }

                    return JSON.stringify(result);
                })()
            """)

            if not product_json:
                return None

            try:
                data = json.loads(product_json)
            except Exception as e:
                log.error(f"Ошибка декодирования JSON товара: {e}")
                return None

            return {
                "url": product_url,
                "name": data.get("name"),
                "weight": data.get("weight"),
                "price": data.get("price"),
                "nutrition": data.get("nutrition"),
                "composition": data.get("composition"),
                "scraped_at": datetime.now().isoformat()
            }

    async def run(self):
            await self.start_browser()
            try:
                await self.ensure_site_access()

                if FIX_MISSING_WEIGHTS:
                    # ==============================================================
                    # РЕЖИМ ИСПРАВЛЕНИЯ ПУСТОГО ВЕСА
                    # ==============================================================
                    log.info("=== Включен режим исправления пустого веса (FIX_MISSING_WEIGHTS = True) ===")
                    urls_to_fix = []
                    
                    for url, data in self.products.items():
                        weight = data.get("weight")
                        # Проверяем, если вес None, пустая строка, "0" или "0 г"
                        if not weight or str(weight).strip() in ["", "None", "0", "0 г", "0 кг"]:
                            urls_to_fix.append(url)
                    
                    log.info(f"В базе найдено товаров для перепроверки: {len(urls_to_fix)}")
                    
                    for idx, product_url in enumerate(urls_to_fix, start=1):
                        log.info(f"[{idx}/{len(urls_to_fix)}] Обновляем: {product_url}")
                        try:
                            new_data = await self.scrape_product(product_url)
                            if new_data and new_data.get("name"):
                                # Обновляем данные товара в нашем словаре
                                self.products[product_url] = new_data
                                log.info(f"✓ Новый вес: {new_data.get('weight')} | {new_data['name'][:60]}")
                            else:
                                log.warning("Товар не загрузился (возможно, снят с продажи). Оставляем как есть.")
                        except Exception as e:
                            log.error(f"Ошибка при обновлении товара: {e}")

                        self._save()
                        await asyncio.sleep(random.uniform(*PRODUCT_DELAY))
                        
                    log.info("=== Исправление веса успешно завершено! ===")
                    log.info("Поменяйте FIX_MISSING_WEIGHTS = False в коде, чтобы продолжить обычный парсинг.")

                else:
                    # ==============================================================
                    # ОБЫЧНЫЙ РЕЖИМ СБОРА КАТЕГОРИЙ И НОВЫХ ТОВАРОВ
                    # ==============================================================
                    log.info("=== Включен обычный режим парсинга ===")
                    # Получаем все категории с сайта динамически
                    categories = await self.get_category_links()
                    if not categories:
                        log.error("Не удалось найти категории. Проверьте селекторы.")
                        return

                    log.info(f"Уже собрано: {len(self.products)} товаров")

                    for idx, category_url in enumerate(categories, start=1):
                        log.info("=" * 80)
                        log.info(f"Категория {idx}/{len(categories)}: {category_url}")

                        try:
                            links = await self.collect_category_product_links(category_url)
                            log.info(f"Всего ссылок в категории: {len(links)}")

                            new_links = [x for x in links if x not in self.seen_urls]
                            log.info(f"Новых товаров: {len(new_links)}")

                            for p_idx, product_url in enumerate(new_links, start=1):
                                log.info(f"[{p_idx}/{len(new_links)}] {product_url}")
                                try:
                                    product = await self.scrape_product(product_url)
                                    if product and product.get("name"):
                                        self.products[product_url] = product
                                        self.seen_urls.add(product_url)
                                        log.info(
                                            f"✓ {product['name'][:70]} | "
                                            f"Цена: {product.get('price')} | "
                                            f"Вес: {product.get('weight')} | "
                                            f"КБЖУ={'да' if product.get('nutrition') else 'нет'} | "
                                            f"Состав={'да' if product.get('composition') else 'нет'}"
                                        )
                                    else:
                                        self.errors.append({
                                            "url": product_url,
                                            "error": "empty product or blocked",
                                            "time": datetime.now().isoformat(),
                                        })
                                except Exception as e:
                                    self.errors.append({
                                        "url": product_url,
                                        "error": str(e),
                                        "time": datetime.now().isoformat(),
                                    })
                                    log.error(f"Ошибка товара: {e}")

                                self._save()
                                await asyncio.sleep(random.uniform(*PRODUCT_DELAY))

                        except Exception as e:
                            self.errors.append({
                                "url": category_url,
                                "error": str(e),
                                "time": datetime.now().isoformat(),
                            })
                            log.error(f"Ошибка категории: {e}")

                        self._save(force=True)
                        await asyncio.sleep(random.uniform(*CATEGORY_DELAY))

            except KeyboardInterrupt:
                log.info("Остановлено пользователем")
            finally:
                self._save(force=True)
                await self.stop_browser()
                log.info(f"Готово. Всего товаров в базе: {len(self.products)}")


async def main():
    scraper = VkusVillScraper()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())