# VkusVill Quality Ratings

Пример работы и презентация: https://drive.google.com/drive/folders/1z76reV00_pyHas4CT03bQj6c7fOxximH?usp=sharing

Браузерное расширение, которое показывает оценки полезности товаров на [vkusvill.ru](https://vkusvill.ru). Оценки формируются на основе LLM-скоринга и TrueSkill.

## Возможности

- **Каталог** — цветные плашки (0–100) рядом с каждым товаром
- **Tooltip** — при наведении на оценку отображаются плюсы и минусы продукта
- **Страница товара** — полный блок с оценкой, плюсами и минусами
- **Рекомендации** — лучшие альтернативы и топ в категории с фотографиями товаров
- **Корзина** — оценки рядом с товарами в корзине

## Быстрый старт

### 1. Запуск API

```bash
# Docker (рекомендуется)
docker compose up -d

# Или локально
pip install fastapi uvicorn
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

### 2. Установка расширения

1. Откройте Chrome или Edge
2. Перейдите в `chrome://extensions` (или `edge://extensions`)
3. Включите «Режим разработчика»
4. Нажмите «Загрузить распакованное расширение»
5. Выберите папку `extension/`

### 3. Использование

Откройте [vkusvill.ru](https://vkusvill.ru) — оценки появятся автоматически в каталоге, на страницах товаров и в корзине.

## Архитектура

```
vkusvill_data/          FastAPI (localhost:8000)       Расширение
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│ products_p1.db  │────▶│ /get_ratings_batch   │◀────│ content.js      │
│ recommendations │────▶│ /get_product_extended│     │ (плашки, tooltip│
│ clusters.db     │     └──────────────────────┘     │  рекомендации)  │
└─────────────────┘                                  └─────────────────┘
```

## API

| Endpoint | Описание |
|----------|----------|
| `GET /get_rating?name=...` | Оценка по названию |
| `POST /get_ratings_batch` | Batch-запрос оценок |
| `GET /get_recommendations?url=...` | Рекомендации по URL товара |
| `GET /get_product_extended?url=...&name=...` | Рейтинг + кластер + рекомендации |
| `GET /health` | Проверка работы |

## Структура проекта

```
HSE_hack/
├── api_server.py          # FastAPI-сервер
├── extension/
│   ├── manifest.json
│   ├── content.js
│   ├── styles.css
│   └── fonts/
├── vkusvill_data/
│   ├── products_p1.db     # Оценки товаров
│   ├── recommendations.db # Рекомендации
│   └── clusters.db       # Кластеры и топы
├── scorer.py              # LLM-скоринг (Phase 1 + 2)
├── docker-compose.yml
└── Dockerfile
```

## Скоринг (опционально)

Для пересчёта оценок:

```bash
python -m venv venv
.\venv\Scripts\Activate.ps1   # Windows
pip install -r requirements.txt
python scorer.py solo
```

## Требования

- Python 3.12+
- Chrome или Edge
- Cerebras API Key (только для скоринга)
