FROM python:3.12-slim

WORKDIR /app

# Минимальные зависимости только для API
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY api_server.py .
# Данные монтируются через volume в docker-compose
RUN mkdir -p vkusvill_data

EXPOSE 8000

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
