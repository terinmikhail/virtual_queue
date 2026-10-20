FROM python:3.11-slim

WORKDIR /app

# системные пакеты (gcc + librdkafka) для confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# python-зависимости
COPY pyproject.toml ./
RUN pip install --no-cache-dir \
    "confluent-kafka>=2.4.0" \
    "pydantic>=2.7"

# код
COPY core/ ./core/
COPY processors/ ./processors/
COPY generator/ ./generator/
COPY scripts/ ./scripts/

CMD ["python", "-c", "print('use specific command')"]