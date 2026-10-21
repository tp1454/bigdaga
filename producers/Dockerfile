FROM python:3.12-slim-bookworm

RUN for i in 1 2 3 4 5; do \
      apt-get update && break || { \
        echo "apt-get update failed (attempt $i), retrying in 5s..."; \
        sleep 5; \
      }; \
    done && \
    apt-get install -y --no-install-recommends \
        gcc librdkafka-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*
    
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

# Default — overridden per service in compose
CMD ["python", "-m", "src.equities.yahoo_producer"]