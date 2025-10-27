FROM python:3.11-slim

# Dependencias nativas para confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]