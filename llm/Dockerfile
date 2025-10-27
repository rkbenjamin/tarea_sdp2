FROM python:3.11-slim

WORKDIR /app
COPY app.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

ENV OLLAMA_HOST=http://ollama:11434 \
    OLLAMA_MODEL=tinyllama \
    LLM_DELAY_MS=0

EXPOSE 5002
CMD ["python", "app.py"]