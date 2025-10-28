import os, json, sys
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_VALIDATED", "answers.success")
ALMACENAMIENTO_URL = os.getenv("ALMACENAMIENTO_URL", "http://almacenamiento:5004/save")
GROUP_ID = os.getenv("GROUP_ID", "persistidor_group")

def get_consumer():
    return Consumer({
        "bootstrap.servers": BOOT,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

def main():
    cons = get_consumer()
    cons.subscribe([TOPIC])
    print(f"[persistidor] escuchando {TOPIC} -> POST {ALMACENAMIENTO_URL}", flush=True)
    try:
        while True:
            msg = cons.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[persistidor] error de Kafka: {msg.error()}", file=sys.stderr, flush=True)
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[persistidor] json invÃ¡lido: {e}", file=sys.stderr, flush=True)
                continue

            payload = {
                "question": data.get("question",""),
                "human_answer": data.get("human_answer",""),
                "llm_answer": data.get("llm_answer",""),
                "score": float(data.get("score", 0.0))
            }
            try:
                r = requests.post(ALMACENAMIENTO_URL, json=payload, timeout=10)
                r.raise_for_status()
                print(f"[persistidor] guardado OK: {payload['question'][:60]}...", flush=True)
            except Exception as e:
                print(f"[persistidor] error POST almacenamiento: {e}", file=sys.stderr, flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        cons.close()

if __name__ == "__main__":
    main()