import os, json, time, sys, math
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

TOPIC_OVERLOAD = os.getenv("TOPIC_OVERLOAD", "errors.overload")
TOPIC_QUOTA    = os.getenv("TOPIC_QUOTA", "errors.quota")
TOPIC_PENDING  = os.getenv("TOPIC_PENDING", "questions.pending")
TOPIC_DLQ      = os.getenv("TOPIC_DLQ", "results.deadletter")

GROUP_ID       = os.getenv("GROUP_ID", "retry_manager_group")
POLL_MS        = int(os.getenv("POLL_MS", "500"))

# límites y backoff
MAX_RETRIES          = int(os.getenv("MAX_RETRIES", "3"))
OVERLOAD_BASE_SEC    = float(os.getenv("OVERLOAD_BASE_SEC", "1"))   # 1,2,4,...
QUOTA_STEP_SEC       = float(os.getenv("QUOTA_STEP_SEC", "60"))     # 60,120,...

def get_consumer():
    return Consumer({
        "bootstrap.servers": BOOT,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

def get_producer():
    return Producer({"bootstrap.servers": BOOT})

def produce_json(p, topic, obj):
    p.produce(topic, json.dumps(obj).encode("utf-8"))
    p.flush()

def handle(msg, p, is_quota):
    raw = msg.value()
    if not raw:
        return
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception as e:
        print(f"[retry_manager] mensaje inválido: {e}", file=sys.stderr)
        return

    retry_count = int(data.get("retry_count", 0))
    if retry_count >= MAX_RETRIES:
        data["reason"] = "max_retries_reached"
        produce_json(p, TOPIC_DLQ, data)
        return

    # backoff
    if is_quota:
        delay = QUOTA_STEP_SEC * (retry_count + 1)   # 60s, 120s, ...
    else:
        delay = OVERLOAD_BASE_SEC * (2 ** retry_count)  # 1s, 2s, 4s, ...

    print(f"[retry_manager] reintentando ({'quota' if is_quota else 'overload'}) en {delay:.1f}s; retry_count={retry_count+1}")
    time.sleep(delay)

    data["retry_count"] = retry_count + 1
    # republicar a pendientes
    produce_json(p, TOPIC_PENDING, data)

def main():
    cons = get_consumer()
    prod = get_producer()
    cons.subscribe([TOPIC_OVERLOAD, TOPIC_QUOTA])
    print(f"[retry_manager] escuchando {TOPIC_OVERLOAD}, {TOPIC_QUOTA} -> {TOPIC_PENDING}")

    try:
        while True:
            msg = cons.poll(timeout=POLL_MS/1000.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            topic = msg.topic()
            is_quota = (topic == TOPIC_QUOTA)
            handle(msg, prod, is_quota)
    except KeyboardInterrupt:
        pass
    finally:
        cons.close()

if __name__ == "__main__":
    main()