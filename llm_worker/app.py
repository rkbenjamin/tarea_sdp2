import os, json, time, sys
import requests
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_PENDING   = os.getenv("TOPIC_PENDING", "questions.pending")
TOPIC_SUCCESS   = os.getenv("TOPIC_SUCCESS", "answers.success")
TOPIC_OVERLOAD  = os.getenv("TOPIC_OVERLOAD", "errors.overload")
TOPIC_QUOTA     = os.getenv("TOPIC_QUOTA", "errors.quota")

LLM_URL         = os.getenv("LLM_URL", "http://llm:5002/answer")
GROUP_ID        = os.getenv("GROUP_ID", "llm_worker_group")
POLL_MS         = int(os.getenv("POLL_MS", "500"))

def get_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

def get_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def produce_json(producer, topic, obj):
    # envío síncrono y simple; si prefieres no bloquear, cambia a producer.poll(0)
    producer.produce(topic, json.dumps(obj).encode("utf-8"))
    producer.flush()

def is_quota(resp, err):
    if isinstance(resp, requests.Response):
        if resp.status_code in (402, 403, 429):
            return True
        try:
            txt = (resp.text or "").lower()
            for k in ("quota", "rate limit", "too many requests"):
                if k in txt:
                    return True
        except Exception:
            pass
    if err:
        s = str(err).lower()
        return any(k in s for k in ("quota", "rate limit", "too many requests"))
    return False

def is_overload(resp, err):
    if err:
        return True
    if isinstance(resp, requests.Response) and 500 <= resp.status_code < 600:
        return True
    return False

def main():
    print(f"[llm_worker] usando {KAFKA_BOOTSTRAP}, topic={TOPIC_PENDING}", flush=True)
    cons = get_consumer()
    prod = get_producer()
    cons.subscribe([TOPIC_PENDING])

    # Espera activa a que el broker esté listo y el tópico sea visible
    for _ in range(30):  # ~30s
        try:
            md = cons.list_topics(timeout=1.0)
            if TOPIC_PENDING in md.topics:
                break
            print("[llm_worker] esperando a que el tópico exista...", flush=True)
        except Exception as e:
            print(f"[llm_worker] esperando broker... {e}", flush=True)
        time.sleep(1)

    try:
        while True:
            msg = cons.poll(timeout=POLL_MS / 1000.0)
            if msg is None:
                continue

            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    # fin de partición, continuar
                    continue
                if code in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.LEADER_NOT_AVAILABLE):
                    print("[llm_worker] tópico aún no disponible, reintentando...", flush=True)
                    time.sleep(2)
                    continue
                print(f"[llm_worker] error de poll: {msg.error()}", flush=True)
                time.sleep(2)
                continue

            # Mensaje válido
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[llm_worker] mensaje inválido: {e}", file=sys.stderr, flush=True)
                continue

            qid          = payload.get("qid")
            question     = payload.get("question", "")
            human_answer = payload.get("human_answer", "")
            attempts     = int(payload.get("attempts", 0))
            retry_count  = int(payload.get("retry_count", 0))

            resp, err = None, None
            try:
                resp = requests.post(LLM_URL, json={"question": question}, timeout=60)
                ok = resp.ok
            except Exception as ex:
                ok, err = False, ex

            if ok:
                try:
                    data = resp.json()
                    llm_answer = (data.get("answer") or "").strip()
                except Exception as ex:
                    # Problemas parseando la respuesta del LLM -> tratar como overload
                    produce_json(prod, TOPIC_OVERLOAD, {
                        "qid": qid,
                        "question": question,
                        "human_answer": human_answer,
                        "attempts": attempts,
                        "retry_count": retry_count + 1,
                        "error": f"parse_error: {str(ex)[:120]}"
                    })
                    continue

                produce_json(prod, TOPIC_SUCCESS, {
                    "qid": qid,
                    "question": question,
                    "human_answer": human_answer,
                    "llm_answer": llm_answer,
                    "attempts": attempts
                })
            else:
                topic = TOPIC_OVERLOAD
                if is_quota(resp, err):
                    topic = TOPIC_QUOTA
                elif is_overload(resp, err):
                    topic = TOPIC_OVERLOAD
                produce_json(prod, topic, {
                    "qid": qid,
                    "question": question,
                    "human_answer": human_answer,
                    "attempts": attempts,
                    "retry_count": retry_count + 1,
                    "error": f"http_error: status={getattr(resp, 'status_code', None)} detail={str(err)[:120]}"
                })
    except KeyboardInterrupt:
        pass
    finally:
        cons.close()

if __name__ == "__main__":
    main()