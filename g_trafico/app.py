from flask import Flask, jsonify, request
import requests, os, uuid, json
from confluent_kafka import Producer

app = Flask(__name__)

ALMACENAMIENTO_URL = os.getenv("ALMACENAMIENTO_URL", "http://almacenamiento:5004")
CACHE_URL   = os.getenv("CACHE_URL", "http://cache:5001")

DEFAULT_DIST   = os.getenv("TRAFFIC_DIST", "uniform")
ZIPF_ALPHA     = os.getenv("ZIPF_ALPHA", "1.2")
POISSON_LAMBDA = os.getenv("POISSON_LAMBDA", "10")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_PENDING   = os.getenv("TOPIC_PENDING", "questions.pending")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def send_to_kafka(topic, data):
    try:
        producer.produce(topic, json.dumps(data).encode("utf-8"))
        producer.poll(0)
        return True, None
    except Exception as e:
        print(f"[g_trafico] ERROR produce Kafka: {e}", flush=True)
        return False, str(e)

@app.get("/consulta")
def consulta():
    dist  = request.args.get("dist", DEFAULT_DIST).lower()
    alpha = request.args.get("alpha", ZIPF_ALPHA)
    lam   = request.args.get("lambda", POISSON_LAMBDA)
    params = {"dist": dist}
    if dist == "zipf":
        params["alpha"] = alpha
    elif dist == "poisson":
        params["lambda"] = lam

    r = requests.get(f"{ALMACENAMIENTO_URL}/random", params=params, timeout=10)
    r.raise_for_status()
    rj = r.json()
    if not rj.get("ok"):
        return jsonify({"error": "sin datos en almacenamiento"}), 500

    qrow = rj["data"]
    q = qrow["title"]
    human_a = qrow["best_answer"]

    try:
        existing = requests.get(f"{ALMACENAMIENTO_URL}/result", params={"question": q}, timeout=5).json()
    except Exception as e:
        existing = {"ok": False, "err": str(e)}

    if existing.get("ok") and existing.get("found"):
        data = existing["data"]
        try:
            requests.post(f"{CACHE_URL}/insert", json={"question": q, "answer": data["llm_answer"]}, timeout=2)
        except Exception:
            pass
        return jsonify({"from": "almacenamiento", "question": q, **data})

    qid = str(uuid.uuid4())
    payload = {
        "qid": qid,
        "question": q,
        "human_answer": human_a,
        "attempts": 0,
        "retry_count": 0
    }
    ok, err = send_to_kafka(TOPIC_PENDING, payload)

    return jsonify({
        "accepted": True,
        "qid": qid,
        "kafka_ok": ok,
        "kafka_err": err,
        "message": "procesando asíncronamente" if ok else "en cola local; reintentar"
    }), 202

@app.get("/health")
def health():
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)