import os
import time
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://ollama:11434")
MODEL_NAME   = os.getenv("OLLAMA_MODEL", "tinyllama")
ART_DELAY_MS = int(os.getenv("LLM_DELAY_MS", "0"))

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "model": MODEL_NAME})

@app.route("/answer", methods=["POST"])
def answer():
    payload = request.get_json(force=True) or {}
    q = (payload.get("question") or "").strip()

    if ART_DELAY_MS > 0:
        time.sleep(ART_DELAY_MS / 1000.0)

    body = {
        "model": MODEL_NAME,
        "prompt": q,
        "stream": False,
        "options": {
            "num_predict": 60,
            "temperature": 0.4,
            "top_k": 40
        }
    }

    t0 = time.time()
    try:
        resp = requests.post(f"{OLLAMA_HOST}/api/generate", json=body, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        print(data)

        text = data.get("response", "").strip()

        return jsonify({
            "answer": text or "No puedo responder ahora.",
            "latency_ms": int((time.time() - t0) * 1000)
        })
    except Exception as e:
        return jsonify({
            "answer": f"Error: {str(e)[:100]}",
            "latency_ms": int((time.time() - t0) * 1000)
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)