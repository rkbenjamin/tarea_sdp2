from flask import Flask, request, jsonify
from collections import OrderedDict
import os, time

app = Flask(__name__)

CAPACITY = int(os.getenv("CACHE_CAPACITY", "500"))
TTL_SEC  = int(os.getenv("CACHE_TTL", "3600"))

store = OrderedDict()  # key -> (value, expires_at)

def now(): return int(time.time())

def evict_expired():
    t = now()
    to_del = [k for k,(v,exp) in store.items() if exp < t]
    for k in to_del:
        store.pop(k, None)

def set_item(k, v):
    evict_expired()
    if k in store:
        store.pop(k)
    elif len(store) >= CAPACITY:
        store.popitem(last=False)  # LRU: saca el más antiguo
    store[k] = (v, now() + TTL_SEC)

def get_item(k):
    evict_expired()
    if k not in store:
        return None
    v, exp = store.pop(k)
    if exp < now():
        return None
    # mover a “más reciente”
    store[k] = (v, exp)
    return v

@app.post("/check")
def check():
    q = (request.json or {}).get("question","").strip()
    v = get_item(q)
    if v is None:
        return jsonify({"hit": False})
    return jsonify({"hit": True, "answer": v})

@app.post("/insert")
def insert():
    data = request.json or {}
    set_item((data.get("question") or "").strip(), data.get("answer",""))
    return jsonify({"ok": True})

@app.get("/health")
def health():
    return jsonify({"ok": True, "items": len(store)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)