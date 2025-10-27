from flask import Flask, request, jsonify
import sqlite3, os, math, random
import pandas as pd

app = Flask(__name__)
DB_PATH = "/data/results.db"
CSV_PATH = "/data/test.csv"

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category INTEGER,
        title TEXT,
        question TEXT,
        best_answer TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question TEXT,
        human_answer TEXT,
        llm_answer TEXT,
        score REAL,
        count INTEGER
    )""")
    conn.commit()
    conn.close()

@app.route("/init", methods=["POST"])
def init():
    init_db()
    if not os.path.exists(CSV_PATH):
        return jsonify({"ok": False, "error": f"CSV not found at {CSV_PATH}"}), 400

    try:
        df = pd.read_csv(CSV_PATH)
        lower_map = {c.lower(): c for c in df.columns}
        # alias flexibles
        title_aliases = ["title", "titulo", "question_title"]
        question_aliases = ["question", "content", "pregunta", "question_content", "body", "texto"]
        best_answer_aliases = ["best_answer", "bestanswer", "answer", "mejor_respuesta"]

        def pick(cols):
            for name in cols:
                if name in lower_map:
                    return lower_map[name]
            return None

        col_title = pick(title_aliases)
        col_question = pick(question_aliases)
        col_best = pick(best_answer_aliases)
        col_class = lower_map.get("class", None)

        need_headerless = not (col_title and col_question and col_best)
    except Exception:
        need_headerless = True

    if need_headerless:
        df = pd.read_csv(CSV_PATH, header=None, names=["class","title","question","best_answer"])
        col_title = "title"
        col_question = "question"
        col_best = "best_answer"
        col_class = "class"

    missing = []
    for req, col in [("title", col_title), ("question", col_question), ("best_answer", col_best)]:
        if not col:
            missing.append(req)
    if missing:
        return jsonify({
            "ok": False,
            "error": "CSV must contain (or alias) columns",
            "missing": missing,
            "detected_columns": list(df.columns)
        }), 400

    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM questions")
    rows = []
    for _, row in df.iterrows():
        try:
            category = int(row[col_class]) if col_class in df.columns and not pd.isna(row[col_class]) else 0
        except Exception:
            category = 0
        title = "" if pd.isna(row[col_title]) else str(row[col_title])
        question = "" if pd.isna(row[col_question]) else str(row[col_question])
        best = "" if pd.isna(row[col_best]) else str(row[col_best])
        rows.append((category, title, question, best))

    c.executemany("INSERT INTO questions (category,title,question,best_answer) VALUES (?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "se insertaron": len(rows), "usadas": bool(need_headerless)})

def get_count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM questions")
    return cur.fetchone()[0]

def get_by_offset(conn, offset):
    cur = conn.cursor()
    cur.execute("SELECT id, category, title, question, best_answer FROM questions LIMIT 1 OFFSET ?", (offset,))
    r = cur.fetchone()
    if not r: return None
    return {"id": r[0], "category": r[1], "title": r[2], "question": r[3], "best_answer": r[4]}

def sample_poisson(lam: float) -> int:
    if lam <= 0:
        return 0
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1

@app.route("/random", methods=["GET"])
def random_question():
    dist = request.args.get("dist", "uniform").lower()
    alpha = float(request.args.get("alpha", "1.2"))
    lam   = float(request.args.get("lambda", "10"))

    conn = get_conn()
    n = get_count(conn)
    if n == 0:
        conn.close()
        return jsonify({"ok": False, "error": "no questions loaded"}), 400

    if dist == "zipf":
        k = 1
        limit = 10 * n
        while True:
            k_candidate = random.randint(1, max(1, min(limit, 10*n)))
            p = 1.0 / (k_candidate ** alpha)
            if random.random() < p:
                k = k_candidate
                break
        offset = (k - 1) % n

    elif dist == "poisson":
        k = sample_poisson(lam)
        offset = k % n

    else:
        offset = random.randint(0, n - 1)

    row = get_by_offset(conn, offset)
    conn.close()
    return jsonify({"ok": True, "data": row})

@app.route("/save", methods=["POST"])
def save():
    data = request.json
    q, ha, la, s = data["question"], data["human_answer"], data["llm_answer"], float(data["score"])
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id, count FROM results WHERE question=?", (q,))
    row = c.fetchone()
    if row:
        c.execute("UPDATE results SET count=? WHERE id=?", (row[1] + 1, row[0]))
    else:
        c.execute("INSERT INTO results (question,human_answer,llm_answer,score,count) VALUES (?,?,?,?,?)",
                  (q, ha, la, s, 1))
    conn.commit()
    conn.close()
    return jsonify({"status": "saved"})

@app.route("/stats", methods=["GET"])
def stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM questions")
    qn = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM results")
    rn = cur.fetchone()[0]
    conn.close()
    return jsonify({"questions": qn, "results": rn})

@app.route("/result", methods=["GET"])
def result_by_question():
    q = (request.args.get("question") or "").strip()
    if not q:
        return jsonify({"ok": False, "error": "missing question"}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT human_answer, llm_answer, score, count FROM results WHERE question=?", (q,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({"ok": True, "found": False})

    return jsonify({"ok": True, "found": True, "data": {
        "human_answer": row[0],
        "llm_answer": row[1],
        "score": float(row[2]),
        "count": row[3]
    }})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5004)