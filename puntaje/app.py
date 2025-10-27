from flask import Flask, request, jsonify
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

app = Flask(__name__)

@app.route("/compute", methods=["POST"])
def compute():
    human_a = request.json["human_answer"]
    llm_a = request.json["llm_answer"]

    vectorizer = TfidfVectorizer()
    vectors = vectorizer.fit_transform([human_a, llm_a])

    score = cosine_similarity(vectors[0], vectors[1])[0][0]

    return jsonify({"puntaje": float(score)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003)