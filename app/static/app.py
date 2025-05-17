# app.py
from flask import Flask, render_template, request
from query_module import search_players, generate_explanation, generate_comment

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    prompt      = ""
    results     = []
    explanation = ""
    comment     = ""

    if request.method == "POST":
        prompt = request.form.get("prompt", "").strip()
        if prompt:
            # 1) Oyuncuları ara
            results = search_players(prompt, top_k=5)

            # 2) Kendi açıklama fonksiyonunu çağır
            explanation = generate_explanation(results)
            comment = generate_comment(results)

    return render_template(
        "index.html",
        prompt=prompt,
        results=results,
        explanation=explanation,
        comment=comment 
        
    )
    print("COMMENT:", comment)


if __name__ == "__main__":
    app.run(debug=True)
