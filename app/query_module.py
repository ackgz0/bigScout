# query_module.py
import json
import numpy as np
from openai import OpenAI
from sklearn.metrics.pairwise import cosine_similarity
from pymongo import MongoClient

client = OpenAI(api_key="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")

# Veriyi bir kez yükleyip bellekten tekrar kullanıyoruz
mongo_client = MongoClient("mongodb://127.0.0.1:27017")
_data = list(mongo_client["bigscout"]["embeddings"].find({}, {"_id": 0}))

_embedding_matrix = np.array([p["embedding"] for p in _data])
_players        = [p["Player"] for p in _data]
_profiles       = [p["scouting_profile"] for p in _data]
_latest_infos   = [p["latest"] for p in _data]

def _embed_text(text: str):
    resp = client.embeddings.create(
        input=[text],
        model="text-embedding-3-small"
    )
    return np.array(resp.data[0].embedding).reshape(1, -1)

def search_players(query: str, top_k: int = 5):
    """En iyi eşleşen top_k oyuncuyu döner."""
    qvec = _embed_text(query)
    sims = cosine_similarity(qvec, _embedding_matrix)[0]
    idxs = sims.argsort()[::-1][:top_k]

    results = []
    for i in idxs:
        info = _latest_infos[i]
        results.append({
            "name":     _players[i],
            "season":   info.get("season"),
            "team":     info.get("Squad"),    # template’de player.team kullanılıyor
            "league":   info.get("league"),
            "position": info.get("Pos"),      # template’de player.position
            "age":      info.get("Age"),
            "xA":       info.get("xA"),
            "KP":       info.get("KP"),
            "Ast":      info.get("Ast"),
            "PrgP":     info.get("PrgP"),
            "profile":  _profiles[i]          # açıklama üretiminde kullanacağız
        })
    return results

# query_module.py
# … mevcut import’lar ve search_players tanımı …

def generate_explanation(selected_players):
    """Seçilen oyuncular üzerinden HTML listesi halinde açıklama üretir."""
    explanation_lines = []
    for p in selected_players:
        explanation_lines.append(
            f"<div class='player-line'>🟣 {p['name']} ({p['team']}, {p['season']}) • "
            f"Position: {p['position']}, Age: {p['age']} • "
            f"xA: {p['xA']} → Expected Assists – indicates creative play • "
            f"KP: {p['KP']} → Key Passes – reflects playmaking ability • "
            f"Ast: {p['Ast']} → Assists – direct contributions to goals • "
            f"PrgP: {p['PrgP']} → Progressive Passes – involvement in advancing play</div>"
        )
    # Satırları alt alta birleştir
    return "\n".join(explanation_lines)

def generate_comment(selected_players):
    """Generate a detailed scouting comment about the selected players."""
    names = [p["name"] for p in selected_players]

    prompt = (
        f"You are a professional football scout. Please provide a detailed, insightful scouting commentary "
        f"about the following players: {', '.join(names)}.\n\n"
        "Do not number the players. Instead, write in full, structured paragraphs—one paragraph per player. "
        "Mention each player's strengths and weaknesses, especially in terms of creativity, positioning, and passing ability. "
        "Your response should be analytical and natural, as if preparing a scouting report for a coach.\n\n"
        "Start a new paragraph for each player. Do not use bullet points or numbers."
    )

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful and professional football scout assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=600
    )

    return response.choices[0].message.content.strip()
