import json
import time
from openai import OpenAI
from collections import defaultdict
from pymongo import MongoClient

# 🔐 Set your API key
client = OpenAI(api_key="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")

# 📥 Load raw player stats
client_mongo = MongoClient("mongodb://127.0.0.1")  # IP'yi güncelle
db = client_mongo["bigscout"]
collection = db["cleaned_players_agg4"]

# İsteğe bağlı olarak sadece bazılarını alabilirsin
raw_data = list(collection.find({}, {"_id": 0}))

# 🔁 Group all data by Player name and summarize full seasons
def parse_season(season): return int(season.split("-")[1]) if season else 0

grouped = defaultdict(list)
for row in raw_data:
    grouped[row["Player"]].append(row)

merged = []
for name, entries in grouped.items():
    entries.sort(key=lambda x: parse_season(x.get("season", "")), reverse=True)
    latest = entries[0]
    full_summary = f"{name} season overview:\n"
    for e in entries:
        season = e.get("season", "NA")
        team = e.get("Squad", "NA")
        league = e.get("league", "NA")
        stats = ", ".join(f"{k}: {v}" for k, v in e.items() if isinstance(v, (int, float)) and k not in ['_id'])
        full_summary += f"- {season}, {team} ({league}): {stats}\n"
    merged.append({
        "Player": name,
        "latest": latest,
        "stat_summary": full_summary
    })

# 🧠 Generate GPT scouting profiles
def generate_scouting_profile(summary):
    system = "You are a professional football scout assistant."
    user_prompt = (
        f"Given the following stats across multiple seasons, generate a concise scouting-style player profile. "
        f"Describe the player's position, strengths, and play style based on the stats below:\n\n{summary}"
    )
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",  # Use gpt-4 if you prefer quality over speed
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_prompt}
        ]
    )
    return response.choices[0].message.content.strip()

# 🔄 Step 1: Generate all GPT scouting profiles
scouting_profiles = []
for i, p in enumerate(merged):
    print(f"🧠 Generating GPT profile {i+1}/{len(merged)} – {p['Player']}")
    try:
        text = generate_scouting_profile(p["stat_summary"])
        scouting_profiles.append({
            "Player": p["Player"],
            "latest": p["latest"],
            "scouting_profile": text
        })
    except Exception as e:
        print(f"❌ GPT error on {p['Player']}: {e}")
        scouting_profiles.append({
            "Player": p["Player"],
            "latest": p["latest"],
            "scouting_profile": ""
        })
        time.sleep(10)

# 🔄 Step 2: Batch embed scouting profiles
def embed_batch(texts):
    response = client.embeddings.create(
        input=texts,
        model="text-embedding-3-small"
    )
    return [item.embedding for item in response.data]

batch_size = 100
final_data = []
for i in range(0, len(scouting_profiles), batch_size):
    batch = scouting_profiles[i:i + batch_size]
    texts = [b["scouting_profile"] for b in batch]
    print(f"🔗 Embedding batch {i}–{i+len(batch)}")
    try:
        embeddings = embed_batch(texts)
        for j in range(len(batch)):
            final_data.append({
                "Player": batch[j]["Player"],
                "latest": batch[j]["latest"],
                "scouting_profile": batch[j]["scouting_profile"],
                "embedding": embeddings[j]
            })
    except Exception as e:
        print(f"❌ Embedding failed on batch {i}: {e}")
        time.sleep(10)

# 💾 Save to file
mongo_client = MongoClient("mongodb://127.0.0.1:27017")
db = mongo_client["bigscout"]
embedCollection = db["embeddings"]

# Önce koleksiyonu temizle (isteğe bağlı)
embedCollection.delete_many({})
embedCollection.insert_many(final_data)

print("✅ All done! Saved to MongoDB collection: bigscout.embeddings")
