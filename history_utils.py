from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client["indexer"]
history_col = db["search_history"]

def store_search_query(query: str):
    history_col.update_one(
        {"query": query},
        {
            "$set": {"last_searched": datetime.utcnow()},
            "$inc": {"frequency": 1}
        },
        upsert=True
    )

def get_search_history(prefix: str, limit=5):
    return list(
        history_col.find({"query": {"$regex": f"^{prefix}"}})
        .sort("last_searched", -1)
        .limit(limit)
    )
