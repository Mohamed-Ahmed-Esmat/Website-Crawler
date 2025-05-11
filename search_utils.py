import logging
import pysolr
import requests
from pymongo import MongoClient
from datetime import datetime

solr = pysolr.Solr('http://10.10.0.43:8983/solr/indexer_core', always_commit=False)

class IndexerSearch:
    @staticmethod
    def index_document(doc: dict):
        try:
            solr.add([doc])
            logging.info(f"✅ Document indexed: {doc['id']}")
        except Exception as e:
            logging.error(f"❌ Failed to index document {doc['id']}: {e}")

    @staticmethod
    def index_autocomplete_doc(doc: dict):
        try:
            solr.add([doc])
            logging.info(f"✅ Autocomplete doc indexed: {doc['id']}")
        except Exception as e:
            logging.error(f"❌ Failed to index autocomplete doc {doc['id']}: {e}")

    @staticmethod
    def search_stemmed_query(query_text: str):
        try:
            results = solr.search(f"content_stemmed:{query_text}")
            print(f"\n🔎 Found {len(results)} match(es) for query '{query_text}':")
            for result in results:
                doc_id = result.get("id", "No ID")
                snippet = result.get("content_stemmed", "")[:100]
                print(f"🔹 {doc_id} → Snippet: {snippet}...")
        except Exception as e:
            logging.error(f"⚠️ Error in search_stemmed_query: {e}")
            print("Solr query failed.")

    @staticmethod
    def suggest_autocomplete_prefix(prefix: str):
        try:
            results = solr.search(f"suggest_text:{prefix}")
            suggestions = set()

            for result in results:
                text = result.get("suggest_text", "")
                for word in text.split():
                    if word.startswith(prefix):
                        suggestions.add(word)

            if suggestions:
                print(f"\n🔮 Suggestions for '{prefix}': {', '.join(sorted(suggestions))}")
            else:
                print(f"⚠️ No suggestions found for '{prefix}'.")

        except Exception as e:
            logging.error(f"Autocomplete error: {e}")
            print("❌ Failed to fetch autocomplete suggestions.")

    @staticmethod
    def fuzzy_query_search(query: str):
        try:
            fuzzy_query = f"{query}~"
            results = solr.search(f"content_stemmed:{fuzzy_query}")
            if results:
                print(f"\n🔎 Found {len(results)} fuzzy matches for query '{query}':")
                for result in results:
                    doc_id = result.get("id", "No ID")
                    snippet = result.get("content_stemmed", "")[:100]
                    print(f"🔹 {doc_id} → Snippet: {snippet}...")
            else:
                print(f"❌ No fuzzy matches found for '{query}'.")
        except Exception as e:
            logging.error(f"Fuzzy search error: {e}")
            print("⚠️ Fuzzy search failed.")

# User-specific query history for pinky suggestion
client = MongoClient("mongodb://localhost:27017/")
db = client["search_database"]
search_collection = db["search_history"]

def store_search_query(query, user_id):
    try:
        timestamp = datetime.utcnow()
        search_data = {
            "user_id": user_id,
            "query": query,
            "timestamp": timestamp
        }
        search_collection.insert_one(search_data)
        print(f"Search query '{query}' stored for user '{user_id}' at {timestamp}.")
    except Exception as e:
        print(f"Error storing search query: {e}")

def get_recent_queries(user_id, limit=8):
    try:
        recent_queries = list(search_collection.find({"user_id": user_id})
                              .sort("timestamp", -1)
                              .limit(limit))
        return [entry["query"] for entry in recent_queries]
    except Exception as e:
        print(f"Error retrieving recent searches: {e}")
        return []

def log_solr_stats():
    try:
        res = requests.get("http://10.10.0.43:8983/solr/admin/cores?wt=json")
        if res.ok:
            stats = res.json()
            logging.info(f"📊 Solr Core Stats: {stats}")
    except Exception as e:
        logging.warning(f"Failed to fetch Solr stats: {e}")
