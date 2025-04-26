import time
import logging
import os
import json

# Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

# --- Checkpointing System ---
def save_checkpoint(current_state, progress_point, data):
    with open('indexer_checkpoint.json', 'w', encoding='utf-8') as f:
        json.dump({
            "state_name": current_state,
            "progress_point": progress_point,
            "data": data
        }, f, indent=4)

def load_checkpoint():
    if os.path.exists('indexer_checkpoint.json'):
        with open('indexer_checkpoint.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

# --- IDLE State ---
def idle_state():
    logging.info("State: IDLE - Waiting for crawler JSON output...")

    if os.path.exists("crawler_output.json"):
        with open("crawler_output.json", "r", encoding="utf-8") as f:
            page_data = json.load(f)

        if not page_data:
            logging.info("No valid data found. Exiting.")
            return "EXIT", None

        save_checkpoint("Receiving_Data", "received_message", page_data)
        return "Receiving_Data", page_data
    else:
        time.sleep(0.5)
        return "IDLE", None

# --- RECEIVING_DATA State ---
def receiving_data_state(page_data, progress_point=None):
    logging.info("State: RECEIVING_DATA - Validating input...")
    try:
        if progress_point is None or progress_point == "received_message":
            if not isinstance(page_data, dict):
                logging.warning("Received data is not a dictionary.")
                return "IDLE", None

        url = page_data.get("url")
        content = page_data.get("content")

        if not url or not isinstance(url, str):
            logging.warning("Invalid or missing 'url'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_url", page_data)

        if not content or not isinstance(content, str):
            logging.warning("Invalid or missing 'content'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_content", page_data)
        logging.info(f"Input validated successfully for URL: {url}")
        return "Parsing", {"url": url, "content": content}

    except Exception as e:
        logging.error(f"Error during receiving data validation: {e}")
        return "Recovery", {"original_state": "Receiving_Data", "page_data": page_data}

# --- Parsing State ---
def parsing_state(data, progress_point=None):
    logging.info("State: PARSING - Starting parsing process...")
    try:
        url = data.get("url")
        content = data.get("content")

        if progress_point is None or progress_point == "validated_content":
            if not content or not isinstance(content, str):
                logging.warning("Invalid content format during parsing.")
                return "IDLE", None
            save_checkpoint("Parsing", "loaded_content", data)
            logging.info("Content loaded successfully.")

        if progress_point is None or progress_point == "loaded_content":
            tokens = content.split()
            data["tokens"] = tokens
            save_checkpoint("Parsing", "tokenized", data)
            logging.info(f"Tokenization complete. {len(tokens)} tokens found.")

        if progress_point is None or progress_point == "tokenized":
            clean_words = [word.lower() for word in data["tokens"] if word.isalpha()]
            data["words"] = clean_words
            save_checkpoint("Parsing", "filtered_words", data)
            logging.info(f"Filtering complete. {len(clean_words)} clean words kept.")

        parsed_data = {"url": url, "words": data["words"]}
        return "Indexing", parsed_data

    except Exception as e:
        logging.error(f"Error during parsing: {e}")
        return "Recovery", {"original_state": "Parsing", "data": data}

# --- Indexing State ---
def load_index():
    if os.path.exists("simple_index.json"):
        with open("simple_index.json", "r", encoding="utf-8") as f:
            index = json.load(f)
        return {word: set(urls) for word, urls in index.items()}
    else:
        return {}

def save_index(index):
    index_serializable = {word: list(urls) for word, urls in index.items()}
    with open("simple_index.json", "w", encoding="utf-8") as f:
        json.dump(index_serializable, f, indent=4)

def indexing_state(data, progress_point=None):
    logging.info("State: INDEXING - Starting indexing process...")
    try:
        url = data.get("url")
        words = data.get("words")

        if not url or not words:
            logging.warning("Missing URL or words for indexing.")
            return "IDLE", None

        index = load_index()

        if progress_point is None or progress_point == "filtered_words":
            for word in words:
                index.setdefault(word, set()).add(url)
            save_checkpoint("Indexing", "words_inserted", data)
            logging.info(f"Inserted {len(words)} words into the index.")

        save_index(index)
        save_checkpoint("Indexing", "index_saved", data)
        logging.info("Index saved successfully.")

        return "Ready_For_Querying", None

    except Exception as e:
        logging.error(f"Error during indexing: {e}")
        return "Recovery", {"original_state": "Indexing", "data": data}

# --- Ready_for_Querying State ---
def ready_for_querying_state():
    logging.info("State: READY_FOR_QUERYING - Ready to accept queries or new crawl tasks.")
    try:
        if not os.path.exists("simple_index.json"):
            logging.error("Index file not found. Cannot perform queries.")
            return "IDLE", None

        with open("simple_index.json", "r", encoding="utf-8") as f:
            index = json.load(f)

        print("\n Indexer Ready for Queries! Type a keyword to search or 'exit' to stop.")

        while True:
            if os.path.exists("crawler_output.json"):
                with open("crawler_output.json", "r", encoding="utf-8") as f:
                    page_data = json.load(f)
                if page_data:
                    logging.info("New crawler JSON data detected! Switching to Receiving_Data...")
                    save_checkpoint("Receiving_Data", "received_message", page_data)
                    return "Receiving_Data", page_data

            print("\n(Press Enter if you want to skip checking for crawler JSON)")
            query = input("Enter keyword (or 'exit' to stop): ").strip().lower()

            if query == "exit":
                logging.info("Exiting query mode...")
                break

            if query:
                results = index.get(query, None)
                if results:
                    print(f" Found {len(results)} result(s) for '{query}':")
                    for url in results:
                        print(f" - {url}")
                else:
                    print(f" No results found for '{query}'.")

            time.sleep(0.2)

        return "IDLE", None

    except Exception as e:
        logging.error(f"Error during querying: {e}")
        return "Recovery", {"original_state": "Ready_For_Querying", "data": None}
    
# --- Recovery State ---
MAX_RECOVERY_RETRIES = 3
recovery_attempts = 0

def recovery_state(data):
    """
    RECOVERY State:
    - Attempts to reload last checkpoint and retry the original state
    - If fails repeatedly, exits permanently
    """
    global recovery_attempts
    logging.warning("State: RECOVERY - Attempting to recover...")

    try:
        if not data or "original_state" not in data:
            logging.error("Recovery data missing original_state info.")
            return "IDLE", None

        original_state = data["original_state"]
        checkpoint_data = load_checkpoint()

        if not checkpoint_data:
            logging.error("No checkpoint data found during recovery.")
            return "IDLE", None

        progress_point = checkpoint_data.get("progress_point")
        recovered_data = checkpoint_data.get("data")

        logging.info(f"Attempting to recover from state: {original_state} with progress: {progress_point}")

        if original_state == "Receiving_Data":
            return receiving_data_state(recovered_data, progress_point)
        elif original_state == "Parsing":
            return parsing_state(recovered_data, progress_point)
        elif original_state == "Indexing":
            return indexing_state(recovered_data, progress_point)
        elif original_state == "Ready_For_Querying":
            return ready_for_querying_state()
        else:
            logging.error(f"Unknown original state '{original_state}' during recovery.")
            return "IDLE", None

    except Exception as e:
        recovery_attempts += 1
        logging.error(f"Recovery attempt {recovery_attempts} failed: {e}")

        if recovery_attempts >= MAX_RECOVERY_RETRIES:
            logging.critical("Maximum recovery attempts exceeded. Exiting node permanently.")
            return "EXIT", None
        else:
            logging.info(f"Retrying recovery (attempt {recovery_attempts}/{MAX_RECOVERY_RETRIES})...")
            time.sleep(1)
            return "Recovery", data


# --- Main Control Loop ---
if __name__ == "__main__":
    checkpoint = load_checkpoint()
    if checkpoint:
        state = checkpoint["state_name"]
        data = checkpoint["data"]
        progress_point = checkpoint.get("progress_point")
        logging.info(f"Resuming from checkpoint: State={state}, Progress={progress_point}")
    else:
        state = "IDLE"
        data = None
        progress_point = None

    while True:
        if state == "IDLE":
            state, data = idle_state()
            progress_point = None
        elif state == "Receiving_Data":
            state, data = receiving_data_state(data, progress_point)
            progress_point = None
        elif state == "Parsing":
            state, data = parsing_state(data, progress_point)
            progress_point = None
        elif state == "Indexing":
            state, data = indexing_state(data, progress_point)
            progress_point = None
        elif state == "Ready_For_Querying":
            state, data = ready_for_querying_state()
            progress_point = None
        elif state == "EXIT":
            break
        elif state == "Recovery":
            state, data = recovery_state(data)
            progress_point = None
