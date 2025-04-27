from mpi4py import MPI
import time
import logging
import os
import pickle
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# --- Checkpointing System Functions ---
def save_checkpoint(current_state, progress_point, data):
    with open('indexer_checkpoint.pkl', 'wb') as f:
        pickle.dump({"state_name": current_state, "progress_point": progress_point, "data": data}, f)

def load_checkpoint():
    if os.path.exists('indexer_checkpoint.pkl'):
        with open('indexer_checkpoint.pkl', 'rb') as f:
            return pickle.load(f)
    return None

# --- Index Functions ---
def load_index():
    if os.path.exists("simple_index.pkl"):
        with open("simple_index.pkl", "rb") as f:
            return pickle.load(f)
    else:
        return {}

def save_index(index):
    with open("simple_index.pkl", "wb") as f:
        pickle.dump(index, f)

# --- State Functions ---
last_heartbeat = time.time()

def idle_state(comm):
    global last_heartbeat
    logging.info("State: IDLE - Waiting for new task...")

    current_time = time.time()
    if current_time - last_heartbeat >= 10:
        logging.info("[IDLE] Heartbeat: Indexer is alive and waiting for tasks.")
        last_heartbeat = current_time

    if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
        page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)
        if not page_data:
            logging.info("Shutdown signal received. Exiting.")
            return "EXIT", None
        save_checkpoint("Receiving_Data", "received_message", page_data)
        return "Receiving_Data", page_data
    else:
        time.sleep(0.5)
        return "IDLE", None

def receiving_data_state(page_data, progress_point=None):
    logging.info("State: RECEIVING_DATA - Validating input...")
    try:
        if len(content.split()) < 10:
            logging.warning(f"Received content is too small (only {len(content.split())} words). Skipping indexing.")
            return "IDLE", None

        if progress_point is None or progress_point == "received_message":
            if not isinstance(page_data, dict):
                logging.warning("Received data is not a dictionary.")
                return "IDLE", None

        url = page_data.get("url")
        content = page_data.get("content")

        if (progress_point is None or progress_point == "received_message") and (not url or not isinstance(url, str)):
            logging.warning("Invalid or missing 'url'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_url", page_data)

        if (progress_point is None or progress_point == "validated_url") and (not content or not isinstance(content, str)):
            logging.warning("Invalid or missing 'content'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_content", page_data)
        logging.info(f"Input validated successfully for URL: {url}")
        return "Parsing", {"url": url, "content": content}

    except Exception as e:
        logging.error(f"Error during receiving data validation: {e}")
        return "Recovery", {"original_state": "Receiving_Data", "page_data": page_data}

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
            content = re.sub(r'<[^>]+>', '', content)
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

def save_top_words(index):
    word_counts = {word: sum(url_freq.values()) for word, url_freq in index.items()}
    top_words = dict(sorted(word_counts.items(), key=lambda item: item[1], reverse=True)[:100])
    with open("top_words.pkl", "wb") as f:
        pickle.dump(top_words, f)
    logging.info("Saved top frequent words to 'top_words.pkl'.")

def indexing_state(data, progress_point=None):
    logging.info("State: INDEXING - Starting indexing process...")
    inserted = 0
    for word in words:
        if word not in index:
            index[word] = {}
        if url not in index[word]:
            index[word][url] = 0
        index[word][url] += 1

        inserted += 1
        if inserted % 1000 == 0:
            save_index(index)
            logging.info(f"[Batch Save] Autosaved after {inserted} words.")
    try:
        url = data.get("url")
        words = data.get("words")
        

        if not url or not words:
            logging.warning("Missing URL or words for indexing.")
            return "IDLE", None

        index = load_index()

        if progress_point is None or progress_point == "filtered_words":
            for word in words:
                if word not in index:
                    index[word] = {}
                if url not in index[word]:
                    index[word][url] = 0
                index[word][url] += 1

            save_checkpoint("Indexing", "words_inserted", data)
            logging.info(f"Inserted {len(words)} words into the index.")

        save_index(index)
        save_top_words(index)  # Save top frequent words here!
        save_checkpoint("Indexing", "index_saved", data)
        logging.info("Index saved successfully.")
        return "Ready_For_Querying", None

    except Exception as e:
        logging.error(f"Error during indexing: {e}")
        return "Recovery", {"original_state": "Indexing", "data": data}


def ready_for_querying_state(comm):
    logging.info("State: READY_FOR_QUERYING - Accepting queries (smart search).")
    try:
        if not os.path.exists("simple_index.pkl"):
            logging.error("Index file not found. Cannot perform queries.")
            return "IDLE", None

        with open("simple_index.pkl", "rb") as f:
            index = pickle.load(f)

        print("\n[Indexer] Ready for Queries! Type a keyword (supports prefix search) or 'exit' to stop.")

        while True:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
                page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)
                if page_data:
                    logging.info("New crawler data received! Switching to Receiving_Data...")
                    save_checkpoint("Receiving_Data", "received_message", page_data)
                    return "Receiving_Data", page_data

            query = input("Enter keyword (or 'exit' to stop): ").strip().lower()

            if query == "exit":
                logging.info("Exiting query mode...")
                break

            if query:
                # --- Auto-Suggest Top 3 Words ---
                suggestions = [word for word in index if word.startswith(query)]
                if suggestions:
                    print(f"Suggestions: {', '.join(suggestions[:3])}")

                # --- Partial Search and Display ---
                matches = {word: index[word] for word in index if word.startswith(query)}

                if matches:
                    print(f"\nFound {len(matches)} matching word(s) for prefix '{query}':")
                    best_match_url = None
                    best_match_score = 0

                    for word, urls in matches.items():
                        print(f"\n🔹 {word}:")
                        sorted_urls = sorted(urls.items(), key=lambda item: item[1], reverse=True)
                        for url, freq in sorted_urls:
                            print(f"   - {url} (frequency: {freq})")
                            if freq > best_match_score:
                                best_match_score = freq
                                best_match_url = (url, word, freq)

                    if best_match_url:
                        print(f"\n Top Best Match: Word '{best_match_url[1]}' at {best_match_url[0]} (frequency {best_match_url[2]})")

                else:
                    print(f" No matches found for prefix '{query}'.")
                    # Try to suggest a close spelling
                    suggestion = None
                    smallest_dist = 999
                    for word in index.keys():
                        dist = simple_edit_distance(query, word)
                        if dist < smallest_dist and dist <= 2:
                            suggestion = word
                            smallest_dist = dist
                    if suggestion:
                        print(f" Did you mean: '{suggestion}'?")

            time.sleep(0.2)

        return "IDLE", None

    except Exception as e:
        logging.error(f"Error during querying: {e}")
        return "Recovery", {"original_state": "Ready_For_Querying", "data": None}

# --- Mini Function for Fuzzy Suggest ---
def simple_edit_distance(a, b):
    """Very basic edit distance calculator."""
    if len(a) > len(b):
        a, b = b, a
    if len(b) - len(a) > 2:
        return 999
    return sum(1 for x, y in zip(a, b) if x != y) + abs(len(a) - len(b))


# --- Main Control Function ---
def indexer_node():
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
            state, data = idle_state(comm)
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
            state, data = ready_for_querying_state(comm)
            progress_point = None

        elif state == "EXIT":
            break


if __name__ == "__main__":
    indexer_node()