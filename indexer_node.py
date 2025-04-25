from mpi4py import MPI
import time
import logging
import os
import pickle

# Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# --- Checkpointing System ---
def save_checkpoint(current_state, progress_point, data):
    with open('indexer_checkpoint.pkl', 'wb') as f:
        pickle.dump({
            "state_name": current_state,
            "progress_point": progress_point,
            "data": data
        }, f)

def load_checkpoint():
    if os.path.exists('indexer_checkpoint.pkl'):
        with open('indexer_checkpoint.pkl', 'rb') as f:
            return pickle.load(f)
    return None

# --- IDLE State ---
def idle_state(comm):
    """
    IDLE State:
    - Waits for new message (tag=2) from crawler
    - If message received, move to 'Receiving_Data'
    - Else, stay in 'IDLE'
    """
    logging.info("State: IDLE - Waiting for new task...")

    if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
        page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)

        if not page_data:
            logging.info("Shutdown signal received. Exiting.")
            return "EXIT", None

        save_checkpoint("Receiving_Data", "received_message", page_data)  # Save checkpoint
        return "Receiving_Data", page_data
    else:
        time.sleep(0.5)
        return "IDLE", None

# --- RECEIVING_DATA State ---
def receiving_data_state(page_data, progress_point=None):
    """
    RECEIVING_DATA State:
    - Validates the input data (should contain 'url' and 'content')
    - If valid, go to 'Parsing'
    - If invalid, return to 'IDLE'
    - On error, go to 'Recovery'
    """
    logging.info("State: RECEIVING_DATA - Validating input...")

    try:
        if progress_point is None or progress_point == "received_message":
            if not isinstance(page_data, dict):
                logging.warning("Received data is not a dictionary.")
                return "IDLE", None

        url = page_data.get("url")
        content = page_data.get("content")

        if (progress_point is None or progress_point == "received_message") and (not url or not isinstance(url, str)):
            logging.warning("Invalid or missing 'url'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_url", page_data)  # Save checkpoint after url validation

        if (progress_point is None or progress_point == "validated_url") and (not content or not isinstance(content, str)):
            logging.warning("Invalid or missing 'content'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_content", page_data)  # Save checkpoint after full validation

        logging.info(f"Input validated successfully for URL: {url}")
        return "Parsing", {"url": url, "content": content}

    except Exception as e:
        logging.error(f"Error during receiving data validation: {e}")
        return "Recovery", {"original_state": "Receiving_Data", "page_data": page_data}
    
# --- Parsing State ---
def parsing_state(data, progress_point=None):
    """
    PARSING State:
    - Tokenizes and cleans the text content
    - Prepares data for indexing
    """
    logging.info("State: PARSING - Starting parsing process...")

    try:
        url = data.get("url")
        content = data.get("content")

        if progress_point is None or progress_point == "validated_content":
            # Step 1: Basic content loading
            if not content or not isinstance(content, str):
                logging.warning("Invalid content format during parsing.")
                return "IDLE", None

            save_checkpoint("Parsing", "loaded_content", data)
            logging.info("Content loaded successfully.")

        if progress_point is None or progress_point == "loaded_content":
            # Step 2: Tokenization
            tokens = content.split()
            data["tokens"] = tokens
            save_checkpoint("Parsing", "tokenized", data)
            logging.info(f"Tokenization complete. {len(tokens)} tokens found.")

        if progress_point is None or progress_point == "tokenized":
            # Step 3: Filtering clean words
            clean_words = [word.lower() for word in data["tokens"] if word.isalpha()]
            data["words"] = clean_words
            save_checkpoint("Parsing", "filtered_words", data)
            logging.info(f"Filtering complete. {len(clean_words)} clean words kept.")

        # Final preparation
        parsed_data = {
            "url": url,
            "words": data["words"]
        }

        return "Indexing", parsed_data

    except Exception as e:
        logging.error(f"Error during parsing: {e}")
        return "Recovery", { "original_state": "Parsing", "data": data }

# --- Indexing State ---
def load_index():
    if os.path.exists("simple_index.pkl"):
        with open("simple_index.pkl", "rb") as f:
            return pickle.load(f)
    else:
        return {}

def save_index(index):
    with open("simple_index.pkl", "wb") as f:
        pickle.dump(index, f)

def indexing_state(data, progress_point=None):
    """
    INDEXING State:
    - Updates the index with parsed words
    - Saves the index persistently
    """
    logging.info("State: INDEXING - Starting indexing process...")

    try:
        url = data.get("url")
        words = data.get("words")

        if not url or not words:
            logging.warning("Missing URL or words for indexing.")
            return "IDLE", None

        # Step 1: Load existing index
        index = load_index()

        if progress_point is None or progress_point == "filtered_words":
            # Step 2: Insert words into index
            for word in words:
                if word not in index:
                    index[word] = set()
                index[word].add(url)

            save_checkpoint("Indexing", "words_inserted", data)
            logging.info(f"Inserted {len(words)} words into the index.")

        # Step 3: Save the updated index
        save_index(index)
        save_checkpoint("Indexing", "index_saved", data)
        logging.info("Index saved successfully.")

        return "Ready_For_Querying", None

    except Exception as e:
        logging.error(f"Error during indexing: {e}")
        return "Recovery", { "original_state": "Indexing", "data": data }
    
# --- Ready_for_Querying State ---
def ready_for_querying_state(comm):
    """
    READY_FOR_QUERYING State (Smart Version):
    - Accepts search queries
    - Listens for new crawler data in the background
    - If new data arrives, switches back to 'Receiving_Data'
    """
    logging.info("State: READY_FOR_QUERYING - Ready to accept queries or new crawl tasks.")

    try:
        if not os.path.exists("simple_index.pkl"):
            logging.error("Index file not found. Cannot perform queries.")
            return "IDLE", None

        with open("simple_index.pkl", "rb") as f:
            index = pickle.load(f)

        print("\n Indexer Ready for Queries! Type a keyword to search or 'exit' to stop.")

        while True:
            # --- Background listening for new crawl tasks ---
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
                page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)
                if page_data:
                    logging.info("New crawler data received! Switching to Receiving_Data...")
                    save_checkpoint("Receiving_Data", "received_message", page_data)
                    return "Receiving_Data", page_data

            # --- Otherwise, accept user search input ---
            print("\n(Press Enter if you want to skip checking for crawler messages)")
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


            # Short delay to avoid CPU overuse
            time.sleep(0.2)

        return "IDLE", None

    except Exception as e:
        logging.error(f"Error during querying: {e}")
        return "Recovery", { "original_state": "Ready_For_Querying", "data": None }


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
            state, data = idle_state(comm)
            progress_point = None

        elif state == "Receiving_Data":
            state, data = receiving_data_state(data, progress_point)
            progress_point = None

        elif state == "EXIT":
            break
        elif state == "Parsing":
            state, data = parsing_state(data, progress_point)
            progress_point = None
        elif state == "Indexing":
            state, data = indexing_state(data, progress_point)
            progress_point = None
        elif state == "Ready_For_Querying":
            state, data = ready_for_querying_state(comm)
            progress_point = None



        # Future states (Parsing, Indexing, etc.) will go here
