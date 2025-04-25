from mpi4py import MPI
import time
import pickle
import os

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# -------- Rank 0 = CRAWLER --------
if comm.Get_size() < 2:
    raise Exception(" This script requires at least 2 MPI processes. Run it using: mpiexec -n 2 python test_driver_combined.py")

if rank == 0:
    print("[Crawler] Sending test data to Indexer...")

    page = {
        "url": "http://example.com",
        "content": "This is a sample content to be indexed by the system"
    }

    comm.send(page, dest=1, tag=2)
    print("[Crawler] Sent data.")
    time.sleep(3)

# -------- Rank 1 = INDEXER --------
elif rank == 1:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

    def save_checkpoint(state, point, data):
        with open("indexer_checkpoint.pkl", "wb") as f:
            pickle.dump({"state_name": state, "progress_point": point, "data": data}, f)

    def load_checkpoint():
        if os.path.exists("indexer_checkpoint.pkl"):
            with open("indexer_checkpoint.pkl", "rb") as f:
                return pickle.load(f)
        return None

    def idle_state(comm):
        logging.info("State: IDLE - Waiting...")
        if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
            msg = comm.recv(source=MPI.ANY_SOURCE, tag=2)
            save_checkpoint("Receiving_Data", "received_message", msg)
            return "Receiving_Data", msg
        time.sleep(0.5)
        return "IDLE", None

    def receiving_data_state(data, progress_point=None):
        if not data.get("url") or not data.get("content"):
            return "IDLE", None
        save_checkpoint("Receiving_Data", "validated_content", data)
        return "Parsing", data

    def parsing_state(data, progress_point=None):
        tokens = data["content"].split()
        words = [word.lower() for word in tokens if word.isalpha()]
        return "Indexing", {"url": data["url"], "words": words}

    def load_index():
        if os.path.exists("simple_index.pkl"):
            with open("simple_index.pkl", "rb") as f:
                return pickle.load(f)
        return {}

    def save_index(index):
        with open("simple_index.pkl", "wb") as f:
            pickle.dump(index, f)

    def indexing_state(data, progress_point=None):
        index = load_index()
        for word in data["words"]:
            index.setdefault(word, set()).add(data["url"])
        save_index(index)
        return "Ready_For_Querying", None

    def ready_for_querying_state(comm):
        with open("simple_index.pkl", "rb") as f:
            index = pickle.load(f)
        print("Ready for Queries. Type a word (or 'exit'):")
        while True:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
                msg = comm.recv(source=MPI.ANY_SOURCE, tag=2)
                save_checkpoint("Receiving_Data", "received_message", msg)
                return "Receiving_Data", msg
            query = input("> ").strip().lower()
            if query == "exit": break
            print(index.get(query, "Not found."))
        return "IDLE", None

    checkpoint = load_checkpoint()
    state = checkpoint["state_name"] if checkpoint else "IDLE"
    data = checkpoint["data"] if checkpoint else None
    progress = checkpoint["progress_point"] if checkpoint else None

    while True:
        if state == "IDLE":
            state, data = idle_state(comm)
        elif state == "Receiving_Data":
            state, data = receiving_data_state(data, progress)
        elif state == "Parsing":
            state, data = parsing_state(data, progress)
        elif state == "Indexing":
            state, data = indexing_state(data, progress)
        elif state == "Ready_For_Querying":
            state, data = ready_for_querying_state(comm)
        elif state == "EXIT":
            break
