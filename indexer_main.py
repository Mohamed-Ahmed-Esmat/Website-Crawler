from mpi4py import MPI
import logging
import socket
import os
from Indexer_States import IndexerStates
from utils import load_checkpoint, delete_checkpoint

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

hostname = socket.gethostname()
try:
    ip_address = socket.gethostbyname(hostname)
except:
    ip_address = "unknown-ip"

logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - {ip_address} - Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"indexer_{ip_address}.log"),
        logging.StreamHandler()
    ]
)

def indexer_node():
    logging.info(f"Indexer node started with rank {rank} of {size}")

    checkpoint = load_checkpoint(rank)
    if checkpoint:
        state = checkpoint["state_name"]
        data = checkpoint["data"]
        progress_point = checkpoint.get("progress_point")
        logging.info(f"[Recovery] Node {rank} resumes from {state} with progress {progress_point}")
    else:
        delete_checkpoint(rank)
        state = "IDLE"
        data = None
        progress_point = None

    while True:
        if state == "IDLE":
            state, data = IndexerStates.idle_state(comm)
        elif state == "Receiving_Data":
            state, data = IndexerStates.receiving_data_state(data, progress_point, rank)
        elif state == "Parsing":
            state, data = IndexerStates.parsing_state(data, progress_point, rank)
        elif state == "Indexing":
            state, data = IndexerStates.indexing_state(data, progress_point, rank)
        elif state == "Ready_For_Querying":
            state, data = IndexerStates.ready_for_querying_state(comm)
        elif state == "EXIT":
            break
        progress_point = None

if __name__ == "__main__":
    indexer_node()