from mpi4py import MPI
import logging
import sys
import os
import socket
import signal

# Configure logging
hostname = socket.gethostname()
try:
    ip_address = socket.gethostbyname(hostname)
except:
    ip_address = "unknown-ip"

logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - {ip_address} - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"main_{ip_address}.log"),
        logging.StreamHandler()
    ]
)

def handle_sigterm(signum, frame):
    os._exit(1)

signal.signal(signal.SIGTERM, signal.SIG_DFL)
signal.signal(signal.SIGINT, signal.SIG_DFL)

def main():
    """
    Entry point for the distributed web crawler.
    Determines the role of the current process based on its MPI rank
    and runs the appropriate function.
    """
    # Initialize MPI
    if not MPI.Is_initialized():
        MPI.Init()
    
    comm = MPI.COMM_WORLD
    comm.Set_errhandler(MPI.ERRORS_RETURN)  # Prevent automatic abort
    
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Set up logging for this specific node
    node_type = "Unknown"
    
    # Verify we have enough processes
    if size < 3:
        print(f"Error: Not enough processes. Need at least 3, but got {size}.")
        MPI.Finalize()
        return
    
    if size > 5:
        print(f"Warning: More than 5 processes detected. This may lead to performance issues.")
        # Determine role based on rank
        if rank == 0:
            node_type = "Master"
            logging.info(f"Process {rank} starting as Master node")
            # Import master module and run master process
            from master_node import master_process
            master_process(comm)
        elif rank == 1:  # server node
            node_type = "Server"
            logging.info(f"Process {rank} starting as Server")
            from server import start_server
            start_server(comm)
        elif rank == size - 1:  # Last rank is the indexer
            node_type = "Indexer"
            logging.info(f"Process {rank} starting as Indexer node")
            # Import indexer module and run indexer process
            from indexer_main import indexer_node
            indexer_node(comm)
        elif rank == size - 2:  # Second last rank is also an indexer
            node_type = "Indexer"
            logging.info(f"Process {rank} starting as Indexer node")
            # Import indexer module and run indexer process
            from indexer_main import indexer_node
            indexer_node(comm)
        else:  # Other ranks are crawlers
            node_type = "Crawler"
            logging.info(f"Process {rank} starting as Crawler node")
            # Import crawler module and run crawler process
            from crawler_node import crawler_process
            crawler_process(comm)
    else:
        # Determine role based on rank
        if rank == 0:
            node_type = "Master"
            logging.info(f"Process {rank} starting as Master node")
            # Import master module and run master process
            from master_node import master_process
            master_process(comm)
        elif rank == 1:  # server node
            node_type = "Server"
            logging.info(f"Process {rank} starting as Server")
            from server import start_server
            start_server(comm)
        elif rank == size - 1:  # Last rank is the indexer
            node_type = "Indexer"
            logging.info(f"Process {rank} starting as Indexer node")
            # Import indexer module and run indexer process
            from indexer_main import indexer_node
            indexer_node(comm)
        else:  # Other ranks are crawlers
            node_type = "Crawler"
            logging.info(f"Process {rank} starting as Crawler node")
            # Import crawler module and run crawler process
            from crawler_node import crawler_process
            crawler_process(comm)
    
    # Wait for all processes to finish their tasks
    comm.Barrier()
    logging.info(f"{node_type} node with rank {rank} finished")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error in main process: {e}", exc_info=True)
    finally:
        # Clean up MPI resources
        if MPI.Is_initialized() and not MPI.Is_finalized():
            MPI.Finalize()