from mpi4py import MPI
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests
import json
import os
import sys
import logging

TAG_START_CRAWLING = 10
TAG_SEARCH = 11
TAG_NODES_STATUS = 12
TAG_SHUTDOWN_MASTER = 13
TAG_CRAWL_PROGRESS_REQUEST = 14 # Server to Master for crawl progress
TAG_CRAWL_PROGRESS_UPDATE = 15  # Master to Server with crawl progress

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__, template_folder="./templates")
CORS(app)  

@app.route('/')
def index():
    """Serve the main web interface"""
    return render_template('index.html')

@app.route('/start-crawl', methods=['POST'])
def start_crawl():
    """API endpoint to start a crawling task"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        seed_urls_input = data.get('seed_urls', '')
        max_depth = data.get('max_depth', 3)

        if isinstance(seed_urls_input, list) and len(seed_urls_input) > 0:
            seed_urls_input = seed_urls_input[0]

        if isinstance(seed_urls_input, str):
            seed_urls = [url.strip() for url in seed_urls_input.split(',') if url.strip()]
        elif isinstance(seed_urls_input, list):
            seed_urls = seed_urls_input
        else:
            seed_urls = []

        
        if not seed_urls:
            return jsonify({"error": "Invalid or empty seed_urls. Must be a comma-separated string of URLs or a list."}), 400
            
        if not isinstance(max_depth, int) or max_depth <= 0:
            return jsonify({"error": "Invalid max_depth. Must be a positive integer."}), 400
        
        comm.send({
            "seed_urls": seed_urls, 
            "max_depth": max_depth
        }, dest=0, tag=TAG_START_CRAWLING)
        
        resulted_urls = comm.recv(source=0, tag=TAG_START_CRAWLING)
        
        return jsonify({
            "message": "Crawl task started successfully",
            "resulted_urls": resulted_urls
        }), 200
        
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@app.route('/search', methods=['GET'])
def search():
    """API endpoint to search crawled content"""
    try:
        query = request.args.get('query', '')
        search_type = request.args.get('type', 'keyword')
        
        if not query:
            return jsonify({"error": "No search query provided"}), 400
        
        comm.send({
            "query": query, 
            "search_type": search_type
        }, dest=0, tag=TAG_SEARCH)
        
        resulted_urls = comm.recv(source=0, tag=TAG_SEARCH)
        
        return jsonify({
            "resulted_urls": resulted_urls
        }), 200
        
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@app.route('/status', methods=['GET'])
def get_system_status():
    """API endpoint to get the status of all crawler nodes and indexers"""
    try:

        comm.send(None, dest=0, tag=TAG_NODES_STATUS)

        nodes = comm.recv(source=0, tag=TAG_NODES_STATUS)

        return jsonify({
            "nodes": nodes,
            "total_active_crawlers": sum(1 for node in nodes if node["type"] == "crawler" and node["active"]),
            "total_active_indexers": sum(1 for node in nodes if node["type"] == "indexer" and node["active"])
        }), 200
        
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@app.route('/crawl-progress', methods=['GET'])
def get_crawl_progress():
    """API endpoint to get the progress of the current crawl job"""
    try:
        if rank == 1: # Ensure only server rank interacts with master for this
            logging.info(f"Server (rank {rank}): Received HTTP request for crawl progress.")
            comm.send(None, dest=0, tag=TAG_CRAWL_PROGRESS_REQUEST) # Send request to master
            
            progress_data = comm.recv(source=0, tag=TAG_CRAWL_PROGRESS_UPDATE) # Receive progress update
            
            logging.info(f"Server (rank {rank}): Received crawl progress from master: {progress_data}")
            return jsonify(progress_data), 200
        else:
            return jsonify({"error": "This MPI rank is not the designated server."}), 403
            
    except MPI.Exception as mpi_e:
        logging.error(f"Server (rank {rank}): MPI Error fetching crawl progress: {mpi_e}")
        return jsonify({"error": f"MPI error fetching crawl progress: {str(mpi_e)}"}), 500
    except Exception as e:
        logging.error(f"Server (rank {rank}): General error fetching crawl progress: {e}")
        return jsonify({"error": f"Server error fetching crawl progress: {str(e)}"}), 500

@app.route('/shutdown-master', methods=['POST'])
def shutdown_master_endpoint():
    """API endpoint to trigger the shutdown of the master node."""
    try:
        if rank == 1: # Only the designated server rank should send this
            logging.info(f"Server (rank {rank}): Received HTTP request to shutdown master node.")
            # Send shutdown signal to master node (rank 0)
            comm.send(None, dest=0, tag=TAG_SHUTDOWN_MASTER)
            logging.info(f"Server (rank {rank}): Sent TAG_SHUTDOWN_MASTER to master (rank 0).")
            
            # Optionally, master could send an ack. For simplicity, we don't wait for one here.
            # If master sends an ack, server would do a comm.recv here.
            
            return jsonify({"message": "Shutdown signal sent to master node."}), 200
        else:
            # This should ideally not happen if only rank 1 runs the Flask app.
            return jsonify({"error": "This MPI rank is not authorized to shut down the master."}), 403

    except MPI.Exception as mpi_e:
        logging.error(f"Server (rank {rank}): MPI Error during master shutdown: {mpi_e}")
        return jsonify({"error": f"MPI error during master shutdown: {str(mpi_e)}"}), 500
    except Exception as e:
        logging.error(f"Server (rank {rank}): General error during master shutdown: {e}")
        return jsonify({"error": f"Server error during master shutdown: {str(e)}"}), 500

def start_server():
    """Start the Flask server"""
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    app.run(host='0.0.0.0', port=port, debug=debug)

if __name__ == '__main__':
    start_server()