from mpi4py import MPI
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests
import json
import os
import sys

TAG_START_CRAWLING = 10
TAG_SEARCH = 11
TAG_NODES_STATUS = 12

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Add the parent directory to sys.path to import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__, template_folder="../templates")
CORS(app)  # Enable CORS for all routes

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
            
        seed_urls = data.get('seed_urls', [])
        max_depth = data.get('max_depth', 3)
        
        if not seed_urls or not isinstance(seed_urls, list):
            return jsonify({"error": "Invalid or empty seed_urls. Must be a list of URLs."}), 400
            
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

        resulted_urls = ['http://example.com/search1', 'http://example.com/search2']  # Example URLs

        return jsonify({
            "resulted_urls": resulted_urls
        }), 200
        
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@app.route('/status', methods=['GET'])
def get_system_status():
    """API endpoint to get the status of all crawler nodes and indexers"""
    try:

        comm.send(None, dest=0, tag=TAG_START_CRAWLING)

        nodes = comm.recv(source=0, tag=TAG_START_CRAWLING)

        return jsonify({
            "nodes": nodes,
            "total_active_crawlers": sum(1 for node in nodes if node["type"] == "crawler" and node["active"]),
            "total_active_indexers": sum(1 for node in nodes if node["type"] == "indexer" and node["active"])
        }), 200
        
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

def start_server():
    """Start the Flask server"""
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    app.run(host='0.0.0.0', port=port, debug=debug)

if __name__ == '__main__':
    # Start the Flask server
    start_server()