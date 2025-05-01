import argparse
import requests
import logging
from flask import Flask, request, jsonify, render_template

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Flask app for web interface
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit_query():
    master_url = request.form.get('master_url')
    query = request.form.get('query')
    max_depth = int(request.form.get('max_depth', 3))

    if not master_url or not query:
        return jsonify({"error": "Master URL and query are required."}), 400

    try:
        payload = {"urls": [query], "max_depth": max_depth}
        response = requests.post(f"{master_url}/submit", json=payload)
        if response.status_code == 200:
            return jsonify({"message": "Query submitted successfully.", "response": response.json()}), 200
        else:
            return jsonify({"error": f"Failed to submit query. Status code: {response.status_code}", "response": response.text}), 500
    except Exception as e:
        return jsonify({"error": f"Error submitting query: {e}"}), 500

@app.route('/status', methods=['GET'])
def get_status():
    master_url = request.args.get('master_url')

    if not master_url:
        return jsonify({"error": "Master URL is required."}), 400

    try:
        response = requests.get(f"{master_url}/status")
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({"error": f"Failed to fetch status. Status code: {response.status_code}", "response": response.text}), 500
    except Exception as e:
        return jsonify({"error": f"Error fetching status: {e}"}), 500

@app.route('/results', methods=['GET'])
def get_results():
    master_url = request.args.get('master_url')

    if not master_url:
        return jsonify({"error": "Master URL is required."}), 400

    try:
        response = requests.get(f"{master_url}/results")
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({"error": f"Failed to fetch results. Status code: {response.status_code}", "response": response.text}), 500
    except Exception as e:
        return jsonify({"error": f"Error fetching results: {e}"}), 500

@app.route('/search', methods=['GET'])
def search_query():
    """Endpoint to submit a search query."""
    master_url = request.args.get('master_url')
    query = request.args.get('query')

    if not master_url or not query:
        return jsonify({"error": "Master URL and query are required."}), 400

    try:
        response = requests.get(f"{master_url}/search", params={"query": query})
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({"error": f"Failed to fetch search results. Status code: {response.status_code}", "response": response.text}), 500
    except Exception as e:
        return jsonify({"error": f"Error fetching search results: {e}"}), 500

@app.route('/monitor', methods=['GET'])
def monitor_progress():
    """Endpoint to monitor crawling and indexing progress."""
    master_url = request.args.get('master_url')

    if not master_url:
        return jsonify({"error": "Master URL is required."}), 400

    try:
        response = requests.get(f"{master_url}/status")
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({"error": f"Failed to fetch status. Status code: {response.status_code}", "response": response.text}), 500
    except Exception as e:
        return jsonify({"error": f"Error fetching status: {e}"}), 500

def submit_task(master_url, urls, max_depth):
    """Submit a crawling task to the master node."""
    try:
        payload = {
            "urls": urls,
            "max_depth": max_depth
        }
        response = requests.post(f"{master_url}/submit", json=payload)
        if response.status_code == 200:
            logging.info("Task submitted successfully.")
            logging.info(f"Response: {response.json()}")
        else:
            logging.error(f"Failed to submit task. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error submitting task: {e}")

def check_status(master_url):
    """Check the status of the crawler."""
    try:
        response = requests.get(f"{master_url}/status")
        if response.status_code == 200:
            logging.info("Crawler status:")
            logging.info(response.json())
        else:
            logging.error(f"Failed to fetch status. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error checking status: {e}")

def fetch_results(master_url):
    """Fetch the results of the crawl."""
    try:
        response = requests.get(f"{master_url}/results")
        if response.status_code == 200:
            logging.info("Crawl results:")
            logging.info(response.json())
        else:
            logging.error(f"Failed to fetch results. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error fetching results: {e}")

def run_flask_client():
    app.run(host='0.0.0.0', port=8080, debug=True)

def main():
    parser = argparse.ArgumentParser(description="Client for the distributed web crawler.")
    parser.add_argument("--master-url", required=False, help="The URL of the master node (e.g., http://<master-ip>:<port>)")
    parser.add_argument("--web", action="store_true", help="Run the web interface.")
    subparsers = parser.add_subparsers(dest="command", required=False)

    # Submit task command
    submit_parser = subparsers.add_parser("submit", help="Submit a crawling task.")
    submit_parser.add_argument("--urls", nargs="+", required=True, help="List of URLs to crawl.")
    submit_parser.add_argument("--max-depth", type=int, default=3, help="Maximum crawl depth.")

    # Check status command
    subparsers.add_parser("status", help="Check the status of the crawler.")

    # Fetch results command
    subparsers.add_parser("results", help="Fetch the results of the crawl.")

    args = parser.parse_args()

    if args.web:
        run_flask_client()
    elif args.command == "submit":
        submit_task(args.master_url, args.urls, args.max_depth)
    elif args.command == "status":
        check_status(args.master_url)
    elif args.command == "results":
        fetch_results(args.master_url)

if __name__ == "__main__":
    main()