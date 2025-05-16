from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import socket
import traceback
from datetime import datetime
import redis
import hashlib
import json
from google.cloud import pubsub_v1

# Configure logging
hostname = socket.gethostname()
try:
    ip_address = socket.gethostbyname(hostname)
except:
    ip_address = "unknown-ip"

logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - {ip_address} - Crawler - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"crawler_{ip_address}.log"),
        logging.StreamHandler()
    ]
)

# Constants for crawler status
STATUS_IDLE = "IDLE"
STATUS_WORKING = "WORKING"
STATUS_ERROR = "ERROR"

# Message tags
TAG_TASK_ASSIGNMENT = 0
TAG_DISCOVERED_URLS = 1
TAG_PAGE_CONTENT = 2
TAG_STATUS_UPDATE = 99
TAG_HEARTBEAT = 98
TAG_ERROR_REPORT = 999

# Redis key for set of crawled URLs
REDIS_CRAWLED_URLS_SET = "crawled_urls"
REDIS_CRAWL_RESULTS_HASH = "crawl_results"

# Google Cloud project and subscription details
PROJECT_ID = "spheric-arcadia-457314-c8"  # Replace with your actual project ID
SUBSCRIPTION_NAME = "crawl-tasks-sub"

# Add these constants at the top with your other constants
CONTENT_TOPIC_NAME = "crawler-indexer"

# Initialize the publisher in the crawler_process function (or at module level)
publisher = pubsub_v1.PublisherClient()
content_topic_path = publisher.topic_path(PROJECT_ID, CONTENT_TOPIC_NAME)

# Create Redis connection once as a global variable
r = redis.Redis(host='10.10.0.2', port=6379, decode_responses=True, password='password123')

# Initialize the MPI communicator
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def hash_url(url):
    return hashlib.sha256(url.encode()).hexdigest()

def check_robots_txt(url):
    try:
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        
        return rp.can_fetch("*", url)
    except Exception as e:
        logging.warning(f"Error checking robots.txt for {url}: {e}")
        return True

def fetch_url(url, session):
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'
        return True, response.text
    except requests.RequestException as e:
        return False, str(e)

def extract_content(url, html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(url, href)
            
            if absolute_url.startswith(('http://', 'https://')):
                links.append(absolute_url)
        
        for script in soup(["script", "style"]):
            script.extract()
            
        text = soup.get_text(separator=' ', strip=True)
        
        return links, text
    except Exception as e:
        logging.error(f"Error extracting content from {url}: {e}")
        return [], f"Error extracting content: {e}"

def process_url_batch(urls_batch, max_depth, comm, rank, session, current_depth=1):
    global r
    all_new_urls = []
    parent_map = {}

    total_urls = len(urls_batch)
    processed_urls = 0
    
    url_hierarchy = {}
    for url in urls_batch:
        url_hierarchy[url] = {
            "parent": parent_map.get(url, None),
            "children": [],
            "depth": current_depth
        }

    for url in urls_batch:
        try:
            processed_urls += 1
            logging.info(f"Crawler {rank} processing URL: {url} (depth {current_depth}/{max_depth}) - Progress: {processed_urls}/{total_urls}")
            
            url_hash = hash_url(url)

            if r.sismember(REDIS_CRAWLED_URLS_SET, url_hash):
                logging.info(f"URL {url} has already been crawled. Skipping.")
                cached_results_json = r.hget(REDIS_CRAWL_RESULTS_HASH, url_hash)
                cached_results = json.loads(cached_results_json)
                extracted_urls = cached_results.get('extracted_urls', [])
                extracted_text = cached_results.get('extracted_text', '')
                logging.info(f"Retrieved cached results for {url}, found {len(extracted_urls)} URLs")
                url_hierarchy[url]["crawled"] = True
                url_hierarchy[url]["children"] = extracted_urls
                
                for child_url in extracted_urls:
                    parent_map[child_url] = url
            else:
            
                if not check_robots_txt(url):
                    error_msg = f"Crawling disallowed by robots.txt for {url}"
                    logging.warning(error_msg)
                    comm.send({
                        "status": STATUS_ERROR, 
                        "url": url, 
                        "error": error_msg,
                        "progress": {
                            "current": processed_urls,
                            "total": total_urls
                        }
                    }, dest=0, tag=TAG_ERROR_REPORT)
                    continue
                    
                success, content = fetch_url(url, session)
                
                if not success:
                    error_msg = f"Failed to fetch {url}: {content}"
                    logging.error(error_msg)
                    comm.send({
                        "status": STATUS_ERROR, 
                        "url": url, 
                        "error": error_msg,
                        "progress": {
                            "current": processed_urls,
                            "total": total_urls
                        }
                    }, dest=0, tag=TAG_ERROR_REPORT)
                    continue
                
                extracted_urls, extracted_text = extract_content(url, content)

                url_hierarchy[url]["crawled"] = True
                url_hierarchy[url]["children"] = extracted_urls
                
                for child_url in extracted_urls:
                    parent_map[child_url] = url

                crawl_results = {
                'extracted_urls': extracted_urls,
                'extracted_text': extracted_text,
                'crawl_timestamp': datetime.now().isoformat()
                }

                r.sadd(REDIS_CRAWLED_URLS_SET, hash_url(url))
                r.hset(REDIS_CRAWL_RESULTS_HASH, url_hash, json.dumps(crawl_results))
                
                logging.info(f"Crawler {rank} crawled {url}, extracted {len(extracted_urls)} URLs (depth {current_depth}/{max_depth})")
                
                time.sleep(1)
            
            page_data = {
                'url': url,
                'content': extracted_text,
                'crawler_rank': rank,
                'depth': current_depth,
                'timestamp': datetime.now().isoformat()
            }
            page_data_json = json.dumps(page_data).encode('utf-8')
            future = publisher.publish(content_topic_path, page_data_json)
            message_id = future.result()
            logging.info(f"Published extracted content for {url} to Pub/Sub topic '{CONTENT_TOPIC_NAME}', message ID: {message_id}")
            
            if current_depth <= max_depth:
                all_new_urls.extend(extracted_urls)
                
            comm.send({
                "status": STATUS_WORKING,
                "url": url, 
                "urls_found": len(extracted_urls),
                "depth": current_depth,
                "completed_url": True,
                "progress": {
                    "current": processed_urls,
                    "total": total_urls,
                    "percentage": round((processed_urls / total_urls) * 100, 1)
                }
            }, dest=0, tag=TAG_STATUS_UPDATE)
            
        except Exception as e:
            error_msg = f"Crawler {rank} error crawling {url}: {e}"
            stack_trace = traceback.format_exc()
            logging.error(f"{error_msg}\n{stack_trace}")
            comm.send({
                "status": STATUS_ERROR, 
                "url": url, 
                "error": error_msg, 
                "stack_trace": stack_trace,
                "progress": {
                    "current": processed_urls,
                    "total": total_urls
                }
            }, dest=0, tag=TAG_ERROR_REPORT)
    
    comm.send({
        "status": STATUS_IDLE,
        "message": f"Completed processing {total_urls} URLs at depth {current_depth}",
        "batch_complete": True
    }, dest=0, tag=TAG_STATUS_UPDATE)
    
    if current_depth < max_depth and all_new_urls:
        if current_depth + 1 <= max_depth:
            comm.send({"urls": all_new_urls, "depth": current_depth + 1}, dest=0, tag=TAG_STATUS_UPDATE)
            process_url_batch(all_new_urls, max_depth, comm, rank, session, current_depth + 1)
        else:
            print(url_hierarchy)
            comm.send({"urls": all_new_urls, "depth": current_depth + 1}, dest=0, tag=TAG_DISCOVERED_URLS)
            logging.info(f"Crawler {rank} finished and send to master")
    else:
        print(url_hierarchy)
        comm.send({"urls": all_new_urls, "depth": current_depth + 1}, dest=0, tag=TAG_DISCOVERED_URLS)
        logging.info(f"Crawler {rank} finished and send to master")
        
    return all_new_urls

def pubsub_callback(message):
    global comm, rank, r

    message.ack()

    session = requests.Session()
    session.headers.update({'User-Agent': 'DistributedWebCrawler/1.0'})
    
    logging.info("Received a task message from Pub/Sub")

    try:
        crawl_task = json.loads(message.data.decode("utf-8"))
        logging.info(f"Processing crawl task: {crawl_task}")
        
        comm.send({
            "status": STATUS_WORKING,
            "message": "Received new task via Pub/Sub",
            "timestamp": datetime.now().isoformat()
        }, dest=0, tag=TAG_STATUS_UPDATE)
        
        if isinstance(crawl_task, dict):
            urls_batch = crawl_task.get("urls", [])
            max_depth = crawl_task.get("max_depth", 3)

            if urls_batch and all(isinstance(url, str) for url in urls_batch):
                logging.info(f"Crawler {rank} received batch of {len(urls_batch)} URLs with max depth {max_depth}")
                process_url_batch(urls_batch, max_depth, comm, rank, session)
            else:
                logging.warning(f"Crawler {rank} received invalid URL batch: {urls_batch}")
                
        else:
            url_to_crawl = crawl_task
            logging.info(f"Crawler {rank} received single URL (legacy format): {url_to_crawl}")
            process_url_batch([url_to_crawl], 3, comm, rank, session)
        
        comm.send({
            "status": STATUS_IDLE,
            "message": "Finished processing URL batch from Pub/Sub, waiting for new tasks",
            "timestamp": datetime.now().isoformat()
        }, dest=0, tag=TAG_STATUS_UPDATE)
        
    except Exception as e:
        error_msg = f"Crawler {rank} error processing Pub/Sub message: {e}"
        stack_trace = traceback.format_exc()
        logging.error(f"{error_msg}\n{stack_trace}")
        
        comm.send({
            "status": STATUS_ERROR, 
            "error": error_msg, 
            "stack_trace": stack_trace,
            "timestamp": datetime.now().isoformat()
        }, dest=0, tag=TAG_ERROR_REPORT)
        
        message.nack()

def crawler_process():
    global comm, rank, r

    r.delete(REDIS_CRAWLED_URLS_SET)
    r.delete(REDIS_CRAWL_RESULTS_HASH)

    
    comm.send({
        "status": STATUS_IDLE,
        "timestamp": datetime.now().isoformat(), 
        "rank": rank,
        "node_type": "crawler",
        "ip_address": ip_address
    }, dest=0, tag=TAG_HEARTBEAT)
    logging.debug(f"Sent heartbeat to master")
    
    time.sleep(0.1)
    
    logging.info(f"Crawler node started with rank {rank} of {size}")
    
    comm.send({
        "status": STATUS_IDLE, 
        "message": "Crawler node initialized and ready",
        "timestamp": datetime.now().isoformat()
    }, dest=0, tag=TAG_STATUS_UPDATE)
    
    last_heartbeat = time.time()
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    
    logging.info(f"Subscribing to Pub/Sub subscription: {SUBSCRIPTION_NAME}")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=pubsub_callback, flow_control=flow_control)
    logging.info(f"Listening for messages on {subscription_path}...")
    
    try:
        while True:
            current_time = time.time()
            
            if current_time - last_heartbeat > 5:
                comm.send({
                    "status": STATUS_IDLE,
                    "timestamp": datetime.now().isoformat(), 
                    "rank": rank,
                    "node_type": "crawler",
                    "ip_address": ip_address
                }, dest=0, tag=TAG_HEARTBEAT)
                last_heartbeat = current_time
                logging.info(f"Sent Heartbeat to Master: crawler {rank}")
            
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logging.info("Crawler stopped")
    except Exception as e:
        error_msg = f"Crawler {rank} encountered unexpected error: {e}"
        stack_trace = traceback.format_exc()
        logging.error(f"{error_msg}\n{stack_trace}")
        
        try:
            comm.send({
                "status": STATUS_ERROR, 
                "error": error_msg, 
                "stack_trace": stack_trace,
                "timestamp": datetime.now().isoformat()
            }, dest=0, tag=TAG_ERROR_REPORT)
        except:
            logging.critical("Failed to send error report to master")
        
        streaming_pull_future.cancel()

if __name__ == '__main__':
    crawler_process()