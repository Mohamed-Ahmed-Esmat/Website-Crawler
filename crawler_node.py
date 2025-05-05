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

# Create Redis connection once as a global variable
r = redis.Redis(host='10.10.0.2', port=6379, decode_responses=True, password='password123')

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
        # If we can't check robots.txt, assume it's allowed
        return True


def fetch_url(url):
    try:
        headers = {
            'User-Agent': 'DistributedWebCrawler/1.0',
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Ensure we're using UTF-8 encoding to handle all Unicode characters
        response.encoding = 'utf-8'
        
        return True, response.text
    except requests.RequestException as e:
        return False, str(e)


def extract_content(url, html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract all links and convert to absolute URLs
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(url, href)
            
            # Only include HTTP/HTTPS links
            if absolute_url.startswith(('http://', 'https://')):
                links.append(absolute_url)
        
        # Extract visible text
        # Remove script and style elements that contain non-visible content
        for script in soup(["script", "style"]):
            script.extract()
            
        # Get text content
        text = soup.get_text(separator=' ', strip=True)
        
        return links, text
    except Exception as e:
        logging.error(f"Error extracting content from {url}: {e}")
        return [], f"Error extracting content: {e}"


def process_url_batch(urls_batch, max_depth, comm, rank, current_depth=1):
    """
    Process a batch of URLs and crawl to the specified depth
    
    Args:
        urls_batch (list): List of URLs to crawl
        max_depth (int): Maximum depth to crawl to
        comm: MPI communicator
        rank (int): Rank of this crawler node
        current_depth (int): Current crawl depth (default: 1)
        
    Returns:
        list: New URLs discovered that need further crawling
    """
    all_new_urls = []
    indexer_rank = 2  # First indexer node's rank
    
    total_urls = len(urls_batch)
    processed_urls = 0
    
    for url in urls_batch:
        try:
            processed_urls += 1
            logging.info(f"Crawler {rank} processing URL: {url} (depth {current_depth}/{max_depth}) - Progress: {processed_urls}/{total_urls}")
            
            # Check if URL has already been crawled
            if r.sismember(REDIS_CRAWLED_URLS_SET, hash_url(url)):
                logging.info(f"URL {url} has already been crawled. Skipping.")
                continue
            
            # Check robots.txt first
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
                
            # Fetch the web page content
            success, content = fetch_url(url)
            
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
            
            # Extract links and text from the page
            extracted_urls, extracted_text = extract_content(url, content)
            
            logging.info(f"Crawler {rank} crawled {url}, extracted {len(extracted_urls)} URLs (depth {current_depth}/{max_depth})")
            
            # Implement crawl delay to be polite
            time.sleep(1)  # 1 second delay between requests
            
            # Send the extracted content to an indexer node
            page_data = {
                'url': url,
                'content': extracted_text,
                'crawler_rank': rank,
                'depth': current_depth,
                'timestamp': datetime.now().isoformat()
            }
            comm.send(page_data, dest=0, tag=TAG_PAGE_CONTENT)
            logging.info(f"Sent extracted content to indexer node {indexer_rank}")
            
            # Add extracted URLs to the collection if we haven't reached max depth
            if current_depth < max_depth:
                all_new_urls.extend(extracted_urls)
            
            # Mark URL as crawled in Redis
            r.sadd(REDIS_CRAWLED_URLS_SET, hash_url(url))
                
            # Send status update with SUCCESS
            comm.send({
                "status": STATUS_WORKING,  # Still WORKING until batch is complete
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
    
    # Batch is complete, now set status to IDLE
    comm.send({
        "status": STATUS_IDLE,
        "message": f"Completed processing {total_urls} URLs at depth {current_depth}",
        "batch_complete": True
    }, dest=0, tag=TAG_STATUS_UPDATE)
    
    # Send all discovered URLs back to master if we're not at max depth
    if current_depth < max_depth and all_new_urls:
        # Process next depth level recursively
        if current_depth + 1 < max_depth:
            # First send current urls to master
            comm.send({"urls": all_new_urls, "depth": current_depth + 1}, dest=0, tag=TAG_DISCOVERED_URLS)
            # Then process the next level
            process_url_batch(all_new_urls, max_depth, comm, rank, current_depth + 1)
        else:
            # Just send the urls to master and don't process further
            comm.send({"urls": all_new_urls, "depth": current_depth + 1}, dest=0, tag=TAG_DISCOVERED_URLS)
        
    return all_new_urls


def crawler_process():
    """
    Process for a crawler node.
    Fetches web pages, extracts URLs, and sends results back to the master.
    """
    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logging.info(f"Crawler node started with rank {rank} of {size}")
    
    # Initial status report - start as IDLE
    comm.send({
        "status": STATUS_IDLE, 
        "message": "Crawler node initialized and ready",
        "timestamp": datetime.now().isoformat()
    }, dest=0, tag=TAG_STATUS_UPDATE)
    
    last_heartbeat = time.time()
    is_processing = False  # Flag to track if we're currently processing URLs
    
    while True:
        try:
            current_time = time.time()
            
            # Simple heartbeat only when IDLE (no detailed status needed)
            if not is_processing and current_time - last_heartbeat > 5:  # Send heartbeat every 5 seconds when idle
                comm.send({
                    "status": STATUS_IDLE,
                    "timestamp": datetime.now().isoformat()
                }, dest=0, tag=TAG_HEARTBEAT)  # Using separate tag for simple heartbeat
                last_heartbeat = current_time
                logging.debug(f"Sent heartbeat to master (idle)")
                
            # Check for incoming task with non-blocking probe
            if comm.iprobe(source=0, tag=TAG_TASK_ASSIGNMENT):
                status = MPI.Status()
                crawl_task = comm.recv(source=0, tag=TAG_TASK_ASSIGNMENT, status=status)
                
                # Check if it's a shutdown signal
                if not crawl_task:
                    logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
                    break
                
                # Set processing flag to true - we're now working
                is_processing = True
                    
                # Check message format to handle different types of tasks
                if isinstance(crawl_task, dict):
                    urls_batch = crawl_task.get("urls", [])
                    max_depth = crawl_task.get("max_depth", 3)

                    # Ensure `urls_batch` contains only strings (actual URLs)
                    if urls_batch and all(isinstance(url, str) for url in urls_batch):
                        logging.info(f"Crawler {rank} received batch of {len(urls_batch)} URLs with max depth {max_depth}")

                        # Process the URLs
                        process_url_batch(urls_batch, max_depth, comm, rank)
                    else:
                        logging.warning(f"Crawler {rank} received invalid URL batch: {urls_batch}")
                        
                else:  # Backward compatibility for single URL
                    url_to_crawl = crawl_task
                    logging.info(f"Crawler {rank} received single URL (legacy format): {url_to_crawl}")
                    process_url_batch([url_to_crawl], 3, comm, rank)  # Default to max depth 3
                
                # We've finished processing this batch - back to IDLE
                is_processing = False
                comm.send({
                    "status": STATUS_IDLE,
                    "message": "Finished processing URL batch, waiting for new tasks",
                    "timestamp": datetime.now().isoformat()
                }, dest=0, tag=TAG_STATUS_UPDATE)
                
                # Reset heartbeat timer after task completion
                last_heartbeat = time.time()
            
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
            
            # Reset heartbeat timer and processing flag
            last_heartbeat = time.time()
            is_processing = False


if __name__ == '__main__':
    crawler_process()