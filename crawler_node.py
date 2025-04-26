from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')


def check_robots_txt(url):
    """
    Check if a URL is allowed to be crawled according to robots.txt
    
    Args:
        url (str): The URL to check
        
    Returns:
        bool: True if allowed, False if disallowed
    """
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
    """
    Fetch content from a URL
    
    Args:
        url (str): The URL to fetch
        
    Returns:
        tuple: (success, content) - success is a boolean, content is the page content or error message
    """
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
    """
    Extract links and text content from HTML
    
    Args:
        url (str): The base URL for resolving relative links
        html_content (str): The HTML content to parse
        
    Returns:
        tuple: (links, text) - links is a list of extracted URLs, text is the extracted text
    """
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


def crawler_process():
    """
    Process for a crawler node.
    Fetches web pages, extracts URLs, and sends results back to the master.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Crawler node started with rank {rank} of {size}")

    while True:
        status = MPI.Status()
        url_to_crawl = comm.recv(source=0, tag=0, status=status)  # Receive URL from master (tag 0)

        if not url_to_crawl:  # Could be a shutdown signal
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")

        try:
            # Check robots.txt first
            if not check_robots_txt(url_to_crawl):
                error_msg = f"Crawling disallowed by robots.txt for {url_to_crawl}"
                logging.warning(error_msg)
                comm.send(error_msg, dest=0, tag=999)  # Report error to master
                continue
                
            # Fetch the web page content
            success, content = fetch_url(url_to_crawl)
            
            if not success:
                error_msg = f"Failed to fetch {url_to_crawl}: {content}"
                logging.error(error_msg)
                comm.send(error_msg, dest=0, tag=999)  # Report error to master
                continue
            
            # Extract links and text from the page
            extracted_urls, extracted_text = extract_content(url_to_crawl, content)
            
            logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")


            # Implement crawl delay to be polite
            time.sleep(1)  # 1 second delay between requests

            # --- Send extracted URLs back to master ---
            comm.send(extracted_urls, dest=0, tag=1)  # Tag 1 for sending extracted URLs

            # --- Send extracted content to indexer node ---
            # Assuming indexer node ranks come after crawler node ranks
            indexer_rank = 2  # First indexer node's rank
            
            # Send the extracted content to an indexer node
            page_data = {
                'url': url_to_crawl,
                'content': extracted_text,
                'crawler_rank': rank
            }
            comm.send(page_data, dest=indexer_rank, tag=2)  # Tag 2 for sending content to indexer

            # Send status update to master
            comm.send(f"Crawler {rank} - Crawled URL: {url_to_crawl} - Found {len(extracted_urls)} links", dest=0, tag=99)

        except Exception as e:
            error_msg = f"Crawler {rank} error crawling {url_to_crawl}: {e}"
            logging.error(error_msg)
            comm.send(error_msg, dest=0, tag=999)  # Report error to master


if __name__ == '__main__':
    crawler_process()