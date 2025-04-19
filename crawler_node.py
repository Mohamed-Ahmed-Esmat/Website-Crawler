from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

def is_allowed_by_robots(url):
    """
    Check if the crawler is allowed to crawl the given URL according to robots.txt
    """
    parsed_url = urlparse(url)
    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
    rp = RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch("*", url)
    except:
        return True  # Fail-safe: if robots.txt fails, allow

def extract_links_and_text(url, html_content):
    """
    Parses HTML content and extracts links and main body text.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract all absolute links
    links = []
    for a_tag in soup.find_all('a', href=True):
        absolute_url = urljoin(url, a_tag['href'])
        links.append(absolute_url)

    # Extract visible text
    page_text = soup.get_text(separator=' ', strip=True)

    return links, page_text

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

        if not url_to_crawl:
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")

        try:
            if not is_allowed_by_robots(url_to_crawl):
                logging.warning(f"Robots.txt disallowed crawling: {url_to_crawl}")
                comm.send([], dest=0, tag=1)
                continue

            response = requests.get(url_to_crawl, timeout=10)
            response.raise_for_status()

            extracted_urls, extracted_content = extract_links_and_text(url_to_crawl, response.text)

            logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")

            # Send extracted URLs to master (tag 1)
            comm.send(extracted_urls, dest=0, tag=1)

            # Optionally send content to indexer (you can modify this logic as needed)
            # content_payload = {
            #     'url': url_to_crawl,
            #     'content': extracted_content
            # }
            # indexer_rank = 1 + (rank - 1) % (size - 2)
            # comm.send(content_payload, dest=indexer_rank, tag=2)

            # Send status update to master (tag 99)
            comm.send(f"Crawler {rank} - Crawled URL: {url_to_crawl}", dest=0, tag=99)

        except Exception as e:
            logging.error(f"Crawler {rank} error crawling {url_to_crawl}: {e}")
            comm.send([], dest=0, tag=1)
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)

if __name__ == '__main__':
    crawler_process()
