# from elasticsearch import Elasticsearch, ConnectionError, ConnectionTimeout
# from config import ES_CONFIG, INDEX_SETTINGS
# import logging
# from typing import Dict, List
# import backoff

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class ElasticsearchManager:
#     def __init__(self):
#         """Initialize Elasticsearch connection"""
#         self.es = Elasticsearch(**ES_CONFIG)
#         self.index_name = INDEX_SETTINGS['name']
#         self._setup_index()

#     @backoff.on_exception(
#         backoff.expo,
#         (ConnectionError, ConnectionTimeout),
#         max_tries=5
#     )
#     def _setup_index(self):
#         """Create and configure the index if it doesn't exist"""
#         try:
#             if not self.es.indices.exists(index=self.index_name):
#                 self.es.indices.create(
#                     index=self.index_name,
#                     settings=INDEX_SETTINGS['settings'],
#                     mappings=INDEX_SETTINGS['mappings']
#                 )
#                 logger.info(f"Created index: {self.index_name}")
#         except Exception as e:
#             logger.error(f"Error setting up index: {e}")
#             raise

#     def index_document(self, doc: Dict) -> bool:
#         """Index a document with basic error handling"""
#         try:
#             self.es.index(
#                 index=self.index_name,
#                 document=doc,
#                 refresh=True
#             )
#             return True
#         except Exception as e:
#             logger.error(f"Error indexing document: {e}")
#             return False

#     def search(self, query: str, size: int = 10) -> Dict:
#         """Search with basic error handling"""
#         try:
#             response = self.es.search(
#                 index=self.index_name,
#                 body={
#                     "query": {
#                         "match": {
#                             "content": query
#                         }
#                     }
#                 },
#                 size=size
#             )
#             return {
#                 'total': response['hits']['total']['value'],
#                 'results': [hit['_source'] for hit in response['hits']['hits']]
#             }
#         except Exception as e:
#             logger.error(f"Search error: {e}")
#             return {'total': 0, 'results': []}

# # Create singleton instance
# es_manager = ElasticsearchManager()