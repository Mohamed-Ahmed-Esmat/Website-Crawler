from elasticsearch import Elasticsearch
from config import ES_CONFIG, INDEX_SETTINGS, REMOTE_CLUSTERS, BM25_CONFIG
from ssl_config import setup_cross_cluster_ssl
import logging
from typing import Dict, List, Optional
import math
from datetime import datetime

class ElasticsearchManager:
    def __init__(self):
        # Setup SSL configuration for cross-cluster communication
        ssl_config = setup_cross_cluster_ssl()
        self.es_config = {
            **ES_CONFIG,
            **ssl_config
        }
        
        self.es = Elasticsearch(**self.es_config)
        self.index_name = INDEX_SETTINGS['name']
        self._setup_remote_clusters()
        self._setup_index()

    def _setup_remote_clusters(self):
        """Configure remote clusters for distributed search with SSL"""
        for cluster_name, config in REMOTE_CLUSTERS.items():
            try:
                cluster_config = {
                    "seeds": config['seeds'],
                    "skip_unavailable": config.get('skip_unavailable', True),
                    "transport.ssl.enabled": True,
                    "transport.ssl.verification_mode": "certificate",
                    "transport.ssl.certificate_authorities": [self.es_config["xpack.security.transport.ssl.truststore.path"]],
                    "transport.ssl.certificate": self.es_config["xpack.security.transport.ssl.keystore.path"],
                    "transport.ssl.key": self.es_config["xpack.security.transport.ssl.keystore.path"]
                }
                
                self.es.cluster.put_settings(body={
                    "persistent": {
                        f"cluster.remote.{cluster_name}": cluster_config
                    }
                })
                logging.info(f"Successfully configured remote cluster with SSL: {cluster_name}")
            except Exception as e:
                logging.error(f"Error configuring remote cluster {cluster_name}: {e}")

    def _setup_index(self):
        """Create and configure the Elasticsearch index"""
        try:
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(
                    index=self.index_name,
                    body={
                        "settings": INDEX_SETTINGS['settings'],
                        "mappings": INDEX_SETTINGS['mappings']
                    }
                )
                logging.info(f"Created index: {self.index_name}")
        except Exception as e:
            logging.error(f"Error setting up index: {e}")

    def calculate_tf_idf(self, doc_id: str) -> float:
        """Calculate TF-IDF score for document boosting"""
        try:
            doc = self.es.termvectors(
                index=self.index_name,
                id=doc_id,
                fields=["content"],
                term_statistics=True
            )
            
            if not doc.get('term_vectors', {}).get('content'):
                return 1.0

            terms = doc['term_vectors']['content']['terms']
            doc_length = sum(term_info['term_freq'] for term_info in terms.values())
            
            total_docs = self.es.count(index=self.index_name)['count']
            
            tfidf_sum = 0
            for term_info in terms.values():
                tf = term_info['term_freq'] / doc_length
                idf = math.log((total_docs + 1) / (term_info['doc_freq'] + 1)) + 1
                tfidf_sum += tf * idf
                
            return tfidf_sum / len(terms)
            
        except Exception as e:
            logging.error(f"Error calculating TF-IDF: {e}")
            return 1.0

    async def index_document(self, doc: Dict) -> bool:
        """Index a document with advanced scoring features"""
        try:
            # Add location if available in URL or content
            if 'location' not in doc and doc.get('content'):
                # Here you could implement location extraction from content
                # For example, using NLP to identify location entities
                pass

            # Calculate TF-IDF boost
            response = await self.es.index(
                index=self.index_name,
                document=doc,
                refresh=True
            )
            
            # Update document with TF-IDF score
            tf_idf_score = self.calculate_tf_idf(response['_id'])
            await self.es.update(
                index=self.index_name,
                id=response['_id'],
                body={
                    "doc": {
                        "tf_idf_boost": tf_idf_score
                    }
                }
            )
            
            return True
        except Exception as e:
            logging.error(f"Error indexing document: {e}")
            return False

    async def search(self, query, search_type="keyword", fuzziness=None, size=10):
        try:
            if search_type == "keyword":
                query_body = {
                    "query": {
                        "match": {
                            "content": {
                                "query": query,
                                "fuzziness": fuzziness if fuzziness else 0
                            }
                        }
                    },
                    "highlight": {
                        "fields": {
                            "content": {
                                "type": "unified",
                                "number_of_fragments": 3,
                                "fragment_size": 150,
                                "pre_tags": ["**"],
                                "post_tags": ["**"]
                            }
                        }
                    }
                }
            elif search_type == "phrase":
                query_body = {
                    "query": {
                        "match_phrase": {
                            "content": query
                        }
                    },
                    "highlight": {
                        "fields": {
                            "content": {
                                "type": "unified",
                                "number_of_fragments": 3,
                                "fragment_size": 150,
                                "pre_tags": ["**"],
                                "post_tags": ["**"]
                            }
                        }
                    }
                }
            elif search_type == "regex":
                query_body = {
                    "query": {
                        "regexp": {
                            "content": query
                        }
                    },
                    "highlight": {
                        "fields": {
                            "content": {
                                "type": "unified",
                                "number_of_fragments": 3,
                                "fragment_size": 150,
                                "pre_tags": ["**"],
                                "post_tags": ["**"]
                            }
                        }
                    }
                }
            
            response = await self.es.search(index=self.index_name, body=query_body, size=size)
            
            results = []
            for hit in response['hits']['hits']:
                result = {
                    'url': hit['_source']['url'],
                    'score': hit['_score'],
                    'word_count': hit['_source'].get('word_count', 0),
                    'domain': hit['_source'].get('domain', ''),
                    'timestamp': hit['_source'].get('timestamp', ''),
                    'highlights': hit.get('highlight', {}).get('content', [])
                }
                results.append(result)
                
            return {
                'total': response['hits']['total']['value'],
                'results': results
            }
            
        except Exception as e:
            logging.error(f"Search error: {e}")
            return {'total': 0, 'results': []}

    def search_across_clusters(self, query: str, **kwargs) -> Dict:
        """Perform search across all configured clusters"""
        try:
            # Build the list of indices to search across
            indices = [self.index_name]  # Local index
            indices.extend([f"{cluster}:{self.index_name}" for cluster in REMOTE_CLUSTERS.keys()])
            
            # Build the search query
            search_body = self._build_search_query(query, **kwargs)
            
            # Add cluster preference for load balancing
            search_body["preference"] = "_local"  # Prefer local cluster when possible
            
            # Execute search across all clusters
            response = self.es.search(
                index=",".join(indices),
                body=search_body,
                ignore_unavailable=True,  # Continue if some clusters are unavailable
                allow_partial_search_results=True  # Return partial results if some shards fail
            )
            
            # Process and merge results
            merged_results = self._merge_cross_cluster_results(response)
            return merged_results
            
        except Exception as e:
            logging.error(f"Error performing cross-cluster search: {e}")
            return {"error": str(e)}

    def _merge_cross_cluster_results(self, response: Dict) -> Dict:
        """Merge and normalize results from multiple clusters"""
        hits = response['hits']['hits']
        results = []
        
        # Track clusters that contributed results
        clusters_found = set()
        
        for hit in hits:
            # Extract cluster name from _index field (format: "cluster_name:index_name")
            index_parts = hit['_index'].split(':')
            cluster = index_parts[0] if len(index_parts) > 1 else 'local'
            clusters_found.add(cluster)
            
            result = {
                'id': hit['_id'],
                'url': hit['_source']['url'],
                'score': hit['_score'],
                'domain': hit['_source'].get('domain'),
                'timestamp': hit['_source'].get('timestamp'),
                'word_count': hit['_source'].get('word_count'),
                'location': hit['_source'].get('location'),
                'cluster': cluster  # Add cluster information
            }
            
            if 'highlight' in hit:
                result['highlights'] = hit['highlight'].get('content', [])
                
            results.append(result)
            
        # Get aggregations per cluster if available
        cluster_stats = {}
        if 'aggregations' in response:
            for cluster in clusters_found:
                cluster_docs = [r for r in results if r['cluster'] == cluster]
                cluster_stats[cluster] = {
                    'doc_count': len(cluster_docs),
                    'avg_score': sum(r['score'] for r in cluster_docs) / len(cluster_docs) if cluster_docs else 0
                }
            
        return {
            'total': response['hits']['total']['value'],
            'results': results,
            'clusters': {
                'participating': list(clusters_found),
                'stats': cluster_stats
            },
            'facets': {
                'domains': response['aggregations']['domains']['buckets'],
                'word_count_stats': response['aggregations']['word_count_stats'],
                'timeline': response['aggregations']['timeline']['buckets']
            } if 'aggregations' in response else {}
        }

    def _build_search_query(self, query: str, **kwargs) -> Dict:
        """Build an advanced search query"""
        search_type = kwargs.get('search_type', 'keyword')
        
        # Base query structure
        search_body = {
            "size": kwargs.get('size', 10),
            "from": kwargs.get('from', 0),
            "highlight": {
                "fields": {
                    "content": {
                        "number_of_fragments": 3,
                        "fragment_size": 150,
                        "pre_tags": ["<mark>"],
                        "post_tags": ["</mark>"]
                    }
                }
            },
            "_source": ["url", "content", "timestamp", "domain", "word_count", "location"]
        }

        # Build query based on search type
        if search_type == 'keyword':
            search_body["query"] = {
                "function_score": {
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": ["content^3", "url^2", "domain"],
                            "fuzziness": "AUTO"
                        }
                    },
                    "functions": [
                        {
                            "field_value_factor": {
                                "field": "tf_idf_boost",
                                "factor": BM25_CONFIG['k1'],
                                "missing": 1
                            }
                        }
                    ],
                    "boost_mode": "multiply"
                }
            }
        elif search_type == 'phrase':
            search_body["query"] = {
                "match_phrase": {
                    "content": {
                        "query": query,
                        "slop": kwargs.get('slop', 0)
                    }
                }
            }
        elif search_type == 'regex':
            search_body["query"] = {
                "regexp": {
                    "content": {
                        "value": query,
                        "flags": "ALL",
                        "case_insensitive": True
                    }
                }
            }

        # Add filters
        filters = []
        if kwargs.get('from_date') or kwargs.get('to_date'):
            date_filter = {"range": {"timestamp": {}}}
            if kwargs.get('from_date'):
                date_filter["range"]["timestamp"]["gte"] = kwargs['from_date']
            if kwargs.get('to_date'):
                date_filter["range"]["timestamp"]["lte"] = kwargs['to_date']
            filters.append(date_filter)

        if kwargs.get('domain'):
            filters.append({"term": {"domain": kwargs['domain']}})

        if kwargs.get('location'):
            filters.append({
                "geo_distance": {
                    "distance": f"{kwargs.get('distance', '10km')}",
                    "location": kwargs['location']
                }
            })

        if filters:
            if "query" in search_body:
                search_body["query"] = {
                    "bool": {
                        "must": search_body["query"],
                        "filter": filters
                    }
                }
            else:
                search_body["query"] = {"bool": {"filter": filters}}

        # Add aggregations for faceted search
        search_body["aggs"] = {
            "domains": {
                "terms": {
                    "field": "domain",
                    "size": 10
                }
            },
            "word_count_stats": {
                "stats": {
                    "field": "word_count"
                }
            },
            "timeline": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": kwargs.get('interval', 'day')
                }
            }
        }

        return search_body

    def _process_search_response(self, response: Dict) -> Dict:
        """Process and format search response"""
        hits = response['hits']['hits']
        results = []
        
        for hit in hits:
            result = {
                'id': hit['_id'],
                'url': hit['_source']['url'],
                'score': hit['_score'],
                'domain': hit['_source'].get('domain'),
                'timestamp': hit['_source'].get('timestamp'),
                'word_count': hit['_source'].get('word_count'),
                'location': hit['_source'].get('location')
            }
            
            if 'highlight' in hit:
                result['highlights'] = hit['highlight'].get('content', [])
                
            results.append(result)
            
        return {
            'total': response['hits']['total']['value'],
            'results': results,
            'facets': {
                'domains': response['aggregations']['domains']['buckets'],
                'word_count_stats': response['aggregations']['word_count_stats'],
                'timeline': response['aggregations']['timeline']['buckets']
            }
        }

    def suggest(self, prefix: str, size: int = 5) -> List[str]:
        """Get autocomplete suggestions"""
        try:
            response = self.es.search(
                index=self.index_name,
                body={
                    "suggest": {
                        "text": prefix,
                        "completion": {
                            "field": "content.suggest",
                            "size": size,
                            "skip_duplicates": True,
                            "fuzzy": {
                                "fuzziness": "AUTO"
                            }
                        }
                    }
                }
            )
            
            suggestions = []
            for suggestion in response['suggest']['completion'][0]['options']:
                suggestions.append(suggestion['text'])
                
            return suggestions
            
        except Exception as e:
            logging.error(f"Error getting suggestions: {e}")
            return []

    async def get_stats(self):
        """Get comprehensive statistics about the index"""
        try:
            # Get basic index stats
            index_stats = await self.es.indices.stats(index=self.index_name)
            
            # Get aggregations for domains and word counts
            aggs_response = await self.es.search(
                index=self.index_name,
                body={
                    "size": 0,
                    "aggs": {
                        "domains": {
                            "terms": {
                                "field": "domain",
                                "size": 10
                            }
                        },
                        "word_count": {
                            "stats": {
                                "field": "word_count"
                            }
                        },
                        "locations": {
                            "filter": {
                                "exists": {
                                    "field": "location"
                                }
                            }
                        },
                        "timeline": {
                            "date_histogram": {
                                "field": "timestamp",
                                "calendar_interval": "day"
                            }
                        }
                    }
                }
            )
            
            # Calculate index size in MB
            size_bytes = index_stats['indices'][self.index_name]['total']['store']['size_in_bytes']
            size_mb = size_bytes / (1024 * 1024)
            
            return {
                'total_docs': index_stats['indices'][self.index_name]['total']['docs']['count'],
                'size_mb': size_mb,
                'domains': aggs_response['aggregations']['domains']['buckets'],
                'word_count': aggs_response['aggregations']['word_count'],
                'locations': aggs_response['aggregations']['locations']['doc_count'],
                'timeline': aggs_response['aggregations']['timeline']['buckets']
            }
            
        except Exception as e:
            logging.error(f"Error getting index stats: {e}")
            return {
                'total_docs': 0,
                'size_mb': 0,
                'domains': [],
                'word_count': {'avg': 0, 'min': 0, 'max': 0},
                'locations': 0,
                'timeline': []
            }

# Create a singleton instance
es_manager = ElasticsearchManager()