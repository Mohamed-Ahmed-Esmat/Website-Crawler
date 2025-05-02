from dotenv import load_dotenv
from datetime import timedelta
import os

# Load environment variables
load_dotenv()

# Basic Elasticsearch configuration
ES_CONFIG = {
    'hosts': ['http://localhost:9200'],
    'basic_auth': ('elastic', 'changeme'),  # Default username and initial password
    'verify_certs': False
}

# Basic index settings
INDEX_SETTINGS = {
    'name': 'webcrawler',
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        'properties': {
            'url': {'type': 'keyword'},
            'content': {'type': 'text'},
            'timestamp': {'type': 'date'},
            'crawler_rank': {'type': 'integer'},
            'domain': {'type': 'keyword'},
            'word_count': {'type': 'integer'},
            'indexed_by': {'type': 'keyword'}
        }
    }
}

# Empty remote clusters for single-node setup
REMOTE_CLUSTERS = {}

# Basic BM25 settings
BM25_CONFIG = {
    'k1': 1.2,
    'b': 0.75
}

# Security Configuration
SECURITY_CONFIG = {
    'secret_key': 'your-secret-key',  # Used for JWT token signing
    'token_expiration': timedelta(hours=24),
    'cors_origins': ['*'],  # Allow all origins in development
    'require_auth': False  # Disable auth requirement for development
}