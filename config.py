import os
from datetime import timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Elasticsearch Configuration
ES_CONFIG = {
    'hosts': [os.getenv('ES_HOST', 'http://localhost:9200')],
    'basic_auth': (
        os.getenv('ES_USER', 'elastic'),
        os.getenv('ES_PASSWORD', 'changeme')
    ) if os.getenv('ES_USER') else None,
    'verify_certs': os.getenv('ES_VERIFY_CERTS', 'true').lower() == 'true',
    'ca_certs': os.getenv('ES_CA_CERTS', None),
    'client_cert': os.getenv('ES_CLIENT_CERT', None),
    'client_key': os.getenv('ES_CLIENT_KEY', None),
    'timeout': int(os.getenv('ES_TIMEOUT', '30')),
    'max_retries': int(os.getenv('ES_MAX_RETRIES', '3')),
}

# Cross-Cluster Search Configuration
REMOTE_CLUSTERS = {
    cluster.replace('ES_REMOTE_CLUSTER_', ''): {
        'seeds': value.split(','),
        'skip_unavailable': True
    }
    for cluster, value in os.environ.items()
    if cluster.startswith('ES_REMOTE_CLUSTER_')
}

# SSL Configuration
SSL_CONFIG = {
    'cert_dir': os.getenv('SSL_CERT_DIR', 'certs'),
    'key_password': os.getenv('SSL_KEY_PASSWORD', 'changeme'),
}

# Index Settings
INDEX_SETTINGS = {
    'name': 'webcrawler',
    'settings': {
        'number_of_shards': int(os.getenv('ES_SHARDS', '1')),
        'number_of_replicas': int(os.getenv('ES_REPLICAS', '1')),
        'refresh_interval': '1s',
        'analysis': {
            'analyzer': {
                'custom_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'standard',
                    'filter': [
                        'lowercase',
                        'stop',
                        'snowball',
                        'synonym'
                    ]
                }
            },
            'filter': {
                'synonym': {
                    'type': 'synonym',
                    'synonyms': [
                        'ai, artificial intelligence',
                        'ml, machine learning',
                        'nlp, natural language processing'
                    ]
                }
            }
        }
    }
}

# Security Configuration
SECURITY_CONFIG = {
    'jwt_secret': os.getenv('JWT_SECRET', 'your-secret-key-change-in-production'),
    'jwt_algorithm': 'HS256',
    'jwt_expiry': timedelta(hours=24),
    'allowed_origins': os.getenv('ALLOWED_ORIGINS', '*').split(','),
    'rate_limit': {
        'default': os.getenv('RATE_LIMIT_DEFAULT', '100/hour'),
        'search': os.getenv('RATE_LIMIT_SEARCH', '1000/hour'),
        'suggest': os.getenv('RATE_LIMIT_SUGGEST', '2000/hour')
    }
}

# BM25 Custom Scoring Configuration
BM25_CONFIG = {
    'k1': float(os.getenv('BM25_K1', '1.2')),
    'b': float(os.getenv('BM25_B', '0.75')),
    'field_weights': {
        'content': 1.0,
        'url': 0.5,
        'domain': 0.3
    }
}