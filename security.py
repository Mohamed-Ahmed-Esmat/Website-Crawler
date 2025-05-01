import functools
from typing import Dict, Optional
import jwt
from datetime import datetime, timedelta
from config import SECURITY_CONFIG
import logging
from flask import request, jsonify
import time
from collections import defaultdict

class RateLimiter:
    def __init__(self):
        self._request_counts = defaultdict(list)
        self._cleanup_interval = 3600  # Cleanup old records every hour
        self._last_cleanup = time.time()

    def _cleanup(self):
        """Remove old request timestamps"""
        current_time = time.time()
        if current_time - self._last_cleanup > self._cleanup_interval:
            for ip in list(self._request_counts.keys()):
                self._request_counts[ip] = [ts for ts in self._request_counts[ip] 
                                          if current_time - ts < 3600]  # Keep last hour
                if not self._request_counts[ip]:
                    del self._request_counts[ip]
            self._last_cleanup = current_time

    def is_rate_limited(self, ip: str, endpoint: str) -> bool:
        """Check if request should be rate limited"""
        self._cleanup()
        current_time = time.time()
        
        # Get rate limit for endpoint
        if endpoint in SECURITY_CONFIG['rate_limit']:
            limit = SECURITY_CONFIG['rate_limit'][endpoint]
        else:
            limit = SECURITY_CONFIG['rate_limit']['default']
            
        # Parse rate limit (e.g., "100/hour")
        max_requests, period = limit.split('/')
        max_requests = int(max_requests)
        
        # Convert period to seconds
        if period == 'hour':
            period_seconds = 3600
        elif period == 'minute':
            period_seconds = 60
        else:
            period_seconds = 3600  # Default to hour
            
        # Count requests in time window
        key = f"{ip}:{endpoint}"
        self._request_counts[key] = [ts for ts in self._request_counts[key] 
                                   if current_time - ts < period_seconds]
        
        if len(self._request_counts[key]) >= max_requests:
            return True
            
        self._request_counts[key].append(current_time)
        return False

class SecurityManager:
    def __init__(self):
        self.rate_limiter = RateLimiter()
        self._token_blacklist = set()

    def generate_token(self, user_data: Dict) -> str:
        """Generate a JWT token"""
        try:
            expiry = datetime.utcnow() + SECURITY_CONFIG['jwt_expiry']
            payload = {
                **user_data,
                'exp': expiry
            }
            return jwt.encode(
                payload,
                SECURITY_CONFIG['jwt_secret'],
                algorithm=SECURITY_CONFIG['jwt_algorithm']
            )
        except Exception as e:
            logging.error(f"Error generating token: {e}")
            raise

    def verify_token(self, token: str) -> Optional[Dict]:
        """Verify and decode a JWT token"""
        try:
            if token in self._token_blacklist:
                return None
                
            payload = jwt.decode(
                token,
                SECURITY_CONFIG['jwt_secret'],
                algorithms=[SECURITY_CONFIG['jwt_algorithm']]
            )
            return payload
        except jwt.ExpiredSignatureError:
            logging.warning("Token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logging.warning(f"Invalid token: {e}")
            return None
        except Exception as e:
            logging.error(f"Error verifying token: {e}")
            return None

    def blacklist_token(self, token: str):
        """Add token to blacklist"""
        self._token_blacklist.add(token)

def require_auth(f):
    """Decorator for requiring authentication"""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        security_manager = SecurityManager()
        
        # Check rate limiting
        if security_manager.rate_limiter.is_rate_limited(
            request.remote_addr, 
            request.endpoint
        ):
            return jsonify({
                "error": "Rate limit exceeded"
            }), 429
            
        # Check authentication
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({
                "error": "No authorization header"
            }), 401
            
        try:
            token_type, token = auth_header.split()
            if token_type.lower() != 'bearer':
                raise ValueError("Invalid token type")
        except ValueError:
            return jsonify({
                "error": "Invalid authorization header format"
            }), 401
            
        payload = security_manager.verify_token(token)
        if not payload:
            return jsonify({
                "error": "Invalid or expired token"
            }), 401
            
        # Store user info in request context
        request.user = payload
        return f(*args, **kwargs)
        
    return decorated

def cors_headers(f):
    """Decorator for adding CORS headers"""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        response = f(*args, **kwargs)
        
        if isinstance(response, tuple):
            response, status_code = response
        else:
            status_code = 200
            
        if isinstance(response, dict):
            response = jsonify(response)
            
        # Add CORS headers
        allowed_origins = SECURITY_CONFIG['allowed_origins']
        origin = request.headers.get('Origin')
        if origin and (allowed_origins == ['*'] or origin in allowed_origins):
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
            response.headers['Access-Control-Max-Age'] = '3600'
            
        return response, status_code
        
    return decorated

# Create singleton instance
security_manager = SecurityManager()