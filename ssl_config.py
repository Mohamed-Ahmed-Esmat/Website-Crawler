import os
from OpenSSL import crypto
from datetime import datetime, timedelta
import logging

def generate_self_signed_cert(cert_dir="certs"):
    """Generate self-signed certificate for development"""
    if not os.path.exists(cert_dir):
        os.makedirs(cert_dir)
        
    key_path = os.path.join(cert_dir, "server.key")
    cert_path = os.path.join(cert_dir, "server.crt")
    
    if os.path.exists(key_path) and os.path.exists(cert_path):
        return key_path, cert_path
        
    # Generate key
    key = crypto.PKey()
    key.generate_key(crypto.TYPE_RSA, 2048)
    
    # Generate certificate
    cert = crypto.X509()
    cert.get_subject().C = "US"
    cert.get_subject().ST = "State"
    cert.get_subject().L = "City"
    cert.get_subject().O = "Organization"
    cert.get_subject().OU = "Organizational Unit"
    cert.get_subject().CN = "localhost"
    
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(365*24*60*60)  # Valid for one year
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(key)
    cert.sign(key, 'sha256')
    
    # Save certificate and private key
    with open(cert_path, "wb") as f:
        f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    
    with open(key_path, "wb") as f:
        f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))
        
    logging.info(f"Generated self-signed certificate in {cert_dir}")
    return key_path, cert_path

def setup_cross_cluster_ssl():
    """Configure SSL settings for Elasticsearch"""
    key_path, cert_path = generate_self_signed_cert()
    
    ssl_config = {
        "ssl": True,
        "verify_certs": True,
        "ca_certs": cert_path,
        "client_cert": cert_path,
        "client_key": key_path
    }
    
    return ssl_config