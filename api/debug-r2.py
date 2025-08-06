"""
Debug R2 connection and list all objects
"""

import json
import os
from datetime import datetime
from http.server import BaseHTTPRequestHandler
import boto3

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Initialize R2 client
            r2_client = boto3.client(
                's3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # List ALL objects
            all_objects = r2_client.list_objects_v2(Bucket=bucket_name)
            
            # List objects with unprocessed prefix
            unprocessed_objects = r2_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='unprocessed/'
            )
            
            debug_response = {
                'timestamp': datetime.now().isoformat(),
                'bucket_name': bucket_name,
                'environment': {
                    'R2_BUCKET_NAME': os.getenv('R2_BUCKET_NAME'),
                    'R2_ENDPOINT_URL': os.getenv('R2_ENDPOINT_URL'),
                    'has_access_key': os.getenv('R2_ACCESS_KEY_ID') is not None,
                    'has_secret_key': os.getenv('R2_SECRET_ACCESS_KEY') is not None
                },
                'all_objects': {
                    'count': len(all_objects.get('Contents', [])),
                    'objects': [obj['Key'] for obj in all_objects.get('Contents', [])]
                },
                'unprocessed_objects': {
                    'count': len(unprocessed_objects.get('Contents', [])),
                    'objects': [obj['Key'] for obj in unprocessed_objects.get('Contents', [])]
                }
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(debug_response, indent=2).encode())
            
        except Exception as e:
            error_response = {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())