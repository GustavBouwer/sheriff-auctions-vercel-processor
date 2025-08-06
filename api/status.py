"""
Status endpoint for Sheriff Auctions PDF Processor
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
            r2_client = None
            if os.getenv('R2_ACCESS_KEY_ID') and os.getenv('R2_SECRET_ACCESS_KEY'):
                r2_client = boto3.client(
                    's3',
                    endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                    aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                    region_name='auto'
                )
            
            # Get unprocessed PDFs count
            unprocessed_count = 0
            sample_files = []
            
            if r2_client:
                try:
                    response = r2_client.list_objects_v2(
                        Bucket=os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs'),
                        Prefix='unprocessed/'
                    )
                    
                    if 'Contents' in response:
                        pdf_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.pdf')]
                        unprocessed_count = len(pdf_files)
                        sample_files = pdf_files[:5]
                except Exception as e:
                    print(f"R2 error: {e}")
            
            status_response = {
                'timestamp': datetime.now().isoformat(),
                'status': 'operational',
                'configuration': {
                    'processing_enabled': os.getenv('ENABLE_PROCESSING', 'false').lower() == 'true',
                    'max_auctions_per_run': int(os.getenv('MAX_AUCTIONS_PER_RUN', '50')),
                    'max_tokens_per_run': int(os.getenv('MAX_OPENAI_TOKENS_PER_RUN', '100000'))
                },
                'services': {
                    'openai': os.getenv('OPENAI_API_KEY') is not None,
                    'supabase': os.getenv('SUPABASE_URL') is not None and os.getenv('SUPABASE_KEY') is not None,
                    'r2_storage': r2_client is not None
                },
                'bucket_status': {
                    'unprocessed_pdfs': unprocessed_count,
                    'sample_files': sample_files
                }
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(status_response, indent=2).encode())
            
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
            self.wfile.write(json.dumps(error_response).encode())