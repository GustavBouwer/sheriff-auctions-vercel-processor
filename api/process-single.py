"""
Process a single PDF from R2 storage
Test endpoint for PDF text extraction
"""

import json
import os
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from io import BytesIO
import boto3
import pdfplumber

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Get PDF filename from query parameter
            query_string = self.path.split('?', 1)
            pdf_key = "unprocessed/test-989.pdf"  # Default for testing
            
            if len(query_string) > 1:
                params = dict(param.split('=') for param in query_string[1].split('&') if '=' in param)
                if 'pdf' in params:
                    pdf_key = f"unprocessed/{params['pdf']}"
            
            # Initialize R2 client
            r2_client = boto3.client(
                's3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # Download PDF from R2
            pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
            pdf_content = pdf_obj['Body'].read()
            
            # Extract text starting from page 13 (index 12)
            extracted_text = ""
            page_count = 0
            
            # Create BytesIO object for pdfplumber
            pdf_stream = BytesIO(pdf_content)
            
            with pdfplumber.open(pdf_stream) as pdf:
                total_pages = len(pdf.pages)
                start_page = 12 if total_pages > 12 else 0
                
                for i, page in enumerate(pdf.pages[start_page:], start=start_page + 1):
                    page_text = page.extract_text()
                    if page_text:
                        # Stop if we hit the PAUC section
                        if "PAUC" in page_text.upper():
                            break
                        extracted_text += f"\n--- PAGE {i} ---\n{page_text}"
                        page_count += 1
                        
                        # Limit for testing
                        if page_count >= 3:
                            extracted_text += "\n--- TRUNCATED FOR TESTING ---"
                            break
            
            response = {
                'timestamp': datetime.now().isoformat(),
                'status': 'success',
                'pdf_key': pdf_key,
                'pdf_size': len(pdf_content),
                'total_pages': total_pages,
                'pages_processed': page_count,
                'text_length': len(extracted_text),
                'text_sample': extracted_text[:2000] + "..." if len(extracted_text) > 2000 else extracted_text
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response, indent=2).encode())
            
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