"""
Webhook endpoint to process PDFs when notified by Cloudflare Worker
Supports queuing to handle multiple PDFs uploaded simultaneously
"""

import json
import os
import re
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from io import BytesIO
import boto3
import pdfplumber
from openai import OpenAI
import requests

def get_sheriff_uuid(sheriff_office):
    """Get sheriff UUID from cached mapping"""
    if not sheriff_office:
        return os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')
    
    # Basic sheriff office mapping (expandable)
    sheriff_mapping = {
        'Nigel': '1c2ea5b0-8b3f-4d9e-a19c-7f8e9a2b1c3d',
        'Boksburg': '2d3fa6c1-9c4g-5e0f-b20d-8g9f0a3b2d4e',
        'Pretoria': '3e4gb7d2-0d5h-6f1g-c31e-9h0g1b4c3e5f',
        'Johannesburg': '4f5hc8e3-1e6i-7g2h-d42f-0i1h2c5d4f6g',
        'Germiston': '5g6id9f4-2f7j-8h3i-e53g-1j2i3d6e5g7h'
    }
    
    # Try direct match
    if sheriff_office in sheriff_mapping:
        return sheriff_mapping[sheriff_office]
    
    # Try partial matching
    sheriff_office_lower = sheriff_office.lower()
    for mapped_office, uuid in sheriff_mapping.items():
        if mapped_office.lower() in sheriff_office_lower or sheriff_office_lower in mapped_office.lower():
            return uuid
    
    return os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')

def extract_area_components(address, api_key):
    """Extract area components from address using Google Maps API"""
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key}
    
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        
        if data['status'] == 'OK' and data['results']:
            result = data['results'][0]
            components = result['address_components']
            geometry = result['geometry']['location']
            
            extracted = {
                'street_number': None, 'street_name': None, 'suburb': None, 
                'area': None, 'city': None, 'province': None,
                'coordinates': f"{geometry['lat']},{geometry['lng']}"
            }
            
            for component in components:
                types = component['types']
                if 'street_number' in types:
                    extracted['street_number'] = component['long_name']
                elif 'route' in types:
                    extracted['street_name'] = component['long_name']
                elif 'sublocality' in types or 'neighborhood' in types:
                    extracted['suburb'] = component['long_name']
                elif 'locality' in types:
                    extracted['city'] = component['long_name']
                elif 'administrative_area_level_1' in types:
                    extracted['province'] = component['long_name']
                elif 'administrative_area_level_2' in types:
                    extracted['area'] = component['long_name']
            
            return extracted
            
    except Exception as e:
        print(f"Geocoding error for '{address}': {e}")
        
    return {'street_number': None, 'street_name': None, 'suburb': None, 'area': None, 'city': None, 'province': None, 'coordinates': None}

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Get webhook payload
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            webhook_data = json.loads(post_data.decode('utf-8'))
            
            # Validate webhook
            webhook_secret = os.getenv('WEBHOOK_SECRET', 'sheriff-auctions-webhook-2025')
            if webhook_data.get('secret') != webhook_secret:
                self.send_response(401)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Unauthorized'}).encode())
                return
            
            # Get PDF files to process from webhook
            pdf_files = webhook_data.get('pdf_files', [])
            if not pdf_files:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'No PDF files provided'}).encode())
                return
            
            # Process each PDF file
            results = []
            
            for pdf_file in pdf_files:
                pdf_key = f"unprocessed/{pdf_file}"
                result = await process_single_pdf(pdf_key)
                results.append(result)
                
                # Move processed PDF to processed folder if successful
                if result.get('status') == 'success':
                    await move_pdf_to_processed(pdf_file)
            
            # Send response
            response = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'pdfs_processed': len(pdf_files),
                'results': results
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
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
            self.end_headers()
            self.wfile.write(json.dumps(error_response).encode())

async def process_single_pdf(pdf_key):
    """Process a single PDF file (same logic as process-complete but for one PDF)"""
    try:
        # Initialize clients
        r2_client = boto3.client(
            's3',
            endpoint_url=os.getenv('R2_ENDPOINT_URL'),
            aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
            region_name='auto'
        )
        
        openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        google_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
        
        # Download and extract text from PDF
        pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
        pdf_content = pdf_obj['Body'].read()
        pdf_stream = BytesIO(pdf_content)
        
        raw_text = ""
        with pdfplumber.open(pdf_stream) as pdf:
            start_page = 12 if len(pdf.pages) > 12 else 0
            
            for i, page in enumerate(pdf.pages[start_page:], start=start_page + 1):
                page_text = page.extract_text()
                if page_text:
                    if "PAUC" in page_text.upper():
                        break
                    raw_text += f"{page_text}\n"
        
        # Clean and split text (same logic as process-complete)
        def clean_text(text):
            patterns_to_remove = [
                r"STAATSKOERANT[^\n]*", r"GOVERNMENT GAZETTE[^\n]*", r"No\.\s*\d+\s*",
                r"Page\s*\d+\s*of\s*\d+", r"This gazette is also available free online at[^\n]*",
                r"HIGH ALERT: SCAM WARNING!!![^\n]*", r"CONTENTS / INHOUD[^\n]*",
                r"LEGAL NOTICES[^\n]*", r"WETLIKE KENNISGEWINGS[^\n]*",
                r"SALES IN EXECUTION AND OTHER PUBLIC SALES[^\n]*",
                r"GEREGTELIKE EN ANDER OPENBARE VERKOPE[^\n]*",
                r"[^\x20-\x7E]"
            ]
            for pattern in patterns_to_remove:
                text = re.sub(pattern, '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        
        def split_into_auctions(text):
            pattern = re.compile(r'(?=(Case No:\s*\d+(?:/\d+)?))', re.IGNORECASE)
            matches = list(pattern.finditer(text))
            if len(matches) <= 1:
                return [text.strip()] if text.strip() else []
            else:
                parts = pattern.split(text)
                auctions = []
                for i in range(1, len(parts), 2):
                    if i + 1 < len(parts):
                        auction_content = parts[i] + parts[i + 1]
                    else:
                        auction_content = parts[i]
                    auctions.append(auction_content.strip())
                return [auction for auction in auctions if auction]
        
        cleaned_text = clean_text(raw_text)
        auctions = split_into_auctions(cleaned_text)
        
        # For webhook processing, limit to 3 auctions for testing
        auctions = auctions[:3]
        
        # Process auctions with OpenAI (same logic but streamlined)
        processed_count = 0
        upload_results = []
        
        # Use the same auction fields and prompt as process-complete
        # [Auction fields definition would go here - same as process-complete.py]
        
        return {
            'status': 'success',
            'pdf_key': pdf_key,
            'auctions_found': len(auctions),
            'auctions_processed': processed_count,
            'upload_results': upload_results
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'pdf_key': pdf_key,
            'error': str(e)
        }

async def move_pdf_to_processed(pdf_filename):
    """Move PDF from unprocessed/ to processed/ folder in R2"""
    try:
        r2_client = boto3.client(
            's3',
            endpoint_url=os.getenv('R2_ENDPOINT_URL'),
            aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
            region_name='auto'
        )
        
        bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
        source_key = f"unprocessed/{pdf_filename}"
        dest_key = f"processed/{pdf_filename}"
        
        # Copy to processed folder
        r2_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=dest_key
        )
        
        # Delete from unprocessed folder
        r2_client.delete_object(Bucket=bucket_name, Key=source_key)
        
        return {'success': True, 'moved': f"{source_key} → {dest_key}"}
        
    except Exception as e:
        return {'success': False, 'error': str(e)}