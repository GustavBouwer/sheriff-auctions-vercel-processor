"""
Complete ETL pipeline with geocoding and data enrichment
Processes auctions from PDF with all enrichment steps before Supabase upload
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
from urllib.parse import quote

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Safety check
            enable_processing = os.getenv('ENABLE_PROCESSING', 'false').lower() == 'true'
            max_auctions = int(os.getenv('MAX_AUCTIONS_PER_RUN', '3'))
            
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
            pdf_key = "unprocessed/test-989.pdf"
            
            # Download and extract text from PDF
            pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
            pdf_content = pdf_obj['Body'].read()
            pdf_stream = BytesIO(pdf_content)
            
            extracted_text = ""
            with pdfplumber.open(pdf_stream) as pdf:
                total_pages = len(pdf.pages)
                start_page = 12 if total_pages > 12 else 0
                
                for i, page in enumerate(pdf.pages[start_page:], start=start_page + 1):
                    page_text = page.extract_text()
                    if page_text:
                        if "PAUC" in page_text.upper():
                            break
                        extracted_text += f"{page_text}\n"
            
            # Split into auctions
            auction_pattern = r'Case No:\s*\d+(?:/\d+)?'
            auction_parts = re.split(auction_pattern, extracted_text)
            case_numbers = re.findall(auction_pattern, extracted_text)
            
            # Process auctions with full enrichment
            processed_auctions = []
            total_tokens_used = 0
            
            for i in range(min(max_auctions, len(case_numbers))):
                case_no = case_numbers[i].replace('Case No:', '').strip()
                auction_text = auction_parts[i + 1] if i + 1 < len(auction_parts) else ""
                full_auction_text = f"Case No: {case_no}\n{auction_text}"
                
                # Extract with OpenAI
                openai_prompt = f"""Extract the following information from this sheriff auction notice:

{full_auction_text}

Please return ONLY a valid JSON object with these exact fields:
{{
    "case_number": "extracted case number",
    "court": "court name and division",
    "plaintiff": "plaintiff name",
    "defendant": "defendant name",
    "sheriff_office": "sheriff office location",
    "auction_date": "date in YYYY-MM-DD format if possible",
    "auction_time": "auction time",
    "property_description": "full property description including ERF details",
    "address": "physical address",
    "extent": "property size/extent",
    "zoning": "property zoning",
    "reserve_price": "reserve price amount or null if not specified",
    "improvements": "property improvements description",
    "registration_division": "registration division if mentioned",
    "title_deed": "title deed number if mentioned"
}}

Return ONLY the JSON, no other text."""

                try:
                    response = openai_client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON."},
                            {"role": "user", "content": openai_prompt}
                        ],
                        max_tokens=1000,
                        temperature=0.1
                    )
                    
                    # Track token usage
                    total_tokens_used += response.usage.total_tokens
                    
                    extracted_data = json.loads(response.choices[0].message.content)
                    
                    # Geocode the address if available
                    lat, lng = None, None
                    if extracted_data.get('address') and google_api_key:
                        try:
                            geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={quote(extracted_data['address'])}&key={google_api_key}"
                            geo_response = requests.get(geocode_url)
                            geo_data = geo_response.json()
                            
                            if geo_data['status'] == 'OK' and geo_data['results']:
                                location = geo_data['results'][0]['geometry']['location']
                                lat = location['lat']
                                lng = location['lng']
                        except Exception as e:
                            print(f"Geocoding error: {e}")
                    
                    # Parse reserve price to float
                    reserve_price = None
                    if extracted_data.get('reserve_price'):
                        price_str = extracted_data['reserve_price']
                        # Remove currency symbols and spaces
                        price_str = re.sub(r'[R$,\s]', '', price_str)
                        try:
                            reserve_price = float(price_str)
                        except:
                            pass
                    
                    # Build enriched auction data
                    enriched_auction = {
                        "auction_number": i + 1,
                        "case_number": extracted_data.get('case_number', case_no),
                        "source_pdf": pdf_key,
                        "extraction_date": datetime.now().isoformat(),
                        
                        # Core auction details
                        "court": extracted_data.get('court'),
                        "plaintiff": extracted_data.get('plaintiff'),
                        "defendant": extracted_data.get('defendant'),
                        "sheriff_office": extracted_data.get('sheriff_office'),
                        "auction_date": extracted_data.get('auction_date'),
                        "auction_time": extracted_data.get('auction_time'),
                        
                        # Property details
                        "property_description": extracted_data.get('property_description'),
                        "address": extracted_data.get('address'),
                        "latitude": lat,
                        "longitude": lng,
                        "extent": extracted_data.get('extent'),
                        "zoning": extracted_data.get('zoning'),
                        "improvements": extracted_data.get('improvements'),
                        "registration_division": extracted_data.get('registration_division'),
                        "title_deed": extracted_data.get('title_deed'),
                        
                        # Financial
                        "reserve_price": reserve_price,
                        "reserve_price_raw": extracted_data.get('reserve_price'),
                        
                        # Metadata
                        "tokens_used": response.usage.total_tokens,
                        "text_length": len(full_auction_text),
                        "processing_status": "ready_for_upload"
                    }
                    
                    processed_auctions.append(enriched_auction)
                    
                except Exception as e:
                    processed_auctions.append({
                        "auction_number": i + 1,
                        "case_number": case_no,
                        "error": f"Processing failed: {str(e)}",
                        "processing_status": "error"
                    })
            
            # Prepare response
            response = {
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "processing_enabled": enable_processing,
                "pdf_key": pdf_key,
                "total_case_numbers_found": len(case_numbers),
                "auctions_processed": len(processed_auctions),
                "total_tokens_used": total_tokens_used,
                "estimated_cost": f"${total_tokens_used * 0.000002:.4f}",  # GPT-3.5 pricing
                "auctions": processed_auctions,
                "next_step": "Ready for Supabase upload" if enable_processing else "Test mode - upload disabled"
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