"""
Process auctions from PDF using original methodology
Split auctions and extract data with OpenAI
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

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Remove proxy environment variables that conflict with OpenAI
            for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
                if proxy_var in os.environ:
                    del os.environ[proxy_var]
            
            # Initialize clients
            r2_client = boto3.client(
                's3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            
            openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
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
            
            # Split into auctions using original regex pattern
            auction_pattern = r'Case No:\s*\d+(?:/\d+)?'
            auction_parts = re.split(auction_pattern, extracted_text)
            case_numbers = re.findall(auction_pattern, extracted_text)
            
            # Process first 3 auctions
            auctions = []
            for i in range(min(3, len(case_numbers))):
                case_no = case_numbers[i].replace('Case No:', '').strip()
                auction_text = auction_parts[i + 1] if i + 1 < len(auction_parts) else ""
                
                # Clean and prepare auction text
                full_auction_text = f"Case No: {case_no}\n{auction_text}"
                
                # Extract structured data with OpenAI
                openai_prompt = f"""Extract the following information from this sheriff auction notice:

{full_auction_text}

Please return ONLY a valid JSON object with these exact fields:
{{
    "case_number": "extracted case number",
    "court": "court name and division",
    "plaintiff": "plaintiff name",
    "defendant": "defendant name",
    "sheriff_office": "sheriff office location",
    "auction_date": "date in YYYY-MM-DD format if possible, or as written",
    "auction_time": "auction time",
    "property_description": "full property description including ERF details",
    "address": "physical address",
    "extent": "property size/extent",
    "zoning": "property zoning",
    "reserve_price": "reserve price amount",
    "improvements": "property improvements description"
}}

Return ONLY the JSON, no other text."""

                try:
                    # Call OpenAI API
                    response = openai_client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON."},
                            {"role": "user", "content": openai_prompt}
                        ],
                        max_tokens=1000,
                        temperature=0.1
                    )
                    
                    extracted_data = json.loads(response.choices[0].message.content)
                    
                    auctions.append({
                        "auction_number": i + 1,
                        "case_number": case_no,
                        "text_length": len(full_auction_text),
                        "extracted_data": extracted_data,
                        "raw_text_sample": full_auction_text[:500] + "..." if len(full_auction_text) > 500 else full_auction_text
                    })
                    
                except Exception as e:
                    auctions.append({
                        "auction_number": i + 1,
                        "case_number": case_no,
                        "error": f"OpenAI extraction failed: {str(e)}",
                        "text_length": len(full_auction_text),
                        "raw_text_sample": full_auction_text[:500] + "..." if len(full_auction_text) > 500 else full_auction_text
                    })
            
            response = {
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "pdf_key": pdf_key,
                "total_case_numbers_found": len(case_numbers),
                "auctions_processed": len(auctions),
                "auctions": auctions
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