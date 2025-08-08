"""
Webhook endpoint to process PDFs when notified by Cloudflare Worker
Supports queuing to handle multiple PDFs uploaded simultaneously
"""

import json
import logging
import os
import re
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from io import BytesIO
import boto3
import pdfplumber
from openai import OpenAI
import requests
import traceback

# Configure httpx to only log actual errors, not successful requests
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

# Add utils directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from sheriff_mapping import get_sheriff_uuid, is_sheriff_associated
from supabase_storage import upload_pdf_to_supabase_storage


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
            batch_info = webhook_data.get('batch_info', {})
            
            if not pdf_files:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'No PDF files provided'}).encode())
                return
            
            # Log batch information if available
            if batch_info:
                print(f"📦 Processing batch {batch_info.get('batch_number', 'unknown')}/{batch_info.get('total_batches', 'unknown')} - {len(pdf_files)} PDFs")
            else:
                print(f"📦 Processing {len(pdf_files)} PDFs (no batch info)")
            
            print(f"📁 PDF files to process: {pdf_files}")
            print(f"🕐 Webhook received at: {webhook_data.get('timestamp', 'unknown')}")
            
            # Generate unique processing ID for log isolation
            processing_id = f"{datetime.now().strftime('%H%M%S')}_{len(pdf_files)}PDFs"
            print(f"🏷️ Processing ID: {processing_id}")
            print(f"⏱️ Sequential processing {len(pdf_files)} PDFs (no collisions)")
            
            # Process each PDF file SEQUENTIALLY to avoid collisions
            results = []
            
            for i, pdf_file in enumerate(pdf_files, 1):
                print(f"\n[{processing_id}] 🔄 === Processing PDF {i}/{len(pdf_files)}: {pdf_file} ===")
                pdf_key = f"unprocessed/{pdf_file}"
                
                # Process single PDF completely before moving to next
                print(f"[{processing_id}] 📥 Starting processing for {pdf_file}")
                result = process_single_pdf(pdf_key, processing_id)
                results.append(result)
                
                if result.get('status') == 'success':
                    print(f"[{processing_id}] ✅ PDF {i} processed successfully - {result.get('auctions_processed', 0)} auctions")
                else:
                    print(f"[{processing_id}] ❌ PDF {i} processing failed: {result.get('error', 'unknown error')}")
                
                # Upload to Supabase storage and cleanup R2 if successful
                if result.get('status') == 'success':
                    print(f"[{processing_id}] 📤 Uploading {pdf_file} to Supabase storage and cleaning up R2...")
                    storage_result = upload_and_cleanup_pdf(pdf_file, result)
                    result['storage_cleanup'] = storage_result
                    
                    if storage_result.get('success'):
                        print(f"[{processing_id}] ✅ Storage and cleanup completed for {pdf_file}")
                    else:
                        print(f"[{processing_id}] ❌ Storage or cleanup failed for {pdf_file}: {storage_result.get('error', 'unknown')}")
                else:
                    print(f"[{processing_id}] ⏭️ Skipping storage cleanup for {pdf_file} due to processing failure")
                
                print(f"[{processing_id}] 🏁 Completed PDF {i}/{len(pdf_files)}, moving to next...")
                
            print(f"[{processing_id}] 🎉 All PDFs processed sequentially!")
            
            # Send response
            successful_processes = len([r for r in results if r.get('status') == 'success'])
            
            response = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'pdfs_processed': len(pdf_files),
                'successful_processes': successful_processes,
                'failed_processes': len(pdf_files) - successful_processes,
                'batch_info': batch_info if batch_info else None,
                'results': results,
                'processing_method': 'webhook-triggered-batch' if batch_info else 'webhook-triggered-single'
            }
            
            print(f"\n📨 === WEBHOOK PROCESSING COMPLETE ===")
            print(f"   Total PDFs: {len(pdf_files)}")
            print(f"   Successful: {successful_processes}")
            print(f"   Failed: {len(pdf_files) - successful_processes}")
            print(f"   Batch: {batch_info.get('batch_number', 'N/A') if batch_info else 'Single'}")
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response, indent=2).encode())
            
        except Exception as e:
            print(f"❌ WEBHOOK ERROR: {str(e)}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")
            
            error_response = {
                'status': 'error',
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(error_response).encode())

def process_single_pdf(pdf_key, processing_id="unknown"):
    """Process a single PDF file (same logic as process-complete but for one PDF)"""
    try:
        print(f"[{processing_id}] 🔄 Starting processing for PDF: {pdf_key}")
        
        # Initialize clients
        print(f"[{processing_id}] 📡 Initializing R2 client...")
        r2_client = boto3.client(
            's3',
            endpoint_url=os.getenv('R2_ENDPOINT_URL'),
            aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
            region_name='auto'
        )
        
        print(f"[{processing_id}] 🤖 Initializing OpenAI client...")
        openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        google_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
        
        print(f"[{processing_id}] 📦 Using R2 bucket: {bucket_name}")
        
        # Download and extract text from PDF
        print(f"[{processing_id}] 📈 Attempting to download PDF from R2: {pdf_key}")
        try:
            pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
            pdf_content = pdf_obj['Body'].read()
            pdf_size = len(pdf_content)
            print(f"✅ Successfully downloaded PDF: {pdf_size} bytes")
            
            pdf_stream = BytesIO(pdf_content)
        except Exception as e:
            print(f"❌ Failed to download PDF from R2: {str(e)}")
            # List what's available in unprocessed folder
            try:
                list_result = r2_client.list_objects_v2(Bucket=bucket_name, Prefix='unprocessed/', MaxKeys=10)
                available_files = [obj['Key'] for obj in list_result.get('Contents', [])]
                print(f"📁 Available files in unprocessed/: {available_files}")
            except Exception as list_error:
                print(f"❌ Cannot list R2 bucket contents: {str(list_error)}")
            raise e
        
        # Extract text from PDF
        print(f"📄 Extracting text from PDF...")
        raw_text = ""
        with pdfplumber.open(pdf_stream) as pdf:
            total_pages = len(pdf.pages)
            start_page = 12 if total_pages > 12 else 0
            print(f"📃 PDF has {total_pages} pages, starting from page {start_page + 1}")
            
            pages_processed = 0
            for i, page in enumerate(pdf.pages[start_page:], start=start_page + 1):
                page_text = page.extract_text()
                if page_text:
                    if "PAUC" in page_text.upper():
                        print(f"⏹️ Found PAUC section on page {i}, stopping extraction")
                        break
                    raw_text += f"{page_text}\n"
                    pages_processed += 1
            
            print(f"✅ Processed {pages_processed} pages, extracted {len(raw_text)} characters")
        
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
            # Fixed pattern to handle case numbers with letter prefixes like D5071/2024
            pattern = re.compile(r'(?=(Case No:\s*[A-Z]*\d+/\d+))', re.IGNORECASE)
            matches = list(pattern.finditer(text))
            print(f"🔍 Found {len(matches)} Case No matches in text")
            for match in matches:
                print(f"   Match: {match.group(1)}")
            
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
        
        print(f"🧽 Cleaning extracted text...")
        cleaned_text = clean_text(raw_text)
        print(f"✅ Text cleaned: {len(cleaned_text)} characters after cleaning")
        
        print(f"✂️ Splitting text into individual auctions...")
        auctions = split_into_auctions(cleaned_text)
        print(f"📄 Found {len(auctions)} auctions in PDF")
        
        # Process all auctions found (no artificial limits)
        print(f"📋 Processing all {len(auctions)} auctions found in PDF")
        
        # Show first few characters of each auction for debugging
        for i, auction in enumerate(auctions, 1):
            preview = auction[:100].replace('\n', ' ').strip()
            print(f"📜 Auction {i}: {preview}...")
        
        # Process auctions with OpenAI
        print(f"🤖 Processing {len(auctions)} auctions with OpenAI...")
        
        processed_count = 0
        upload_results = []
        
        enable_processing = os.getenv('ENABLE_PROCESSING', 'false').lower() == 'true'
        
        if not enable_processing:
            print(f"⚠️ ENABLE_PROCESSING is false - skipping OpenAI processing")
            upload_results.append({
                'status': 'skipped',
                'auctions_found': len(auctions),
                'auctions_extracted': 0,
                'note': 'OpenAI processing disabled (ENABLE_PROCESSING=false)'
            })
        else:
            print(f"✅ ENABLE_PROCESSING is true - proceeding with OpenAI processing")
            
            # Initialize processed count and token tracking
            processed_count = 0
            total_tokens_used = 0
            max_tokens = int(os.getenv('MAX_OPENAI_TOKENS_PER_RUN', '100000'))
            
            # Batch auctions to stay within 15-minute Vercel limit
            AUCTION_BATCH_SIZE = 50
            auction_batches = [auctions[i:i + AUCTION_BATCH_SIZE] for i in range(0, len(auctions), AUCTION_BATCH_SIZE)]
            
            print(f"[{processing_id}] 📦 Processing {len(auctions)} auctions in {len(auction_batches)} batches of {AUCTION_BATCH_SIZE}")
            
            # Process auction batches sequentially
            for batch_num, auction_batch in enumerate(auction_batches, 1):
                print(f"[{processing_id}] 🔄 === Processing Auction Batch {batch_num}/{len(auction_batches)} ({len(auction_batch)} auctions) ===")
                
                # Process each auction in the current batch
                for i, auction in enumerate(auction_batch, 1):
                    global_auction_num = (batch_num - 1) * AUCTION_BATCH_SIZE + i
                    print(f"[{processing_id}] 🤖 Processing auction {global_auction_num}/{len(auctions)} (Batch {batch_num}, Item {i}/{len(auction_batch)}) with OpenAI...")
                    
                    try:
                        # Complete fine-tuned auction fields specification (from process-complete.py)
                        auction_fields = [
                            {"column_name": "case_number", "data_type": "text", "allow_null": False, "additional_info": "The official case number for the auction, typically in the format '1234/2024'."},
                            {"column_name": "court_name", "data_type": "text", "allow_null": True, "additional_info": "The name of the court where the case is filed (e.g., 'Gauteng Division, Pretoria')."},
                            {"column_name": "plaintiff", "data_type": "text", "allow_null": True, "additional_info": "Name of the plaintiff or applicant in the case."},
                            {"column_name": "defendant", "data_type": "text", "allow_null": True, "additional_info": "Name(s) of the defendant(s) or respondent(s) in the case."},
                            {"column_name": "auction_date", "data_type": "date", "allow_null": True, "additional_info": "The date on which the auction will be held (e.g., '2025-01-28')."},
                            {"column_name": "auction_time", "data_type": "time without time zone", "allow_null": True, "additional_info": "The time when the auction is scheduled to start (e.g., '11:00')."},
                            {"column_name": "sheriff_office", "data_type": "text", "allow_null": True, "additional_info": "Name of the sheriff's office conducting the auction. Exclude words like acting, sheriff, office, the high court, and just return the name. Return it as a proper Noun not all caps. This should be the name of the area, not the name of the sheriff"},
                            {"column_name": "sheriff_address", "data_type": "text", "allow_null": True, "additional_info": "Physical address of the sheriff's office or auction venue."},
                            {"column_name": "erf_number", "data_type": "text", "allow_null": True, "additional_info": "ERF number or property identifier related to the auctioned property."},
                            {"column_name": "township", "data_type": "text", "allow_null": True, "additional_info": "The township or area where the property is located."},
                            {"column_name": "extension", "data_type": "text", "allow_null": True, "additional_info": "Extension number or name, if applicable, for the property."},
                            {"column_name": "registration_division", "data_type": "text", "allow_null": True, "additional_info": "Registration division for the property (e.g., 'IR', 'JR')."},
                            {"column_name": "province", "data_type": "text", "allow_null": True, "additional_info": "Province where the property is located (e.g., 'Gauteng')."},
                            {"column_name": "stand_size", "data_type": "bigint", "allow_null": True, "additional_info": "Size of the stand or property, usually in square meters."},
                            {"column_name": "deed_of_transfer_number", "data_type": "text", "allow_null": True, "additional_info": "Official deed of transfer number for the property."},
                            {"column_name": "street_address", "data_type": "text", "allow_null": True, "additional_info": "Physical street address of the property being auctioned. Be sure to not give the auctioneer's address, but the actual property address. Just give the street number, road name, suburb, and city if available, leave out things like what section it is and or what the door number is"},
                            {"column_name": "zoning", "data_type": "text", "allow_null": True, "additional_info": "Classify the property zoning type (e.g., 'Residential', 'Commercial', 'Agricultural', 'Industrial' etc.)."},
                            {"column_name": "reserve_price", "data_type": "bigint", "allow_null": True, "additional_info": "Minimum price required for the sale, remember that '.' indicates the cents seperator So R10.57 is 10,57 not 1057."},
                            {"column_name": "bedrooms", "data_type": "bigint", "allow_null": True, "additional_info": "Number of bedrooms in the property."},
                            {"column_name": "bathrooms", "data_type": "bigint", "allow_null": True, "additional_info": "Number of bathrooms in the property."},
                            {"column_name": "kitchen", "data_type": "text", "allow_null": True, "additional_info": "Description of kitchen facilities (e.g., 'Yes', 'Scullery', 'Open plan')."},
                            {"column_name": "scullery", "data_type": "text", "allow_null": True, "additional_info": "Presence or description of a scullery (e.g., 'Yes', 'No')."},
                            {"column_name": "laundry", "data_type": "text", "allow_null": True, "additional_info": "Presence or description of a laundry (e.g., 'Yes', 'No')."},
                            {"column_name": "living_areas", "data_type": "bigint", "allow_null": True, "additional_info": "Number of living areas (lounges, dining rooms, etc.)."},
                            {"column_name": "garage", "data_type": "text", "allow_null": True, "additional_info": "Garage details (e.g., 'Single', 'Double', 'Yes', 'None')."},
                            {"column_name": "carport", "data_type": "text", "allow_null": True, "additional_info": "Carport details (e.g., 'Single', 'Double', 'Yes', 'None')."},
                            {"column_name": "other_structures", "data_type": "text", "allow_null": True, "additional_info": "Any additional structures on the property (e.g., 'Flatlet', 'Shed', 'Office')."},
                            {"column_name": "registration_fee_required", "data_type": "text", "allow_null": True, "additional_info": "Amount and description of registration fee required to participate in the auction."},
                            {"column_name": "fica_requirements", "data_type": "text", "allow_null": True, "additional_info": "FICA or legal compliance requirements for buyers."},
                            {"column_name": "attorney", "data_type": "text", "allow_null": True, "additional_info": "Name of the attorney or firm representing the plaintiff."},
                            {"column_name": "attorney_contact", "data_type": "text", "allow_null": True, "additional_info": "Contact details for the attorney (phone, fax, or email)."},
                            {"column_name": "attorney_reference", "data_type": "text", "allow_null": True, "additional_info": "Attorney's internal reference number or code for the case."},
                            {"column_name": "notice_date", "data_type": "date", "allow_null": True, "additional_info": "Date when the auction notice was published."},
                            {"column_name": "additional_fees", "data_type": "text", "allow_null": True, "additional_info": "Explanation of any additional fees (e.g., 'attorney fees, sheriff fees, etc.')."},
                            {"column_name": "total_estimated_cost", "data_type": "bigint", "allow_null": True, "additional_info": "Calculate Total estimated cost, including all fees and reserve price."},
                            {"column_name": "currency", "data_type": "text", "allow_null": True, "additional_info": "Currency of all monetary values (e.g., 'ZAR')."},
                            {"column_name": "conditions_of_sale", "data_type": "text", "allow_null": True, "additional_info": "Return the conditions of sale for the auction. It is usually a few lines of information following text like 'THE CONDITIONS OF SALE:' or 'Material conditions of sale:'. Give the full details of the structure including the sheriff's fees and deposit amount required from the purchaser. If nothing is found return 'See Auction Desription'"}
                        ]
                    
                        # Create OpenAI prompt
                        prompt = f"""You are a data extractor. From the following sheriff auction notice, extract the VALUES for these fields and return as a JSON array with ONE object.

Field specifications (extract the VALUES for each of these):
{json.dumps(auction_fields, indent=2)}

IMPORTANT INSTRUCTIONS:
- Return a JSON array containing ONE object with the extracted VALUES
- Each key should be the column_name, each value should be the extracted data
- Do NOT return the field definitions, return the actual VALUES from the auction text
- Do NOT wrap the JSON in markdown code blocks (no ```json or ``` tags)
- Return ONLY the raw JSON array, starting with [ and ending with ]
- If a value is missing or unknown, return 'None' for text fields and 0 for number fields
- For missing dates use '2000-01-01' and missing times use '00:00:00'
- Do NOT include any explanatory text outside of the JSON

Example format: [{{"case_number": "123/2024", "court_name": "Gauteng Division", ...}}]

Auction text to extract from:
{auction}"""
                        
                        response = openai_client.chat.completions.create(
                            model="gpt-3.5-turbo",
                            messages=[
                                {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON array."},
                                {"role": "user", "content": prompt}
                            ],
                            max_tokens=1500,
                            temperature=0.1
                        )
                    
                        # Track token usage (matching process-complete.py)
                        total_tokens_used += response.usage.total_tokens
                    
                        # Parse OpenAI response
                        content = response.choices[0].message.content.strip()
                    
                        # Remove markdown code blocks if present
                        if content.startswith('```json'):
                            content = content[7:]  # Remove ```json
                        elif content.startswith('```'):
                            content = content[3:]  # Remove ```
                    
                        if content.endswith('```'):
                            content = content[:-3]  # Remove trailing ```
                    
                        content = content.strip()
                    
                        print(f"📋 OpenAI response for auction {i}: {content[:100]}...")
                    
                        if content.startswith('['):
                            extracted_data = json.loads(content)
                            if isinstance(extracted_data, list) and len(extracted_data) > 0:
                                auction_data = extracted_data[0]  # Take first item from array
                            else:
                                raise ValueError("Empty array returned")
                        else:
                            auction_data = json.loads(content)
                    
                        # Add metadata (matching process-complete.py exactly)
                        auction_data['gov_pdf_name'] = pdf_key  # Use gov_pdf_name instead of source_pdf
                        auction_data['data_extraction_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # timestamp format
                        auction_data['pdf_file_name'] = pdf_key.split('/')[-1]
                    
                        # Sheriff association logic using JSON mapping
                        sheriff_uuid = get_sheriff_uuid(auction_data.get('sheriff_office'))
                        auction_data['sheriff_uuid'] = sheriff_uuid
                        auction_data['sheriff_associated'] = is_sheriff_associated(sheriff_uuid)
                    
                        auction_data['auction_description'] = auction
                        # Set default values for other boolean fields
                        auction_data['processed_nearby_sales'] = False
                        auction_data['online_auction'] = False
                        auction_data['is_streaming'] = False
                    
                        # Geocode addresses (based on your original process)
                        google_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
                        if google_api_key:
                            # Sheriff address geocoding
                            if auction_data.get('sheriff_address'):
                                try:
                                sheriff_geocode = extract_area_components(auction_data['sheriff_address'], google_api_key)
                                auction_data['sheriff_area'] = sheriff_geocode.get('area')
                                auction_data['sheriff_city'] = sheriff_geocode.get('city')
                                auction_data['sheriff_province'] = sheriff_geocode.get('province')
                                auction_data['sheriff_coordinates'] = sheriff_geocode.get('coordinates')
                            except Exception as e:
                                print(f"Sheriff geocoding error: {e}")
                                auction_data['sheriff_area'] = None
                                auction_data['sheriff_city'] = None
                                auction_data['sheriff_province'] = None
                                auction_data['sheriff_coordinates'] = None
                        
                        # House address geocoding
                        if auction_data.get('street_address'):
                            try:
                                house_geocode = extract_area_components(auction_data['street_address'], google_api_key)
                                auction_data['house_street_number'] = house_geocode.get('street_number')
                                auction_data['house_street_name'] = house_geocode.get('street_name')
                                auction_data['house_suburb'] = house_geocode.get('suburb')
                                auction_data['house_area'] = house_geocode.get('area')
                                auction_data['house_city'] = house_geocode.get('city')
                                auction_data['house_province'] = house_geocode.get('province')
                                auction_data['house_coordinates'] = house_geocode.get('coordinates')
                            except Exception as e:
                                print(f"House geocoding error: {e}")
                                auction_data['house_street_number'] = None
                                auction_data['house_street_name'] = None
                                auction_data['house_suburb'] = None
                                auction_data['house_area'] = None
                                auction_data['house_city'] = None
                                auction_data['house_province'] = None
                                auction_data['house_coordinates'] = None
                    
                        # Add auction_number for display (but remove before upload)
                        auction_data['auction_number'] = i
                    
                        print(f"📤 Uploading auction {i} to Supabase database...")
                    
                        # Upload to Supabase auctions table
                        supabase_url = os.getenv('SUPABASE_URL')
                        supabase_key = os.getenv('SUPABASE_KEY')
                    
                        headers = {
                        'apikey': supabase_key,
                        'Authorization': f'Bearer {supabase_key}',
                        'Content-Type': 'application/json',
                        'Prefer': 'return=minimal'
                        }
                    
                        # Remove auction_number if exists
                        upload_data = auction_data.copy()
                        upload_data.pop('auction_number', None)
                        
                        upload_url = f"{supabase_url}/rest/v1/auctions"
                        upload_response = requests.post(upload_url, json=upload_data, headers=headers)
                        
                        if upload_response.status_code in [200, 201]:
                            print(f"[{processing_id}] ✅ Auction {global_auction_num} uploaded successfully to Supabase")
                            processed_count += 1
                        else:
                            print(f"[{processing_id}] ❌ Auction {global_auction_num} upload failed: {upload_response.status_code} - {upload_response.text}")
                        
                    except Exception as e:
                        print(f"[{processing_id}] ❌ Auction {global_auction_num} processing failed: {str(e)}")
                        print(f"[{processing_id}]    Traceback: {traceback.format_exc()}")
                        continue
                
                # Batch completion and token limit check
                print(f"[{processing_id}] ✅ Batch {batch_num}/{len(auction_batches)} completed - {processed_count} auctions processed so far")
                
                # Check token limits between batches
                if total_tokens_used > max_tokens:
                    print(f"[{processing_id}] ⚠️ Token limit reached: {total_tokens_used}/{max_tokens} - stopping processing")
                    break
                    
            print(f"[{processing_id}] 🎉 All auction batches completed - {processed_count}/{len(auctions)} auctions processed")
            
            upload_results.append({
                'status': 'processed',
                'auctions_found': len(auctions),
                'auctions_extracted': processed_count,
                'note': f'OpenAI processing completed - {processed_count}/{len(auctions)} auctions uploaded to Supabase'
            })
        
        result = {
            'status': 'success',
            'pdf_key': pdf_key,
            'pdf_size_bytes': pdf_size,
            'pages_in_pdf': total_pages,
            'pages_processed': pages_processed,
            'raw_text_length': len(raw_text),
            'cleaned_text_length': len(cleaned_text),
            'auctions_found': len(auctions),
            'auctions_processed': processed_count,
            'upload_results': upload_results,
            'processing_enabled': enable_processing,
            'total_tokens_used': total_tokens_used if enable_processing else 0,
            'estimated_cost': f"${total_tokens_used * 0.000002:.4f}" if enable_processing else "$0.0000"
        }
        
        print(f"✅ Processing completed successfully:")
        print(f"   - PDF: {pdf_size} bytes, {total_pages} pages")
        print(f"   - Text: {len(raw_text)} -> {len(cleaned_text)} chars")
        print(f"   - Auctions: {len(auctions)} found, {processed_count} processed")
        if enable_processing:
            print(f"   - Tokens: {total_tokens_used} used, cost ${total_tokens_used * 0.000002:.4f}")
        
        return result
        
    except Exception as e:
        print(f"❌ ERROR processing PDF {pdf_key}: {str(e)}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        
        return {
            'status': 'error',
            'pdf_key': pdf_key,
            'error': str(e),
            'error_type': type(e).__name__
        }

def upload_and_cleanup_pdf(pdf_filename, processing_result):
    """Upload PDF to Supabase storage and delete from R2 unprocessed folder"""
    try:
        print(f"📤 Starting upload and cleanup for: {pdf_filename}")
        
        r2_client = boto3.client(
            's3',
            endpoint_url=os.getenv('R2_ENDPOINT_URL'),
            aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
            region_name='auto'
        )
        
        bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
        source_key = f"unprocessed/{pdf_filename}"
        
        print(f"📁 Looking for PDF at R2 key: {source_key}")
        print(f"📦 Using bucket: {bucket_name}")
        
        # Download PDF content for Supabase upload
        print(f"📈 Downloading PDF from R2 for Supabase upload...")
        
        try:
            # List files in unprocessed folder first
            list_result = r2_client.list_objects_v2(Bucket=bucket_name, Prefix='unprocessed/', MaxKeys=10)
            available_files = [obj['Key'] for obj in list_result.get('Contents', [])]
            print(f"📁 Available files in unprocessed/: {available_files}")
            
            pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=source_key)
            pdf_content = pdf_obj['Body'].read()
            pdf_size = len(pdf_content)
            
            print(f"✅ Downloaded {pdf_filename} from R2: {pdf_size} bytes")
            
        except Exception as download_error:
            print(f"❌ Failed to download {source_key} from R2: {str(download_error)}")
            raise download_error
        
        # Create metadata for the PDF
        pdf_metadata = {
            'processed_date': datetime.now().isoformat(),
            'auctions_found': processing_result.get('auctions_found', 0),
            'auctions_processed': processing_result.get('auctions_processed', 0),
            'processing_method': 'webhook-trigger',
            'source': 'hybrid-cloudflare-vercel-system'
        }
        
        # Upload to Supabase storage
        print(f"📤 Uploading to Supabase storage with filename: {pdf_filename}")
        print(f"📊 Metadata: {pdf_metadata}")
        
        storage_result = upload_pdf_to_supabase_storage(
            pdf_content, 
            pdf_filename, 
            pdf_metadata
        )
        
        print(f"📊 Storage result: {storage_result}")
        
        if storage_result.get('success'):
            # Delete from R2 unprocessed folder to save storage costs
            print(f"🗊 Deleting {source_key} from R2 unprocessed folder...")
            r2_client.delete_object(Bucket=bucket_name, Key=source_key)
            print(f"✅ Successfully deleted {source_key} from R2")
            
            return {
                'success': True, 
                'action': 'uploaded_to_supabase_and_deleted_from_r2',
                'storage_result': storage_result,
                'r2_cleanup': f'PDF {pdf_filename} deleted from R2 unprocessed folder',
                'uploaded_filename': pdf_filename,
                'uploaded_size': pdf_size
            }
        else:
            return {
                'success': False, 
                'action': 'supabase_upload_failed',
                'storage_result': storage_result,
                'note': 'PDF kept in R2 due to upload failure'
            }
        
    except Exception as e:
        print(f"❌ Upload and cleanup failed for {pdf_filename}: {str(e)}")
        print(f"   Traceback: {traceback.format_exc()}")
        return {
            'success': False, 
            'action': 'upload_and_cleanup_error',
            'error': str(e),
            'pdf_filename': pdf_filename
        }