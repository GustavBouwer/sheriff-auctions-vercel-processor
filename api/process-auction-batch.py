"""
Process a batch of 25 auctions using the EXACT methodology from process-complete.py
This receives pre-extracted auction texts and processes them with OpenAI + geocoding + Supabase upload
"""

import json
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

# Add utils directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from sheriff_mapping import get_sheriff_uuid, is_sheriff_associated
from supabase_storage import upload_pdf_to_supabase_storage


def extract_area_components(address, api_key):
    """Extract area components from address using Google Maps API - EXACT copy from process-complete.py"""
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
                'street_number': None,
                'street_name': None,
                'suburb': None,
                'area': None,
                'city': None,
                'province': None,
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
        
    return {
        'street_number': None,
        'street_name': None, 
        'suburb': None,
        'area': None,
        'city': None,
        'province': None,
        'coordinates': None
    }

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Get batch payload
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            batch_data = json.loads(post_data.decode('utf-8'))
            
            # Validate webhook
            webhook_secret = os.getenv('WEBHOOK_SECRET', 'sheriff-auctions-webhook-2025')
            if batch_data.get('secret') != webhook_secret:
                self.send_response(401)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Unauthorized'}).encode())
                return
            
            # Extract batch info
            pdf_file = batch_data.get('pdf_file')
            batch_info = batch_data.get('batch_info', {})
            processing_id = batch_data.get('processing_id', 'unknown')
            
            batch_number = batch_info.get('batch_number', 1)
            start_auction = batch_info.get('start_auction', 1)
            end_auction = batch_info.get('end_auction', 25)
            
            print(f"[{processing_id}] 🔄 Processing batch {batch_number}: auctions {start_auction}-{end_auction}")
            
            # Get auctions from PDF (same logic as process-complete.py)
            auctions = self.extract_auctions_from_pdf(pdf_file, start_auction, end_auction, processing_id)
            
            if not auctions:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json') 
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'No auctions found in batch range'}).encode())
                return
            
            # Process auctions with EXACT same logic as process-complete.py
            processed_auctions = self.process_auctions_with_openai(auctions, pdf_file, processing_id)
            
            # Upload to Supabase
            upload_results = self.upload_to_supabase(processed_auctions, processing_id)
            
            # Return results
            response = {
                'status': 'success',
                'batch_number': batch_number,
                'auctions_processed': len(processed_auctions),
                'auctions_uploaded': len([r for r in upload_results if r.get('status') == 'success']),
                'processing_id': processing_id,
                'upload_results': upload_results
            }
            
            print(f"[{processing_id}] ✅ Batch {batch_number} completed: {len(processed_auctions)} auctions")
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            print(f"❌ Batch processing error: {str(e)}")
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'status': 'error',
                'error': str(e)
            }).encode())

    def extract_auctions_from_pdf(self, pdf_file, start_auction, end_auction, processing_id):
        """Extract specific auction range from PDF using EXACT process-complete.py logic"""
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
            pdf_key = f"unprocessed/{pdf_file}"
            
            # Download PDF
            pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
            pdf_content = pdf_obj['Body'].read()
            pdf_stream = BytesIO(pdf_content)
            
            # Extract text (EXACT same logic as process-complete.py)
            raw_text = ""
            with pdfplumber.open(pdf_stream) as pdf:
                total_pages = len(pdf.pages)
                start_page = 12 if total_pages > 12 else 0
                
                for i, page in enumerate(pdf.pages[start_page:], start=start_page + 1):
                    page_text = page.extract_text()
                    if page_text:
                        if "PAUC" in page_text.upper():
                            break
                        raw_text += f"{page_text}\n"
            
            # Clean text (EXACT same function as process-complete.py)
            def clean_text(text):
                patterns_to_remove = [
                    r"STAATSKOERANT[^\n]*", r"GOVERNMENT GAZETTE[^\n]*", r"No\.\s*\d+\s*",
                    r"Page\s*\d+\s*of\s*\d+", r"This gazette is also available free online at[^\n]*",
                    r"HIGH ALERT: SCAM WARNING!!![^\n]*", r"CONTENTS / INHOUD[^\n]*",
                    r"LEGAL NOTICES[^\n]*", r"WETLIKE KENNISGEWINGS[^\n]*",
                    r"SALES IN EXECUTION AND OTHER PUBLIC SALES[^\n]*",
                    r"GEREGTELIKE EN ANDER OPENBARE VERKOPE[^\n]*",
                    r"[^\x20-\x7E]"  # Remove non-ASCII characters
                ]
                for pattern in patterns_to_remove:
                    text = re.sub(pattern, '', text, flags=re.IGNORECASE)
                text = re.sub(r'\s+', ' ', text).strip()
                return text
            
            # Split into auctions (EXACT same function as process-complete.py)
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
            all_auctions = split_into_auctions(cleaned_text)
            
            # Extract the specific batch range (1-indexed)
            batch_auctions = all_auctions[start_auction-1:end_auction]
            
            print(f"[{processing_id}] 📊 Extracted {len(batch_auctions)} auctions from range {start_auction}-{end_auction}")
            
            return batch_auctions
            
        except Exception as e:
            print(f"[{processing_id}] ❌ PDF extraction error: {str(e)}")
            return []

    def process_auctions_with_openai(self, auctions, pdf_file, processing_id):
        """Process auctions with OpenAI using EXACT same logic as process-complete.py"""
        try:
            # Initialize OpenAI
            openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            google_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
            
            # EXACT same auction fields from process-complete.py
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
            
            processed_auctions = []
            
            for i, auction in enumerate(auctions):
                # EXACT same prompt as process-complete.py
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
                
                try:
                    response = openai_client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON array."},
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=1500,
                        temperature=0.1
                    )
                    
                    # Parse response with improved error handling
                    content = response.choices[0].message.content.strip()
                    
                    # Remove markdown code block if present
                    if content.startswith('```json'):
                        content = content[7:]
                    elif content.startswith('```'):
                        content = content[3:]
                    
                    if content.endswith('```'):
                        content = content[:-3]
                    
                    content = content.strip()
                    
                    # Try to extract valid JSON even if malformed
                    try:
                        if content.startswith('['):
                            extracted_data = json.loads(content)
                            if isinstance(extracted_data, list) and len(extracted_data) > 0:
                                auction_data = extracted_data[0]
                            else:
                                raise ValueError("Empty array returned")
                        else:
                            auction_data = json.loads(content)
                    except json.JSONDecodeError as e:
                        # Try to fix common JSON issues
                        print(f"[{processing_id}] 🔧 Attempting JSON repair for auction {i+1}: {str(e)}")
                        
                        # Remove trailing commas and fix common issues
                        fixed_content = content
                        fixed_content = re.sub(r',(\s*[}\]])', r'\1', fixed_content)  # Remove trailing commas
                        fixed_content = re.sub(r'(["\'])\s*:\s*(["\'])([^"\']*)\2', r'\1: "\3"', fixed_content)  # Fix unquoted values
                        
                        try:
                            if fixed_content.startswith('['):
                                extracted_data = json.loads(fixed_content)
                                if isinstance(extracted_data, list) and len(extracted_data) > 0:
                                    auction_data = extracted_data[0]
                                else:
                                    raise ValueError("Empty array returned after repair")
                            else:
                                auction_data = json.loads(fixed_content)
                            print(f"[{processing_id}] ✅ JSON repair successful for auction {i+1}")
                        except:
                            # If all fails, create error data and continue
                            raise ValueError(f"Could not parse JSON even after repair: {str(e)[:200]}")
                    
                    # Add metadata (EXACT same as process-complete.py)
                    auction_data['gov_pdf_name'] = f"unprocessed/{pdf_file}"
                    auction_data['data_extraction_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    auction_data['pdf_file_name'] = pdf_file
                    
                    # Sheriff association (EXACT same logic)
                    sheriff_uuid = get_sheriff_uuid(auction_data.get('sheriff_office'))
                    auction_data['sheriff_uuid'] = sheriff_uuid
                    auction_data['sheriff_associated'] = is_sheriff_associated(sheriff_uuid)
                    
                    auction_data['auction_description'] = auction
                    auction_data['processed_nearby_sales'] = False
                    auction_data['online_auction'] = False
                    auction_data['is_streaming'] = False
                    
                    # Geocode addresses (EXACT same logic as process-complete.py)
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
                    
                    processed_auctions.append(auction_data)
                    
                except Exception as e:
                    print(f"[{processing_id}] ❌ OpenAI processing error for auction {i+1}: {str(e)}")
                    error_data = {
                        "case_number": f"ERROR_{i+1}",
                        "error": f"Processing failed: {str(e)}",
                        "raw_text": auction[:500] + "..." if len(auction) > 500 else auction
                    }
                    processed_auctions.append(error_data)
            
            return processed_auctions
            
        except Exception as e:
            print(f"[{processing_id}] ❌ OpenAI processing error: {str(e)}")
            return []

    def upload_to_supabase(self, processed_auctions, processing_id):
        """Upload auctions to Supabase using EXACT same logic as process-complete.py"""
        try:
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            upload_results = []
            
            for auction_data in processed_auctions:
                try:
                    # Remove auction_number if present (same as process-complete.py)
                    auction_data.pop('auction_number', None)
                    
                    headers = {
                        'apikey': supabase_key,
                        'Authorization': f'Bearer {supabase_key}',
                        'Content-Type': 'application/json'
                    }
                    
                    # Upload to auctions table
                    response = requests.post(
                        f"{supabase_url}/rest/v1/auctions",
                        headers=headers,
                        json=auction_data
                    )
                    
                    if response.status_code in [200, 201]:
                        upload_results.append({'status': 'success', 'case_number': auction_data.get('case_number')})
                        print(f"[{processing_id}] ✅ Uploaded: {auction_data.get('case_number')}")
                    else:
                        upload_results.append({
                            'status': 'error',
                            'case_number': auction_data.get('case_number'),
                            'error': f"HTTP {response.status_code}: {response.text}"
                        })
                        print(f"[{processing_id}] ❌ Upload failed: {auction_data.get('case_number')}")
                        
                except Exception as e:
                    upload_results.append({
                        'status': 'error',
                        'case_number': auction_data.get('case_number', 'unknown'),
                        'error': str(e)
                    })
                    
            return upload_results
            
        except Exception as e:
            print(f"[{processing_id}] ❌ Supabase upload error: {str(e)}")
            return []