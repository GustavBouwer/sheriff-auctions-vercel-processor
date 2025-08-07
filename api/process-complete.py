"""
Complete Sheriff Auctions ETL Pipeline with Fine-Tuned Prompt
Includes your optimized fields, cleaning, and Supabase upload
Based on /Users/gustavbouwer/D4/github/Development/Auction-Data-Extraction/__main__.py
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
    def do_GET(self):
        try:
            # Safety checks - FORCE 3 AUCTIONS FOR TESTING
            enable_processing = os.getenv('ENABLE_PROCESSING', 'false').lower() == 'true'
            max_auctions = 3  # HARDCODED TO 3 FOR TESTING
            max_tokens = int(os.getenv('MAX_OPENAI_TOKENS_PER_RUN', '100000'))
            
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
            
            # Clean text (improved to prevent JSON issues)
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
                # Replace multiple spaces and newlines with single space
                text = re.sub(r'\s+', ' ', text).strip()
                # Don't escape quotes here - let Python handle it in the prompt
                return text
            
            # Split into auctions (based on your original split_into_auctions)
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
            
            # Limit auctions for testing
            auctions = auctions[:max_auctions]
            
            # Your fine-tuned auction fields specification
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
            
            # Process each auction individually with your fine-tuned prompt
            processed_auctions = []
            total_tokens_used = 0
            
            for i, auction in enumerate(auctions):
                # Your exact fine-tuned prompt - with explicit NO MARKDOWN instruction
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
                    
                    total_tokens_used += response.usage.total_tokens
                    
                    # Parse response - handle markdown code blocks and JSON
                    content = response.choices[0].message.content.strip()
                    
                    # Remove markdown code block if present
                    if content.startswith('```json'):
                        content = content[7:]  # Remove ```json
                    elif content.startswith('```'):
                        content = content[3:]  # Remove ```
                    
                    if content.endswith('```'):
                        content = content[:-3]  # Remove trailing ```
                    
                    content = content.strip()
                    
                    # Debug: Log the cleaned response
                    print(f"Cleaned OpenAI response for auction {i+1}: {content[:200]}...")
                    
                    if content.startswith('['):
                        extracted_data = json.loads(content)
                        if isinstance(extracted_data, list) and len(extracted_data) > 0:
                            auction_data = extracted_data[0]  # Take first item from array
                        else:
                            raise ValueError("Empty array returned")
                    else:
                        auction_data = json.loads(content)
                    
                    # Add metadata matching Supabase schema
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
                    auction_data['auction_number'] = i + 1
                    processed_auctions.append(auction_data)
                    
                    # Check token limits
                    if total_tokens_used > max_tokens:
                        print(f"Token limit reached: {total_tokens_used}/{max_tokens}")
                        break
                    
                except Exception as e:
                    error_data = {
                        "auction_number": i + 1,
                        "case_number": f"ERROR_{i+1}",
                        "error": f"Processing failed: {str(e)}",
                        "raw_text": auction[:500] + "..." if len(auction) > 500 else auction
                    }
                    
                    # Try to include the OpenAI response if available
                    try:
                        if 'response' in locals() and response:
                            error_data["openai_response"] = response.choices[0].message.content[:500]
                    except:
                        pass
                    
                    processed_auctions.append(error_data)
            
            # Upload to Supabase if enabled
            upload_results = []
            if enable_processing:
                supabase_url = os.getenv('SUPABASE_URL')
                supabase_key = os.getenv('SUPABASE_KEY')
                
                if supabase_url and supabase_key:
                    for auction_data in processed_auctions:
                        if 'error' not in auction_data:
                            try:
                                # Create a copy without auction_number for upload
                                upload_data = auction_data.copy()
                                upload_data.pop('auction_number', None)  # Remove auction_number
                                
                                # Upload to Supabase auctions table
                                headers = {
                                    'apikey': supabase_key,
                                    'Authorization': f'Bearer {supabase_key}',
                                    'Content-Type': 'application/json',
                                    'Prefer': 'return=minimal'
                                }
                                
                                upload_url = f"{supabase_url}/rest/v1/auctions"
                                upload_response = requests.post(upload_url, json=upload_data, headers=headers)
                                
                                if upload_response.status_code in [200, 201]:
                                    upload_results.append({"case_number": auction_data.get('case_number'), "status": "success"})
                                else:
                                    upload_results.append({
                                        "case_number": auction_data.get('case_number'), 
                                        "status": "error",
                                        "error": upload_response.text,
                                        "status_code": upload_response.status_code
                                    })
                                    
                            except Exception as e:
                                upload_results.append({
                                    "case_number": auction_data.get('case_number'),
                                    "status": "error", 
                                    "error": str(e)
                                })
            
            # Prepare response
            response_data = {
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "processing_enabled": enable_processing,
                "pdf_key": pdf_key,
                "total_pages_processed": total_pages,
                "raw_text_length": len(raw_text),
                "cleaned_text_length": len(cleaned_text),
                "total_auctions_found": len(auctions),
                "auctions_processed": len(processed_auctions),
                "total_tokens_used": total_tokens_used,
                "estimated_cost": f"${total_tokens_used * 0.000002:.4f}",
                "supabase_uploads": upload_results if enable_processing else "Upload disabled - test mode",
                "auctions": processed_auctions
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, indent=2).encode())
            
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