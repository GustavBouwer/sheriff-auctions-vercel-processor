"""
Stage 2: Auction Batch Processor
This endpoint processes a batch of 25 auctions with OpenAI and uploads to Supabase.
Designed to complete within 2-3 minutes, well under any timeout limits.
"""

from http.server import BaseHTTPRequestHandler
import json
import os
import boto3
from datetime import datetime
import requests
import openai
import traceback
import time

# Sheriff mapping functions (copied from webhook-process)
def get_sheriff_uuid(sheriff_office):
    """Get sheriff UUID from office name using JSON mapping"""
    if not sheriff_office:
        return os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')
    
    # Load sheriff mapping (you may want to cache this)
    sheriff_mapping = {
        "Alberton": "c5d67890-1234-5678-9012-3456789abcde",
        "Johannesburg Central": "23af5f09-eafb-4a6f-b970-1cfb3f614689",
        "Boksburg": "23af5f09-eafb-4a6f-b970-1cfb3f614689",
        # Add more mappings as needed
    }
    
    # Try exact match first
    if sheriff_office in sheriff_mapping:
        return sheriff_mapping[sheriff_office]
    
    # Try fuzzy match
    office_lower = sheriff_office.lower()
    for office, uuid in sheriff_mapping.items():
        if office.lower() in office_lower or office_lower in office.lower():
            return sheriff_mapping[office]
    
    # Return default if no match
    return os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')

def is_sheriff_associated(sheriff_uuid):
    """Check if sheriff UUID is associated (not default)"""
    default_uuid = os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')
    return sheriff_uuid != default_uuid

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        """Process a single batch of auctions"""
        try:
            # Parse request
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            batch_id = data.get('batch_id')
            if not batch_id:
                print("❌ No batch_id provided")
                self.send_error(400, "batch_id required")
                return
            
            print(f"📦 Stage 2: Processing batch {batch_id}")
            start_time = time.time()
            
            # Initialize clients
            r2_client = boto3.client('s3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # Initialize OpenAI
            openai_client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            
            # Stage 2.1: Retrieve batch from R2
            temp_key = f"auction-batches/{batch_id}.json"
            print(f"📥 Retrieving batch from R2: {temp_key}")
            
            try:
                response = r2_client.get_object(Bucket=bucket_name, Key=temp_key)
                batch_data = json.loads(response['Body'].read().decode('utf-8'))
                print(f"✅ Retrieved batch: {batch_data['auction_count']} auctions")
            except Exception as e:
                print(f"❌ Failed to retrieve batch: {e}")
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'status': 'error',
                    'error': f'Batch not found: {batch_id}'
                }).encode())
                return
            
            # Extract batch info
            auctions = batch_data['auctions']
            pdf_file = batch_data['pdf_file']
            batch_index = batch_data['batch_index']
            total_batches = batch_data['total_batches']
            processing_id = batch_data.get('processing_id', 'unknown')
            
            print(f"[{processing_id}] 📊 Batch {batch_index}/{total_batches} from {pdf_file}")
            print(f"[{processing_id}] 🤖 Processing {len(auctions)} auctions with OpenAI")
            
            # Stage 2.2: Process each auction with OpenAI
            processed_auctions = []
            failed_auctions = []
            total_tokens_used = 0
            
            # Auction fields specification (same as webhook-process.py)
            auction_fields = [
                {"column_name": "case_number", "data_type": "text", "allow_null": False, "additional_info": "The official case number for the auction, typically in the format '1234/2024'."},
                {"column_name": "court_name", "data_type": "text", "allow_null": True, "additional_info": "The name of the court where the case is filed (e.g., 'Gauteng Division, Pretoria')."},
                {"column_name": "plaintiff", "data_type": "text", "allow_null": True, "additional_info": "Name of the plaintiff or applicant in the case."},
                {"column_name": "defendant", "data_type": "text", "allow_null": True, "additional_info": "Name(s) of the defendant(s) or respondent(s) in the case."},
                {"column_name": "auction_date", "data_type": "date", "allow_null": True, "additional_info": "The date on which the auction will be held (e.g., '2025-01-28')."},
                {"column_name": "auction_time", "data_type": "time without time zone", "allow_null": True, "additional_info": "The time when the auction is scheduled to start (e.g., '11:00')."},
                {"column_name": "sheriff_office", "data_type": "text", "allow_null": True, "additional_info": "Name of the sheriff's office conducting the auction."},
                {"column_name": "sheriff_address", "data_type": "text", "allow_null": True, "additional_info": "Physical address of the sheriff's office or auction venue."},
                {"column_name": "erf_number", "data_type": "text", "allow_null": True, "additional_info": "ERF number or property identifier."},
                {"column_name": "township", "data_type": "text", "allow_null": True, "additional_info": "The township or area where the property is located."},
                {"column_name": "street_address", "data_type": "text", "allow_null": True, "additional_info": "Physical street address of the property being auctioned."},
                {"column_name": "reserve_price", "data_type": "bigint", "allow_null": True, "additional_info": "Minimum price required for the sale."},
                {"column_name": "bedrooms", "data_type": "bigint", "allow_null": True, "additional_info": "Number of bedrooms in the property."},
                {"column_name": "bathrooms", "data_type": "bigint", "allow_null": True, "additional_info": "Number of bathrooms in the property."},
                {"column_name": "conditions_of_sale", "data_type": "text", "allow_null": True, "additional_info": "Conditions of sale for the auction."}
            ]
            
            for auction_index, auction_text in enumerate(auctions, 1):
                try:
                    print(f"[{batch_id}] 🔄 Processing auction {auction_index}/{len(auctions)}")
                    
                    # Create OpenAI prompt
                    prompt = f"""Extract auction data from this text and return as JSON array with one object.

Field specifications:
{json.dumps(auction_fields, indent=2)}

IMPORTANT:
- Return JSON array with ONE object containing extracted values
- Use 'None' for missing text fields, 0 for missing numbers
- Use '2000-01-01' for missing dates, '00:00:00' for missing times
- Do NOT include markdown code blocks

Auction text:
{auction_text[:4000]}"""  # Limit text to avoid token issues
                    
                    # Call OpenAI
                    response = openai_client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON."},
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=800,
                        temperature=0.1
                    )
                    
                    total_tokens_used += response.usage.total_tokens
                    
                    # Parse response
                    content = response.choices[0].message.content.strip()
                    if content.startswith('```'):
                        content = content.split('```')[1]
                        if content.startswith('json'):
                            content = content[4:]
                    content = content.strip()
                    
                    # Parse JSON
                    if content.startswith('['):
                        extracted_data = json.loads(content)
                        auction_data = extracted_data[0] if isinstance(extracted_data, list) else extracted_data
                    else:
                        auction_data = json.loads(content)
                    
                    # Add metadata
                    auction_data['gov_pdf_name'] = pdf_file
                    auction_data['data_extraction_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    auction_data['pdf_file_name'] = pdf_file
                    auction_data['batch_id'] = batch_id
                    auction_data['auction_description'] = auction_text[:2000]  # Truncate for storage
                    
                    # Sheriff association
                    sheriff_uuid = get_sheriff_uuid(auction_data.get('sheriff_office'))
                    auction_data['sheriff_uuid'] = sheriff_uuid
                    auction_data['sheriff_associated'] = is_sheriff_associated(sheriff_uuid)
                    
                    # Set defaults
                    auction_data['processed_nearby_sales'] = False
                    auction_data['online_auction'] = False
                    auction_data['is_streaming'] = False
                    
                    processed_auctions.append(auction_data)
                    print(f"[{batch_id}] ✅ Auction {auction_index} extracted successfully")
                    
                except Exception as e:
                    print(f"[{batch_id}] ❌ Failed to process auction {auction_index}: {e}")
                    failed_auctions.append({
                        'index': auction_index,
                        'error': str(e),
                        'text_preview': auction_text[:200]
                    })
            
            # Stage 2.3: Upload to Supabase
            print(f"[{batch_id}] 📤 Uploading {len(processed_auctions)} auctions to Supabase")
            
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            upload_success = 0
            upload_failed = 0
            
            for auction_data in processed_auctions:
                try:
                    # Remove batch_id before upload (not in schema)
                    upload_data = auction_data.copy()
                    upload_data.pop('batch_id', None)
                    
                    headers = {
                        'apikey': supabase_key,
                        'Authorization': f'Bearer {supabase_key}',
                        'Content-Type': 'application/json',
                        'Prefer': 'return=minimal'
                    }
                    
                    response = requests.post(
                        f"{supabase_url}/rest/v1/auctions",
                        json=upload_data,
                        headers=headers
                    )
                    
                    if response.status_code in [200, 201]:
                        upload_success += 1
                    else:
                        upload_failed += 1
                        print(f"[{batch_id}] Upload failed: {response.status_code} - {response.text[:200]}")
                        
                except Exception as e:
                    upload_failed += 1
                    print(f"[{batch_id}] Upload error: {e}")
            
            # Stage 2.4: Cleanup batch from R2
            try:
                print(f"[{batch_id}] 🗑️ Cleaning up batch from R2")
                r2_client.delete_object(Bucket=bucket_name, Key=temp_key)
            except Exception as e:
                print(f"[{batch_id}] ⚠️ Failed to cleanup batch: {e}")
            
            # Calculate processing time
            elapsed_time = time.time() - start_time
            
            # Return response
            response_data = {
                'status': 'success',
                'batch_id': batch_id,
                'pdf_file': pdf_file,
                'batch_index': batch_index,
                'total_batches': total_batches,
                'auctions_processed': len(processed_auctions),
                'auctions_failed': len(failed_auctions),
                'upload_success': upload_success,
                'upload_failed': upload_failed,
                'tokens_used': total_tokens_used,
                'estimated_cost': f"${total_tokens_used * 0.000002:.4f}",
                'processing_time': f"{elapsed_time:.1f} seconds",
                'timestamp': datetime.now().isoformat()
            }
            
            print(f"[{batch_id}] ✅ Batch complete in {elapsed_time:.1f}s - {upload_success} uploaded")
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode())
            
        except Exception as e:
            print(f"❌ Stage 2 ERROR: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'status': 'error',
                'error': str(e),
                'batch_id': batch_id if 'batch_id' in locals() else None,
                'timestamp': datetime.now().isoformat()
            }).encode())