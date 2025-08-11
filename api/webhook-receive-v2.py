"""
Stage 1: Fast PDF Reception & Auction Discovery
This endpoint receives PDFs, extracts text, splits into auctions,
and queues them for parallel processing without any OpenAI calls.
"""

from http.server import BaseHTTPRequestHandler
import json
import os
import boto3
import pdfplumber
from io import BytesIO
import re
from datetime import datetime
import requests
import time
import uuid
import traceback

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        """Handle webhook from Cloudflare Worker with new PDFs"""
        try:
            # Parse request
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            # Verify webhook secret
            webhook_secret = os.getenv('WEBHOOK_SECRET', 'sheriff-auctions-webhook-2025')
            if data.get('secret') != webhook_secret:
                print("❌ Invalid webhook secret")
                self.send_error(403, "Forbidden")
                return
            
            # Get PDF files from webhook
            pdf_files = data.get('pdf_files', [])
            if not pdf_files:
                print("⚠️ No PDF files in webhook payload")
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'status': 'no_files'}).encode())
                return
            
            print(f"📦 Stage 1: Received {len(pdf_files)} PDFs for processing")
            print(f"📁 PDF files: {pdf_files}")
            
            # Generate processing ID
            processing_id = f"{datetime.now().strftime('%H%M%S')}_{len(pdf_files)}PDFs"
            print(f"🏷️ Processing ID: {processing_id}")
            
            # Initialize R2 client
            r2_client = boto3.client('s3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # Process each PDF
            all_results = []
            total_batches_created = 0
            
            for pdf_index, pdf_file in enumerate(pdf_files, 1):
                print(f"\n[{processing_id}] 📄 Processing PDF {pdf_index}/{len(pdf_files)}: {pdf_file}")
                
                try:
                    # Stage 1.1: Download PDF from R2
                    pdf_key = f"unprocessed/{pdf_file}"
                    print(f"[{processing_id}] 📥 Downloading from R2: {pdf_key}")
                    
                    response = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
                    pdf_content = response['Body'].read()
                    print(f"[{processing_id}] ✅ Downloaded {len(pdf_content)} bytes")
                    
                    # Stage 1.2: Extract text from PDF
                    print(f"[{processing_id}] 📄 Extracting text from PDF...")
                    pdf_stream = BytesIO(pdf_content)
                    
                    raw_text = ""
                    pages_processed = 0
                    
                    with pdfplumber.open(pdf_stream) as pdf:
                        total_pages = len(pdf.pages)
                        print(f"[{processing_id}] 📃 PDF has {total_pages} pages")
                        
                        # Start from page 13 (index 12)
                        for i in range(12, total_pages):
                            page = pdf.pages[i]
                            page_text = page.extract_text() or ""
                            
                            # Check for PAUC section to stop
                            if "PUBLIC AUCTION UNDER COVID" in page_text or "PAUC" in page_text:
                                print(f"[{processing_id}] ⏹️ Found PAUC section on page {i+1}, stopping")
                                break
                            
                            raw_text += f"{page_text}\n"
                            pages_processed += 1
                    
                    print(f"[{processing_id}] ✅ Extracted {len(raw_text)} characters from {pages_processed} pages")
                    
                    # Stage 1.3: Clean and split text into auctions
                    print(f"[{processing_id}] ✂️ Splitting into individual auctions...")
                    
                    # Clean text
                    text = re.sub(r'\s+', ' ', raw_text)
                    text = re.sub(r'(\d+)\s+(\d+)', r'\1\2', text)
                    
                    # Find all auctions using Case No pattern
                    case_pattern = r'Case No:\s*[A-Z]?\d+(?:\/\d+)?'
                    matches = list(re.finditer(case_pattern, text, re.IGNORECASE))
                    
                    auctions = []
                    for i, match in enumerate(matches):
                        start = match.start()
                        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
                        
                        auction_text = text[start:end].strip()
                        if len(auction_text) > 100:  # Filter out too-short entries
                            auctions.append(auction_text)
                    
                    print(f"[{processing_id}] 📊 Found {len(auctions)} valid auctions")
                    
                    # Stage 1.4: Create batches of 25 auctions
                    BATCH_SIZE = 25
                    batches = [auctions[i:i + BATCH_SIZE] for i in range(0, len(auctions), BATCH_SIZE)]
                    print(f"[{processing_id}] 📦 Created {len(batches)} batches of up to {BATCH_SIZE} auctions")
                    
                    # Stage 1.5: Store batches in R2 and trigger processing
                    pdf_batch_results = []
                    
                    for batch_index, batch in enumerate(batches, 1):
                        # Generate unique batch ID
                        batch_id = f"{processing_id}_{pdf_file}_{batch_index}_{uuid.uuid4().hex[:8]}"
                        
                        # Store batch in R2 temp folder
                        batch_data = {
                            'batch_id': batch_id,
                            'pdf_file': pdf_file,
                            'batch_index': batch_index,
                            'total_batches': len(batches),
                            'auctions': batch,
                            'auction_count': len(batch),
                            'created_at': datetime.now().isoformat(),
                            'processing_id': processing_id
                        }
                        
                        # Upload to R2 temp storage
                        temp_key = f"auction-batches/{batch_id}.json"
                        print(f"[{processing_id}] 💾 Storing batch {batch_index}/{len(batches)} to R2: {temp_key}")
                        
                        r2_client.put_object(
                            Bucket=bucket_name,
                            Key=temp_key,
                            Body=json.dumps(batch_data).encode('utf-8'),
                            ContentType='application/json'
                        )
                        
                        # Trigger Stage 2 processing (async, don't wait)
                        try:
                            vercel_url = os.getenv('VERCEL_BASE_URL', 'https://sheriff-auctions-data-etl-zzd2.vercel.app')
                            process_url = f"{vercel_url}/api/process-auction-batch"
                            
                            print(f"[{processing_id}] 🚀 Triggering batch processing: {batch_id}")
                            
                            # Fire and forget - don't wait for response
                            requests.post(
                                process_url,
                                json={'batch_id': batch_id},
                                timeout=2  # Short timeout, just ensure request is sent
                            )
                            
                            pdf_batch_results.append({
                                'batch_id': batch_id,
                                'status': 'queued',
                                'auctions': len(batch)
                            })
                            
                        except requests.Timeout:
                            # This is expected - we don't wait for processing
                            pdf_batch_results.append({
                                'batch_id': batch_id,
                                'status': 'triggered',
                                'auctions': len(batch)
                            })
                        except Exception as trigger_error:
                            print(f"[{processing_id}] ⚠️ Failed to trigger batch {batch_id}: {trigger_error}")
                            pdf_batch_results.append({
                                'batch_id': batch_id,
                                'status': 'trigger_failed',
                                'auctions': len(batch),
                                'error': str(trigger_error)
                            })
                        
                        # Small delay between triggers to avoid overwhelming
                        time.sleep(0.5)
                        total_batches_created += 1
                    
                    all_results.append({
                        'pdf': pdf_file,
                        'status': 'success',
                        'auctions_found': len(auctions),
                        'batches_created': len(batches),
                        'batch_results': pdf_batch_results
                    })
                    
                except Exception as pdf_error:
                    print(f"[{processing_id}] ❌ Failed to process PDF {pdf_file}: {str(pdf_error)}")
                    print(f"[{processing_id}] Traceback: {traceback.format_exc()}")
                    all_results.append({
                        'pdf': pdf_file,
                        'status': 'error',
                        'error': str(pdf_error)
                    })
            
            # Return success response
            response_data = {
                'status': 'success',
                'processing_id': processing_id,
                'pdfs_processed': len(pdf_files),
                'total_batches_created': total_batches_created,
                'results': all_results,
                'timestamp': datetime.now().isoformat()
            }
            
            print(f"\n[{processing_id}] ✅ Stage 1 Complete: {total_batches_created} batches queued for processing")
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode())
            
        except Exception as e:
            print(f"❌ Stage 1 ERROR: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }).encode())