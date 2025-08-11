"""
Enhanced webhook coordinator that receives PDFs from Cloudflare and processes them in parallel batches
Each batch contains 25 auctions processed by separate Vercel instances
"""

import json
import os
import re
import requests
import concurrent.futures
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from io import BytesIO
import boto3
import pdfplumber
import traceback

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Get webhook payload
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            webhook_data = json.loads(post_data.decode('utf-8'))
            
            # Validate webhook secret
            webhook_secret = os.getenv('WEBHOOK_SECRET', 'sheriff-auctions-webhook-2025')
            if webhook_data.get('secret') != webhook_secret:
                self.send_response(401)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Unauthorized'}).encode())
                return
            
            # Get PDF files from webhook
            pdf_files = webhook_data.get('pdf_files', [])
            
            if not pdf_files:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'No PDF files provided'}).encode())
                return
            
            # Generate unique processing ID
            processing_id = f"{datetime.now().strftime('%H%M%S')}_{len(pdf_files)}PDFs"
            print(f"🚀 WEBHOOK COORDINATOR - Processing ID: {processing_id}")
            print(f"📦 Received {len(pdf_files)} PDFs for parallel batch processing")
            
            # Process all PDFs
            all_results = []
            for pdf_file in pdf_files:
                print(f"\n[{processing_id}] 📄 Processing PDF: {pdf_file}")
                result = self.process_pdf_with_batches(pdf_file, processing_id)
                all_results.append(result)
            
            # Compile response
            successful = len([r for r in all_results if r.get('status') == 'success'])
            response = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'processing_id': processing_id,
                'pdfs_received': len(pdf_files),
                'pdfs_processed': successful,
                'pdfs_failed': len(pdf_files) - successful,
                'results': all_results
            }
            
            print(f"\n🎉 WEBHOOK COORDINATOR COMPLETE")
            print(f"   Processing ID: {processing_id}")
            print(f"   Total PDFs: {len(pdf_files)}")
            print(f"   Successful: {successful}")
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response, indent=2).encode())
            
        except Exception as e:
            print(f"❌ WEBHOOK COORDINATOR ERROR: {str(e)}")
            print(f"   Traceback: {traceback.format_exc()}")
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }).encode())

    def process_pdf_with_batches(self, pdf_file, processing_id):
        """Process a single PDF by splitting into 25-auction batches"""
        try:
            pdf_key = f"unprocessed/{pdf_file}"
            
            # Analyze PDF to get auction count
            print(f"[{processing_id}] 🔍 Analyzing PDF: {pdf_key}")
            analysis = self.analyze_pdf(pdf_key, processing_id)
            
            if analysis.get('status') == 'error':
                return analysis
            
            auction_count = analysis.get('auction_count', 0)
            print(f"[{processing_id}] 📊 Found {auction_count} auctions")
            
            if auction_count == 0:
                return {
                    'status': 'error',
                    'pdf_file': pdf_file,
                    'error': 'No auctions found in PDF'
                }
            
            # Calculate batches (25 auctions per batch)
            BATCH_SIZE = 25
            num_batches = (auction_count + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"[{processing_id}] 🎯 Creating {num_batches} batches of {BATCH_SIZE} auctions each")
            
            # Prepare batch requests
            batch_requests = []
            for batch_num in range(1, num_batches + 1):
                start_idx = (batch_num - 1) * BATCH_SIZE
                end_idx = min(batch_num * BATCH_SIZE, auction_count)
                
                batch_requests.append({
                    'batch_number': batch_num,
                    'start_auction': start_idx + 1,
                    'end_auction': end_idx,
                    'pdf_file': pdf_file,
                    'processing_id': f"{processing_id}_B{batch_num}"
                })
            
            # Process batches in parallel using ThreadPoolExecutor
            print(f"[{processing_id}] 🚀 Launching {num_batches} parallel batch processors...")
            
            batch_results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # Submit all batch processing tasks
                futures = []
                for batch_req in batch_requests:
                    future = executor.submit(self.process_single_batch, batch_req)
                    futures.append((batch_req['batch_number'], future))
                
                # Collect results as they complete
                for batch_num, future in futures:
                    try:
                        result = future.result(timeout=300)  # 5 minute timeout per batch
                        batch_results.append(result)
                        if result.get('status') == 'success':
                            print(f"[{processing_id}] ✅ Batch {batch_num}/{num_batches} completed")
                        else:
                            print(f"[{processing_id}] ❌ Batch {batch_num}/{num_batches} failed")
                    except concurrent.futures.TimeoutError:
                        print(f"[{processing_id}] ⏰ Batch {batch_num}/{num_batches} timed out")
                        batch_results.append({
                            'status': 'error',
                            'batch_number': batch_num,
                            'error': 'Batch processing timeout'
                        })
                    except Exception as e:
                        print(f"[{processing_id}] ❌ Batch {batch_num}/{num_batches} error: {str(e)}")
                        batch_results.append({
                            'status': 'error',
                            'batch_number': batch_num,
                            'error': str(e)
                        })
            
            # Aggregate results
            successful_batches = len([r for r in batch_results if r.get('status') == 'success'])
            total_processed = sum(r.get('auctions_processed', 0) for r in batch_results)
            
            print(f"[{processing_id}] 📊 PDF Complete: {successful_batches}/{num_batches} batches, {total_processed}/{auction_count} auctions")
            
            return {
                'status': 'success' if successful_batches > 0 else 'error',
                'pdf_file': pdf_file,
                'auction_count': auction_count,
                'auctions_processed': total_processed,
                'batches_total': num_batches,
                'batches_successful': successful_batches,
                'batch_results': batch_results
            }
            
        except Exception as e:
            print(f"[{processing_id}] ❌ PDF processing error: {str(e)}")
            return {
                'status': 'error',
                'pdf_file': pdf_file,
                'error': str(e)
            }

    def analyze_pdf(self, pdf_key, processing_id):
        """Analyze PDF to count auctions"""
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
            
            # Download and analyze PDF
            pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
            pdf_content = pdf_obj['Body'].read()
            pdf_stream = BytesIO(pdf_content)
            
            # Extract text and count auctions
            raw_text = ""
            with pdfplumber.open(pdf_stream) as pdf:
                total_pages = len(pdf.pages)
                start_page = 12 if total_pages > 12 else 0
                
                for page in pdf.pages[start_page:]:
                    page_text = page.extract_text()
                    if page_text:
                        if "PAUC" in page_text.upper():
                            break
                        raw_text += f"{page_text}\n"
            
            # Count auctions using case number pattern
            pattern = re.compile(r'(?=(Case No:\s*[A-Z]*\d+/\d+))', re.IGNORECASE)
            matches = list(pattern.finditer(raw_text))
            auction_count = len(matches)
            
            return {
                'status': 'success',
                'pdf_key': pdf_key,
                'total_pages': total_pages,
                'auction_count': auction_count,
                'text_length': len(raw_text)
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'pdf_key': pdf_key,
                'error': str(e)
            }

    def process_single_batch(self, batch_request):
        """Process a single batch of auctions via API call"""
        try:
            # Prepare the request to process-auction-batch endpoint
            vercel_url = os.getenv('VERCEL_URL', 'sheriff-auctions-data-etl-zzd2.vercel.app')
            batch_endpoint = f"https://{vercel_url}/api/process-auction-batch"
            
            payload = {
                'secret': os.getenv('WEBHOOK_SECRET', 'sheriff-auctions-webhook-2025'),
                'pdf_file': batch_request['pdf_file'],
                'batch_info': {
                    'batch_number': batch_request['batch_number'],
                    'start_auction': batch_request['start_auction'],
                    'end_auction': batch_request['end_auction']
                },
                'processing_id': batch_request['processing_id']
            }
            
            # Make HTTP request to process batch
            response = requests.post(batch_endpoint, json=payload, timeout=300)
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    'status': 'error',
                    'batch_number': batch_request['batch_number'],
                    'error': f"HTTP {response.status_code}: {response.text}"
                }
                
        except requests.Timeout:
            return {
                'status': 'error',
                'batch_number': batch_request['batch_number'],
                'error': 'Request timeout after 5 minutes'
            }
        except Exception as e:
            return {
                'status': 'error',
                'batch_number': batch_request['batch_number'],
                'error': str(e)
            }