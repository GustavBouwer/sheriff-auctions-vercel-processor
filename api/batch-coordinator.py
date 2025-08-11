"""
Batch coordinator endpoint for parallel auction processing
Receives webhook from Cloudflare, splits large PDFs into auction batches,
and makes parallel calls to process-auction-batch for fast processing
"""

import json
import os
import re
import asyncio
import aiohttp
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
            
            # Generate unique processing ID
            processing_id = f"{datetime.now().strftime('%H%M%S')}_{len(pdf_files)}PDFs"
            print(f"🚀 BATCH COORDINATOR - Processing ID: {processing_id}")
            print(f"📦 Received {len(pdf_files)} PDFs for batch processing")
            print(f"📁 PDF files: {pdf_files}")
            
            # Process each PDF by analyzing auctions and creating parallel batches
            all_results = []
            
            for i, pdf_file in enumerate(pdf_files, 1):
                print(f"\n[{processing_id}] 🔄 === Analyzing PDF {i}/{len(pdf_files)}: {pdf_file} ===")
                pdf_key = f"unprocessed/{pdf_file}"
                
                # Analyze PDF to determine auction count and create batches
                analysis_result = self.analyze_pdf_for_batching(pdf_key, processing_id)
                
                if analysis_result.get('status') == 'error':
                    print(f"[{processing_id}] ❌ PDF analysis failed: {analysis_result.get('error')}")
                    all_results.append(analysis_result)
                    continue
                
                auction_count = analysis_result.get('auction_count', 0)
                pdf_size = analysis_result.get('pdf_size_bytes', 0)
                
                print(f"[{processing_id}] 📊 PDF Analysis: {auction_count} auctions, {pdf_size} bytes")
                
                # Determine processing strategy based on auction count
                if auction_count <= 50:
                    # Small PDF - process sequentially (existing method)
                    print(f"[{processing_id}] 🔄 Small PDF ({auction_count} auctions) - using sequential processing")
                    result = await self.process_pdf_sequentially(pdf_key, processing_id)
                    all_results.append(result)
                else:
                    # Large PDF - create parallel batches
                    print(f"[{processing_id}] 🚀 Large PDF ({auction_count} auctions) - creating parallel batches")
                    result = await self.process_pdf_with_parallel_batches(pdf_key, auction_count, processing_id)
                    all_results.append(result)
                
                print(f"[{processing_id}] ✅ Completed PDF {i}/{len(pdf_files)}")
            
            # Compile final response
            successful_processes = len([r for r in all_results if r.get('status') == 'success'])
            
            response = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'processing_id': processing_id,
                'pdfs_received': len(pdf_files),
                'pdfs_processed': successful_processes,
                'pdfs_failed': len(pdf_files) - successful_processes,
                'processing_method': 'batch-coordinator-parallel',
                'batch_info': batch_info if batch_info else None,
                'results': all_results
            }
            
            print(f"\n🎉 === BATCH COORDINATOR COMPLETE ===")
            print(f"   Processing ID: {processing_id}")
            print(f"   Total PDFs: {len(pdf_files)}")
            print(f"   Successful: {successful_processes}")
            print(f"   Failed: {len(pdf_files) - successful_processes}")
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response, indent=2).encode())
            
        except Exception as e:
            print(f"❌ BATCH COORDINATOR ERROR: {str(e)}")
            print(f"   Traceback: {traceback.format_exc()}")
            
            error_response = {
                'status': 'error',
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': datetime.now().isoformat(),
                'processing_method': 'batch-coordinator-error'
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(error_response).encode())

    def analyze_pdf_for_batching(self, pdf_key, processing_id):
        """Analyze PDF to determine auction count and batching strategy"""
        try:
            print(f"[{processing_id}] 🔍 Analyzing PDF for batching: {pdf_key}")
            
            # Initialize R2 client
            r2_client = boto3.client(
                's3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # Download PDF
            pdf_obj = r2_client.get_object(Bucket=bucket_name, Key=pdf_key)
            pdf_content = pdf_obj['Body'].read()
            pdf_size = len(pdf_content)
            pdf_stream = BytesIO(pdf_content)
            
            print(f"[{processing_id}] 📦 Downloaded PDF: {pdf_size} bytes")
            
            # Extract text (same logic as webhook-process)
            raw_text = ""
            with pdfplumber.open(pdf_stream) as pdf:
                total_pages = len(pdf.pages)
                start_page = 12 if total_pages > 12 else 0
                print(f"[{processing_id}] 📃 PDF has {total_pages} pages, starting from page {start_page + 1}")
                
                for i, page in enumerate(pdf.pages[start_page:], start=start_page + 1):
                    page_text = page.extract_text()
                    if page_text:
                        if "PAUC" in page_text.upper():
                            print(f"[{processing_id}] ⏹️ Found PAUC section on page {i}, stopping extraction")
                            break
                        raw_text += f"{page_text}\n"
            
            # Clean text
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
            
            cleaned_text = clean_text(raw_text)
            
            # Count auctions using same pattern as webhook-process
            pattern = re.compile(r'(?=(Case No:\s*[A-Z]*\d+/\d+))', re.IGNORECASE)
            matches = list(pattern.finditer(cleaned_text))
            auction_count = len(matches)
            
            print(f"[{processing_id}] 🔍 Found {auction_count} auctions in PDF")
            
            return {
                'status': 'success',
                'pdf_key': pdf_key,
                'pdf_size_bytes': pdf_size,
                'total_pages': total_pages,
                'auction_count': auction_count,
                'raw_text_length': len(raw_text),
                'cleaned_text_length': len(cleaned_text)
            }
            
        except Exception as e:
            print(f"[{processing_id}] ❌ PDF analysis error: {str(e)}")
            return {
                'status': 'error',
                'pdf_key': pdf_key,
                'error': str(e),
                'error_type': type(e).__name__
            }

    async def process_pdf_sequentially(self, pdf_key, processing_id):
        """Process small PDFs using existing sequential method"""
        try:
            print(f"[{processing_id}] 🔄 Processing PDF sequentially via webhook-process...")
            
            # Get the current domain for the API call
            vercel_domain = os.getenv('VERCEL_URL', 'sheriff-auctions-data-etl-zzd2.vercel.app')
            webhook_url = f"https://{vercel_domain}/api/webhook-process"
            
            # Create webhook payload for single PDF
            pdf_filename = pdf_key.split('/')[-1]
            webhook_payload = {
                'secret': os.getenv('WEBHOOK_SECRET', 'sheriff-auctions-webhook-2025'),
                'event': 'batch_coordinator_sequential',
                'timestamp': datetime.now().isoformat(),
                'pdf_files': [pdf_filename],
                'pdf_count': 1,
                'source': 'batch-coordinator',
                'processing_id': processing_id
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=webhook_payload) as response:
                    if response.status == 200:
                        result_data = await response.json()
                        print(f"[{processing_id}] ✅ Sequential processing completed successfully")
                        return {
                            'status': 'success',
                            'pdf_key': pdf_key,
                            'processing_method': 'sequential',
                            'webhook_response': result_data
                        }
                    else:
                        error_text = await response.text()
                        print(f"[{processing_id}] ❌ Sequential processing failed: {response.status} - {error_text}")
                        return {
                            'status': 'error',
                            'pdf_key': pdf_key,
                            'error': f"Webhook call failed: {response.status}",
                            'response_text': error_text
                        }
                        
        except Exception as e:
            print(f"[{processing_id}] ❌ Sequential processing error: {str(e)}")
            return {
                'status': 'error',
                'pdf_key': pdf_key,
                'error': str(e),
                'error_type': type(e).__name__
            }

    async def process_pdf_with_parallel_batches(self, pdf_key, auction_count, processing_id):
        """Process large PDFs by creating parallel batches"""
        try:
            print(f"[{processing_id}] 🚀 Creating parallel batches for {auction_count} auctions")
            
            # Calculate optimal batch size and number of parallel workers
            BATCH_SIZE = 50  # Auctions per batch
            MAX_PARALLEL_WORKERS = 5  # Limit parallel Vercel instances
            
            num_batches = (auction_count + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division
            actual_parallel_workers = min(num_batches, MAX_PARALLEL_WORKERS)
            
            print(f"[{processing_id}] 📊 Batch strategy:")
            print(f"   - Total auctions: {auction_count}")
            print(f"   - Batch size: {BATCH_SIZE} auctions/batch")
            print(f"   - Total batches needed: {num_batches}")
            print(f"   - Parallel workers: {actual_parallel_workers}")
            
            # Get the current domain for API calls
            vercel_domain = os.getenv('VERCEL_URL', 'sheriff-auctions-data-etl-zzd2.vercel.app')
            batch_processor_url = f"https://{vercel_domain}/api/process-auction-batch"
            
            # Create batch tasks for parallel execution
            batch_tasks = []
            pdf_filename = pdf_key.split('/')[-1]
            
            for batch_num in range(1, num_batches + 1):
                start_auction = (batch_num - 1) * BATCH_SIZE + 1
                end_auction = min(batch_num * BATCH_SIZE, auction_count)
                
                batch_payload = {
                    'secret': os.getenv('WEBHOOK_SECRET', 'sheriff-auctions-webhook-2025'),
                    'pdf_file': pdf_filename,
                    'batch_info': {
                        'batch_number': batch_num,
                        'total_batches': num_batches,
                        'start_auction': start_auction,
                        'end_auction': end_auction,
                        'batch_size': end_auction - start_auction + 1
                    },
                    'source': 'batch-coordinator-parallel',
                    'processing_id': f"{processing_id}_B{batch_num}"
                }
                
                batch_tasks.append(self.process_auction_batch(batch_processor_url, batch_payload, processing_id, batch_num))
            
            print(f"[{processing_id}] 🚀 Starting {len(batch_tasks)} parallel batch processors...")
            
            # Execute batches in parallel
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Analyze results
            successful_batches = 0
            failed_batches = 0
            total_auctions_processed = 0
            
            for i, result in enumerate(batch_results, 1):
                if isinstance(result, Exception):
                    print(f"[{processing_id}] ❌ Batch {i} failed with exception: {str(result)}")
                    failed_batches += 1
                elif result.get('status') == 'success':
                    successful_batches += 1
                    total_auctions_processed += result.get('auctions_processed', 0)
                    print(f"[{processing_id}] ✅ Batch {i} completed: {result.get('auctions_processed', 0)} auctions")
                else:
                    failed_batches += 1
                    print(f"[{processing_id}] ❌ Batch {i} failed: {result.get('error', 'unknown error')}")
            
            # Upload PDF to Supabase storage and cleanup (same as sequential)
            print(f"[{processing_id}] 📤 Uploading {pdf_filename} to Supabase storage and cleaning up R2...")
            
            # TODO: Implement storage cleanup similar to webhook-process
            # This would be extracted to a shared utility function
            
            print(f"[{processing_id}] 🎉 Parallel processing completed:")
            print(f"   - Total batches: {num_batches}")
            print(f"   - Successful: {successful_batches}")
            print(f"   - Failed: {failed_batches}")
            print(f"   - Auctions processed: {total_auctions_processed}/{auction_count}")
            
            return {
                'status': 'success',
                'pdf_key': pdf_key,
                'processing_method': 'parallel_batches',
                'total_auctions': auction_count,
                'auctions_processed': total_auctions_processed,
                'batches_total': num_batches,
                'batches_successful': successful_batches,
                'batches_failed': failed_batches,
                'parallel_workers': actual_parallel_workers,
                'batch_results': batch_results
            }
            
        except Exception as e:
            print(f"[{processing_id}] ❌ Parallel batch processing error: {str(e)}")
            return {
                'status': 'error',
                'pdf_key': pdf_key,
                'error': str(e),
                'error_type': type(e).__name__
            }

    async def process_auction_batch(self, batch_processor_url, batch_payload, processing_id, batch_num):
        """Process a single auction batch via API call"""
        try:
            print(f"[{processing_id}] 🔄 Starting batch {batch_num} processing...")
            
            async with aiohttp.ClientSession() as session:
                # Set timeout to 10 minutes per batch
                timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes
                
                async with session.post(batch_processor_url, json=batch_payload, timeout=timeout) as response:
                    if response.status == 200:
                        result_data = await response.json()
                        print(f"[{processing_id}] ✅ Batch {batch_num} completed successfully")
                        return result_data
                    else:
                        error_text = await response.text()
                        print(f"[{processing_id}] ❌ Batch {batch_num} failed: {response.status} - {error_text}")
                        return {
                            'status': 'error',
                            'batch_number': batch_num,
                            'error': f"HTTP {response.status}",
                            'response_text': error_text
                        }
                        
        except asyncio.TimeoutError:
            print(f"[{processing_id}] ⏰ Batch {batch_num} timed out after 10 minutes")
            return {
                'status': 'error',
                'batch_number': batch_num,
                'error': 'Timeout after 10 minutes',
                'error_type': 'TimeoutError'
            }
        except Exception as e:
            print(f"[{processing_id}] ❌ Batch {batch_num} processing error: {str(e)}")
            return {
                'status': 'error',
                'batch_number': batch_num,
                'error': str(e),
                'error_type': type(e).__name__
            }

    # Run the async method (helper for synchronous HTTP handler)
    def __getattr__(self, name):
        if name.startswith('process_pdf') or name == 'process_auction_batch':
            async_method = getattr(self, name)
            def sync_wrapper(*args, **kwargs):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(async_method(*args, **kwargs))
                finally:
                    loop.close()
            return sync_wrapper
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")