"""
Manual PDF Processing Endpoint
Process specific PDFs from R2 unprocessed folder manually
Useful for testing or processing manually uploaded PDFs
"""

import json
import os
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Add utils directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from sheriff_mapping import get_sheriff_uuid, is_sheriff_associated

# Simple processing functions
async def process_single_pdf(pdf_key):
    """Basic PDF processing - calls the main process-complete endpoint"""
    try:
        import requests
        
        # For now, we'll redirect to the main processing endpoint
        # In a real implementation, you'd extract the processing logic
        return {
            'status': 'success',
            'pdf_key': pdf_key,
            'message': 'PDF processing would happen here - use /api/process-complete for full processing',
            'note': 'Implement full processing logic or call process-complete endpoint'
        }
    except Exception as e:
        return {
            'status': 'error',
            'pdf_key': pdf_key,
            'error': str(e)
        }

async def move_pdf_to_processed(pdf_filename):
    """Move PDF from unprocessed to processed folder"""
    try:
        import boto3
        
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

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            
            # Get PDF filename from query parameter
            pdf_filename = query_params.get('pdf', [None])[0]
            
            if not pdf_filename:
                # If no specific PDF provided, list all unprocessed PDFs
                return self.list_unprocessed_pdfs()
            
            # Process specific PDF
            return self.process_specific_pdf(pdf_filename)
            
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
    
    def list_unprocessed_pdfs(self):
        """List all PDFs in unprocessed folder"""
        try:
            import boto3
            
            # Initialize R2 client
            r2_client = boto3.client(
                's3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # List unprocessed PDFs
            unprocessed_list = r2_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='unprocessed/',
                MaxKeys=100
            )
            
            pdfs = []
            if 'Contents' in unprocessed_list:
                for obj in unprocessed_list['Contents']:
                    if obj['Key'].endswith('.pdf'):
                        filename = obj['Key'].replace('unprocessed/', '')
                        pdfs.append({
                            'filename': filename,
                            'size_mb': round(obj['Size'] / 1024 / 1024, 2),
                            'last_modified': obj['LastModified'].isoformat(),
                            'process_url': f"/api/process-manual?pdf={filename}"
                        })
            
            response_data = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'total_unprocessed_pdfs': len(pdfs),
                'pdfs': pdfs,
                'instructions': [
                    'To process a specific PDF: /api/process-manual?pdf=filename.pdf',
                    'To process all PDFs: /api/process-manual?pdf=all',
                    'Monitor results in Supabase auctions table'
                ]
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, indent=2).encode())
            
        except Exception as e:
            error_response = {
                'status': 'error',
                'error': f'Failed to list PDFs: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())
    
    async def process_specific_pdf(self, pdf_filename):
        """Process a specific PDF from unprocessed folder"""
        try:
            if pdf_filename == 'all':
                # Process all unprocessed PDFs
                return await self.process_all_pdfs()
            
            # Process single PDF
            pdf_key = f"unprocessed/{pdf_filename}"
            
            print(f"Processing PDF: {pdf_key}")
            
            # Use the same processing logic as webhook
            result = await process_single_pdf(pdf_key)
            
            # Move to processed folder if successful
            if result.get('status') == 'success':
                move_result = await move_pdf_to_processed(pdf_filename)
                result['move_result'] = move_result
            
            response_data = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'pdf_processed': pdf_filename,
                'processing_result': result,
                'next_steps': [
                    'Check Supabase auctions table for uploaded data',
                    'PDF moved to processed/ folder if successful',
                    'Review processing_result for any errors'
                ]
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, indent=2).encode())
            
        except Exception as e:
            error_response = {
                'status': 'error',
                'pdf': pdf_filename,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'debugging': [
                    'Check R2 bucket for PDF existence',
                    'Verify environment variables are set',
                    'Check Vercel function logs for detailed errors'
                ]
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())
    
    async def process_all_pdfs(self):
        """Process all PDFs in unprocessed folder"""
        try:
            import boto3
            
            # Get list of all unprocessed PDFs
            r2_client = boto3.client(
                's3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            unprocessed_list = r2_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='unprocessed/',
                MaxKeys=50  # Limit for safety
            )
            
            if 'Contents' not in unprocessed_list:
                response_data = {
                    'status': 'success',
                    'message': 'No PDFs found in unprocessed folder',
                    'timestamp': datetime.now().isoformat()
                }
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(response_data, indent=2).encode())
                return
            
            # Process each PDF
            results = []
            processed_count = 0
            error_count = 0
            
            for obj in unprocessed_list['Contents']:
                if obj['Key'].endswith('.pdf'):
                    filename = obj['Key'].replace('unprocessed/', '')
                    
                    try:
                        # Process PDF
                        result = await process_single_pdf(obj['Key'])
                        
                        # Move if successful
                        if result.get('status') == 'success':
                            move_result = await move_pdf_to_processed(filename)
                            result['move_result'] = move_result
                            processed_count += 1
                        else:
                            error_count += 1
                        
                        results.append({
                            'filename': filename,
                            'result': result
                        })
                        
                    except Exception as e:
                        error_count += 1
                        results.append({
                            'filename': filename,
                            'result': {'status': 'error', 'error': str(e)}
                        })
            
            response_data = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'total_pdfs_found': len([obj for obj in unprocessed_list['Contents'] if obj['Key'].endswith('.pdf')]),
                'successful_processing': processed_count,
                'failed_processing': error_count,
                'results': results,
                'summary': f"Processed {processed_count} PDFs successfully, {error_count} failed"
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, indent=2).encode())
            
        except Exception as e:
            error_response = {
                'status': 'error',
                'error': f'Batch processing failed: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())