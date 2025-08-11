"""
Batch Monitor: Check the status of auction batch processing
"""

from http.server import BaseHTTPRequestHandler
import json
import os
import boto3
from datetime import datetime, timedelta

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Monitor batch processing status"""
        try:
            # Initialize R2 client
            r2_client = boto3.client('s3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # List all pending batches in R2
            print("📊 Checking batch processing status...")
            
            pending_batches = []
            completed_count = 0
            
            try:
                # List objects in auction-batches folder
                response = r2_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix='auction-batches/'
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        # Get batch details
                        batch_key = obj['Key']
                        batch_id = batch_key.replace('auction-batches/', '').replace('.json', '')
                        
                        # Get batch data
                        try:
                            batch_response = r2_client.get_object(Bucket=bucket_name, Key=batch_key)
                            batch_data = json.loads(batch_response['Body'].read().decode('utf-8'))
                            
                            # Calculate age
                            created_at = datetime.fromisoformat(batch_data['created_at'])
                            age_minutes = (datetime.now() - created_at).total_seconds() / 60
                            
                            pending_batches.append({
                                'batch_id': batch_id,
                                'pdf_file': batch_data.get('pdf_file'),
                                'batch_index': batch_data.get('batch_index'),
                                'total_batches': batch_data.get('total_batches'),
                                'auction_count': batch_data.get('auction_count'),
                                'created_at': batch_data.get('created_at'),
                                'age_minutes': round(age_minutes, 1),
                                'status': 'pending' if age_minutes < 5 else 'stale'
                            })
                        except Exception as e:
                            print(f"Error reading batch {batch_id}: {e}")
                            
            except Exception as e:
                print(f"Error listing batches: {e}")
            
            # Sort by age
            pending_batches.sort(key=lambda x: x['age_minutes'])
            
            # Check unprocessed PDFs
            unprocessed_pdfs = []
            try:
                response = r2_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix='unprocessed/'
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        pdf_name = obj['Key'].replace('unprocessed/', '')
                        if pdf_name and not pdf_name.endswith('/'):
                            unprocessed_pdfs.append({
                                'name': pdf_name,
                                'size_mb': round(obj['Size'] / (1024 * 1024), 2),
                                'last_modified': obj['LastModified'].isoformat() if hasattr(obj['LastModified'], 'isoformat') else str(obj['LastModified'])
                            })
            except Exception as e:
                print(f"Error listing unprocessed PDFs: {e}")
            
            # Build status report
            status_report = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'batch_processing': {
                    'pending_batches': len(pending_batches),
                    'stale_batches': len([b for b in pending_batches if b['status'] == 'stale']),
                    'total_pending_auctions': sum(b['auction_count'] for b in pending_batches),
                    'batches': pending_batches[:10]  # Show first 10
                },
                'unprocessed_pdfs': {
                    'count': len(unprocessed_pdfs),
                    'pdfs': unprocessed_pdfs
                },
                'recommendations': []
            }
            
            # Add recommendations
            if len(pending_batches) > 20:
                status_report['recommendations'].append("High number of pending batches - processing may be slow")
            
            stale_count = len([b for b in pending_batches if b['status'] == 'stale'])
            if stale_count > 0:
                status_report['recommendations'].append(f"{stale_count} stale batches detected - may need reprocessing")
            
            if len(unprocessed_pdfs) > 5:
                status_report['recommendations'].append(f"{len(unprocessed_pdfs)} PDFs waiting to be processed")
            
            print(f"✅ Status check complete: {len(pending_batches)} batches pending")
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(status_report, indent=2).encode())
            
        except Exception as e:
            print(f"❌ Monitor ERROR: {str(e)}")
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }).encode())