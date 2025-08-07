"""
System Monitoring and Error Tracking Endpoint
Monitor PDF processing status, errors, and system health
"""

import json
import os
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler
import boto3
import requests

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Get comprehensive system status
            status_data = self.get_comprehensive_status()
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(status_data, indent=2).encode())
            
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
    
    def get_comprehensive_status(self):
        """Get comprehensive system monitoring data"""
        try:
            # R2 Bucket Status
            r2_status = self.get_r2_status()
            
            # Supabase Status  
            supabase_status = self.get_supabase_status()
            
            # Recent Processing Activity
            recent_activity = self.get_recent_activity()
            
            # Environment Check
            env_status = self.check_environment()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'system_health': 'healthy' if all([
                    r2_status.get('status') == 'healthy',
                    supabase_status.get('status') == 'healthy',
                    env_status.get('all_configured', False)
                ]) else 'issues_detected',
                'r2_bucket': r2_status,
                'supabase': supabase_status,
                'recent_activity': recent_activity,
                'environment': env_status,
                'monitoring_urls': {
                    'manual_processing': '/api/process-manual',
                    'sheriff_update': '/api/update-sheriffs',
                    'system_status': '/api/status',
                    'this_monitor': '/api/monitor'
                },
                'troubleshooting': {
                    'upload_pdf_manually': 'Upload PDF to R2 unprocessed/ folder',
                    'process_manually': 'Visit /api/process-manual?pdf=filename.pdf',
                    'check_errors': 'Look at error_pdfs_count and recent_errors below',
                    'view_logs': 'Check Vercel function logs in dashboard'
                }
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': f'Failed to get comprehensive status: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
    
    def get_r2_status(self):
        """Check R2 bucket status and PDF counts"""
        try:
            r2_client = boto3.client(
                's3',
                endpoint_url=os.getenv('R2_ENDPOINT_URL'),
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
                region_name='auto'
            )
            
            bucket_name = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
            
            # Count PDFs in each folder
            unprocessed = r2_client.list_objects_v2(Bucket=bucket_name, Prefix='unprocessed/')
            processed = r2_client.list_objects_v2(Bucket=bucket_name, Prefix='processed/')
            errors = r2_client.list_objects_v2(Bucket=bucket_name, Prefix='errors/')
            
            # Get recent files
            recent_unprocessed = []
            if 'Contents' in unprocessed:
                for obj in sorted(unprocessed['Contents'], key=lambda x: x['LastModified'], reverse=True)[:5]:
                    if obj['Key'].endswith('.pdf'):
                        recent_unprocessed.append({
                            'filename': obj['Key'].replace('unprocessed/', ''),
                            'size_mb': round(obj['Size'] / 1024 / 1024, 2),
                            'uploaded': obj['LastModified'].isoformat()
                        })
            
            return {
                'status': 'healthy',
                'bucket_name': bucket_name,
                'pdf_counts': {
                    'unprocessed': len([obj for obj in unprocessed.get('Contents', []) if obj['Key'].endswith('.pdf')]),
                    'processed': len([obj for obj in processed.get('Contents', []) if obj['Key'].endswith('.pdf')]),
                    'errors': len([obj for obj in errors.get('Contents', []) if obj['Key'].endswith('.pdf')])
                },
                'recent_unprocessed_pdfs': recent_unprocessed,
                'connection': 'successful'
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'connection': 'failed'
            }
    
    def get_supabase_status(self):
        """Check Supabase connection and recent auction data"""
        try:
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            if not supabase_url or not supabase_key:
                return {
                    'status': 'error',
                    'error': 'Supabase configuration missing'
                }
            
            headers = {
                'apikey': supabase_key,
                'Authorization': f'Bearer {supabase_key}',
                'Content-Type': 'application/json'
            }
            
            # Get recent auctions count
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            count_url = f"{supabase_url}/rest/v1/auctions?select=case_number&data_extraction_date=gte.{yesterday}"
            count_response = requests.get(count_url, headers=headers)
            
            if count_response.status_code == 200:
                recent_count = len(count_response.json())
            else:
                recent_count = 'unknown'
            
            # Get total auctions count
            total_url = f"{supabase_url}/rest/v1/auctions?select=case_number"
            total_response = requests.get(total_url, headers=headers)
            
            if total_response.status_code == 200:
                total_count = len(total_response.json())
            else:
                total_count = 'unknown'
            
            # Check for recent errors (auctions with error in case_number)
            error_url = f"{supabase_url}/rest/v1/auctions?select=case_number,data_extraction_date&case_number=like.ERROR*"
            error_response = requests.get(error_url, headers=headers)
            
            error_count = 0
            if error_response.status_code == 200:
                error_count = len(error_response.json())
            
            return {
                'status': 'healthy',
                'connection': 'successful',
                'auction_counts': {
                    'total_auctions': total_count,
                    'recent_24h': recent_count,
                    'processing_errors': error_count
                },
                'database_url': supabase_url.replace('//', '//***:***@') # Hide credentials
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'connection': 'failed'
            }
    
    def get_recent_activity(self):
        """Get recent processing activity indicators"""
        try:
            # This would ideally come from logs, but we can infer from Supabase
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            if not supabase_url or not supabase_key:
                return {'status': 'no_supabase_config'}
            
            headers = {
                'apikey': supabase_key,
                'Authorization': f'Bearer {supabase_key}',
                'Content-Type': 'application/json'
            }
            
            # Get most recent auctions
            recent_url = f"{supabase_url}/rest/v1/auctions?select=case_number,data_extraction_date,gov_pdf_name,sheriff_office&order=data_extraction_date.desc&limit=10"
            response = requests.get(recent_url, headers=headers)
            
            recent_auctions = []
            if response.status_code == 200:
                for auction in response.json():
                    recent_auctions.append({
                        'case_number': auction.get('case_number'),
                        'processed_at': auction.get('data_extraction_date'),
                        'pdf_source': auction.get('gov_pdf_name', '').split('/')[-1],  # Just filename
                        'sheriff_office': auction.get('sheriff_office')
                    })
            
            return {
                'status': 'success',
                'recent_processing': recent_auctions,
                'last_activity': recent_auctions[0]['processed_at'] if recent_auctions else 'none'
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def check_environment(self):
        """Check environment variable configuration"""
        required_vars = [
            'R2_ENDPOINT_URL',
            'R2_ACCESS_KEY_ID', 
            'R2_SECRET_ACCESS_KEY',
            'R2_BUCKET_NAME',
            'SUPABASE_URL',
            'SUPABASE_KEY',
            'OPENAI_API_KEY',
            'WEBHOOK_SECRET'
        ]
        
        optional_vars = [
            'GOOGLE_MAPS_API_KEY',
            'DEFAULT_SHERIFF_UUID',
            'ENABLE_PROCESSING'
        ]
        
        configured = {}
        missing = []
        
        for var in required_vars:
            value = os.getenv(var)
            if value:
                configured[var] = 'configured' if len(value) > 10 else 'short_value'
            else:
                missing.append(var)
        
        for var in optional_vars:
            value = os.getenv(var)
            configured[var] = 'configured' if value else 'not_set'
        
        return {
            'required_variables': configured,
            'missing_required': missing,
            'all_configured': len(missing) == 0,
            'processing_enabled': os.getenv('ENABLE_PROCESSING', 'false').lower() == 'true'
        }