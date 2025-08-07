"""
Supabase Storage Monitoring Endpoint
Monitor PDFs stored in Supabase storage bucket
"""

import json
import os
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Add utils directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from supabase_storage import list_pdfs_in_supabase_storage, delete_pdf_from_supabase_storage

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            
            # Check for delete action
            delete_file = query_params.get('delete', [None])[0]
            if delete_file:
                return self.delete_pdf(delete_file)
            
            # Default: List all PDFs in storage
            return self.list_storage_pdfs()
            
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
    
    def list_storage_pdfs(self):
        """List all PDFs in Supabase storage"""
        try:
            # Get list of PDFs from Supabase storage
            storage_result = list_pdfs_in_supabase_storage()
            
            if storage_result.get('success'):
                # Format files for display
                formatted_files = []
                for file in storage_result.get('files', []):
                    formatted_files.append({
                        'filename': file['name'],
                        'size_mb': round(file['metadata']['size'] / 1024 / 1024, 2) if file.get('metadata', {}).get('size') else 'unknown',
                        'uploaded_at': file['created_at'],
                        'last_accessed': file.get('last_accessed_at'),
                        'public_url': f"https://esfyvihtnzwlnlrllewb.supabase.co/storage/v1/object/public/sa-auction-pdf-processed/{file['name']}",
                        'delete_url': f"/api/storage-monitor?delete={file['name']}"
                    })
                
                # Sort by upload date (newest first)
                formatted_files.sort(key=lambda x: x['uploaded_at'], reverse=True)
                
                response_data = {
                    'status': 'success',
                    'timestamp': datetime.now().isoformat(),
                    'bucket_name': 'sa-auction-pdf-processed',
                    'total_pdfs': len(formatted_files),
                    'total_size_mb': sum(f['size_mb'] for f in formatted_files if isinstance(f['size_mb'], (int, float))),
                    'pdfs': formatted_files,
                    'storage_summary': {
                        'newest_pdf': formatted_files[0]['filename'] if formatted_files else None,
                        'oldest_pdf': formatted_files[-1]['filename'] if formatted_files else None,
                        'average_size_mb': round(sum(f['size_mb'] for f in formatted_files if isinstance(f['size_mb'], (int, float))) / len(formatted_files), 2) if formatted_files else 0
                    },
                    'management': {
                        'view_bucket': 'https://supabase.com/dashboard/project/esfyvihtnzwlnlrllewb/storage/buckets/sa-auction-pdf-processed',
                        'delete_file': '/api/storage-monitor?delete=filename.pdf'
                    }
                }
            else:
                response_data = {
                    'status': 'error',
                    'error': storage_result.get('error'),
                    'timestamp': datetime.now().isoformat()
                }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, indent=2).encode())
            
        except Exception as e:
            error_response = {
                'status': 'error',
                'error': f'Failed to list storage: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())
    
    def delete_pdf(self, filename):
        """Delete a PDF from Supabase storage"""
        try:
            # Delete the PDF
            delete_result = delete_pdf_from_supabase_storage(filename)
            
            response_data = {
                'status': 'success' if delete_result.get('success') else 'error',
                'action': 'delete',
                'filename': filename,
                'result': delete_result,
                'timestamp': datetime.now().isoformat()
            }
            
            status_code = 200 if delete_result.get('success') else 500
            
            self.send_response(status_code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, indent=2).encode())
            
        except Exception as e:
            error_response = {
                'status': 'error',
                'action': 'delete',
                'filename': filename,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())