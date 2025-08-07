"""
Update Sheriff Mapping Cache
Fetches sheriffs from Supabase and stores as JSON for lookup
Should be called weekly via webhook or manually
"""

import json
import os
from datetime import datetime
from http.server import BaseHTTPRequestHandler
import requests

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            if not supabase_url or not supabase_key:
                raise ValueError("Missing Supabase credentials")
            
            # Fetch sheriffs from Supabase
            headers = {
                'apikey': supabase_key,
                'Authorization': f'Bearer {supabase_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(f"{supabase_url}/rest/v1/sheriffs?select=*", headers=headers)
            
            if response.status_code != 200:
                raise ValueError(f"Failed to fetch sheriffs: {response.status_code} - {response.text}")
            
            sheriffs = response.json()
            
            # Create sheriff office to UUID mapping
            sheriff_mapping = {}
            for sheriff in sheriffs:
                sheriff_office = sheriff.get('sheriff_office')
                sheriff_id = sheriff.get('id')
                if sheriff_office and sheriff_id:
                    sheriff_mapping[sheriff_office] = sheriff_id
            
            # Store mapping with metadata
            sheriff_cache = {
                'updated_at': datetime.now().isoformat(),
                'total_sheriffs': len(sheriff_mapping),
                'mapping': sheriff_mapping
            }
            
            # Store in environment variable format (for caching)
            # In production, this would be stored in a database or file system
            # For now, we'll return it and store it as an environment variable
            
            response_data = {
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'sheriffs_fetched': len(sheriffs),
                'mapping_created': len(sheriff_mapping),
                'cache_data': sheriff_cache,
                'sample_mapping': dict(list(sheriff_mapping.items())[:5])  # Show first 5 for reference
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, indent=2).encode())
            
        except Exception as e:
            error_response = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())