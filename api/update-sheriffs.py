"""
Sheriff Mapping Update Endpoint
Queries Supabase for sheriff data and updates the local JSON mapping file
Run manually or schedule weekly
"""

import json
import os
from datetime import datetime
from http.server import BaseHTTPRequestHandler
import requests

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Get Supabase configuration
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            if not supabase_url or not supabase_key:
                raise Exception("Supabase configuration missing (SUPABASE_URL or SUPABASE_KEY)")
            
            # Query sheriffs table
            headers = {
                'apikey': supabase_key,
                'Authorization': f'Bearer {supabase_key}',
                'Content-Type': 'application/json'
            }
            
            query_url = f"{supabase_url}/rest/v1/sheriffs?select=id,sheriff_office"
            response = requests.get(query_url, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Supabase query failed: {response.status_code} - {response.text}")
            
            sheriffs_data = response.json()
            
            # Format data for JSON file (same structure as provided)
            formatted_sheriffs = []
            for sheriff in sheriffs_data:
                formatted_sheriffs.append({
                    "id": sheriff["id"],
                    "sheriff_office": sheriff["sheriff_office"]
                })
            
            # Sort by sheriff_office for consistency
            formatted_sheriffs.sort(key=lambda x: x["sheriff_office"])
            
            # Simulate saving to file system (Vercel doesn't support file writes)
            # In production, you'd want to store this in a database or external storage
            # For now, we'll return the data that should be saved to sheriff-mapping.json
            
            response_data = {
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "total_sheriffs": len(formatted_sheriffs),
                "message": "Sheriff mapping updated successfully",
                "note": "In Vercel, this data should be manually saved to data/sheriff-mapping.json",
                "sheriff_mapping_json": formatted_sheriffs,
                "instructions": [
                    "Copy the 'sheriff_mapping_json' array from this response",
                    "Save it to /data/sheriff-mapping.json in your repository",
                    "Commit and deploy the changes",
                    "The JSON file will be available for the processing scripts"
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
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'message': 'Failed to update sheriff mapping'
            }
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode())