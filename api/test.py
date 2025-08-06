"""
Test endpoint for Sheriff Auctions PDF Processor
"""

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        response = {
            'service': 'Sheriff Auctions PDF Processor',
            'status': 'operational',
            'timestamp': datetime.now().isoformat(),
            'version': '2.0.0-vercel-hybrid'
        }
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response, indent=2).encode())