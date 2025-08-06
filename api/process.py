"""
Sheriff Auctions PDF Processor - Vercel Python API
Hybrid Architecture: Fetches PDFs from Cloudflare R2 and processes with Python
"""

import json
import os
import re
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import boto3
import pdfplumber
from openai import OpenAI
import pandas as pd
from supabase import create_client
from fuzzywuzzy import process
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# ─── CONFIGURATION ──────────────────────────────────────────────────────────
# Environment variables (set in Vercel)
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL') 
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
DEFAULT_SHERIFF_UUID = os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')

# Cloudflare R2 Configuration (S3-compatible)
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY') 
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME', 'sheriff-auction-pdfs')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')

# Safety configuration to prevent excessive OpenAI usage
MAX_AUCTIONS_PER_RUN = int(os.getenv('MAX_AUCTIONS_PER_RUN', '50'))
MAX_OPENAI_TOKENS_PER_RUN = int(os.getenv('MAX_OPENAI_TOKENS_PER_RUN', '100000'))
ENABLE_PROCESSING = os.getenv('ENABLE_PROCESSING', 'true').lower() == 'true'

# Initialize clients
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

# Initialize R2 client (S3-compatible)
r2_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name='auto'
) if R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY else None

# ─── AUCTION FIELD DEFINITIONS ─────────────────────────────────────────────
auction_fields = [
    {
        "column_name": "case_number",
        "data_type": "text",
        "allow_null": False,
        "additional_info": "The official case number for the auction, typically in the format '1234/2024'."
    },
    {
        "column_name": "court_name", 
        "data_type": "text",
        "allow_null": True,
        "additional_info": "The name of the court where the case is filed (e.g., 'Gauteng Division, Pretoria')."
    },
    {
        "column_name": "plaintiff",
        "data_type": "text", 
        "allow_null": True,
        "additional_info": "Name of the plaintiff or applicant in the case."
    },
    {
        "column_name": "defendant",
        "data_type": "text",
        "allow_null": True, 
        "additional_info": "Name(s) of the defendant(s) or respondent(s) in the case."
    },
    {
        "column_name": "auction_date",
        "data_type": "date",
        "allow_null": True,
        "additional_info": "The date on which the auction will be held (format: YYYY-MM-DD)."
    },
    {
        "column_name": "auction_time",
        "data_type": "time", 
        "allow_null": True,
        "additional_info": "The time when the auction is scheduled to start (format: HH:MM:SS)."
    },
    {
        "column_name": "sheriff_office",
        "data_type": "text",
        "allow_null": True,
        "additional_info": "Name of the sheriff's office conducting the auction. Return as proper noun (not all caps), area name only."
    },
    {
        "column_name": "sheriff_address",
        "data_type": "text",
        "allow_null": True, 
        "additional_info": "Physical address of the sheriff's office or auction venue."
    },
    {
        "column_name": "erf_number",
        "data_type": "text",
        "allow_null": True,
        "additional_info": "ERF number or property identifier."
    },
    {
        "column_name": "township", 
        "data_type": "text",
        "allow_null": True,
        "additional_info": "The township or area where the property is located."
    },
    {
        "column_name": "extension",
        "data_type": "text",
        "allow_null": True,
        "additional_info": "Extension number or name, if applicable." 
    },
    {
        "column_name": "registration_division",
        "data_type": "text",
        "allow_null": True,
        "additional_info": "Registration division for the property (e.g., 'IR', 'JR')."
    },
    {
        "column_name": "province",
        "data_type": "text", 
        "allow_null": True,
        "additional_info": "Province where the property is located."
    },
    {
        "column_name": "stand_size",
        "data_type": "bigint",
        "allow_null": True,
        "additional_info": "Size of the stand or property in square meters."
    },
    {
        "column_name": "deed_of_transfer_number",
        "data_type": "text",
        "allow_null": True,
        "additional_info": "Official deed of transfer number."
    },
    {
        "column_name": "street_address",
        "data_type": "text",
        "allow_null": True,
        "additional_info": "Physical street address of the property being auctioned (not the auctioneer's address)."
    },
    {
        "column_name": "zoning",
        "data_type": "text", 
        "allow_null": True,
        "additional_info": "Property zoning type (Residential, Commercial, Agricultural, Industrial, etc.)."
    },
    {
        "column_name": "reserve_price",
        "data_type": "decimal",
        "allow_null": True,
        "additional_info": "Reserve price or minimum bid amount if mentioned."
    },
    {
        "column_name": "attorney_name",
        "data_type": "text",
        "allow_null": True,
        "additional_info": "Name of the attorney handling the auction."
    },
    {
        "column_name": "attorney_phone",
        "data_type": "text",
        "allow_null": True, 
        "additional_info": "Attorney contact phone number."
    },
    {
        "column_name": "attorney_reference", 
        "data_type": "text",
        "allow_null": True,
        "additional_info": "Attorney reference number if mentioned."
    }
]

# ─── HELPER FUNCTIONS ──────────────────────────────────────────────────────
def extract_text_from_pdf(pdf_path: str, start_page: int = 13) -> str:
    """
    Extract text from PDF starting from a specific page and stopping before the 'PAUC' section.
    Enhanced version of your original logic.
    """
    text = ""
    stop_page = None
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"📄 PDF has {total_pages} pages, extracting from page {start_page}")
            
            # Find the 'PAUC' section (original logic)
            for page_number in range(total_pages - 1, start_page - 2, -1):
                if page_number < 0:
                    break
                page_text = pdf.pages[page_number].extract_text()
                if page_text and "PAUC\nPUBLIC AUCTIONS, SALES AND TENDERS" in page_text:
                    stop_page = page_number + 1
                    print(f"📄 Found 'PAUC' section starting at page {stop_page}")
                    break
            
            # Extract text from start_page to stop_page (or end)
            end_page = (stop_page or total_pages)
            for page_number in range(start_page - 1, end_page - 1):
                if page_number < total_pages:
                    page_text = pdf.pages[page_number].extract_text()
                    text += page_text + "\n" if page_text else ""
            
            print(f"✅ Extracted {len(text)} characters from pages {start_page} to {end_page-1}")
            return text.strip()
            
    except Exception as e:
        print(f"❌ PDF extraction error: {e}")
        raise


def clean_text(raw_text: str) -> str:
    """Clean extracted text to make it JSON-compliant (your original logic)."""
    patterns_to_remove = [
        r"STAATSKOERANT[^\n]*", r"GOVERNMENT GAZETTE[^\n]*", r"No\.\s*\d+\s*",
        r"Page\s*\d+\s*of\s*\d+", r"This gazette is also available free online at[^\n]*",
        r"HIGH ALERT: SCAM WARNING!!![^\n]*", r"CONTENTS / INHOUD[^\n]*",
        r"LEGAL NOTICES[^\n]*", r"WETLIKE KENNISGEWINGS[^\n]*",
        r"SALES IN EXECUTION AND OTHER PUBLIC SALES[^\n]*",
        r"GEREGTELIKE EN ANDER OPENBARE VERKOPE[^\n]*",
        r"[^\x20-\x7E]"  # Remove non-ASCII characters
    ]
    for pattern in patterns_to_remove:
        raw_text = re.sub(pattern, '', raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r' +', ' ', raw_text.replace('\n', ' ').replace('\t', ' ')).strip()
    return raw_text.replace('\\', '\\\\').replace('"', '\\"')


def split_into_auctions(text: str, start_row: int = 0) -> List[str]:
    """
    Split text into auction blocks starting with 'Case No:' (your original logic).
    """
    pattern = re.compile(r'(?=(Case No:\s*\d+(?:/\d+)?))', re.IGNORECASE)
    matches = list(pattern.finditer(text))
    
    if len(matches) <= 1:
        # Only one or zero 'Case No:' found; treat the whole text as a single auction
        auctions = [text.strip()] if text.strip() else []
        print(f"📊 Only one or no 'Case No:' found. Returning entire text as single auction.")
        return auctions[start_row:]
    else:
        parts = pattern.split(text)
        auctions = []
        for i in range(1, len(parts), 2):
            header = parts[i]
            body = parts[i + 1] if (i + 1) < len(parts) else ''
            auction_text = (header + body).strip()
            case_number_match = re.search(r'Case No:\s*(\d+(?:/\d+)?)', header, re.IGNORECASE)
            if case_number_match:
                auctions.append(auction_text)
        
        filtered_auctions = auctions[start_row:]
        print(f"📊 Found {len(auctions)} total auctions, processing {len(filtered_auctions)} from row {start_row}")
        return filtered_auctions


def extract_data_with_gpt(auction_text: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Use GPT to extract structured data from single auction notice (your original logic).
    Enhanced with safety checks.
    """
    if not openai_client:
        raise ValueError("OpenAI client not initialized - check API key")
    
    prompt = f"""
You are a data extractor. From the following single sheriff auction notice, extract these fields as a JSON array with ONE object. Ensure the output is fully JSON-compliant:

Fields to extract:
{json.dumps(auction_fields, indent=2)}

Rules:
- Return ONLY valid JSON array with ONE object, no extra text
- If a value is missing, use null (not "None" or "unknown")  
- For numeric fields, use 0 if missing
- For dates, use "2000-01-01" if missing
- For times, use "00:00:00" if missing
- Escape special characters properly
- This is a single auction, so return array with one object

Single auction text to process:
{auction_text}
"""
    
    try:
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=4000
        )
        
        content = response.choices[0].message.content.strip()
        
        # Clean response
        content = re.sub(r"^```json\s*|```$", "", content, flags=re.MULTILINE)
        content = content.replace("\\", "\\\\")
        
        # Parse JSON
        data = json.loads(content)
        if isinstance(data, list) and len(data) > 0:
            return data[0]  # Return first object from array
        return data
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        print(f"Raw content: {content[:500]}...")
        return None
        
    except Exception as e:
        print(f"❌ GPT extraction error: {e}")
        return None


def fetch_pdf_from_r2(pdf_key: str) -> bytes:
    """Fetch PDF file from Cloudflare R2 bucket."""
    if not r2_client:
        raise ValueError("R2 client not initialized - check credentials")
    
    try:
        response = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=pdf_key)
        return response['Body'].read()
    except Exception as e:
        print(f"❌ R2 fetch error for {pdf_key}: {e}")
        raise


def move_pdf_in_r2(source_key: str, dest_key: str):
    """Move PDF from one R2 location to another."""
    if not r2_client:
        return
    
    try:
        # Copy to new location
        r2_client.copy_object(
            Bucket=R2_BUCKET_NAME,
            CopySource={'Bucket': R2_BUCKET_NAME, 'Key': source_key},
            Key=dest_key
        )
        # Delete from old location
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=source_key)
        print(f"✅ Moved {source_key} to {dest_key}")
    except Exception as e:
        print(f"❌ R2 move error: {e}")


def list_unprocessed_pdfs() -> List[str]:
    """Get list of unprocessed PDFs from R2 bucket."""
    if not r2_client:
        return []
    
    try:
        response = r2_client.list_objects_v2(
            Bucket=R2_BUCKET_NAME,
            Prefix='unprocessed/'
        )
        
        if 'Contents' not in response:
            return []
        
        return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.pdf')]
    except Exception as e:
        print(f"❌ R2 list error: {e}")
        return []


def upload_to_supabase(auction_data: Dict[str, Any]) -> bool:
    """Upload auction data to Supabase."""
    if not supabase:
        print("⚠️ Supabase client not initialized")
        return False
    
    try:
        # Add metadata
        auction_data['data_extraction_date'] = datetime.now().strftime("%Y-%m-%d")
        auction_data['sheriff_uuid'] = DEFAULT_SHERIFF_UUID  # TODO: Map from sheriff_office
        auction_data['processed_nearby_sales'] = False
        
        response = supabase.table("auctions").insert(auction_data).execute()
        if response.data:
            print(f"✅ Successfully uploaded: {auction_data.get('case_number', 'Unknown')}")
            return True
        return False
    except Exception as e:
        print(f"❌ Supabase upload error: {e}")
        return False


# ─── MAIN PROCESSING FUNCTION ──────────────────────────────────────────────
def process_single_pdf(pdf_key: str) -> Dict[str, Any]:
    """
    Process a single PDF file from R2 storage.
    Returns processing results with safety checks.
    """
    results = {
        'pdf_key': pdf_key,
        'success': False,
        'auctions_found': 0,
        'auctions_processed': 0,
        'auctions_uploaded': 0,
        'errors': [],
        'processing_time_ms': 0
    }
    
    start_time = time.time()
    
    try:
        print(f"🚀 Processing PDF: {pdf_key}")
        
        # Safety check
        if not ENABLE_PROCESSING:
            raise ValueError("Processing is disabled. Set ENABLE_PROCESSING=true to enable.")
        
        # Fetch PDF from R2
        pdf_data = fetch_pdf_from_r2(pdf_key)
        
        # Save to temporary file for pdfplumber
        temp_pdf_path = f"/tmp/{os.path.basename(pdf_key)}"
        with open(temp_pdf_path, 'wb') as f:
            f.write(pdf_data)
        
        # Extract text from PDF
        raw_text = extract_text_from_pdf(temp_pdf_path, start_page=13)
        if not raw_text:
            raise ValueError("No text extracted from PDF")
        
        # Split into auctions
        auctions = split_into_auctions(raw_text)
        results['auctions_found'] = len(auctions)
        
        if not auctions:
            raise ValueError("No auctions found in PDF")
        
        # Safety check: limit number of auctions per run
        if len(auctions) > MAX_AUCTIONS_PER_RUN:
            print(f"⚠️ Found {len(auctions)} auctions, limiting to {MAX_AUCTIONS_PER_RUN} for safety")
            auctions = auctions[:MAX_AUCTIONS_PER_RUN]
        
        # Process each auction individually (original design)
        for i, auction_text in enumerate(auctions):
            try:
                print(f"🔄 Processing auction {i+1}/{len(auctions)}")
                
                # Clean text
                cleaned_auction = clean_text(auction_text)
                
                # Extract data with GPT
                auction_data = extract_data_with_gpt(cleaned_auction)
                if not auction_data:
                    raise ValueError("No data extracted from GPT")
                
                # TODO: Add geocoding here (extract_area_components from your original code)
                
                # Upload to Supabase
                if upload_to_supabase(auction_data):
                    results['auctions_uploaded'] += 1
                
                results['auctions_processed'] += 1
                
            except Exception as auction_error:
                error_msg = f"Auction {i+1} error: {str(auction_error)}"
                print(f"❌ {error_msg}")
                results['errors'].append(error_msg)
        
        # Move PDF to processed folder
        processed_key = pdf_key.replace('unprocessed/', 'processed/')
        move_pdf_in_r2(pdf_key, processed_key)
        
        results['success'] = True
        print(f"✅ Successfully processed {pdf_key}")
        
    except Exception as e:
        error_msg = f"PDF processing error: {str(e)}"
        print(f"❌ {error_msg}")
        results['errors'].append(error_msg)
        
        # Move PDF to errors folder
        error_key = pdf_key.replace('unprocessed/', 'errors/')
        try:
            move_pdf_in_r2(pdf_key, error_key)
        except:
            pass  # If move fails, leave PDF in unprocessed
    
    finally:
        # Cleanup temp file
        temp_pdf_path = f"/tmp/{os.path.basename(pdf_key)}"
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)
    
    results['processing_time_ms'] = int((time.time() - start_time) * 1000)
    return results


# ─── VERCEL API ENDPOINTS ──────────────────────────────────────────────────
@app.route('/api/process', methods=['POST', 'GET'])
def process_pdfs():
    """
    Main endpoint to process PDFs from R2 bucket.
    Includes safety controls and detailed reporting.
    """
    try:
        # Get processing parameters
        max_pdfs = int(request.args.get('max_pdfs', '10'))  # Limit number of PDFs per request
        specific_pdf = request.args.get('pdf_key')  # Process specific PDF
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'processing_enabled': ENABLE_PROCESSING,
            'pdfs_processed': [],
            'total_auctions_found': 0,
            'total_auctions_uploaded': 0,
            'total_errors': 0,
            'processing_time_ms': 0
        }
        
        start_time = time.time()
        
        if specific_pdf:
            # Process specific PDF
            pdf_keys = [specific_pdf] if specific_pdf.startswith('unprocessed/') else [f'unprocessed/{specific_pdf}']
        else:
            # Get list of unprocessed PDFs
            pdf_keys = list_unprocessed_pdfs()[:max_pdfs]
        
        if not pdf_keys:
            return jsonify({
                **results,
                'message': 'No unprocessed PDFs found',
                'processing_time_ms': int((time.time() - start_time) * 1000)
            })
        
        print(f"📋 Found {len(pdf_keys)} PDFs to process")
        
        # Process each PDF
        for pdf_key in pdf_keys:
            pdf_result = process_single_pdf(pdf_key)
            results['pdfs_processed'].append(pdf_result)
            results['total_auctions_found'] += pdf_result['auctions_found']
            results['total_auctions_uploaded'] += pdf_result['auctions_uploaded']
            results['total_errors'] += len(pdf_result['errors'])
        
        results['processing_time_ms'] = int((time.time() - start_time) * 1000)
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get system status and configuration."""
    try:
        unprocessed_pdfs = list_unprocessed_pdfs()
        
        return jsonify({
            'timestamp': datetime.now().isoformat(),
            'status': 'operational',
            'configuration': {
                'processing_enabled': ENABLE_PROCESSING,
                'max_auctions_per_run': MAX_AUCTIONS_PER_RUN,
                'max_tokens_per_run': MAX_OPENAI_TOKENS_PER_RUN
            },
            'services': {
                'openai': openai_client is not None,
                'supabase': supabase is not None, 
                'r2_storage': r2_client is not None
            },
            'bucket_status': {
                'unprocessed_pdfs': len(unprocessed_pdfs),
                'sample_files': unprocessed_pdfs[:5] if unprocessed_pdfs else []
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/test', methods=['GET'])
def test_endpoint():
    """Test endpoint to verify the service is running."""
    return jsonify({
        'service': 'Sheriff Auctions PDF Processor',
        'status': 'operational',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0.0-vercel-hybrid'
    })


# ─── VERCEL SERVERLESS HANDLER ─────────────────────────────────────────────
def handler(request):
    """Vercel serverless function handler."""
    return app(request.environ, lambda *args: None)


if __name__ == '__main__':
    app.run(debug=True)