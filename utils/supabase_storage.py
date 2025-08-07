"""
Supabase Storage Utility
Handles PDF upload to Supabase storage bucket
"""

import os
import requests
from datetime import datetime

def upload_pdf_to_supabase_storage(pdf_content, filename, metadata=None):
    """
    Upload PDF to Supabase storage bucket
    
    Args:
        pdf_content: PDF file content as bytes
        filename: Name for the file in storage
        metadata: Optional metadata dictionary
    
    Returns:
        dict: Upload result with success status and details
    """
    try:
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            raise Exception("Supabase configuration missing")
        
        # Supabase Storage API endpoint
        bucket_name = 'sa-auction-pdf-processed'  # Your bucket name
        storage_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{filename}"
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/pdf',
            'x-upsert': 'true'  # Allow overwrite if file exists
        }
        
        # Add metadata to headers if provided
        if metadata:
            # Convert metadata to custom headers
            for key, value in metadata.items():
                if isinstance(value, (str, int, float)):
                    headers[f'x-metadata-{key}'] = str(value)
        
        # Upload the PDF
        response = requests.post(storage_url, data=pdf_content, headers=headers)
        
        if response.status_code in [200, 201]:
            # Get the public URL for the uploaded file
            public_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{filename}"
            
            return {
                'success': True,
                'filename': filename,
                'bucket': bucket_name,
                'public_url': public_url,
                'size_bytes': len(pdf_content),
                'uploaded_at': datetime.now().isoformat(),
                'storage_response': response.json() if response.content else {}
            }
        else:
            return {
                'success': False,
                'filename': filename,
                'error': f"Upload failed: {response.status_code} - {response.text}",
                'status_code': response.status_code
            }
            
    except Exception as e:
        return {
            'success': False,
            'filename': filename,
            'error': str(e)
        }

def delete_pdf_from_supabase_storage(filename):
    """
    Delete PDF from Supabase storage bucket
    
    Args:
        filename: Name of the file to delete
    
    Returns:
        dict: Delete result with success status
    """
    try:
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            raise Exception("Supabase configuration missing")
        
        # Supabase Storage API endpoint for deletion
        bucket_name = 'sa-auction-pdf-processed'
        storage_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{filename}"
        
        headers = {
            'Authorization': f'Bearer {supabase_key}',
        }
        
        response = requests.delete(storage_url, headers=headers)
        
        if response.status_code in [200, 204]:
            return {
                'success': True,
                'filename': filename,
                'message': 'File deleted successfully'
            }
        else:
            return {
                'success': False,
                'filename': filename,
                'error': f"Delete failed: {response.status_code} - {response.text}"
            }
            
    except Exception as e:
        return {
            'success': False,
            'filename': filename,
            'error': str(e)
        }

def list_pdfs_in_supabase_storage(prefix='', limit=100):
    """
    List PDFs in Supabase storage bucket
    
    Args:
        prefix: Optional prefix to filter files
        limit: Maximum number of files to return
    
    Returns:
        dict: List result with files
    """
    try:
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            raise Exception("Supabase configuration missing")
        
        # Supabase Storage API endpoint for listing
        bucket_name = 'sa-auction-pdf-processed'
        storage_url = f"{supabase_url}/storage/v1/object/list/{bucket_name}"
        
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'limit': limit,
            'offset': 0
        }
        
        if prefix:
            params['prefix'] = prefix
        
        response = requests.post(storage_url, json=params, headers=headers)
        
        if response.status_code == 200:
            files = response.json()
            pdf_files = [f for f in files if f['name'].endswith('.pdf')]
            
            return {
                'success': True,
                'bucket': bucket_name,
                'total_files': len(pdf_files),
                'files': pdf_files
            }
        else:
            return {
                'success': False,
                'error': f"List failed: {response.status_code} - {response.text}"
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }