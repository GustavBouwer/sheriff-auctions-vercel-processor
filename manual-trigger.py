#!/usr/bin/env python3
"""
Manual trigger for Sheriff Auctions processing
This simulates what the Cloudflare Worker does when it finds new PDFs
"""

import requests
import json
from datetime import datetime
import sys

def manual_trigger(pdf_files=None, webhook_url=None):
    """
    Manually trigger the webhook processing
    
    Args:
        pdf_files: List of PDF filenames to process (without 'unprocessed/' prefix)
        webhook_url: Override the default webhook URL
    """
    
    # Default values
    if not webhook_url:
        webhook_url = "https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-receive-v2"
    
    if not pdf_files:
        # Default test PDFs - replace with actual PDF names from your R2 bucket
        pdf_files = ["2025-989.pdf", "2025-990.pdf"]
        print("⚠️  No PDFs specified, using defaults: ", pdf_files)
        print("   To specify PDFs, run: python3 manual-trigger.py 2025-989.pdf 2025-990.pdf")
        print()
    
    # Build webhook payload (same as Cloudflare Worker sends)
    webhook_payload = {
        "secret": "sheriff-auctions-webhook-2025",
        "event": "new_pdfs_ready",
        "timestamp": datetime.now().isoformat() + "Z",
        "pdf_files": pdf_files,
        "pdf_count": len(pdf_files),
        "source": "manual-trigger"
    }
    
    print("🚀 Manual Trigger for Sheriff Auctions Processing")
    print("=" * 60)
    print(f"📍 Webhook URL: {webhook_url}")
    print(f"📁 PDFs to process: {pdf_files}")
    print(f"📦 Total PDFs: {len(pdf_files)}")
    print()
    
    # Send webhook
    print("📡 Sending webhook to Vercel...")
    print(f"   Payload: {json.dumps(webhook_payload, indent=2)}")
    print()
    
    try:
        response = requests.post(
            webhook_url,
            json=webhook_payload,
            timeout=120  # 2 minute timeout for Stage 1
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Webhook processed successfully!")
            print()
            print("📊 Results:")
            print(f"   - Processing ID: {result.get('processing_id')}")
            print(f"   - PDFs processed: {result.get('pdfs_processed')}")
            print(f"   - Total batches created: {result.get('total_batches_created')}")
            
            # Show details for each PDF
            if result.get('results'):
                print("\n📄 PDF Processing Details:")
                for pdf_result in result['results']:
                    print(f"\n   PDF: {pdf_result['pdf']}")
                    print(f"   - Status: {pdf_result['status']}")
                    if pdf_result['status'] == 'success':
                        print(f"   - Auctions found: {pdf_result.get('auctions_found', 0)}")
                        print(f"   - Batches created: {pdf_result.get('batches_created', 0)}")
                        
                        # Show batch details
                        if pdf_result.get('batch_results'):
                            print(f"   - Batch IDs:")
                            for batch in pdf_result['batch_results'][:3]:  # Show first 3
                                print(f"     • {batch['batch_id'][:50]}... ({batch['auctions']} auctions)")
                            if len(pdf_result['batch_results']) > 3:
                                print(f"     • ... and {len(pdf_result['batch_results']) - 3} more batches")
                    else:
                        print(f"   - Error: {pdf_result.get('error', 'Unknown error')}")
            
            print("\n" + "=" * 60)
            print("🎯 Next Steps:")
            print("1. Monitor batch processing:")
            print(f"   curl {webhook_url.replace('/webhook-receive-v2', '/batch-monitor')}")
            print()
            print("2. Check Vercel logs:")
            print("   https://vercel.com/easy-projects/sheriff-auctions-vercel-processor")
            print()
            print("3. Verify Supabase for new auctions")
            
        else:
            print(f"❌ Webhook failed with status {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            
    except requests.Timeout:
        print("⏱️ Request timed out (this might be okay if processing started)")
        print("   Check the batch monitor to see if batches were created:")
        print(f"   curl {webhook_url.replace('/webhook-receive-v2', '/batch-monitor')}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        
    print("\n✅ Manual trigger complete!")

if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print("Usage: python3 manual-trigger.py [pdf1.pdf] [pdf2.pdf] ...")
            print()
            print("Examples:")
            print("  python3 manual-trigger.py                    # Use default test PDFs")
            print("  python3 manual-trigger.py 2025-989.pdf      # Process single PDF")
            print("  python3 manual-trigger.py 2025-989.pdf 2025-990.pdf  # Process multiple PDFs")
            sys.exit(0)
        
        # Use provided PDF names
        pdf_files = sys.argv[1:]
    else:
        # Use defaults
        pdf_files = None
    
    manual_trigger(pdf_files)