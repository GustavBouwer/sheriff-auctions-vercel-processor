#!/usr/bin/env python3
"""
Test script for the new batch processing system
"""

import requests
import json
import time
import sys

def test_batch_system(base_url="https://sheriff-auctions-data-etl-zzd2.vercel.app"):
    """Test the multi-stage batch processing system"""
    
    print("🧪 Testing Batch Processing System")
    print("=" * 50)
    
    # Step 1: Check batch monitor
    print("\n📊 Checking current batch status...")
    try:
        response = requests.get(f"{base_url}/api/batch-monitor")
        if response.status_code == 200:
            status = response.json()
            print(f"✅ Monitor endpoint working")
            print(f"   - Pending batches: {status['batch_processing']['pending_batches']}")
            print(f"   - Unprocessed PDFs: {status['unprocessed_pdfs']['count']}")
            
            if status['batch_processing']['pending_batches'] > 0:
                print(f"   - Total pending auctions: {status['batch_processing']['total_pending_auctions']}")
                
            if status['recommendations']:
                print("\n   Recommendations:")
                for rec in status['recommendations']:
                    print(f"   - {rec}")
        else:
            print(f"❌ Monitor endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Monitor check failed: {e}")
    
    # Step 2: Test webhook-receive-v2 with sample data
    print("\n🚀 Testing Stage 1 (webhook-receive-v2)...")
    print("   Note: This requires PDFs in R2 unprocessed/ folder")
    
    webhook_payload = {
        "secret": "sheriff-auctions-webhook-2025",
        "event": "new_pdfs_ready",
        "timestamp": "2025-08-08T18:00:00Z",
        "pdf_files": ["test-989.pdf"],  # Change to actual PDF name
        "pdf_count": 1,
        "source": "test-script"
    }
    
    print(f"   Sending test webhook...")
    try:
        response = requests.post(
            f"{base_url}/api/webhook-receive-v2",
            json=webhook_payload,
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Stage 1 completed successfully")
            print(f"   - Processing ID: {result.get('processing_id')}")
            print(f"   - Total batches created: {result.get('total_batches_created')}")
            
            if result.get('results'):
                for pdf_result in result['results']:
                    print(f"\n   PDF: {pdf_result['pdf']}")
                    print(f"   - Status: {pdf_result['status']}")
                    print(f"   - Auctions found: {pdf_result.get('auctions_found', 0)}")
                    print(f"   - Batches created: {pdf_result.get('batches_created', 0)}")
        else:
            print(f"❌ Stage 1 failed: {response.status_code}")
            print(f"   Response: {response.text[:500]}")
    except requests.Timeout:
        print("⏱️ Stage 1 timed out (this is okay if batches were created)")
    except Exception as e:
        print(f"❌ Stage 1 test failed: {e}")
    
    # Step 3: Wait and check batch processing
    print("\n⏳ Waiting 30 seconds for batch processing...")
    time.sleep(30)
    
    print("\n📊 Checking batch status after processing...")
    try:
        response = requests.get(f"{base_url}/api/batch-monitor")
        if response.status_code == 200:
            status = response.json()
            print(f"✅ Final status check")
            print(f"   - Pending batches: {status['batch_processing']['pending_batches']}")
            print(f"   - Stale batches: {status['batch_processing']['stale_batches']}")
            
            if status['batch_processing']['pending_batches'] == 0:
                print("   🎉 All batches processed successfully!")
            else:
                print(f"   ⏳ Still processing {status['batch_processing']['pending_batches']} batches")
        else:
            print(f"❌ Final monitor check failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Final status check failed: {e}")
    
    print("\n" + "=" * 50)
    print("🧪 Test Complete")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        base_url = sys.argv[1]
    else:
        base_url = "https://sheriff-auctions-data-etl-zzd2.vercel.app"
    
    print(f"Testing against: {base_url}")
    test_batch_system(base_url)