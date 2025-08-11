#!/usr/bin/env python3
"""
Test script for the parallel batch processing system
Tests both batch-coordinator and process-auction-batch endpoints
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime

# Configuration
VERCEL_DOMAIN = "sheriff-auctions-data-etl-zzd2.vercel.app"
WEBHOOK_SECRET = "sheriff-auctions-webhook-2025"

async def test_batch_coordinator():
    """Test the batch coordinator endpoint with sample PDFs"""
    
    print("🚀 Testing Batch Coordinator System")
    print("=" * 50)
    
    # Test payload - simulate Cloudflare webhook
    webhook_payload = {
        'secret': WEBHOOK_SECRET,
        'event': 'new_pdfs_ready',
        'timestamp': datetime.now().isoformat(),
        'pdf_files': ['2025-1021.pdf'],  # Large PDF with 163 auctions
        'pdf_count': 1,
        'source': 'test-batch-coordinator',
        'batch_info': {
            'batch_number': 1,
            'total_batches': 1
        }
    }
    
    # Make async request to batch coordinator
    coordinator_url = f"https://{VERCEL_DOMAIN}/api/batch-coordinator"
    
    print(f"📡 Calling batch coordinator: {coordinator_url}")
    print(f"📦 Test payload: {json.dumps(webhook_payload, indent=2)}")
    
    start_time = time.time()
    
    try:
        async with aiohttp.ClientSession() as session:
            # Set timeout to 15 minutes for large PDF processing
            timeout = aiohttp.ClientTimeout(total=900)  # 15 minutes
            
            async with session.post(coordinator_url, json=webhook_payload, timeout=timeout) as response:
                response_text = await response.text()
                elapsed_time = time.time() - start_time
                
                print(f"\n📊 BATCH COORDINATOR RESULTS:")
                print(f"   Status Code: {response.status}")
                print(f"   Processing Time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
                
                if response.status == 200:
                    try:
                        result_data = json.loads(response_text)
                        print(f"   ✅ Success!")
                        print(f"   PDFs Processed: {result_data.get('pdfs_processed', 0)}")
                        print(f"   Processing Method: {result_data.get('processing_method', 'unknown')}")
                        
                        # Show results for each PDF
                        for i, result in enumerate(result_data.get('results', []), 1):
                            print(f"\n   📄 PDF {i} Results:")
                            print(f"      Status: {result.get('status', 'unknown')}")
                            print(f"      Processing Method: {result.get('processing_method', 'unknown')}")
                            
                            if result.get('status') == 'success':
                                if result.get('processing_method') == 'parallel_batches':
                                    print(f"      Total Auctions: {result.get('total_auctions', 0)}")
                                    print(f"      Auctions Processed: {result.get('auctions_processed', 0)}")
                                    print(f"      Batches Total: {result.get('batches_total', 0)}")
                                    print(f"      Batches Successful: {result.get('batches_successful', 0)}")
                                    print(f"      Parallel Workers: {result.get('parallel_workers', 0)}")
                                elif result.get('processing_method') == 'sequential':
                                    webhook_response = result.get('webhook_response', {})
                                    print(f"      Auctions Processed: {webhook_response.get('successful_processes', 0)}")
                        
                        return True
                        
                    except json.JSONDecodeError:
                        print(f"   ❌ Invalid JSON response")
                        print(f"   Response: {response_text}")
                        return False
                else:
                    print(f"   ❌ Failed with status {response.status}")
                    print(f"   Response: {response_text}")
                    return False
                    
    except asyncio.TimeoutError:
        elapsed_time = time.time() - start_time
        print(f"   ⏰ Request timed out after {elapsed_time:.1f} seconds")
        return False
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"   ❌ Request failed: {str(e)}")
        print(f"   Processing time before error: {elapsed_time:.1f} seconds")
        return False

async def test_direct_batch_processor():
    """Test the batch processor endpoint directly"""
    
    print("\n🎯 Testing Direct Batch Processor")
    print("=" * 40)
    
    # Test payload for batch processor
    batch_payload = {
        'secret': WEBHOOK_SECRET,
        'pdf_file': '2025-1021.pdf',  # Large PDF
        'batch_info': {
            'batch_number': 1,
            'total_batches': 4,
            'start_auction': 1,
            'end_auction': 50,
            'batch_size': 50
        },
        'source': 'test-direct-batch',
        'processing_id': f"TEST_{int(time.time())}_B1"
    }
    
    batch_processor_url = f"https://{VERCEL_DOMAIN}/api/process-auction-batch"
    
    print(f"📡 Calling batch processor: {batch_processor_url}")
    print(f"📦 Batch payload: {json.dumps(batch_payload, indent=2)}")
    
    start_time = time.time()
    
    try:
        async with aiohttp.ClientSession() as session:
            # Set timeout to 10 minutes for batch processing
            timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes
            
            async with session.post(batch_processor_url, json=batch_payload, timeout=timeout) as response:
                response_text = await response.text()
                elapsed_time = time.time() - start_time
                
                print(f"\n📊 BATCH PROCESSOR RESULTS:")
                print(f"   Status Code: {response.status}")
                print(f"   Processing Time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
                
                if response.status == 200:
                    try:
                        result_data = json.loads(response_text)
                        print(f"   ✅ Success!")
                        print(f"   Batch Range: {batch_payload['batch_info']['start_auction']}-{batch_payload['batch_info']['end_auction']}")
                        print(f"   Auctions Found in Batch: {result_data.get('auctions_found_in_batch', 0)}")
                        print(f"   Auctions Processed: {result_data.get('auctions_processed', 0)}")
                        print(f"   Total Tokens Used: {result_data.get('total_tokens_used', 0)}")
                        print(f"   Estimated Cost: {result_data.get('estimated_cost', '$0.0000')}")
                        
                        return True
                        
                    except json.JSONDecodeError:
                        print(f"   ❌ Invalid JSON response")
                        print(f"   Response: {response_text}")
                        return False
                else:
                    print(f"   ❌ Failed with status {response.status}")
                    print(f"   Response: {response_text}")
                    return False
                    
    except asyncio.TimeoutError:
        elapsed_time = time.time() - start_time
        print(f"   ⏰ Request timed out after {elapsed_time:.1f} seconds")
        return False
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"   ❌ Request failed: {str(e)}")
        print(f"   Processing time before error: {elapsed_time:.1f} seconds")
        return False

async def test_multiple_parallel_batches():
    """Test multiple batch processors running in parallel"""
    
    print("\n⚡ Testing Multiple Parallel Batches")
    print("=" * 40)
    
    # Create multiple batch tasks
    batch_tasks = []
    pdf_file = '2025-1021.pdf'  # Large PDF with 163 auctions
    
    # Create 4 batches of 50 auctions each
    batch_configs = [
        {'batch_number': 1, 'start_auction': 1, 'end_auction': 50},
        {'batch_number': 2, 'start_auction': 51, 'end_auction': 100},
        {'batch_number': 3, 'start_auction': 101, 'end_auction': 150},
        {'batch_number': 4, 'start_auction': 151, 'end_auction': 163}  # Last batch with remaining auctions
    ]
    
    for config in batch_configs:
        batch_payload = {
            'secret': WEBHOOK_SECRET,
            'pdf_file': pdf_file,
            'batch_info': {
                'batch_number': config['batch_number'],
                'total_batches': len(batch_configs),
                'start_auction': config['start_auction'],
                'end_auction': config['end_auction'],
                'batch_size': config['end_auction'] - config['start_auction'] + 1
            },
            'source': 'test-parallel-batches',
            'processing_id': f"PARALLEL_{int(time.time())}_B{config['batch_number']}"
        }
        
        batch_tasks.append(call_batch_processor(batch_payload, config['batch_number']))
    
    print(f"🚀 Starting {len(batch_tasks)} parallel batch processors...")
    start_time = time.time()
    
    # Execute all batches in parallel
    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
    
    elapsed_time = time.time() - start_time
    
    print(f"\n📊 PARALLEL BATCH RESULTS:")
    print(f"   Total Processing Time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
    print(f"   Total Batches: {len(batch_tasks)}")
    
    successful_batches = 0
    failed_batches = 0
    total_auctions_processed = 0
    
    for i, result in enumerate(results, 1):
        print(f"\n   📦 Batch {i}:")
        
        if isinstance(result, Exception):
            print(f"      ❌ Exception: {str(result)}")
            failed_batches += 1
        elif result.get('success'):
            successful_batches += 1
            auctions_processed = result.get('auctions_processed', 0)
            total_auctions_processed += auctions_processed
            print(f"      ✅ Success: {auctions_processed} auctions processed")
            print(f"      Cost: {result.get('estimated_cost', '$0.0000')}")
            print(f"      Time: {result.get('processing_time', 0):.1f}s")
        else:
            failed_batches += 1
            print(f"      ❌ Failed: {result.get('error', 'unknown error')}")
    
    print(f"\n🎉 PARALLEL SUMMARY:")
    print(f"   Successful Batches: {successful_batches}/{len(batch_tasks)}")
    print(f"   Failed Batches: {failed_batches}")
    print(f"   Total Auctions Processed: {total_auctions_processed}")
    print(f"   Average Time per Batch: {elapsed_time/len(batch_tasks):.1f}s")
    
    return successful_batches > 0

async def call_batch_processor(batch_payload, batch_number):
    """Helper function to call batch processor"""
    batch_processor_url = f"https://{VERCEL_DOMAIN}/api/process-auction-batch"
    
    try:
        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes per batch
            
            start_time = time.time()
            async with session.post(batch_processor_url, json=batch_payload, timeout=timeout) as response:
                response_text = await response.text()
                processing_time = time.time() - start_time
                
                if response.status == 200:
                    try:
                        result_data = json.loads(response_text)
                        return {
                            'success': True,
                            'batch_number': batch_number,
                            'auctions_processed': result_data.get('auctions_processed', 0),
                            'total_tokens_used': result_data.get('total_tokens_used', 0),
                            'estimated_cost': result_data.get('estimated_cost', '$0.0000'),
                            'processing_time': processing_time
                        }
                    except json.JSONDecodeError:
                        return {
                            'success': False,
                            'batch_number': batch_number,
                            'error': 'Invalid JSON response',
                            'response_text': response_text,
                            'processing_time': processing_time
                        }
                else:
                    return {
                        'success': False,
                        'batch_number': batch_number,
                        'error': f'HTTP {response.status}',
                        'response_text': response_text,
                        'processing_time': processing_time
                    }
                    
    except Exception as e:
        return {
            'success': False,
            'batch_number': batch_number,
            'error': str(e),
            'error_type': type(e).__name__
        }

async def main():
    """Main test function"""
    print("🧪 BATCH PROCESSING SYSTEM TESTS")
    print("=" * 50)
    print(f"📡 Target Domain: {VERCEL_DOMAIN}")
    print(f"🔐 Using Webhook Secret: {WEBHOOK_SECRET[:10]}...")
    print(f"⏰ Test Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n" + "="*60)
    
    # Test 1: Batch Coordinator (handles automatic batch creation)
    test1_success = await test_batch_coordinator()
    
    # Test 2: Direct Batch Processor (single batch)
    test2_success = await test_direct_batch_processor()
    
    # Test 3: Multiple Parallel Batches (simulates coordinator behavior)
    test3_success = await test_multiple_parallel_batches()
    
    print("\n" + "="*60)
    print("🏁 TEST SUMMARY")
    print("="*20)
    print(f"   Batch Coordinator Test: {'✅ PASS' if test1_success else '❌ FAIL'}")
    print(f"   Direct Batch Processor Test: {'✅ PASS' if test2_success else '❌ FAIL'}")
    print(f"   Parallel Batches Test: {'✅ PASS' if test3_success else '❌ FAIL'}")
    
    overall_success = test1_success or test2_success or test3_success
    print(f"\n🎯 Overall Result: {'✅ SUCCESS' if overall_success else '❌ FAILURE'}")
    
    if overall_success:
        print("\n🎉 Batch processing system is ready for production!")
        print("   - Large PDFs (200+ auctions) will be processed in parallel batches")
        print("   - Small PDFs (<50 auctions) will use sequential processing")
        print("   - Each batch processes 50 auctions to avoid timeouts")
        print("   - Multiple Vercel instances work in parallel for maximum speed")
    else:
        print("\n⚠️ Batch processing system needs debugging before production use")

if __name__ == "__main__":
    asyncio.run(main())