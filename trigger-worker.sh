#!/bin/bash

# Manual trigger for Cloudflare Worker
# Usage: ./trigger-worker.sh

echo "🚀 Triggering Cloudflare Worker manually..."
echo "=================================="

# Replace with your actual worker URL if different
WORKER_URL="https://sheriff-pdf-checker.gjb8.workers.dev"

# Check if worker URL is accessible
echo "📍 Worker URL: $WORKER_URL"
echo "⏳ Sending request..."
echo

# Trigger the worker
response=$(curl -s -w "HTTPSTATUS:%{http_code}" "$WORKER_URL" 2>&1)

# Extract HTTP status and body
http_code=$(echo "$response" | grep -o "HTTPSTATUS:[0-9]*" | cut -d: -f2)
response_body=$(echo "$response" | sed -E 's/HTTPSTATUS:[0-9]*$//')

if [ "$http_code" = "200" ]; then
    echo "✅ Worker triggered successfully!"
    echo "📝 Response: $response_body"
else
    echo "❌ Worker trigger failed with status: $http_code"
    echo "📝 Response: $response_body"
fi

echo
echo "=================================="
echo "📊 Next steps:"
echo "1. Check Vercel logs at: https://vercel.com/easy-projects/sheriff-auctions-vercel-processor"
echo "2. Monitor batch processing:"
echo "   curl https://sheriff-auctions-data-etl-zzd2.vercel.app/api/batch-monitor"
echo "3. Watch for email notifications"