# Cloudflare Worker Webhook Configuration Update

## Update Required in Cloudflare Worker

Change the webhook URL from:
```javascript
const VERCEL_WEBHOOK_URL = 'https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-process';
```

To:
```javascript
const VERCEL_WEBHOOK_URL = 'https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-coordinator';
```

## Or Update via Wrangler Secret

```bash
wrangler secret put VERCEL_WEBHOOK_URL
# Enter: https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-coordinator
```

## Webhook Payload Format

The Cloudflare worker should send:
```json
{
  "secret": "sheriff-auctions-webhook-2025",
  "pdf_files": ["2025-1050.pdf", "2025-1051.pdf"],
  "pdf_count": 2,
  "source": "cloudflare-pdf-checker"
}
```

## Processing Flow

1. **Cloudflare Worker** discovers PDFs → sends webhook to `/api/webhook-coordinator`
2. **webhook-coordinator** analyzes each PDF and counts auctions
3. **Splits into batches** of 25 auctions each
4. **Parallel processing**: Launches up to 5 concurrent `/api/process-auction-batch` instances
5. **Each batch** processes its 25 auctions with OpenAI and uploads to Supabase
6. **Results aggregated** and returned to Cloudflare

## Expected Behavior for 2 PDFs

If each PDF has ~70 auctions:
- PDF 1: 70 auctions → 3 batches (25, 25, 20)
- PDF 2: 70 auctions → 3 batches (25, 25, 20)
- **Total**: 6 batch processor instances running in parallel (max 5 concurrent)
- **Processing time**: ~60-90 seconds (parallel instead of sequential)

## Vercel Function Instances

For 2 PDFs with 70 auctions each:
1. **webhook-coordinator** (1 instance) - orchestrates everything
2. **process-auction-batch** (6 instances) - processes 25 auctions each
   - Batch 1-1: PDF1 auctions 1-25
   - Batch 1-2: PDF1 auctions 26-50
   - Batch 1-3: PDF1 auctions 51-70
   - Batch 2-1: PDF2 auctions 1-25
   - Batch 2-2: PDF2 auctions 26-50
   - Batch 2-3: PDF2 auctions 51-70

**Total**: 7 Vercel function invocations