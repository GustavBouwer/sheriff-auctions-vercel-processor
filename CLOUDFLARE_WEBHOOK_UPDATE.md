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
3. **Duplicate Prevention**: Checks existing case numbers in Supabase database
4. **Splits into batches** of 50 new auctions each (skips duplicates)
5. **Parallel processing**: Launches up to 5 concurrent `/api/process-auction-batch` instances
6. **Each batch** processes its 50 new auctions with OpenAI and uploads to Supabase
7. **Results aggregated** and returned to Cloudflare

## Expected Behavior for 2 PDFs (with Duplicate Prevention)

If each PDF has ~158 auctions, but many are already processed:
- PDF 1: 158 auctions → 98 already exist → 60 new auctions → 2 batches (50, 10)
- PDF 2: 158 auctions → 128 already exist → 30 new auctions → 1 batch (30)
- **Total**: 3 batch processor instances (90 new auctions, 226 duplicates skipped)
- **Processing time**: ~6 minutes (3 batches × 2 minutes each)
- **Cost savings**: ~75% reduction by skipping duplicates

## Vercel Function Instances (After Duplicate Prevention)

For 2 PDFs with 158 auctions each (example scenario):
1. **webhook-coordinator** (1 instance) - orchestrates everything + duplicate checking
2. **process-auction-batch** (3 instances) - processes 50 new auctions each
   - Batch 1-1: PDF1 new auctions 1-50
   - Batch 1-2: PDF1 new auctions 51-60 (10 auctions)
   - Batch 2-1: PDF2 new auctions 1-30 (30 auctions)

**Total**: 4 Vercel function invocations (instead of 7+ without duplicate prevention)
**Cost Impact**: ~75% reduction in processing costs and function invocations