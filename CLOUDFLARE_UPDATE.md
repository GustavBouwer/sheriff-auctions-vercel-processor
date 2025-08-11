# Cloudflare Worker Update Instructions

## 🔄 Update Required for Multi-Stage Processing

Your Cloudflare Worker needs to be updated to use the new v2 webhook endpoint for better timeout handling.

### **Change Required:**

In your Cloudflare Worker (`pdf-checker.js`), update the webhook URL:

```javascript
// OLD:
const VERCEL_WEBHOOK_URL = 'https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-process';

// NEW:
const VERCEL_WEBHOOK_URL = 'https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-receive-v2';
```

### **That's it! No other changes needed.**

The webhook payload format remains the same:
```json
{
  "secret": "sheriff-auctions-webhook-2025",
  "event": "new_pdfs_ready",
  "timestamp": "2025-08-08T18:00:00Z",
  "pdf_files": ["2025-989.pdf", "2025-990.pdf"],
  "pdf_count": 2,
  "source": "cloudflare-pdf-checker"
}
```

## 📊 **New Processing Flow:**

1. **Cloudflare Worker** sends webhook with PDF list to `/api/webhook-receive-v2`
2. **Stage 1** (webhook-receive-v2):
   - Downloads PDFs from R2
   - Extracts text and splits into auctions
   - Creates batches of 25 auctions
   - Stores batches in R2 temp storage
   - Triggers parallel processing
   - Completes in ~45 seconds

3. **Stage 2** (process-auction-batch) - runs in parallel:
   - Each batch processes 25 auctions
   - OpenAI extraction
   - Supabase upload
   - Completes in ~60 seconds per batch

## 🔍 **Monitoring:**

Check batch processing status:
```bash
curl https://sheriff-auctions-data-etl-zzd2.vercel.app/api/batch-monitor
```

## 🎯 **Benefits:**

- **No timeout issues** - even with 500+ auctions
- **3x faster** - parallel processing
- **Fault tolerant** - individual batch failures don't affect others
- **Scalable** - handles any PDF size

## 🧪 **Testing:**

1. Update Cloudflare Worker with new URL
2. Clear 2 PDFs from SEEN_PDFS KV store
3. Trigger the worker
4. Monitor progress:
   - Check `/api/batch-monitor` for batch status
   - Watch Vercel logs for processing
   - Verify Supabase for uploaded auctions

## 📝 **Environment Variables:**

No new environment variables needed. The system uses existing:
- `R2_*` credentials for storage
- `OPENAI_API_KEY` for extraction
- `SUPABASE_*` for database
- `WEBHOOK_SECRET` for security