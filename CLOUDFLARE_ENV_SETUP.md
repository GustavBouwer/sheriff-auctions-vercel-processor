# Cloudflare Worker Environment Variables Setup

## Required Environment Variables for PDF Checker Worker

### 1. Core Configuration
```bash
cd /Users/gustavbouwer/D4/github/Development/cloudflare-workers/pdf-checker

# Enable/disable PDF downloading
wrangler secret put ENABLE_PDF_DOWNLOAD
# Value: true

# Enable email notifications  
wrangler secret put ENABLE_EMAIL_NOTIFICATIONS
# Value: true (or false if you don't want emails)

# Notification email address
wrangler secret put NOTIFICATION_EMAIL
# Value: your-email@example.com
```

### 2. Webhook Configuration (NEW - for Vercel communication)
```bash
# Enable webhook notifications to Vercel
wrangler secret put ENABLE_WEBHOOK_NOTIFICATIONS  
# Value: true

# Vercel webhook URL (your processor endpoint)
wrangler secret put VERCEL_WEBHOOK_URL
# Value: https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-process

# Webhook secret for security
wrangler secret put WEBHOOK_SECRET
# Value: sheriff-auctions-webhook-2025
```

### 3. Email Configuration (Resend API)
```bash
# Resend API key for email notifications
wrangler secret put RESEND_API_KEY
# Value: [Get from resend.com dashboard - starts with re_]

# From email address (optional)
wrangler secret put RESEND_FROM_EMAIL
# Value: notifications@yourdomain.com (or use default: onboarding@resend.dev)
```

### 4. Environment Variables (via wrangler.toml - not secrets)

Edit your `wrangler.toml` file in the pdf-checker directory:

```toml
name = "sheriff-pdf-checker"
main = "src/index.js"
compatibility_date = "2023-10-30"

[vars]
# These are public environment variables (not secrets)
CURRENT_YEAR = "2025"
PDF_NAMING_FORMAT = "YYYY-number.pdf"

# Cron trigger for automated checking
[triggers]
crons = ["*/5 * * * *"]  # Every 5 minutes

# KV namespace binding
[[kv_namespaces]]
binding = "SEEN_PDFS"
id = "501a26658bc44767b27394e9e16dc6a4"

# R2 bucket binding  
[[r2_buckets]]
binding = "PDF_BUCKET"
bucket_name = "sheriff-auction-pdfs"
```

## Command Sequence for Setup

Run these commands in order in your pdf-checker directory:

```bash
# Navigate to the project
cd /Users/gustavbouwer/D4/github/Development/cloudflare-workers/pdf-checker

# Set core functionality
wrangler secret put ENABLE_PDF_DOWNLOAD
# Enter: true

wrangler secret put ENABLE_EMAIL_NOTIFICATIONS  
# Enter: true

wrangler secret put NOTIFICATION_EMAIL
# Enter: gustav@example.com (your actual email)

# Set webhook configuration for Vercel communication
wrangler secret put ENABLE_WEBHOOK_NOTIFICATIONS
# Enter: true

wrangler secret put VERCEL_WEBHOOK_URL
# Enter: https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-process

wrangler secret put WEBHOOK_SECRET
# Enter: sheriff-auctions-webhook-2025

# Set email API (get key from resend.com)
wrangler secret put RESEND_API_KEY
# Enter: [Your Resend API key]

# Optional: Custom from email
wrangler secret put RESEND_FROM_EMAIL  
# Enter: notifications@yourdomain.com

# Deploy the worker with new configuration
wrangler deploy
```

## Testing the Setup

After setting the environment variables, test the webhook system:

```bash
# Test the worker directly
curl "https://sheriff-pdf-checker.gjb8.workers.dev/"

# Check the status endpoint
curl "https://sheriff-pdf-checker.gjb8.workers.dev/status"

# The status should show:
# - webhook_notifications_enabled: "true"
# - vercel_webhook_url: your webhook URL
# - webhook_secret_configured: "Yes"
```

## Webhook Communication Flow

Once configured, the system works as follows:

1. **Cloudflare Worker** runs every 5 minutes (cron job)
2. **Discovers new PDFs** and downloads them to R2 `unprocessed/` folder
3. **Sends webhook** to Vercel with PDF filenames
4. **Vercel receives webhook** at `/api/webhook-process`
5. **Processes PDFs** (3 auctions max during testing)
6. **Uploads to Supabase** and moves PDFs to `processed/` folder

## Security Notes

- `WEBHOOK_SECRET` validates that requests to Vercel come from your Cloudflare Worker
- All API keys are stored securely using Wrangler secrets (encrypted at rest)
- Email notifications include direct links to PDFs for manual review

## Troubleshooting

If webhooks aren't working:

1. Check Vercel logs for webhook endpoint errors
2. Verify `VERCEL_WEBHOOK_URL` points to correct deployment
3. Ensure `WEBHOOK_SECRET` matches between Cloudflare and Vercel
4. Test webhook endpoint directly with curl:

```bash
curl -X POST "https://sheriff-auctions-data-etl-zzd2.vercel.app/api/webhook-process" \
  -H "Content-Type: application/json" \
  -d '{
    "secret": "sheriff-auctions-webhook-2025",
    "event": "new_pdfs_ready", 
    "pdf_files": ["test.pdf"],
    "source": "manual-test"
  }'
```