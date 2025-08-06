# Sheriff Auctions PDF Processor - Vercel Python Service

## 🏗️ Hybrid Architecture: Cloudflare + Vercel Python

This service is part of a hybrid architecture that combines the best of both platforms:

- **Cloudflare Workers**: PDF discovery and download from saflii.org → R2 storage
- **Vercel Python**: Robust PDF processing, OpenAI extraction, and Supabase upload

## 🎯 Key Features

### ✅ Production-Ready Components
- **R2 Integration**: Fetches PDFs from Cloudflare R2 bucket via S3-compatible API
- **Robust PDF Processing**: Uses pdfplumber for reliable text extraction from large PDFs (180+ pages)
- **Individual Auction Processing**: Processes one auction at a time to minimize OpenAI costs
- **OpenAI GPT Integration**: Structured data extraction with retry logic and safety controls
- **Supabase Integration**: Direct upload to production database
- **Comprehensive Error Handling**: Detailed logging and graceful failure handling

### 🛡️ Safety Controls & Cost Protection

#### OpenAI Credit Safeguards
```env
ENABLE_PROCESSING=false          # Emergency stop - set to 'true' to enable
MAX_AUCTIONS_PER_RUN=50         # Limit auctions per request
MAX_OPENAI_TOKENS_PER_RUN=100000 # Token usage limit per request
```

#### Processing Controls
- **Individual Processing**: One auction at a time (your original design)
- **Error Isolation**: Failed auctions don't stop the entire batch
- **Processing Limits**: Configurable limits to prevent runaway costs
- **Emergency Stop**: `ENABLE_PROCESSING=false` stops all processing immediately

## 📁 Project Structure

```
sheriff-auctions-vercel-processor/
├── api/
│   └── process.py           # Main Vercel serverless function
├── vercel.json              # Vercel configuration
├── requirements.txt         # Python dependencies
├── README.md               # This file
└── .env.example            # Environment variables template
```

## 🚀 Deployment Instructions

### 1. Environment Variables Setup

Create these environment variables in Vercel:

```env
# OpenAI Configuration
OPENAI_API_KEY=sk-proj-your-openai-key

# Supabase Configuration  
SUPABASE_URL=https://esfyvihtnzwlnlrllewb.supabase.co
SUPABASE_KEY=your-supabase-service-key

# Cloudflare R2 Configuration (S3-compatible)
R2_ACCESS_KEY_ID=your-r2-access-key
R2_SECRET_ACCESS_KEY=your-r2-secret-key
R2_BUCKET_NAME=sheriff-auction-pdfs
R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com

# Google Maps (for geocoding)
GOOGLE_MAPS_API_KEY=your-google-maps-key

# Safety Configuration
ENABLE_PROCESSING=false              # IMPORTANT: Start with false!
MAX_AUCTIONS_PER_RUN=50
MAX_OPENAI_TOKENS_PER_RUN=100000

# Sheriff Configuration
DEFAULT_SHERIFF_UUID=f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130
```

### 2. Deploy to Vercel

```bash
# Install Vercel CLI
npm install -g vercel

# Deploy
vercel --prod

# Set environment variables (do this for each variable)
vercel env add OPENAI_API_KEY
vercel env add SUPABASE_URL
# ... (add all variables listed above)

# Redeploy to pick up environment variables
vercel --prod
```

### 3. Verify Deployment

```bash
# Test the service
curl https://your-vercel-app.vercel.app/api/test

# Check status
curl https://your-vercel-app.vercel.app/api/status
```

## 📋 API Endpoints

### GET/POST `/api/process`
Process PDFs from R2 bucket with safety controls.

**Parameters:**
- `max_pdfs` (optional): Maximum PDFs to process per request (default: 10)
- `pdf_key` (optional): Process specific PDF by key

**Example:**
```bash
# Process up to 5 PDFs
curl -X POST "https://your-app.vercel.app/api/process?max_pdfs=5"

# Process specific PDF
curl -X POST "https://your-app.vercel.app/api/process?pdf_key=unprocessed/2025-989.pdf"
```

**Response:**
```json
{
  "timestamp": "2025-08-06T20:00:00Z",
  "processing_enabled": true,
  "pdfs_processed": [
    {
      "pdf_key": "unprocessed/2025-989.pdf",
      "success": true,
      "auctions_found": 70,
      "auctions_processed": 70,
      "auctions_uploaded": 68,
      "errors": [],
      "processing_time_ms": 45000
    }
  ],
  "total_auctions_found": 70,
  "total_auctions_uploaded": 68,
  "total_errors": 0,
  "processing_time_ms": 45000
}
```

### GET `/api/status`
Get system status and configuration.

**Response:**
```json
{
  "status": "operational",
  "configuration": {
    "processing_enabled": false,
    "max_auctions_per_run": 50
  },
  "services": {
    "openai": true,
    "supabase": true,
    "r2_storage": true
  },
  "bucket_status": {
    "unprocessed_pdfs": 3,
    "sample_files": ["unprocessed/2025-989.pdf"]
  }
}
```

### GET `/api/test`
Simple health check endpoint.

## 🔄 Processing Flow

1. **PDF Fetch**: Download PDF from R2 `unprocessed/` folder
2. **Text Extraction**: Extract text starting from page 13, stop at PAUC section
3. **Auction Splitting**: Split by "Case No:" pattern into individual auctions
4. **Individual Processing**: Process ONE auction at a time (cost-efficient)
5. **OpenAI Extraction**: Extract structured data using GPT-3.5-turbo
6. **Data Cleaning**: Apply fuzzy matching and normalization
7. **Geocoding**: Add coordinates for future auctions (TODO: implement)
8. **Supabase Upload**: Insert complete record into auctions table
9. **File Management**: Move PDF to R2 `processed/` or `errors/` folder

## 🛡️ Safety Features

### Cost Protection
- **Processing Toggle**: `ENABLE_PROCESSING=false` stops all processing
- **Auction Limits**: Maximum auctions per run to control costs
- **Token Limits**: OpenAI token usage monitoring
- **Individual Processing**: Process one auction at a time (vs batching)

### Error Handling  
- **Graceful Failures**: Individual auction errors don't stop entire PDF
- **Detailed Logging**: Comprehensive error reporting
- **File Isolation**: Failed PDFs moved to `errors/` folder
- **Retry Logic**: Built-in OpenAI retry with exponential backoff

### Monitoring
- **Real-time Status**: Check processing status and configuration
- **Bucket Monitoring**: View unprocessed PDFs count
- **Performance Tracking**: Processing time and success rates

## 🧪 Testing

### 1. Start with Safety On
```bash
# Verify processing is disabled
curl https://your-app.vercel.app/api/status

# Should show: "processing_enabled": false
```

### 2. Test with Single PDF
```bash
# Enable processing for testing
vercel env add ENABLE_PROCESSING true
vercel --prod

# Process single PDF
curl -X POST "https://your-app.vercel.app/api/process?max_pdfs=1"
```

### 3. Monitor Results
```bash
# Check Supabase for uploaded auctions
# Verify R2 bucket for moved files
# Review processing logs in Vercel dashboard
```

## 🔧 Configuration Options

### Processing Control
```env
ENABLE_PROCESSING=true           # Enable/disable processing
MAX_AUCTIONS_PER_RUN=50         # Limit auctions per request  
MAX_OPENAI_TOKENS_PER_RUN=100000 # Token usage limit
```

### Performance Tuning
- **Vercel Function Timeout**: 300 seconds (5 minutes)
- **Memory**: 1GB (Vercel Pro default)
- **Concurrent Processing**: Handle multiple requests simultaneously

### Cost Optimization
- **Individual Processing**: One auction at a time (your proven approach)
- **Selective Geocoding**: Only for future auctions (cost savings)
- **Efficient PDF Libraries**: pdfplumber for fast extraction
- **Minimal Dependencies**: Only essential packages included

## 🚨 Important Notes

### Before First Use
1. **Set `ENABLE_PROCESSING=false`** initially
2. **Test with single PDF** first
3. **Monitor OpenAI usage** closely
4. **Verify Supabase uploads** work correctly

### Production Recommendations
- **Start with small batches** (max_pdfs=5)
- **Monitor OpenAI costs** regularly
- **Set up Vercel alerts** for function timeouts
- **Backup Supabase data** before large runs

### Emergency Procedures
- **Stop Processing**: Set `ENABLE_PROCESSING=false`
- **Revert Changes**: Use Vercel rollback feature
- **Check Costs**: Monitor OpenAI and Vercel usage dashboards

## 💡 Next Steps

1. **Deploy and Test**: Deploy to Vercel and verify with single PDF
2. **Add Geocoding**: Implement `extract_area_components` from your original code
3. **Sheriff Mapping**: Add sheriff office UUID mapping
4. **Monitoring**: Set up alerts for costs and errors
5. **Automation**: Connect to Cloudflare Worker for automatic triggering

---

*This hybrid architecture gives you the reliability of Python PDF processing with the cost-effectiveness of serverless deployment, while maintaining your proven individual auction processing approach.*