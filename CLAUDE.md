# Sheriff Auctions Vercel Python Processor - Hybrid Architecture Component

## 🏗️ **Project Overview - Python PDF Processing Service**

This is the **Python processing component** of the Sheriff Auctions hybrid architecture. It handles robust PDF text extraction and processing that Cloudflare Workers cannot handle due to memory constraints.

## 🎯 **Architecture Position**

### **Hybrid System Design**
```
┌─────────────────────────────────────────────────────────────┐
│     📡 Cloudflare Workers (PDF Discovery & Download)        │
│  • URL: sheriff-pdf-checker.gjb8.workers.dev               │
│  • Monitors SAFLII.org every 5 minutes                     │
│  • Downloads Legal Notice B PDFs (bypasses blocking)       │
│  • Stores in R2: sheriff-auction-pdfs/unprocessed/         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼ (PDFs in R2 storage)
┌─────────────────────────────────────────────────────────────┐
│    🐍 Vercel Python Processor (THIS SERVICE) ✅            │
│  • URL: sheriff-auctions-processor.vercel.app              │
│  • Fetches PDFs from R2 via S3-compatible API              │
│  • Robust text extraction with pdfplumber                  │
│  • Individual auction processing (cost-optimized)          │
│  • OpenAI GPT-3.5 structured extraction                    │
│  • Supabase database upload                                │
│  • Moves PDFs to processed/ or errors/ folders             │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼ (Structured data)
┌─────────────────────────────────────────────────────────────┐
│         🗄️ Supabase Database (Production)                  │
│  • Table: auctions (unique case_number constraint)         │
│  • Sheriff UUID mapping                                    │
│  • Used by sheriff-auction-finder-sa web app              │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 **Key Features**

### **✅ Production-Ready Components**
- **R2 Integration**: S3-compatible API access to Cloudflare R2 bucket
- **Robust PDF Processing**: pdfplumber handles large PDFs (180+ pages)
- **Individual Auction Processing**: One auction at a time (proven cost-effective)
- **OpenAI Integration**: GPT-3.5-turbo with retry logic and safety controls
- **Data Cleaning**: Fuzzy matching and normalization
- **Supabase Upload**: Direct database integration with schema validation
- **Error Management**: Comprehensive logging and file organization

### **🛡️ Safety Controls**
```python
# Critical cost protection settings
ENABLE_PROCESSING = False           # Emergency stop switch
MAX_AUCTIONS_PER_RUN = 50          # Limit per request
MAX_OPENAI_TOKENS_PER_RUN = 100000 # Token usage cap
```

## 📁 **Project Structure**

```
sheriff-auctions-vercel-processor/
├── api/
│   ├── process.py              # Main processing endpoint
│   ├── process-auctions.py     # Batch auction processing
│   ├── process-single.py       # Single PDF processing
│   ├── status.py              # System status endpoint
│   ├── test.py                # Test endpoint
│   ├── hello.py               # Health check
│   └── debug-r2.py            # R2 debugging utilities
├── requirements.txt            # Python dependencies
├── vercel.json                # Vercel configuration
├── README.md                  # User documentation
└── CLAUDE.md                  # This file (AI context)
```

## 🔧 **Configuration**

### **Required Environment Variables**
```env
# OpenAI Configuration (CRITICAL - Monitor usage!)
OPENAI_API_KEY=sk-proj-your-key-here
ENABLE_PROCESSING=false  # Start with false!
MAX_AUCTIONS_PER_RUN=50
MAX_OPENAI_TOKENS_PER_RUN=100000

# Supabase Configuration
SUPABASE_URL=https://esfyvihtnzwlnlrllewb.supabase.co
SUPABASE_KEY=your-service-role-key

# Cloudflare R2 Configuration (S3-compatible)
R2_ACCESS_KEY_ID=your-r2-access-key
R2_SECRET_ACCESS_KEY=your-r2-secret
R2_BUCKET_NAME=sheriff-auction-pdfs
R2_ENDPOINT_URL=https://account-id.r2.cloudflarestorage.com

# Google Maps (optional - for geocoding)
GOOGLE_MAPS_API_KEY=your-google-key

# Sheriff Configuration
DEFAULT_SHERIFF_UUID=f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130
```

## 🔄 **Processing Pipeline**

### **1. PDF Acquisition (from R2)**
```python
# Fetch PDFs from R2 unprocessed/ folder
s3_client = boto3.client('s3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY
)
```

### **2. Text Extraction**
```python
# Use pdfplumber for robust extraction
with pdfplumber.open(pdf_stream) as pdf:
    # Start from page 13 (skip headers)
    # Stop at PAUC section if found
    text = extract_relevant_pages(pdf)
```

### **3. Auction Splitting**
```python
# Split by Case No pattern
auctions = re.split(r'Case No:\s*\d+(?:\/\d+)?', text)
# Process ONE auction at a time (cost optimization)
```

### **4. OpenAI Extraction**
```python
# Individual auction processing
for auction in auctions[:MAX_AUCTIONS_PER_RUN]:
    data = extract_with_gpt(auction_text)  # GPT-3.5-turbo
    clean_data = normalize_fields(data)    # Fuzzy matching
    upload_to_supabase(clean_data)        # Database insert
```

### **5. File Management**
```python
# Move processed PDFs in R2
if success:
    move_to_folder('processed/')
else:
    move_to_folder('errors/')
```

## 📊 **API Endpoints**

### **Primary Processing Endpoint**
```bash
POST /api/process
# Process PDFs with safety controls
# Parameters:
#   - max_pdfs: Limit PDFs per request (default: 10)
#   - pdf_key: Process specific PDF

# Example:
curl -X POST "https://your-app.vercel.app/api/process?max_pdfs=5"
```

### **Status Monitoring**
```bash
GET /api/status
# Returns system health and configuration

Response:
{
  "status": "operational",
  "processing_enabled": false,
  "services": {
    "openai": true,
    "supabase": true,
    "r2_storage": true
  }
}
```

## 🚨 **Critical Safety Features**

### **Cost Protection Mechanisms**
1. **Emergency Stop**: `ENABLE_PROCESSING=false` stops all processing
2. **Auction Limits**: Hard cap on auctions per run
3. **Token Monitoring**: OpenAI usage tracking
4. **Individual Processing**: One auction at a time (vs expensive batching)

### **Error Isolation**
- Failed auctions don't stop entire batch
- PDFs with errors moved to `errors/` folder
- Comprehensive error logging for debugging
- Graceful handling of API failures

## 🧪 **Testing Protocol**

### **Safe Testing Sequence**
```bash
# 1. Deploy with processing disabled
ENABLE_PROCESSING=false vercel --prod

# 2. Verify configuration
curl https://your-app.vercel.app/api/status

# 3. Test with single PDF
ENABLE_PROCESSING=true MAX_AUCTIONS_PER_RUN=5
curl -X POST "https://your-app.vercel.app/api/process?max_pdfs=1"

# 4. Monitor results in Supabase and R2
```

## 📈 **Performance Metrics**

### **Typical Processing Times**
- **PDF Download from R2**: 2-5 seconds
- **Text Extraction (180 pages)**: 10-15 seconds
- **Per Auction Processing**: 1-2 seconds
- **Total per PDF (70 auctions)**: 45-90 seconds

### **Resource Usage**
- **Memory**: ~500MB peak (within Vercel 1GB limit)
- **Function Timeout**: 300 seconds (5 minutes)
- **Concurrent Requests**: Supported

## 🔐 **Security Considerations**

### **API Key Management**
- All keys stored as Vercel environment variables
- Service role keys used with caution
- Regular key rotation recommended

### **Data Validation**
- Input sanitization before processing
- Schema validation before database upload
- Case number uniqueness enforced

## 💡 **Development Notes**

### **Why Python on Vercel?**
- **PDF Libraries**: Mature Python PDF processing (pdfplumber, PyPDF2)
- **Memory**: 1GB+ available vs Workers' 128MB limit
- **Compatibility**: Existing Python ETL code reuse
- **Cost**: Pay-per-use serverless model

### **Why Not Pure Cloudflare?**
- **Memory Constraints**: Large PDFs exceed Workers limits
- **PDF Processing**: Limited JavaScript PDF libraries
- **Binary Dependencies**: Python libraries handle complex PDFs better

### **Integration Points**
- **R2 Bucket**: Shared storage between Cloudflare and Vercel
- **Supabase**: Common database for all services
- **Monitoring**: Both services report to same status endpoints

## 🚀 **Deployment**

### **Vercel Deployment Steps**
```bash
# 1. Install Vercel CLI
npm install -g vercel

# 2. Deploy project
vercel --prod

# 3. Set environment variables
vercel env add OPENAI_API_KEY
vercel env add SUPABASE_URL
# ... add all required variables

# 4. Redeploy with environment
vercel --prod
```

### **Production Checklist**
- [ ] Set `ENABLE_PROCESSING=false` initially
- [ ] Configure all environment variables
- [ ] Test with single PDF first
- [ ] Monitor OpenAI usage dashboard
- [ ] Set up cost alerts
- [ ] Verify Supabase uploads
- [ ] Check R2 file movements

## 📊 **Monitoring & Maintenance**

### **Daily Monitoring**
- Check `/api/status` endpoint
- Review processed vs error PDFs in R2
- Monitor OpenAI API usage
- Verify Supabase data quality

### **Weekly Tasks**
- Process error PDFs manually if needed
- Review and optimize OpenAI prompts
- Check for duplicate case numbers
- Update sheriff UUID mappings

### **Cost Management**
- Track OpenAI token usage
- Monitor Vercel function invocations
- Review R2 storage costs
- Optimize processing frequency

## 🔄 **Future Enhancements**

### **Planned Features**
1. **Geocoding Integration**: Add Google Maps coordinates for future auctions
2. **Property24 Integration**: Fetch recent sales data
3. **Sheriff Mapping**: Dynamic sheriff office UUID resolution
4. **Batch Optimization**: Intelligent batching for cost reduction
5. **WebSocket Updates**: Real-time processing status

### **Performance Optimizations**
- Implement caching for repeated data
- Parallel processing for independent auctions
- Optimize PDF text extraction algorithms
- Add compression for R2 storage

## 🆘 **Troubleshooting**

### **Common Issues**

**PDF Not Processing**
- Check `ENABLE_PROCESSING` is true
- Verify PDF exists in R2 `unprocessed/` folder
- Check Vercel function logs
- Ensure API keys are valid

**High OpenAI Costs**
- Reduce `MAX_AUCTIONS_PER_RUN`
- Optimize GPT prompts for efficiency
- Consider using GPT-3.5-turbo vs GPT-4
- Implement caching for similar auctions

**Supabase Upload Failures**
- Check for duplicate case numbers
- Verify schema matches expectations
- Ensure service role key has permissions
- Review RLS policies

## 📝 **Version History**

### **v1.0.0 (August 2025)**
- Initial hybrid architecture implementation
- Basic PDF processing from R2
- OpenAI integration with safety controls
- Supabase upload functionality

### **Planned v1.1.0**
- Add geocoding for future auctions
- Implement Property24 integration
- Enhanced error recovery
- Performance optimizations

---

## 🎯 **Mission Critical Information**

### **⚠️ ALWAYS REMEMBER**
1. **Start with `ENABLE_PROCESSING=false`** - Prevent accidental processing
2. **Monitor OpenAI costs closely** - Can escalate quickly
3. **Process individually** - One auction at a time saves money
4. **Test with single PDFs first** - Validate before batch processing
5. **Keep emergency stop ready** - Know how to disable processing instantly

### **💰 Cost Optimization Tips**
- Use GPT-3.5-turbo instead of GPT-4
- Process only necessary fields
- Skip geocoding for past auctions
- Batch similar auctions when safe
- Cache repeated data lookups

### **🔒 Security Best Practices**
- Never commit API keys to repository
- Use Vercel environment variables
- Rotate keys regularly
- Monitor for unusual activity
- Implement rate limiting

---

*This service is a critical component of the Sheriff Auctions data pipeline, providing robust PDF processing capabilities that complement the Cloudflare Workers' PDF discovery and download functionality.*

**Status**: 🟢 Ready for Production Deployment (with safety controls)
*Last Updated: August 2025*