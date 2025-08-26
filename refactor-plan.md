# GCP Data Pipeline Refactor Instructions

You need to refactor the existing MongoDB ingestion pipeline to implement a **bifurcated kappa architecture**. The current pipeline has a single path, but we need to add a parallel processing branch for real-time parquet transformation.

## Current Architecture
```
MongoDB OpLog → Cloud Run (Ingestor) → Pub/Sub → Cloud Function → GCS Raw (JSON)
```

## Target Bifurcated Architecture
```
MongoDB OpLog → Cloud Run (Ingestor) → Pub/Sub → ┌─ Cloud Function → GCS Raw (JSON)
                                                └─ Cloud Run (Transformer) → GCS Processed (Parquet)
```

## Required Changes

### 1. New Cloud Run Service - Parquet Transformer
Work in the directory: `cloud-run-transformer/`

**Files:**
`cloud-run-transformer/`
- `main.py` - Service handling Pub/Sub messages for parquet transformation
- `requirements.txt` - Dependencies: flask, google-cloud-storage, pandas, pyarrow, pymongo, gunicorn
- `Dockerfile` - Standard Python container setup
- `config/schema_mappings.py` - Schema configuration module
- `config/field_mappings.py` - Field transformation logic
- `schema.py` - PyArrow schemas for collections (customers schema already defined)
- `mappings.py` - Field mapping rules (customers mapping already defined)
`scripts/`
- `config.sh` - project hub for settings and variables with sensible data 
- `other scripts for deploying the pipeline pieces.`

### 2. Update config.sh
Add these new variables:

```bash
# --- New Cloud Run Transformer Service ---
export CLOUD_RUN_TRANSFORMER_NAME="parquet-transformer-v1"
export GCS_PROCESSED_BUCKET_NAME="dogfy-data-processed-12345"

# --- EventArc Configuration ---
export EVENTARC_RAW_TRIGGER="raw-storage-trigger"
export EVENTARC_PARQUET_TRIGGER="parquet-transformer-trigger"
```

### 3. Update Infrastructure (Terraform or create manually)
**New GCS Bucket:**
- Name: `dogfy-data-processed-12345`
- Purpose: Store transformed parquet files
- Structure: `processed/{collection}/date={YYYY-MM-DD}/`

**Service Account Permissions:**
- Add Storage Object Admin access to new processed bucket for `data-ingest-sa`

### 4. New Deployment Script
Create `phase-5-deploy-transformer.sh`:

```bash
#!/bin/bash
set -e

# Deploy Cloud Run Transformer
gcloud run deploy "$CLOUD_RUN_TRANSFORMER_NAME" \
  --source ./cloud-run-transformer \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --service-account="$CLOUD_RUN_SA" \
  --set-env-vars "PROJECT_ID=$PROJECT_ID,GCS_PROCESSED_BUCKET_NAME=$GCS_PROCESSED_BUCKET_NAME" \
  --memory=1Gi \
  --timeout=600

# Create EventArc trigger for parquet processing
gcloud eventarc triggers create "$EVENTARC_PARQUET_TRIGGER" \
  --destination-run-service="$CLOUD_RUN_TRANSFORMER_NAME" \
  --destination-run-region="$REGION" \
  --event-filters="type=google.cloud.pubsub.topic.v1.messagePublished" \
  --transport-topic="$PUBSUB_TOPIC_NAME" \
  --project="$PROJECT_ID"
```

### 5. Verify Existing Components
**Check current Cloud Run ingestor:**
- Ensure it's publishing to Pub/Sub in CloudEvents format (double base64 encoding)
- Verify message structure includes: collection, operation, document, timestamp

**Check current Cloud Function:**
- Should continue working as-is for raw JSON storage
- No changes needed to existing functionality

## Implementation Priority
1. Create new transformer Cloud Run service
2. Update config.sh with new variables
3. Create processed bucket and update IAM
4. Deploy transformer service
5. Create EventArc trigger for bifurcated processing
6. Test end-to-end flow

## Validation Criteria
- Raw JSON files continue arriving in `raw/` folder
- New parquet files appear in `processed/customers/` folder
- Both processes handle the same Pub/Sub messages simultaneously
- Schema validation works correctly for customers collection
- Field mappings transform MongoDB documents to target schema

## Critical Notes
- The transformer expects CloudEvents format from your existing ingestor
- Handle both insert/update/delete operations
- Gracefully skip unknown collections until mappings are added
- Maintain existing functionality - this is purely additive