#!/bin/bash
# phase-4-deploy-cloud-function-optimized.sh
# Optimized deployment script for the production-ready Cloud Function
# This replaces the original phase-4-deploy-cloud-function.sh

set -e

# --- Sourcing Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config_prod.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    exit 1
fi
source "$CONFIG_FILE"

# --- Configuration Validation ---
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$PUBSUB_TOPIC_NAME" ] || \
   [ -z "$GCS_DATA_BUCKET_NAME" ] || [ -z "$CLOUD_FUNCTION_NAME" ] || \
   [ -z "$CLOUD_FUNCTION_SA" ]; then
    echo "ERROR: Required environment variables not set in config_prod.sh"
    exit 1
fi

echo "======================================"
echo "  OPTIMIZED CLOUD FUNCTION DEPLOYMENT"
echo "======================================"
echo "Project:              $PROJECT_ID"
echo "Region:               $REGION"
echo "Function Name:        $CLOUD_FUNCTION_NAME"
echo "Pub/Sub Topic:        $PUBSUB_TOPIC_NAME"
echo "GCS Bucket:           $GCS_DATA_BUCKET_NAME"
echo "Service Account:      $CLOUD_FUNCTION_SA"
echo "======================================"

# Step 1: Enable necessary APIs
echo "📦 Enabling required APIs..."
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  eventarc.googleapis.com \
  run.googleapis.com \
  logging.googleapis.com \
  --project "$PROJECT_ID"

echo "✅ APIs enabled"

# Step 2: Verify GCS bucket exists and is accessible
echo "🔍 Verifying GCS bucket access..."
if ! gsutil ls -b "gs://${GCS_DATA_BUCKET_NAME}" &>/dev/null; then
    echo "ERROR: GCS bucket ${GCS_DATA_BUCKET_NAME} not found or not accessible"
    exit 1
fi

# Grant service account access to the bucket
echo "🔐 Granting storage permissions..."
gsutil iam ch "serviceAccount:${CLOUD_FUNCTION_SA}:roles/storage.objectAdmin" \
  "gs://${GCS_DATA_BUCKET_NAME}" || true

echo "✅ Storage permissions configured"

# Step 3: Create error directory structure in GCS
echo "📁 Creating error directory structure..."
# Create placeholder file for errors directory
echo '{"info": "Error logs directory"}' | \
  gsutil cp - "gs://${GCS_DATA_BUCKET_NAME}/errors/README.txt" || true

# Step 4: Check if DLQ subscription exists or needs to be created
echo "🔍 Checking Dead Letter Queue configuration..."
DLQ_SUBSCRIPTION_NAME="${CLOUD_FUNCTION_NAME}-dlq-sub"

# Check if DLQ subscription exists
if ! gcloud pubsub subscriptions describe "$DLQ_SUBSCRIPTION_NAME" \
     --project="$PROJECT_ID" &>/dev/null; then
    
    echo "Creating DLQ subscription..."
    
    # First ensure DLQ topic exists
    if [ ! -z "$READER_DLQ_TOPIC_NAME" ]; then
        gcloud pubsub topics create "$READER_DLQ_TOPIC_NAME" \
          --project="$PROJECT_ID" 2>/dev/null || true
        
        # Create subscription for monitoring DLQ
        gcloud pubsub subscriptions create "$DLQ_SUBSCRIPTION_NAME" \
          --topic="$READER_DLQ_TOPIC_NAME" \
          --project="$PROJECT_ID" \
          --ack-deadline=600 \
          --message-retention-duration=7d || true
    fi
fi

# Step 5: Deploy the optimized Cloud Function
echo "🚀 Deploying Cloud Function '$CLOUD_FUNCTION_NAME'..."

CF_SOURCE_DIR="${SCRIPT_DIR}/../cloud-function-writer"

# Verify source directory
if [ ! -d "$CF_SOURCE_DIR" ]; then
    echo "ERROR: Source directory '$CF_SOURCE_DIR' not found."
    exit 1
fi

# Verify required files exist
if [ ! -f "$CF_SOURCE_DIR/main.py" ] || [ ! -f "$CF_SOURCE_DIR/requirements.txt" ]; then
    echo "ERROR: main.py or requirements.txt not found in $CF_SOURCE_DIR"
    exit 1
fi

# Deploy with optimized settings
gcloud functions deploy "$CLOUD_FUNCTION_NAME" \
  --gen2 \
  --runtime=python311 \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --source="$CF_SOURCE_DIR" \
  --entry-point=process_pubsub_message \
  --trigger-topic="$PUBSUB_TOPIC_NAME" \
  --service-account="$CLOUD_FUNCTION_SA" \
  --set-env-vars="GCS_DATA_BUCKET_NAME=${GCS_DATA_BUCKET_NAME},ENABLE_DETAILED_LOGGING=true,LOG_STATS_EVERY_N=100,COLLECTIONS_TO_LOG=${COLLECTIONS_TO_LOG:-}" \
  --memory=512MiB \
  --timeout=30s \
  --min-instances=0 \
  --max-instances=100 \
  --concurrency=1 \
  --ingress-settings=internal-only

echo "✅ Cloud Function deployed successfully"

echo "🔐 Setting Cloud Run invoker permissions..."

# Grant to service account
gcloud run services add-iam-policy-binding "$CLOUD_FUNCTION_NAME" \
  --member="serviceAccount:${CLOUD_FUNCTION_SA}" \
  --role="roles/run.invoker" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --quiet || true

# Grant to Pub/Sub service account
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
gcloud run services add-iam-policy-binding "$CLOUD_FUNCTION_NAME" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com" \
  --role="roles/run.invoker" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --quiet || true

echo "✅ Invoker permissions configured"

# Step 6: Configure Pub/Sub subscription retry policy
echo "📮 Configuring Pub/Sub subscription retry policy..."

# Get the subscription name created by the function
SUBSCRIPTION_NAME=$(gcloud functions describe "$CLOUD_FUNCTION_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --format="value(eventTrigger.pubsubTopic)" | \
  awk -F'/' '{print $NF}')

if [ ! -z "$SUBSCRIPTION_NAME" ]; then
    # Update subscription with retry and DLQ settings
    if [ ! -z "$READER_DLQ_TOPIC_NAME" ]; then
        echo "Setting up retry policy with DLQ..."
        gcloud pubsub subscriptions update \
          "gcf-${CLOUD_FUNCTION_NAME}-${REGION}-${PUBSUB_TOPIC_NAME}" \
          --project="$PROJECT_ID" \
          --ack-deadline=30 \
          --min-retry-delay=10s \
          --max-retry-delay=600s \
          --dead-letter-topic="$READER_DLQ_TOPIC_NAME" \
          --max-delivery-attempts=5 2>/dev/null || \
        echo "Note: Could not update subscription settings - may need manual configuration"
    fi
fi

# Step 7: Test the function health endpoint
echo "🔍 Testing function deployment..."

# Get function URL
FUNCTION_URL=$(gcloud functions describe "$CLOUD_FUNCTION_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --format="value(serviceConfig.uri)" 2>/dev/null || echo "")

if [ ! -z "$FUNCTION_URL" ]; then
    echo "Function URL: $FUNCTION_URL"
    echo "Testing health endpoint..."
    
    # Test with timeout
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      --max-time 5 \
      "${FUNCTION_URL}/health" 2>/dev/null || echo "failed")
    
    if [ "$HTTP_CODE" == "200" ]; then
        echo "✅ Health check passed"
    else
        echo "⚠️  Health check returned: $HTTP_CODE (may still be deploying)"
    fi
fi

# Step 8: Create monitoring alert (optional)
echo "📊 Setting up monitoring..."

# Create log-based metric for errors
gcloud logging metrics create "${CLOUD_FUNCTION_NAME}_errors" \
  --description="Errors in ${CLOUD_FUNCTION_NAME}" \
  --project="$PROJECT_ID" \
  --log-filter="resource.type=\"cloud_function\" \
    resource.labels.function_name=\"${CLOUD_FUNCTION_NAME}\" \
    severity>=ERROR" 2>/dev/null || \
  echo "Note: Error metric may already exist"

# Display summary
echo ""
echo "======================================"
echo "  DEPLOYMENT COMPLETE!"
echo "======================================"
echo "✅ Function Name:     $CLOUD_FUNCTION_NAME"
echo "✅ Trigger Topic:     $PUBSUB_TOPIC_NAME"
echo "✅ Output Bucket:     gs://$GCS_DATA_BUCKET_NAME"
echo "✅ Error Logs:        gs://$GCS_DATA_BUCKET_NAME/errors/"
if [ ! -z "$READER_DLQ_TOPIC_NAME" ]; then
    echo "✅ Dead Letter Queue: $READER_DLQ_TOPIC_NAME"
fi
echo ""
echo "📊 Monitor function logs:"
echo "gcloud functions logs read $CLOUD_FUNCTION_NAME \\"
echo "  --region=$REGION --project=$PROJECT_ID"
echo ""
echo "📊 View function metrics:"
echo "gcloud functions describe $CLOUD_FUNCTION_NAME \\"
echo "  --region=$REGION --project=$PROJECT_ID"
echo ""
echo "📊 Test with sample message:"
echo "gcloud pubsub topics publish $PUBSUB_TOPIC_NAME \\"
echo '  --message='"'"'{"operation":"test","collection":"test","timestamp":"2024-01-01T00:00:00Z"}'"'"
echo ""
echo "======================================"