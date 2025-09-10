#!/bin/bash
# phase-3-deploy-async.sh
# Deployment script for the ASYNC MongoDB ingestor to Cloud Run.
# Handles all necessary APIs, permissions, and configurations for the async version.
# Features can be enabled/disabled via config_prod.sh settings.
# -----------------------------------------------------------------------------
set -e

# --- Sourcing Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config_prod.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    exit 1
fi
source "$CONFIG_FILE"

# --- Display Configuration ---
echo "======================================"
echo "   ASYNC MONGO INGESTOR DEPLOYMENT   "
echo "======================================"
echo "Project:              $PROJECT_ID"
echo "Region:               $REGION"
echo "Service:              $CLOUD_RUN_SERVICE_NAME"
echo "MongoDB:              $MONGO_DB_NAME"
echo "Pub/Sub Topic:        $PUBSUB_TOPIC_NAME"
echo "DLQ Topic:            $PUBLISHER_DLQ_TOPIC_NAME"
echo ""
echo "--- Resource Allocation ---"
echo "Memory:               ${CLOUD_RUN_MEMORY:-2Gi}"
echo "CPU:                  ${CLOUD_RUN_CPU:-2}"
echo "Min Instances:        ${MIN_INSTANCES:-1}"
echo "Max Instances:        ${MAX_INSTANCES:-10}"
echo ""
echo "--- Feature Flags ---"
echo "Firestore:            ${ENABLE_FIRESTORE:-false}"
echo "Metrics Export:       ${ENABLE_METRICS_EXPORT:-false}"
echo "Detailed Logging:     ${ENABLE_DETAILED_LOGGING:-true}"
echo "======================================"
echo ""

# Ask for confirmation
read -p "Continue with deployment? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi

# Step 1: Enable necessary APIs
echo "🔧 Enabling required APIs..."
APIS_TO_ENABLE="run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  pubsub.googleapis.com \
  logging.googleapis.com"

# Conditionally add APIs based on features
if [ "${ENABLE_FIRESTORE:-false}" == "true" ]; then
    APIS_TO_ENABLE="$APIS_TO_ENABLE firestore.googleapis.com"
fi

if [ "${ENABLE_METRICS_EXPORT:-false}" == "true" ]; then
    APIS_TO_ENABLE="$APIS_TO_ENABLE monitoring.googleapis.com"
fi

gcloud services enable $APIS_TO_ENABLE --project="$PROJECT_ID"
echo "✅ APIs enabled"

# Step 2: Setup Firestore (if enabled)
if [ "${ENABLE_FIRESTORE:-false}" == "true" ]; then
    echo "🔥 Setting up Firestore for resume token persistence..."
    
    # Check if Firestore database exists
    if ! gcloud firestore databases describe --project="$PROJECT_ID" >/dev/null 2>&1; then
        echo "Creating Firestore database..."
        gcloud firestore databases create \
            --location="$REGION" \
            --type=firestore-native \
            --project="$PROJECT_ID" || {
            echo "⚠️  Firestore creation failed. Service will use timestamp-based recovery."
        }
    else
        echo "✅ Firestore database already exists"
    fi
    
    # Grant Firestore permissions
    echo "Granting Firestore permissions..."
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:${CLOUD_RUN_SA}" \
        --role="roles/datastore.user" \
        --condition=None >/dev/null 2>&1 || true
else
    echo "⏭️  Skipping Firestore setup (disabled in config)"
fi

# Step 3: Setup Cloud Monitoring (if enabled)
if [ "${ENABLE_METRICS_EXPORT:-false}" == "true" ]; then
    echo "📊 Setting up Cloud Monitoring..."
    
    # Grant monitoring permissions
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:${CLOUD_RUN_SA}" \
        --role="roles/monitoring.metricWriter" \
        --condition=None >/dev/null 2>&1 || true
    
    echo "✅ Cloud Monitoring configured"
else
    echo "⏭️  Skipping Cloud Monitoring setup (disabled in config)"
fi

# Step 4: Grant core IAM permissions
echo "🔐 Setting up IAM permissions..."

# Secret Manager access
gcloud secrets add-iam-policy-binding "$MONGO_URI" \
    --member="serviceAccount:${CLOUD_RUN_SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --project="$PROJECT_ID" >/dev/null 2>&1 || true

# Pub/Sub Publisher
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${CLOUD_RUN_SA}" \
    --role="roles/pubsub.publisher" \
    --condition=None >/dev/null 2>&1 || true

# Logging Writer
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${CLOUD_RUN_SA}" \
    --role="roles/logging.logWriter" \
    --condition=None >/dev/null 2>&1 || true

echo "✅ IAM permissions configured"

# Step 5: Setup Artifact Registry
REPO_NAME="${ARTIFACT_REGISTRY_REPO:-data-ingestion-repo}"
echo "🐳 Setting up Artifact Registry repository '$REPO_NAME'..."

if ! gcloud artifacts repositories describe "$REPO_NAME" \
    --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "Creating repository..."
    gcloud artifacts repositories create "$REPO_NAME" \
        --repository-format=docker \
        --location="$REGION" \
        --description="Repository for async ingestion services" \
        --project="$PROJECT_ID"
else
    echo "✅ Repository already exists"
fi

# Step 6: Build container image
echo "🔨 Building container image..."

# Generate unique tag
TIMESTAMP=$(date +%Y%m%d%H%M%S)
BUILD_ID=$(openssl rand -hex 4 2>/dev/null || echo "$(date +%s)")
IMAGE_TAG="${TIMESTAMP}-${BUILD_ID}"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${CLOUD_RUN_SERVICE_NAME}:${IMAGE_TAG}"

echo "  Image: ${IMAGE_NAME}"

# Build with Cloud Build
gcloud builds submit "${SCRIPT_DIR}/../cloud-run-ingestor" \
    --tag "$IMAGE_NAME" \
    --project="$PROJECT_ID" \
    --timeout=15m \
    --machine-type=e2-highcpu-8 || {
    echo "❌ Build failed. Check the logs above."
    exit 1
}

echo "✅ Container image built successfully"

# Step 7: Prepare environment variables
echo "📝 Preparing environment variables..."

# Build environment variables string
ENV_VARS=""

# Required variables
ENV_VARS="${ENV_VARS}PROJECT_ID=${PROJECT_ID},"
ENV_VARS="${ENV_VARS}REGION=${REGION},"
ENV_VARS="${ENV_VARS}MONGO_DB_NAME=${MONGO_DB_NAME},"
ENV_VARS="${ENV_VARS}PUBSUB_TOPIC_NAME=${PUBSUB_TOPIC_NAME},"
ENV_VARS="${ENV_VARS}PUBLISHER_DLQ_TOPIC_NAME=${PUBLISHER_DLQ_TOPIC_NAME},"

# Performance settings
ENV_VARS="${ENV_VARS}PUBLISHER_WORKERS=${PUBLISHER_WORKERS:-10},"
ENV_VARS="${ENV_VARS}MAX_CONCURRENT_PUBLISHES=${MAX_CONCURRENT_PUBLISHES:-20},"
ENV_VARS="${ENV_VARS}QUEUE_MAX_SIZE=${QUEUE_MAX_SIZE:-10000},"

# MongoDB settings
ENV_VARS="${ENV_VARS}OPLOG_WINDOW_HOURS=${OPLOG_WINDOW_HOURS:-24},"
ENV_VARS="${ENV_VARS}CHANGE_STREAM_BATCH_SIZE=${CHANGE_STREAM_BATCH_SIZE:-100},"

# Token settings
ENV_VARS="${ENV_VARS}TOKEN_CHECKPOINT_EVENTS=${TOKEN_CHECKPOINT_EVENTS:-1000},"
ENV_VARS="${ENV_VARS}TOKEN_CHECKPOINT_SECONDS=${TOKEN_CHECKPOINT_SECONDS:-30},"

# Feature flags
ENV_VARS="${ENV_VARS}ENABLE_METRICS_EXPORT=${ENABLE_METRICS_EXPORT:-false},"
ENV_VARS="${ENV_VARS}ENABLE_DETAILED_LOGGING=${ENABLE_DETAILED_LOGGING:-true},"
ENV_VARS="${ENV_VARS}ENABLE_COLLECTION_STATS=${ENABLE_COLLECTION_STATS:-true},"

# Optional settings (only if defined)
[ ! -z "$COLLECTIONS_TO_LOG" ] && ENV_VARS="${ENV_VARS}COLLECTIONS_TO_LOG=${COLLECTIONS_TO_LOG},"
[ ! -z "$QUEUE_PRESSURE_THRESHOLD" ] && ENV_VARS="${ENV_VARS}QUEUE_PRESSURE_THRESHOLD=${QUEUE_PRESSURE_THRESHOLD},"
[ ! -z "$CIRCUIT_BREAKER_THRESHOLD" ] && ENV_VARS="${ENV_VARS}CIRCUIT_BREAKER_THRESHOLD=${CIRCUIT_BREAKER_THRESHOLD},"
[ ! -z "$CIRCUIT_BREAKER_WINDOW_SECONDS" ] && ENV_VARS="${ENV_VARS}CIRCUIT_BREAKER_WINDOW_SECONDS=${CIRCUIT_BREAKER_WINDOW_SECONDS},"

# Remove trailing comma
ENV_VARS="${ENV_VARS%,}"

# Step 8: Deploy to Cloud Run
echo "🚀 Deploying to Cloud Run..."

# Base deployment command
DEPLOY_CMD="gcloud run deploy $CLOUD_RUN_SERVICE_NAME"
DEPLOY_CMD="$DEPLOY_CMD --image $IMAGE_NAME"
DEPLOY_CMD="$DEPLOY_CMD --region $REGION"
DEPLOY_CMD="$DEPLOY_CMD --project $PROJECT_ID"
DEPLOY_CMD="$DEPLOY_CMD --service-account=$CLOUD_RUN_SA"
DEPLOY_CMD="$DEPLOY_CMD --no-allow-unauthenticated"
DEPLOY_CMD="$DEPLOY_CMD --execution-environment=gen2"

# Resources
DEPLOY_CMD="$DEPLOY_CMD --memory=${CLOUD_RUN_MEMORY:-2Gi}"
DEPLOY_CMD="$DEPLOY_CMD --cpu=${CLOUD_RUN_CPU:-2}"
DEPLOY_CMD="$DEPLOY_CMD --min-instances=${MIN_INSTANCES:-1}"
DEPLOY_CMD="$DEPLOY_CMD --max-instances=${MAX_INSTANCES:-10}"

# Timeouts and limits
DEPLOY_CMD="$DEPLOY_CMD --timeout=3600"
DEPLOY_CMD="$DEPLOY_CMD --concurrency=1000"
DEPLOY_CMD="$DEPLOY_CMD --port=8080"

# VPC configuration
DEPLOY_CMD="$DEPLOY_CMD --vpc-connector=$VPC_CONNECTOR_NAME"
DEPLOY_CMD="$DEPLOY_CMD --vpc-egress=$VPC_EGRESS_SETTING"

# Environment and secrets
DEPLOY_CMD="$DEPLOY_CMD --set-env-vars=\"$ENV_VARS\""
DEPLOY_CMD="$DEPLOY_CMD --set-secrets=\"MONGO_URI=${MONGO_URI}:latest\""

# CPU boost (if enabled)
if [ "${CPU_BOOST:-true}" == "true" ]; then
    DEPLOY_CMD="$DEPLOY_CMD --cpu-boost"
fi

# Execute deployment
eval $DEPLOY_CMD || {
    echo "❌ Deployment failed. Check the logs above."
    exit 1
}

echo "✅ Cloud Run service deployed successfully"

# Step 9: Get service URL
SERVICE_URL=$(gcloud run services describe "$CLOUD_RUN_SERVICE_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.url)')

# Step 10: Verify deployment
echo "🔍 Verifying deployment..."

# Get auth token
AUTH_TOKEN=$(gcloud auth print-identity-token 2>/dev/null)

# Test health endpoint
if [ ! -z "$AUTH_TOKEN" ]; then
    echo "Testing health endpoint..."
    HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $AUTH_TOKEN" \
        "${SERVICE_URL}/health" 2>/dev/null || echo "failed")
    
    if [ "$HEALTH_RESPONSE" == "200" ]; then
        echo "✅ Health check passed"
    else
        echo "⚠️  Health check returned: $HEALTH_RESPONSE"
        echo "   Service may still be starting up. Check logs for details."
    fi
else
    echo "⚠️  Could not get auth token for health check"
fi

# Step 11: Display summary
echo ""
echo "======================================"
echo "   DEPLOYMENT COMPLETE!              "
echo "======================================"
echo "Service:        $CLOUD_RUN_SERVICE_NAME"
echo "URL:            $SERVICE_URL"
echo "Image:          $IMAGE_NAME"
echo ""
echo "📊 Configuration Summary:"
echo "  Workers:        ${PUBLISHER_WORKERS:-10}"
echo "  Queue Size:     ${QUEUE_MAX_SIZE:-10000}"
echo "  Checkpoints:    Every ${TOKEN_CHECKPOINT_EVENTS:-1000} events or ${TOKEN_CHECKPOINT_SECONDS:-30}s"
echo "  Firestore:      ${ENABLE_FIRESTORE:-false}"
echo "  Metrics:        ${ENABLE_METRICS_EXPORT:-false}"
echo ""
echo "🔍 Useful Commands:"
echo ""
echo "# View logs:"
echo "gcloud logs tail --project=$PROJECT_ID \\"
echo "  --resource.labels.service_name=$CLOUD_RUN_SERVICE_NAME"
echo ""
echo "# Check metrics:"
echo "curl -H \"Authorization: Bearer \$(gcloud auth print-identity-token)\" \\"
echo "  ${SERVICE_URL}/metrics"
echo ""
echo "# Test publish:"
echo "curl -X POST -H \"Authorization: Bearer \$(gcloud auth print-identity-token)\" \\"
echo "  -H \"Content-Type: application/json\" \\"
echo "  -d '{\"test\": true, \"collection\": \"test\"}' \\"
echo "  ${SERVICE_URL}/test-publish"
echo ""
echo "# Enable features later:"
echo "# 1. Set ENABLE_FIRESTORE=true in config_prod.sh"
echo "# 2. Run this script again"
echo ""
echo "======================================"

# Optional: Create basic monitoring dashboard (if metrics enabled)
if [ "${ENABLE_METRICS_EXPORT:-false}" == "true" ]; then
    echo ""
    echo "📊 Creating monitoring dashboard..."
    
    # This is optional and can fail without breaking deployment
    cat > /tmp/basic-dashboard.json << 'EOF'
{
  "displayName": "MongoDB Async Ingestor - Basic",
  "mosaicLayout": {
    "columns": 12,
    "tiles": [
      {
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Processing Rate",
          "xyChart": {
            "dataSets": [{
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "metric.type=\"custom.googleapis.com/mongo_ingestor/events_per_second\""
                }
              }
            }]
          }
        }
      }
    ]
  }
}
EOF
    
    gcloud monitoring dashboards create \
        --config-from-file=/tmp/basic-dashboard.json \
        --project="$PROJECT_ID" 2>/dev/null && \
        echo "✅ Dashboard created" || \
        echo "⚠️  Dashboard creation failed (may already exist)"
    
    rm -f /tmp/basic-dashboard.json
fi

echo ""
echo "🎉 Async MongoDB Ingestor is ready!"
echo "   Start with core functionality, enable features as needed."