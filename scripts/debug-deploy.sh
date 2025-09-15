#!/bin/bash
# debug-deploy.sh - Simple deployment for debugging
set -e

# Load config
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/config_prod.sh"

echo "==================================="
echo "DEBUG DEPLOYMENT"
echo "==================================="

# Build
echo "Building container..."
TIMESTAMP=$(date +%Y%m%d%H%M%S)
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO}/debug-ingestor:${TIMESTAMP}"

gcloud builds submit "${SCRIPT_DIR}/../cloud-run-ingestor" \
    --tag "$IMAGE" \
    --project="$PROJECT_ID" \
    --timeout=10m

# Deploy with minimal config
echo "Deploying service..."
SERVICE_NAME="${CLOUD_RUN_SERVICE_NAME}-debug"

gcloud run deploy "$SERVICE_NAME" \
    --image "$IMAGE" \
    --region "$REGION" \
    --project "$PROJECT_ID" \
    --service-account="$CLOUD_RUN_SA" \
    --no-allow-unauthenticated \
    --memory=2Gi \
    --cpu=2 \
    --min-instances=1 \
    --max-instances=1 \
    --timeout=300 \
    --port=8080 \
    --vpc-connector="$VPC_CONNECTOR_NAME" \
    --vpc-egress="$VPC_EGRESS_SETTING" \
    --set-env-vars="PROJECT_ID=${PROJECT_ID},MONGO_DB_NAME=${MONGO_DB_NAME},PUBSUB_TOPIC_NAME=${PUBSUB_TOPIC_NAME},PUBLISHER_DLQ_TOPIC_NAME=${PUBLISHER_DLQ_TOPIC_NAME},PUBLISHER_WORKERS=2,ENABLE_FIRESTORE=false,ENABLE_METRICS_EXPORT=false" \
    --set-secrets="MONGO_URI=${MONGO_URI}:latest"

# Get URL
URL=$(gcloud run services describe "$SERVICE_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.url)')

echo ""
echo "Service deployed: $SERVICE_NAME"
echo "URL: $URL"
echo ""

# Test health
echo "Testing health endpoint..."
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" "${URL}/health"

echo ""
echo ""
echo "View logs:"
echo "gcloud logs tail --project=$PROJECT_ID \"resource.labels.service_name=$SERVICE_NAME\""