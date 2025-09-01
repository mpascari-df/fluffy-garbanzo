#!/bin/bash

# phase-5-deploy-cloud-run-transformer.sh
# This script deploys the Cloud Run transformer service and creates an EventArc trigger
# to subscribe it to a Pub/Sub topic. This service will process messages in parallel
# to the existing Cloud Function writer.
# -----------------------------------------------------------------------------
# IMPORTANT: This script will exit on any error.
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

# --- Configuration Validation ---
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$CLOUD_RUN_TRANSFORMER_NAME" ] || [ -z "$CLOUD_RUN_SA" ] || [ -z "$GCS_PROCESSED_BUCKET_NAME" ] || [ -z "$PUBSUB_TOPIC_NAME" ] || [ -z "$EVENTARC_PARQUET_TRIGGER" ]; then
    echo "ERROR: Required variables must be set in '$CONFIG_FILE'."
    echo "Please set: PROJECT_ID, REGION, CLOUD_RUN_TRANSFORMER_NAME, CLOUD_RUN_SA, GCS_PROCESSED_BUCKET_NAME, PUBSUB_TOPIC_NAME, EVENTARC_PARQUET_TRIGGER"
    exit 1
fi

echo "--- Configuration for Phase 5: Deploy Cloud Run Transformer ---"
echo "Project ID:                 $PROJECT_ID"
echo "Region:                     $REGION"
echo "Cloud Run Transformer Name: $CLOUD_RUN_TRANSFORMER_NAME"
echo "Cloud Run Service Account:  $CLOUD_RUN_SA"
echo "Processed GCS Bucket:       $GCS_PROCESSED_BUCKET_NAME"
echo "Pub/Sub Topic:              $PUBSUB_TOPIC_NAME"
echo "EventArc Trigger Name:      $EVENTARC_PARQUET_TRIGGER"
echo "-------------------------------------------------------------"

# Step 1: Enable necessary APIs
echo "Enabling required APIs: Run, Build, Artifact Registry, EventArc..."
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  eventarc.googleapis.com \
  --project="$PROJECT_ID"

echo "APIs enabled successfully."
echo "---------------------------------"

# Step 2: Create Artifact Registry repository if it doesn't exist
REPOSITORY_NAME="cloud-pipeline"
echo "Setting up Artifact Registry repository '$REPOSITORY_NAME'..."

if gcloud artifacts repositories describe "$REPOSITORY_NAME" --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "Artifact Registry repository '$REPOSITORY_NAME' already exists."
else
    echo "Creating Artifact Registry repository '$REPOSITORY_NAME'..."
    gcloud artifacts repositories create "$REPOSITORY_NAME" \
        --repository-format=docker \
        --location="$REGION" \
        --project="$PROJECT_ID" \
        --description="Container images for data pipeline services"
    echo "Artifact Registry repository created successfully."
fi
echo "---------------------------------"

# Step 3: Configure Docker authentication
echo "Configuring Docker authentication..."
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
echo "Docker authentication configured."
echo "---------------------------------"

# Step 4: Build and push Docker image with unique naming
IMAGE_NAME="cloud-run-transformer"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BUILD_ID=$(openssl rand -hex 4)
UNIQUE_TAG="${TIMESTAMP}-${BUILD_ID}"
IMAGE_URL_UNIQUE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/$IMAGE_NAME:$UNIQUE_TAG"
IMAGE_URL_LATEST="$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/$IMAGE_NAME:latest"
TRANSFORMER_SOURCE_DIR="${SCRIPT_DIR}/../cloud-run-transformer"

echo "Building and pushing Docker image..."
echo "Source directory: $TRANSFORMER_SOURCE_DIR"
echo "Unique tag: $UNIQUE_TAG"
echo "Image URL: $IMAGE_URL_UNIQUE"

# Verify source directory exists
if [ ! -d "$TRANSFORMER_SOURCE_DIR" ]; then
    echo "ERROR: Source directory '$TRANSFORMER_SOURCE_DIR' not found."
    echo "Please ensure the cloud-run-transformer directory exists relative to this script."
    exit 1
fi

# Build using Cloud Build with unique tag
echo "Building Docker image with unique tag..."
gcloud builds submit "$TRANSFORMER_SOURCE_DIR" \
    --tag="$IMAGE_URL_UNIQUE" \
    --project="$PROJECT_ID" \
    --timeout=10m

echo "Docker image built and pushed successfully: $IMAGE_URL_UNIQUE"
echo "---------------------------------"

# Step 5: Deploy the Cloud Run Transformer service
echo "Deploying Cloud Run service '$CLOUD_RUN_TRANSFORMER_NAME' with unique image..."

gcloud run deploy "$CLOUD_RUN_TRANSFORMER_NAME" \
  --image="$IMAGE_URL_UNIQUE" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --service-account="$CLOUD_RUN_SA" \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,GCS_PROCESSED_BUCKET_NAME=$GCS_PROCESSED_BUCKET_NAME,BUILD_TAG=$UNIQUE_TAG" \
  --memory=2Gi \
  --cpu=2 \
  --timeout=3600 \
  --concurrency=1000 \
  --min-instances=0 \
  --max-instances=10 \
  --execution-environment=gen2 \
  --cpu-boost \
  --port=8080 \
  --no-allow-unauthenticated

echo "Cloud Run service '$CLOUD_RUN_TRANSFORMER_NAME' deployed successfully."
echo "---------------------------------"

# Step 6: Create the EventArc trigger to link Pub/Sub to the Cloud Run service
echo "Creating EventArc trigger '$EVENTARC_PARQUET_TRIGGER'..."

# Check if trigger already exists
if gcloud eventarc triggers describe "$EVENTARC_PARQUET_TRIGGER" --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "EventArc trigger '$EVENTARC_PARQUET_TRIGGER' already exists."
else
    gcloud eventarc triggers create "$EVENTARC_PARQUET_TRIGGER" \
      --destination-run-service="$CLOUD_RUN_TRANSFORMER_NAME" \
      --destination-run-region="$REGION" \
      --event-filters="type=google.cloud.pubsub.topic.v1.messagePublished" \
      --transport-topic="$PUBSUB_TOPIC_NAME" \
      --service-account="$CLOUD_RUN_SA" \
      --location="$REGION" \
      --project="$PROJECT_ID"
    
    echo "EventArc trigger created successfully."
fi
echo "---------------------------------"

# Step 7: Grant the service account the invoker role for the new service
echo "Assigning Cloud Run Invoker role to '$CLOUD_RUN_SA' for service '$CLOUD_RUN_TRANSFORMER_NAME'..."
gcloud run services add-iam-policy-binding "$CLOUD_RUN_TRANSFORMER_NAME" \
  --member="serviceAccount:$CLOUD_RUN_SA" \
  --role="roles/run.invoker" \
  --region="$REGION" \
  --project="$PROJECT_ID"

echo "Cloud Run Invoker permissions updated successfully."
echo "---------------------------------"

# Step 8: Grant GCS permissions for the processed bucket
echo "Granting GCS Storage Object Admin permissions to '$CLOUD_RUN_SA' for bucket '$GCS_PROCESSED_BUCKET_NAME'..."

# Check if the service account already has permissions
EXISTING_POLICY=$(gsutil iam get "gs://$GCS_PROCESSED_BUCKET_NAME" 2>/dev/null || echo "")

if echo "$EXISTING_POLICY" | grep -q "$CLOUD_RUN_SA.*roles/storage.objectAdmin"; then
    echo "Service account already has Storage Object Admin permissions on the bucket."
else
    echo "Adding Storage Object Admin permissions..."
    gsutil iam ch "serviceAccount:$CLOUD_RUN_SA:roles/storage.objectAdmin" "gs://$GCS_PROCESSED_BUCKET_NAME"
    echo "GCS permissions granted successfully."
fi

echo "---------------------------------"

# Step 9: Verify deployment by testing health endpoint
echo "Testing deployment..."
SERVICE_URL=$(gcloud run services describe "$CLOUD_RUN_TRANSFORMER_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.url)')

echo "Service URL: $SERVICE_URL"

# Test health endpoint (this will fail if not authenticated, but that's expected for now)
echo "Service deployed successfully. Use the following to test:"
echo "gcloud run services proxy $CLOUD_RUN_TRANSFORMER_NAME --port=8080 --region=$REGION --project=$PROJECT_ID"
echo "Then visit: http://localhost:8080/health"
echo "---------------------------------"

# Step 10: Display service information
echo "--- Deployment Summary ---"
echo "✓ Cloud Run service deployed: $CLOUD_RUN_TRANSFORMER_NAME"
echo "✓ Image deployed: $IMAGE_URL_UNIQUE"
echo "✓ Build tag: $UNIQUE_TAG"
echo "✓ EventArc trigger created: $EVENTARC_PARQUET_TRIGGER"
echo "✓ Service URL: $SERVICE_URL"
echo "✓ Connected to Pub/Sub topic: $PUBSUB_TOPIC_NAME"
echo "✓ Processing bucket: $GCS_PROCESSED_BUCKET_NAME"
echo ""
echo "Cache-busting features used:"
echo "  • Unique timestamp-based tags: $UNIQUE_TAG"
echo "  • Each deployment gets a unique image tag"
echo ""
echo "The transformer service is now part of your bifurcated pipeline:"
echo "  mongo-ingestor-v2 → $PUBSUB_TOPIC_NAME → [$CLOUD_FUNCTION_NAME (JSON) & $CLOUD_RUN_TRANSFORMER_NAME (Parquet)]"
echo ""
echo "Monitor logs with:"
echo "gcloud logs tail --project=$PROJECT_ID --resource.labels.service_name=$CLOUD_RUN_TRANSFORMER_NAME"
echo ""
echo "For troubleshooting, check the build tag in environment: $UNIQUE_TAG"
echo ""
echo "--- Phase 5: Cloud Run Transformer Deployment Complete ---"