#!/bin/bash
#
# This script builds and deploys the Dataflow Flex Template.
# It containerizes the pipeline code, pushes the image to Artifact Registry,
# and creates a template spec file in GCS.
#
# Prerequisites:
# 1. gcloud CLI is installed and authenticated.
# 2. The user has permissions to enable APIs, manage Artifact Registry,
#    Cloud Build, and GCS.
# 3. A GCS bucket for storing the template file and staging Dataflow jobs.
#
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
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$GCS_DATAFLOW_BUCKET_NAME" ] || [ -z "$ARTIFACT_REGISTRY_REPO_NAME" ] || [ -z "$DATAFLOW_TEMPLATE_NAME" ] || [ -z "$DATAFLOW_SA" ]; then
    echo "ERROR: Required variables must be set in '$CONFIG_FILE'."
    echo "Please set: PROJECT_ID, REGION, GCS_DATAFLOW_BUCKET_NAME, ARTIFACT_REGISTRY_REPO_NAME, DATAFLOW_TEMPLATE_NAME, DATAFLOW_SA"
    exit 1
fi

# --- Template Configuration ---
# Generate a unique timestamp that will serve as the image tag and template identifier.
# This ensures that each deployment is unique, preventing caching issues.
IMAGE_TAG="$(date +%Y%m%d-%H%M%S)"

# Define the full image and template paths using the unique tag.
# Note that the image name now includes the unique tag.
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO_NAME}/${DATAFLOW_TEMPLATE_NAME}:${IMAGE_TAG}"
TEMPLATE_FILE_GCS_PATH="gs://${GCS_DATAFLOW_BUCKET_NAME}/dataflow/templates/${DATAFLOW_TEMPLATE_NAME}:${IMAGE_TAG}.json"

# --- Source Directory ---
SOURCE_DIR="${SCRIPT_DIR}/../dataflow"

echo "--- Configuration ---"
echo "Project ID:           $PROJECT_ID"
echo "Region:               $REGION"
echo "GCS Dataflow Bucket:  $GCS_DATAFLOW_BUCKET_NAME"
echo "Template Name:        $DATAFLOW_TEMPLATE_NAME"
echo "Template Image:       $IMAGE_NAME"
echo "Template GCS Path:    $TEMPLATE_FILE_GCS_PATH"
echo "---------------------"

# Step 1: Enable necessary APIs
echo "Enabling required APIs: Artifact Registry, Cloud Build, and Dataflow..."
gcloud services enable \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  dataflow.googleapis.com \
  --project="$PROJECT_ID"

# Step 2: Grant required permissions to the Service Account
# This ensures the Dataflow worker service account can access GCS and run as a worker.
# These commands are idempotent.
echo "Granting required IAM roles to Service Account: $DATAFLOW_SA"

echo "  - Granting 'roles/dataflow.worker' on the project..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${DATAFLOW_SA}" \
    --role="roles/dataflow.worker" \
    --condition=None > /dev/null # Suppress verbose output

echo "  - Granting 'roles/storage.objectAdmin' on the Dataflow GCS bucket..."
gcloud storage buckets add-iam-policy-binding "gs://${GCS_DATAFLOW_BUCKET_NAME}" \
    --member="serviceAccount:${DATAFLOW_SA}" \
    --role="roles/storage.objectAdmin" > /dev/null # Suppress verbose output

echo "  - Granting 'roles/artifactregistry.reader' on the Dataflow GCS bucket..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$DATAFLOW_SA" \
    --role="roles/artifactregistry.reader" > /dev/null # Suppress verbose output

# Step 3: Create Artifact Registry repository if it doesn't exist
echo "Checking for Artifact Registry repository '$ARTIFACT_REGISTRY_REPO_NAME'..."
if ! gcloud artifacts repositories describe "$ARTIFACT_REGISTRY_REPO_NAME" --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "Repository not found. Creating it..."
    gcloud artifacts repositories create "$ARTIFACT_REGISTRY_REPO_NAME" \
        --repository-format=docker \
        --location="$REGION" \
        --description="Repository for data services" \
        --project="$PROJECT_ID"
else
    echo "Artifact Registry repository '$ARTIFACT_REGISTRY_REPO_NAME' already exists."
fi

# Step 4: Build the container image using Cloud Build and push it to Artifact Registry.
echo "Building and pushing container image to Artifact Registry from '${SOURCE_DIR}'..."
gcloud builds submit "$SOURCE_DIR" --tag "$IMAGE_NAME" --project="$PROJECT_ID"

# Step 5: Build the Dataflow Flex Template.
# This command creates a template spec file in GCS that points to the container image.
echo "Building Dataflow Flex Template..."
gcloud dataflow flex-template build "$TEMPLATE_FILE_GCS_PATH" \
  --image "$IMAGE_NAME" \
  --sdk-language "PYTHON" \
  --project="$PROJECT_ID"

echo "--- Deployment Complete ---"
echo "Dataflow Flex Template created successfully."
echo "Template file is located at: $TEMPLATE_FILE_GCS_PATH"
echo "You can now run this template from the GCP Console, gcloud CLI, or REST API."
echo "---------------------"