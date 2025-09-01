# phase-3-deploy-app.sh
# This script builds a container image and deploys it to a Cloud Run service.
# It handles secret creation, IAM permissions, and deploys from the correct source directory.
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
# **Added KEEP_ALIVE_CLOUD_RUN_INGESTOR to validation**
if [ -z "$PROJECT_ID" ] || [ -z "$PUBSUB_TOPIC_NAME" ] || [ -z "$MONGO_DB_NAME" ] || [ -z "$PUBLISHER_DLQ_TOPIC_NAME" ] || [ -z "$CLOUD_RUN_SERVICE_NAME" ] || [ -z "$REGION" ] || [ -z "$CLOUD_RUN_SA" ] || [ -z "$MONGO_URI" ] || [ -z "$KEEP_ALIVE_CLOUD_RUN_INGESTOR" ] || [ -z "$VPC_CONNECTOR_NAME" ] || [ -z "$VPC_EGRESS_SETTING" ]; then
    echo "ERROR: PROJECT_ID, PUBSUB_TOPIC_NAME, MONGO_DB_NAME, PUBLISHER_DLQ_TOPIC_NAME, CLOUD_RUN_SERVICE_NAME, REGION, CLOUD_RUN_SA, MONGO_URI, KEEP_ALIVE_CLOUD_RUN_INGESTOR, VPC_CONNECTOR_NAME, and VPC_EGRESS_SETTING must be set in '$CONFIG_FILE'."
    exit 1
fi

echo "--- Configuration for Phase 3 ---"
echo "Project ID:           $PROJECT_ID"
echo "Region:               $REGION"
echo "Mongo Secret ID:      $MONGO_URI"
echo "Pub/Sub Topic:        $PUBSUB_TOPIC_NAME"
echo "Pub/Sub DLQ Topic:    $PUBLISHER_DLQ_TOPIC_NAME"
echo "MongoDB Database:     $MONGO_DB_NAME"
echo "Cloud Run Service:    $CLOUD_RUN_SERVICE_NAME"
echo "Cloud Run SA:         $CLOUD_RUN_SA"
echo "Keep Instance Alive:  $KEEP_ALIVE_CLOUD_RUN_INGESTOR" # **Added for clarity**
echo "VPC Connector:        $VPC_CONNECTOR_NAME"
echo "VPC Egress:           $VPC_EGRESS_SETTING"
echo "---------------------------------"

# Step 1: Enable necessary APIs
echo "Enabling required APIs: Secret Manager and Artifact Registry..."
gcloud services enable \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  --project="$PROJECT_ID"

# Step 2: Grant the Cloud Run service account access to the secret
# This step is idempotent and ensures the service account has the necessary permissions.
echo "Granting Secret Accessor role to service account '$CLOUD_RUN_SA' for secret '$MONGO_URI'..."
gcloud secrets add-iam-policy-binding "$MONGO_URI" \
  --member="serviceAccount:${CLOUD_RUN_SA}" \
  --role="roles/secretmanager.secretAccessor" \
  --project="$PROJECT_ID" # Allow verbose output to catch potential permission errors

# Step 3: Set names for Cloud Run service, Artifact Registry, and the container image.
REPO_NAME="data-ingestion-repo" # Name for the Artifact Registry repository

# Create a timestamp for a unique image tag
TIMESTAMP=$(date +%Y%m%d%H%M%S)
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${CLOUD_RUN_SERVICE_NAME}:${TIMESTAMP}"

# Step 4: Create Artifact Registry repository if it doesn't exist
echo "Checking for Artifact Registry repository '$REPO_NAME'..."
if ! gcloud artifacts repositories describe "$REPO_NAME" --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "Repository not found. Creating it..."
    gcloud artifacts repositories create "$REPO_NAME" \
        --repository-format=docker \
        --location="$REGION" \
        --description="Repository for data ingestion services" \
        --project="$PROJECT_ID"
else
    echo "Artifact Registry repository '$REPO_NAME' already exists."
fi

# Step 5: Build the container image and push it to Artifact Registry.
# The build context is now correctly set to the parent directory containing the app code.
echo "Building and pushing container image to Artifact Registry..."
gcloud builds submit "${SCRIPT_DIR}/../cloud-run-ingestor" --tag "$IMAGE_NAME" --project="$PROJECT_ID"

# **Step 6: Conditionally set the min-instances flag**
# We explicitly set the min-instances flag to ensure the configuration is
# applied correctly on every redeployment, overriding any previous settings.
MIN_INSTANCES_FLAG=""
if [ "$KEEP_ALIVE_CLOUD_RUN_INGESTOR" == "true" ]; then
    MIN_INSTANCES_FLAG="--min-instances=1"
    echo "✅ Configuration set to keep at least one instance running (warm)."
else
    MIN_INSTANCES_FLAG="--min-instances=0"
    echo "😴 Configuration set to allow scaling to zero when idle."
fi

# Step 7: Deploy the container image to Cloud Run.
echo "🚀 Deploying the container image to Cloud Run..."
gcloud run deploy "$CLOUD_RUN_SERVICE_NAME" \
  --image "$IMAGE_NAME" \
  --region "$REGION" \
  --project "$PROJECT_ID" \
  --service-account="$CLOUD_RUN_SA" \
  --no-allow-unauthenticated \
  --execution-environment=gen2 \
  --max-instances=1 \
  $MIN_INSTANCES_FLAG \
  --set-env-vars "MONGO_DB_NAME=${MONGO_DB_NAME},PUBSUB_TOPIC_NAME=${PUBSUB_TOPIC_NAME},PROJECT_ID=${PROJECT_ID},PUBLISHER_DLQ_TOPIC_NAME=${PUBLISHER_DLQ_TOPIC_NAME}" \
  --set-secrets="MONGO_URI=${MONGO_URI}:latest" \
  --port=8080 \
  --vpc-connector="$VPC_CONNECTOR_NAME" \
  --vpc-egress="$VPC_EGRESS_SETTING" \
  --cpu=1 \
  --memory=512Mi \
  # --startup-probe='path=/health,period=15,timeout=10,failure-threshold=3' # Temporarily disabled for deployment testing.

echo "Cloud Run service '$CLOUD_RUN_SERVICE_NAME' deployment initiated."
echo "You can check the deployment status in the Google Cloud Console."
echo "---------------------------------"
