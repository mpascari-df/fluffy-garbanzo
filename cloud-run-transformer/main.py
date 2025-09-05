import os
import json
import base64
import io
import uuid
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from bson import json_util
from flask import Flask, request

from config.schema_mappings import get_collection_schema, get_available_collections, has_collection_support
from config.transformer import apply_transformations, validate_transformation_result

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID")
GCS_PROCESSED_BUCKET_NAME = os.getenv("GCS_PROCESSED_BUCKET_NAME")

if not PROJECT_ID:
    print("WARNING: PROJECT_ID environment variable not set")
if not GCS_PROCESSED_BUCKET_NAME:
    print("WARNING: GCS_PROCESSED_BUCKET_NAME environment variable not set")

# Initialize clients
storage_client = storage.Client()
app = Flask(__name__)

class ParquetTransformer:
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self.schema = get_collection_schema(collection_name)
        
        if not self.schema:
            raise ValueError(f"No schema found for collection: {collection_name}")
    
    def determine_collection(self, data):
        """Extract collection name from document or structure"""
        if isinstance(data, list) and len(data) > 0:
            sample = data[0]
        else:
            sample = data
        
        # From payload structure (MongoDB change stream format)
        if isinstance(sample, dict):
            if 'collection' in sample:
                collection = sample['collection']
                print(f"[COLLECTION_DETERMINATION] Found collection field: {collection}")
                return collection
            
            # From document structure inference for available collections
            available_collections = get_available_collections()
            
            # Check document structure against known patterns
            if 'email' in sample and ('subscription' in sample or 'createdAt' in sample):
                if 'customers' in available_collections:
                    print("[COLLECTION_DETERMINATION] Inferred collection: customers (based on email + subscription/createdAt fields)")
                    return 'customers'
            
            # Try to match against any available collection
            # This is a basic heuristic - you might want to enhance this
            if available_collections:
                default_collection = available_collections[0]
                print(f"[COLLECTION_DETERMINATION] Could not determine collection from structure, defaulting to: {default_collection}")
                return default_collection
        
        print("[COLLECTION_DETERMINATION] Unable to determine collection, returning 'unknown'")
        return 'unknown'
    
    def transform_documents(self, documents):
        """
        Transforms raw documents into a schema-enforced PyArrow Table.
        """
        if not documents:
            print("[TRANSFORMATION] No documents received for transformation.")
            return None
        
        if not isinstance(documents, list):
            documents = [documents]
        
        print(f"[TRANSFORMATION] Starting transformation for {len(documents)} documents in collection: {self.collection_name}")
        
        try:
            # 1. Normalize raw JSON documents into a flat pandas DataFrame.
            source_df = pd.json_normalize(documents)
            print(f"[TRANSFORMATION] Normalized to DataFrame with {source_df.shape[0]} rows and {source_df.shape[1]} columns")
            print(f"[TRANSFORMATION] DataFrame columns: {list(source_df.columns)[:10]}..." if len(source_df.columns) > 10 else f"[TRANSFORMATION] DataFrame columns: {list(source_df.columns)}")
            
            # 2. Apply declarative field mappings and transformations.
            transformed_df = apply_transformations(source_df, self.collection_name)
            
            if transformed_df.empty:
                print(f"[TRANSFORMATION] ERROR: DataFrame is empty after transformations for collection: {self.collection_name}")
                return None
            
            print(f"[TRANSFORMATION] After transformations: {transformed_df.shape[0]} rows and {transformed_df.shape[1]} columns")
            
            # 3. Validate transformation result
            is_valid, validation_message = validate_transformation_result(transformed_df, self.schema)
            if not is_valid:
                print(f"[TRANSFORMATION] Validation failed: {validation_message}")
                return None
            
            print(f"[TRANSFORMATION] Validation successful: {validation_message}")
            
            # 4. Convert to PyArrow Table with strict schema enforcement.
            try:
                # Only include columns that exist in the schema
                schema_fields = set(field.name for field in self.schema)
                df_columns = set(transformed_df.columns)
                columns_to_keep = list(schema_fields.intersection(df_columns))
                
                if columns_to_keep:
                    final_df = transformed_df[columns_to_keep]
                    print(f"[TABLE_CREATION] Creating PyArrow Table with {len(columns_to_keep)} columns")
                    table = pa.Table.from_pandas(final_df, schema=self.schema, safe=True)
                    print(f"[TABLE_CREATION] SUCCESS: Created PyArrow Table for {self.collection_name} with {table.num_rows} rows and {table.num_columns} columns")
                    return table
                else:
                    print(f"[TABLE_CREATION] ERROR: No matching columns found between DataFrame and schema for collection: {self.collection_name}")
                    return None
                    
            except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
                print(f"[TABLE_CREATION] ERROR: Failed to convert DataFrame to PyArrow Table for collection '{self.collection_name}'")
                print(f"[TABLE_CREATION] Schema enforcement failed: {e}")
                print(f"[TABLE_CREATION] DataFrame dtypes: {transformed_df.dtypes}")
                return None
                
        except Exception as e:
            print(f"[TRANSFORMATION] CRITICAL ERROR: Transformation failed for collection '{self.collection_name}': {e}")
            import traceback
            print(f"[TRANSFORMATION] Traceback: {traceback.format_exc()}")
            return None
    
    def generate_output_path(self, operation="unknown"):
        """Generate GCS output path with a random prefix to prevent hotspotting."""
        now = datetime.now(timezone.utc)
        date_path = now.strftime("%Y-%m-%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        random_prefix = uuid.uuid4().hex[:8]
        
        return f"processed/{self.collection_name}/{random_prefix}-{date_path}/{operation}_{timestamp}_{unique_id}.parquet"

def process_pubsub_message_to_parquet(message_data):
    """Process Pub/Sub message and convert to parquet"""
    try:
        # Extract documents from your change stream format
        documents = []
        collection_name = "unknown"
        operation = "unknown"
        
        print(f"[PROCESS_MESSAGE] Processing message data of type: {type(message_data)}")
        
        # Handle your MongoDB change stream message format
        if isinstance(message_data, dict):
            if 'document' in message_data:
                documents = [message_data['document']]
                collection_name = message_data.get('collection', 'unknown')
                operation = message_data.get('operation', 'unknown')
                print(f"[PROCESS_MESSAGE] Extracted from change stream format - collection: {collection_name}, operation: {operation}")
            elif 'collection' in message_data:
                # If the whole message_data is the document with collection info
                collection_name = message_data.get('collection', 'unknown')
                documents = [message_data]
                print(f"[PROCESS_MESSAGE] Document contains collection info: {collection_name}")
            else:
                # Assume the whole message_data is a single document
                documents = [message_data]
                print("[PROCESS_MESSAGE] Treating message_data as single document")
        elif isinstance(message_data, list):
            documents = message_data
            print(f"[PROCESS_MESSAGE] Processing list of {len(documents)} documents")
        else:
            documents = [message_data]
            print("[PROCESS_MESSAGE] Converting message_data to list")
        
        if not documents:
            print("[PROCESS_MESSAGE] No documents to process")
            return None
        
        print(f"[PROCESS_MESSAGE] Extracted {len(documents)} documents, collection: {collection_name}, operation: {operation}")
        
        # Determine collection if unknown
        if collection_name == "unknown":
            transformer_temp = ParquetTransformer.__new__(ParquetTransformer)
            collection_name = transformer_temp.determine_collection(documents)
            print(f"[PROCESS_MESSAGE] Determined collection: {collection_name}")
        
        # Skip if no mapping available
        if not has_collection_support(collection_name):
            available = get_available_collections()
            print(f"[PROCESS_MESSAGE] Skipping unsupported collection: {collection_name}. Available collections: {available}")
            return f"skipped_{collection_name}"
        
        # Initialize transformer
        try:
            print(f"[PROCESS_MESSAGE] Initializing transformer for collection: {collection_name}")
            transformer = ParquetTransformer(collection_name)
        except ValueError as e:
            print(f"[PROCESS_MESSAGE] Failed to initialize transformer: {e}")
            return None
        
        # Transform to parquet
        print(f"[PROCESS_MESSAGE] Starting document transformation")
        table = transformer.transform_documents(documents)
        if table is None:
            print("[PROCESS_MESSAGE] No data to transform or transformation failed")
            return None
        
        print(f"[PROCESS_MESSAGE] Transformation successful - {table.num_rows} rows, {table.num_columns} columns")
        
        # Generate output path
        output_path = transformer.generate_output_path(operation)
        print(f"[GCS_UPLOAD] Generated output path: {output_path}")
        
        # Upload to GCS
        if not GCS_PROCESSED_BUCKET_NAME:
            print("[GCS_UPLOAD] ERROR: GCS_PROCESSED_BUCKET_NAME not configured")
            return None
        
        try:
            bucket = storage_client.bucket(GCS_PROCESSED_BUCKET_NAME)
            blob = bucket.blob(output_path)
            
            # Convert table to bytes
            print(f"[BUFFER_WRITE] Converting table to Parquet bytes")
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            buffer_size = buffer.getbuffer().nbytes
            print(f"[BUFFER_WRITE] SUCCESS: Created Parquet buffer of {buffer_size} bytes")
            
            print(f"[GCS_UPLOAD] Uploading {buffer_size} bytes to gs://{GCS_PROCESSED_BUCKET_NAME}/{output_path}")
            blob.upload_from_file(buffer, content_type='application/octet-stream')
            print(f"[GCS_UPLOAD] SUCCESS: Uploaded to gs://{GCS_PROCESSED_BUCKET_NAME}/{output_path}")
            
        except Exception as e:
            print(f"[GCS_UPLOAD] ERROR: Failed to upload to GCS: {e}")
            import traceback
            print(f"[GCS_UPLOAD] Traceback: {traceback.format_exc()}")
            return None
        
        print(f"[PROCESS_MESSAGE] Successfully processed {len(documents)} {collection_name} documents to {output_path}")
        return output_path
        
    except Exception as e:
        print(f"[PROCESS_MESSAGE] CRITICAL ERROR: Failed to process message to parquet: {e}")
        import traceback
        print(f"[PROCESS_MESSAGE] Traceback: {traceback.format_exc()}")
        raise

@app.route("/", methods=["POST"])
def handle_pubsub():
    """Handle Pub/Sub push messages"""
    try:
        envelope = request.get_json()
        if not envelope or 'message' not in envelope:
            print("[PUBSUB_HANDLER] Bad Request: invalid Pub/Sub message format")
            return "Bad Request: invalid Pub/Sub message format", 400

        message = envelope['message']
        if 'data' not in message:
            print("[PUBSUB_HANDLER] Bad Request: no data in Pub/Sub message")
            return "Bad Request: no data in Pub/Sub message", 400

        print("[PUBSUB_HANDLER] Processing Pub/Sub message...")

        # Decode the outer message data. This is the payload from the Pub/Sub topic.
        message_data_str = base64.b64decode(message['data']).decode('utf-8')
        print(f"[PUBSUB_HANDLER] Decoded outer message length: {len(message_data_str)}")

        final_payload_str = message_data_str
        # The ingestor service might wrap its payload inside another message (double-encoding).
        try:
            outer_payload = json.loads(message_data_str)
            if isinstance(outer_payload, dict) and 'message' in outer_payload and 'data' in outer_payload['message']:
                # This is a nested payload. The real data is one level deeper.
                print("[PUBSUB_HANDLER] Detected nested payload, extracting inner data...")
                final_payload_str = base64.b64decode(outer_payload['message']['data']).decode('utf-8')
        except json.JSONDecodeError:
            # If it's not a JSON string, it's likely the direct, non-nested payload.
            print("[PUBSUB_HANDLER] Using direct payload (not nested)")
            pass

        # Parse the final data string, which might contain BSON types like ObjectId.
        final_data = json_util.loads(final_payload_str)
        print(f"[PUBSUB_HANDLER] Parsed final data type: {type(final_data)}")
        if isinstance(final_data, dict) and 'collection' in final_data:
            print(f"[PUBSUB_HANDLER] Processing collection: {final_data.get('collection')}, operation: {final_data.get('operation', 'unknown')}")

        # Process to parquet
        output_path = process_pubsub_message_to_parquet(final_data)

        if output_path and not output_path.startswith('skipped_'):
            print(f"[PUBSUB_HANDLER] SUCCESS: Processed message to {output_path}")
            return f"Processed: {output_path}", 200
        elif output_path and output_path.startswith('skipped_'):
            print(f"[PUBSUB_HANDLER] SKIPPED: {output_path}")
            return f"Skipped: {output_path}", 200
        else:
            print("[PUBSUB_HANDLER] WARNING: Message processed, but no output generated")
            return "Message processed, but no output generated.", 200

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        error_msg = f"Error decoding or parsing Pub/Sub message: {e}"
        print(f"[PUBSUB_HANDLER] ERROR: {error_msg}")
        return f"Bad Request: could not decode message data. Error: {e}", 400
    except Exception as e:
        error_msg = f"Error handling Pub/Sub message: {e}"
        print(f"[PUBSUB_HANDLER] CRITICAL ERROR: {error_msg}")
        import traceback
        print(f"[PUBSUB_HANDLER] Traceback: {traceback.format_exc()}")
        return f"Internal Server Error: {str(e)}", 500

@app.route("/health")
def health_check():
    """Health check endpoint"""
    available_collections = get_available_collections()
    return {
        "status": "healthy",
        "service": "cloud-run-transformer",
        "available_collections": available_collections,
        "project_id": PROJECT_ID,
        "gcs_bucket": GCS_PROCESSED_BUCKET_NAME
    }, 200

@app.route("/debug")
def debug_info():
    """Debug endpoint to check configuration"""
    available_collections = get_available_collections()
    schemas_info = {}
    
    for collection in available_collections:
        schema = get_collection_schema(collection)
        schemas_info[collection] = {
            "fields_count": len(schema) if schema else 0,
            "field_names": [field.name for field in schema] if schema else []
        }
    
    return {
        "service": "cloud-run-transformer",
        "project_id": PROJECT_ID,
        "gcs_processed_bucket": GCS_PROCESSED_BUCKET_NAME,
        "available_collections": available_collections,
        "schemas": schemas_info
    }

if __name__ == "__main__":
    print("[STARTUP] Starting Cloud Run Transformer Service...")
    print(f"[STARTUP] Available collections: {get_available_collections()}")
    print(f"[STARTUP] PROJECT_ID: {PROJECT_ID}")
    print(f"[STARTUP] GCS_PROCESSED_BUCKET_NAME: {GCS_PROCESSED_BUCKET_NAME}")
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))