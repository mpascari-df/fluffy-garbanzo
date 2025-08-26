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
                return sample['collection']
            
            # From document structure inference for available collections
            available_collections = get_available_collections()
            
            # Check document structure against known patterns
            if 'email' in sample and ('subscription' in sample or 'createdAt' in sample):
                if 'customers' in available_collections:
                    return 'customers'
            
            # Try to match against any available collection
            # This is a basic heuristic - you might want to enhance this
            if available_collections:
                print(f"Could not determine collection from structure, defaulting to: {available_collections[0]}")
                return available_collections[0]
        
        return 'unknown'
    
    def transform_documents(self, documents):
        """
        Transforms raw documents into a schema-enforced PyArrow Table.
        """
        if not documents:
            print("No documents received for transformation.")
            return None
        
        if not isinstance(documents, list):
            documents = [documents]
        
        print(f"Processing {len(documents)} documents for collection: {self.collection_name}")
        
        try:
            # 1. Normalize raw JSON documents into a flat pandas DataFrame.
            source_df = pd.json_normalize(documents)
            print(f"Normalized to DataFrame with columns: {list(source_df.columns)}")
            
            # 2. Apply declarative field mappings and transformations.
            transformed_df = apply_transformations(source_df, self.collection_name)
            
            if transformed_df.empty:
                print(f"DataFrame is empty after transformations for collection: {self.collection_name}")
                return None
            
            # 3. Validate transformation result
            is_valid, validation_message = validate_transformation_result(transformed_df, self.schema)
            if not is_valid:
                print(f"Transformation validation failed: {validation_message}")
                return None
            
            print(f"Validation successful: {validation_message}")
            
            # 4. Convert to PyArrow Table with strict schema enforcement.
            try:
                # Only include columns that exist in the schema
                schema_fields = set(field.name for field in self.schema)
                df_columns = set(transformed_df.columns)
                columns_to_keep = list(schema_fields.intersection(df_columns))
                
                if columns_to_keep:
                    final_df = transformed_df[columns_to_keep]
                    table = pa.Table.from_pandas(final_df, schema=self.schema, safe=True)
                    print(f"Successfully created PyArrow Table for {self.collection_name} with {table.num_rows} rows and {table.num_columns} columns.")
                    return table
                else:
                    print(f"No matching columns found between DataFrame and schema for collection: {self.collection_name}")
                    return None
                    
            except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
                print(f"ERROR: Failed to convert DataFrame to PyArrow Table for collection '{self.collection_name}'.")
                print(f"Schema enforcement failed: {e}")
                print(f"DataFrame dtypes: {transformed_df.dtypes}")
                return None
                
        except Exception as e:
            print(f"ERROR: Transformation failed for collection '{self.collection_name}': {e}")
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
        
        print(f"Processing message data: {type(message_data)}")
        
        # Handle your MongoDB change stream message format
        if isinstance(message_data, dict):
            if 'document' in message_data:
                documents = [message_data['document']]
                collection_name = message_data.get('collection', 'unknown')
                operation = message_data.get('operation', 'unknown')
            elif 'collection' in message_data:
                # If the whole message_data is the document with collection info
                collection_name = message_data.get('collection', 'unknown')
                documents = [message_data]
            else:
                # Assume the whole message_data is a single document
                documents = [message_data]
        elif isinstance(message_data, list):
            documents = message_data
        else:
            documents = [message_data]
        
        if not documents:
            print("No documents to process")
            return None
        
        print(f"Extracted {len(documents)} documents, collection: {collection_name}, operation: {operation}")
        
        # Determine collection if unknown
        if collection_name == "unknown":
            transformer_temp = ParquetTransformer.__new__(ParquetTransformer)
            collection_name = transformer_temp.determine_collection(documents)
            print(f"Determined collection: {collection_name}")
        
        # Skip if no mapping available
        if not has_collection_support(collection_name):
            available = get_available_collections()
            print(f"Skipping unsupported collection: {collection_name}. Available: {available}")
            return f"skipped_{collection_name}"
        
        # Initialize transformer
        try:
            transformer = ParquetTransformer(collection_name)
        except ValueError as e:
            print(f"Failed to initialize transformer: {e}")
            return None
        
        # Transform to parquet
        table = transformer.transform_documents(documents)
        if table is None:
            print("No data to transform or transformation failed")
            return None
        
        # Generate output path
        output_path = transformer.generate_output_path(operation)
        
        # Upload to GCS
        if not GCS_PROCESSED_BUCKET_NAME:
            print("ERROR: GCS_PROCESSED_BUCKET_NAME not configured")
            return None
            
        bucket = storage_client.bucket(GCS_PROCESSED_BUCKET_NAME)
        blob = bucket.blob(output_path)
        
        # Convert table to bytes
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        print(f"Successfully processed {len(documents)} {collection_name} documents to {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Error processing message to parquet: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise

@app.route("/", methods=["POST"])
def handle_pubsub():
    """Handle Pub/Sub push messages"""
    try:
        envelope = request.get_json()
        if not envelope or 'message' not in envelope:
            print("Bad Request: invalid Pub/Sub message format")
            return "Bad Request: invalid Pub/Sub message format", 400

        message = envelope['message']
        if 'data' not in message:
            print("Bad Request: no data in Pub/Sub message")
            return "Bad Request: no data in Pub/Sub message", 400

        print("Processing Pub/Sub message...")

        # Decode the outer message data. This is the payload from the Pub/Sub topic.
        message_data_str = base64.b64decode(message['data']).decode('utf-8')
        print(f"Decoded outer message length: {len(message_data_str)}")

        final_payload_str = message_data_str
        # The ingestor service might wrap its payload inside another message (double-encoding).
        try:
            outer_payload = json.loads(message_data_str)
            if isinstance(outer_payload, dict) and 'message' in outer_payload and 'data' in outer_payload['message']:
                # This is a nested payload. The real data is one level deeper.
                print("Detected nested payload, extracting inner data...")
                final_payload_str = base64.b64decode(outer_payload['message']['data']).decode('utf-8')
        except json.JSONDecodeError:
            # If it's not a JSON string, it's likely the direct, non-nested payload.
            print("Using direct payload (not nested)")
            pass

        # Parse the final data string, which might contain BSON types like ObjectId.
        final_data = json_util.loads(final_payload_str)
        print(f"Parsed final data type: {type(final_data)}")

        # Process to parquet
        output_path = process_pubsub_message_to_parquet(final_data)

        if output_path and not output_path.startswith('skipped_'):
            return f"Processed: {output_path}", 200
        elif output_path and output_path.startswith('skipped_'):
            return f"Skipped: {output_path}", 200
        else:
            return "Message processed, but no output generated.", 200

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        error_msg = f"Error decoding or parsing Pub/Sub message: {e}"
        print(error_msg)
        return f"Bad Request: could not decode message data. Error: {e}", 400
    except Exception as e:
        error_msg = f"Error handling Pub/Sub message: {e}"
        print(error_msg)
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
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
    print("Starting Cloud Run Transformer Service...")
    print(f"Available collections: {get_available_collections()}")
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))