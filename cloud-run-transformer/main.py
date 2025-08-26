import os
import json
import base64
import uuid
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from bson import json_util
from flask import Flask, request
from config.schema_mappings import get_collection_schema, get_available_collections
from config.field_mappings import apply_transformations

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID")
GCS_PROCESSED_BUCKET_NAME = os.getenv("GCS_PROCESSED_BUCKET_NAME")

# Initialize clients
storage_client = storage.Client()
app = Flask(__name__)

class ParquetTransformer:
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self.schema = get_collection_schema(collection_name)
    
    def determine_collection(self, data):
        """Extract collection name from document or structure"""
        if isinstance(data, list) and len(data) > 0:
            sample = data[0]
        else:
            sample = data
        
        # From payload structure (your MongoDB change stream format)
        if 'collection' in sample:
            return sample['collection']
        
        # From document structure inference for available collections
        available_collections = get_available_collections()
        
        # Check document structure against known patterns
        if 'email' in sample and 'subscription' in sample:
            return 'customers'
        
        # Default fallback
        if available_collections:
            print(f"Unknown collection pattern, defaulting to: {available_collections[0]}")
            return available_collections[0]
        
        return 'unknown'
    
    def transform_documents(self, documents):
        """Transform documents using your mapping configuration"""
        if not documents:
            return None
        
        # Handle single document vs list
        if not isinstance(documents, list):
            documents = [documents]
        
        # Normalize to DataFrame
        df = pd.json_normalize(documents)
        
        # Apply your mapping transformations
        df = apply_transformations(df, self.collection_name)
        
        if df.empty:
            print(f"No data after transformation for collection: {self.collection_name}")
            return None
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Apply schema if available
        if self.schema:
            try:
                table = table.cast(self.schema)
                print(f"Successfully applied schema for {self.collection_name}")
            except pa.ArrowInvalid as e:
                print(f"Schema cast failed for {self.collection_name}: {e}")
                print(f"Available columns: {table.column_names}")
                print(f"Expected schema: {self.schema}")
        
        return table
    
    def generate_output_path(self, operation="unknown"):
        """Generate partitioned output path"""
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        
        return f"processed/{self.collection_name}/date={date_str}/{operation}_{timestamp}_{unique_id}.parquet"

def process_pubsub_message_to_parquet(message_data):
    """Process Pub/Sub message and convert to parquet"""
    try:
        # Extract documents from your change stream format
        documents = []
        collection_name = "unknown"
        operation = "unknown"
        
        # Handle your MongoDB change stream message format
        if 'document' in message_data:
            documents = [message_data['document']]
            collection_name = message_data.get('collection', 'unknown')
            operation = message_data.get('operation', 'unknown')
        elif isinstance(message_data, list):
            documents = message_data
        else:
            documents = [message_data]
        
        if not documents:
            print("No documents to process")
            return
        
        # Initialize transformer
        transformer = ParquetTransformer(collection_name)
        if collection_name == "unknown":
            collection_name = transformer.determine_collection(documents)
            transformer = ParquetTransformer(collection_name)
        
        # Skip if no mapping available
        if collection_name == "unknown" or collection_name not in get_available_collections():
            print(f"Skipping unknown collection: {collection_name}")
            return f"skipped_{collection_name}"
        
        # Transform to parquet
        table = transformer.transform_documents(documents)
        if table is None:
            print("No data to transform")
            return
        
        # Generate output path
        output_path = transformer.generate_output_path(operation)
        
        # Upload to GCS
        bucket = storage_client.bucket(GCS_PROCESSED_BUCKET_NAME)
        blob = bucket.blob(output_path)
        
        # Convert table to bytes
        import io
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        print(f"Successfully processed {len(documents)} {collection_name} documents to {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Error processing message to parquet: {e}")
        raise

@app.route("/", methods=["POST"])
def handle_pubsub():
    """Handle Pub/Sub push messages"""
    try:
        envelope = request.get_json()
        if not envelope or 'message' not in envelope:
            return "Bad Request: no message", 400
        
        # Decode Pub/Sub message
        message = envelope['message']
        if 'data' not in message:
            return "Bad Request: no data", 400
        
        # Decode base64 data
        message_data = base64.b64decode(message['data']).decode('utf-8')
        
        # Handle nested CloudEvent format from your ingestor
        try:
            parsed_data = json.loads(message_data)
            if 'message' in parsed_data and 'data' in parsed_data['message']:
                # Double-encoded from your Cloud Run ingestor
                inner_data = base64.b64decode(parsed_data['message']['data']).decode('utf-8')
                final_data = json_util.loads(inner_data)
            else:
                final_data = json_util.loads(message_data)
        except:
            final_data = json_util.loads(message_data)
        
        # Process to parquet
        output_path = process_pubsub_message_to_parquet(final_data)
        
        return f"Processed: {output_path}", 200
        
    except Exception as e:
        print(f"Error handling Pub/Sub message: {e}")
        return f"Error: {str(e)}", 500

@app.route("/health")
def health_check():
    return "Parquet Transformer Service Healthy", 200

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))