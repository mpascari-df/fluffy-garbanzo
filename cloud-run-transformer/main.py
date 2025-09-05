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
from google.cloud import logging
from bson import json_util
from flask import Flask, request

from config.schema_mappings import get_collection_schema, get_available_collections, has_collection_support
from config.transformer import apply_transformations

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

# Initialize schema monitoring
schema_monitor = None
if PROJECT_ID:
    try:
        schema_monitor = logging.Client(project=PROJECT_ID).logger('schema-drift')
        print("[MONITORING] Cloud Logging initialized for schema drift tracking")
    except Exception as e:
        print(f"[MONITORING] Warning: Could not initialize Cloud Logging: {e}")
        schema_monitor = None


class SchemaMonitor:
    """Monitor and log schema drift events to Cloud Logging"""
    
    def __init__(self, project_id):
        try:
            self.logging_client = logging.Client(project=project_id)
            self.logger = self.logging_client.logger('schema-drift')
            self.enabled = True
            print("[MONITOR] Schema monitoring enabled")
        except Exception as e:
            print(f"[MONITOR] Could not initialize monitoring: {e}")
            self.enabled = False
            self.logger = None
    
    def log_drift(self, collection, warnings, document_id=None):
        """Log schema drift to Cloud Logging with structured data"""
        if not self.enabled or not self.logger:
            return
            
        try:
            for warning in warnings:
                severity = 'WARNING'
                if 'CRITICAL' in warning:
                    severity = 'ERROR'
                elif 'DATA_QUALITY' in warning:
                    severity = 'INFO'
                elif 'TYPE_MISMATCH' in warning:
                    severity = 'WARNING'
                
                log_entry = {
                    'message': warning,
                    'collection': collection,
                    'severity': severity,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                if document_id:
                    log_entry['document_id'] = document_id
                
                self.logger.log_struct(log_entry, severity=severity)
        except Exception as e:
            print(f"[MONITOR] Failed to log to Cloud Logging: {e}")


def validate_transformation_result(df, schema):
    """
    Validate and auto-fix DataFrame to match target schema.
    Logs issues but continues processing.
    
    Returns:
        tuple: (is_valid, fixed_df, warning_messages)
    """
    if df.empty:
        return False, df, ["DataFrame is empty"]
    
    warnings = []
    
    try:
        import pyarrow as pa
        
        schema_fields = {field.name: field for field in schema}
        df_columns = set(df.columns)
        
        # Handle missing required fields - add with None values
        missing_fields = set(schema_fields.keys()) - df_columns
        if missing_fields:
            for field_name in missing_fields:
                df[field_name] = None
                field_type = schema_fields[field_name].type
                warnings.append(f"SCHEMA_DRIFT: Added missing field '{field_name}' ({field_type}) with None")
        
        # Log extra fields that will be dropped
        extra_fields = df_columns - set(schema_fields.keys())
        if extra_fields:
            warnings.append(f"SCHEMA_DRIFT: Dropping unexpected fields: {extra_fields}")
        
        # Reorder columns to match schema exactly
        df = df[[field.name for field in schema]]
        
        # Try to create PyArrow table with safe casting
        try:
            table = pa.Table.from_pandas(df, schema=schema, safe=True)
            
            # Log any type casting that occurred
            for col_idx, field in enumerate(schema):
                col = table.column(col_idx)
                if col.null_count > 0:
                    null_pct = (col.null_count / len(col)) * 100
                    if null_pct > 50:
                        warnings.append(f"DATA_QUALITY: Field '{field.name}' has {null_pct:.1f}% null values")
                        
        except pa.lib.ArrowTypeError as e:
            warnings.append(f"TYPE_MISMATCH: {str(e)}")
            # Try with safe=False to allow lossy casts
            table = pa.Table.from_pandas(df, schema=schema, safe=False)
            warnings.append("TYPE_MISMATCH: Applied lossy type casting to conform to schema")
        
        if warnings:
            print(f"[SCHEMA_VALIDATION] Warnings detected: {len(warnings)}")
            for warning in warnings:
                print(f"  - {warning}")
        
        return True, df, warnings
        
    except Exception as e:
        return False, df, [f"CRITICAL: Validation error: {e}"]


class ParquetTransformer:
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self.schema = get_collection_schema(collection_name)
        self.monitor = SchemaMonitor(PROJECT_ID) if PROJECT_ID else None
        
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
        Resilient: logs issues but continues processing.
        """
        if not documents:
            print("[TRANSFORMATION] No documents received for transformation.")
            return None
        
        if not isinstance(documents, list):
            documents = [documents]
        
        print(f"[TRANSFORMATION] Starting transformation for {len(documents)} documents in collection: {self.collection_name}")
        
        # Extract document ID for monitoring
        doc_id = None
        if documents and isinstance(documents[0], dict):
            doc_id = documents[0].get('_id', 'unknown')
        
        # ==================== DEBUG LOGGING ====================
        # Log the structure of the first document to understand what we're dealing with
        if documents:
            first_doc = documents[0]
            print(f"[DEBUG] First document type: {type(first_doc)}")
            print(f"[DEBUG] First document keys: {list(first_doc.keys())[:20] if isinstance(first_doc, dict) else 'Not a dict'}")
            
            # Check for specific problematic fields
            if isinstance(first_doc, dict):
                # Check subscription fields (customers)
                if 'subscription' in first_doc and isinstance(first_doc['subscription'], dict):
                    sub = first_doc['subscription']
                    print(f"[DEBUG] subscription.stripeCustId type: {type(sub.get('stripeCustId'))}")
                    print(f"[DEBUG] subscription.statusUpdatedAt type: {type(sub.get('statusUpdatedAt'))}")
                    print(f"[DEBUG] subscription.isMixedPlan type: {type(sub.get('isMixedPlan'))}")
                    if 'reasonForPause' in sub:
                        print(f"[DEBUG] subscription.reasonForPause type: {type(sub.get('reasonForPause'))}")
                
                # Check address fields
                if 'address' in first_doc and isinstance(first_doc['address'], dict):
                    addr = first_doc['address']
                    print(f"[DEBUG] address.line2 type: {type(addr.get('line2'))}")
                    if isinstance(addr.get('line2'), list):
                        print(f"[DEBUG] address.line2 is ARRAY with {len(addr.get('line2'))} elements")
                
                # Check acquisition field
                if 'acquisition' in first_doc:
                    print(f"[DEBUG] acquisition type: {type(first_doc.get('acquisition'))}")
                    if isinstance(first_doc.get('acquisition'), list):
                        print(f"[DEBUG] acquisition is ARRAY with {len(first_doc.get('acquisition'))} elements")
                
                # Log a sample of the document structure (first 500 chars)
                doc_str = json.dumps(first_doc, default=str)[:500]
                print(f"[DEBUG] Document sample: {doc_str}...")
        # ==================== END DEBUG LOGGING ====================
        
        try:
            # 1. Normalize raw JSON documents into a flat pandas DataFrame
            source_df = pd.json_normalize(documents)
            print(f"[TRANSFORMATION] Normalized to DataFrame with {source_df.shape[0]} rows and {source_df.shape[1]} columns")
            print(f"[TRANSFORMATION] DataFrame columns: {list(source_df.columns)[:10]}..." if len(source_df.columns) > 10 else f"[TRANSFORMATION] DataFrame columns: {list(source_df.columns)}")
            
            # 2. Apply declarative field mappings and transformations
            transformed_df = apply_transformations(source_df, self.collection_name)
            
            if transformed_df.empty:
                print(f"[TRANSFORMATION] ERROR: DataFrame is empty after transformations for collection: {self.collection_name}")
                return None
            
            print(f"[TRANSFORMATION] After transformations: {transformed_df.shape[0]} rows and {transformed_df.shape[1]} columns")
            
            # 3. Validate and fix transformation result
            is_valid, fixed_df, warnings = validate_transformation_result(transformed_df, self.schema)
            
            if not is_valid:
                print(f"[TRANSFORMATION] CRITICAL: Cannot proceed even with fixes: {warnings}")
                # Log to monitoring
                if self.monitor:
                    self.monitor.log_drift(self.collection_name, warnings, doc_id)
                return None
            
            if warnings:
                # Log warnings to monitoring system
                print(f"[TRANSFORMATION] Processing with {len(warnings)} warnings")
                if self.monitor:
                    self.monitor.log_drift(self.collection_name, warnings, doc_id)
            
            # 4. Convert to PyArrow Table
            try:
                # Use fixed DataFrame that has been aligned with schema
                table = pa.Table.from_pandas(fixed_df, schema=self.schema, safe=False)
                
                print(f"[TABLE_CREATION] SUCCESS: Created PyArrow Table for {self.collection_name}")
                print(f"  - Rows: {table.num_rows}, Columns: {table.num_columns}")
                if warnings:
                    print(f"  - Schema drift detected: {len(warnings)} issues logged")
                
                # Add metadata about schema drift to the table
                if warnings:
                    metadata = {
                        b'schema_drift_count': str(len(warnings)).encode(),
                        b'schema_drift_summary': '; '.join(warnings[:3]).encode()  # First 3 warnings
                    }
                    existing_metadata = table.schema.metadata or {}
                    combined_metadata = {**existing_metadata, **metadata}
                    table = table.replace_schema_metadata(combined_metadata)
                
                return table
                
            except Exception as e:
                print(f"[TABLE_CREATION] FALLBACK: Error creating table, attempting recovery: {e}")
                fallback_warning = f"CRITICAL: Table creation failed, attempting recovery: {str(e)}"
                if self.monitor:
                    self.monitor.log_drift(self.collection_name, [fallback_warning], doc_id)
                
                # Last resort: try to save what we can
                try:
                    # Drop problematic columns one by one until it works
                    working_df = fixed_df.copy()
                    dropped_columns = []
                    
                    for col in fixed_df.columns:
                        try:
                            test_df = working_df[[col]]
                            pa.Table.from_pandas(test_df, schema=pa.schema([field for field in self.schema if field.name == col]))
                        except:
                            print(f"[TABLE_CREATION] Dropping problematic column: {col}")
                            dropped_columns.append(col)
                            working_df = working_df.drop(columns=[col])
                    
                    if not working_df.empty:
                        partial_schema = pa.schema([field for field in self.schema if field.name in working_df.columns])
                        table = pa.Table.from_pandas(working_df, schema=partial_schema)
                        
                        success_msg = f"PARTIAL SUCCESS: Saved {len(working_df.columns)}/{len(fixed_df.columns)} columns"
                        print(f"[TABLE_CREATION] {success_msg}")
                        
                        if dropped_columns and self.monitor:
                            drop_warning = f"RECOVERY: Dropped columns due to errors: {dropped_columns}"
                            self.monitor.log_drift(self.collection_name, [drop_warning], doc_id)
                        
                        # Add recovery metadata
                        metadata = {
                            b'recovery_mode': b'true',
                            b'dropped_columns': ','.join(dropped_columns).encode(),
                            b'original_column_count': str(len(fixed_df.columns)).encode()
                        }
                        table = table.replace_schema_metadata(metadata)
                        
                        return table
                except Exception as recovery_error:
                    print(f"[TABLE_CREATION] RECOVERY FAILED: {recovery_error}")
                    if self.monitor:
                        self.monitor.log_drift(self.collection_name, 
                                              [f"CRITICAL: Recovery failed: {str(recovery_error)}"], 
                                              doc_id)
                
                return None
                
        except Exception as e:
            print(f"[TRANSFORMATION] CRITICAL ERROR: Transformation failed for collection '{self.collection_name}': {e}")
            import traceback
            print(f"[TRANSFORMATION] Traceback: {traceback.format_exc()}")
            
            if self.monitor:
                self.monitor.log_drift(self.collection_name, 
                                      [f"CRITICAL: Complete transformation failure: {str(e)}"], 
                                      doc_id)
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
        # ==================== MESSAGE STRUCTURE LOGGING ====================
        print(f"[MESSAGE_STRUCTURE] ============ BEGIN MESSAGE ANALYSIS ============")
        print(f"[MESSAGE_STRUCTURE] Raw message_data type: {type(message_data)}")
        
        if isinstance(message_data, dict):
            print(f"[MESSAGE_STRUCTURE] Message keys: {list(message_data.keys())}")
            
            # Log structure of key fields
            if 'operation' in message_data:
                print(f"[MESSAGE_STRUCTURE] operation: {message_data['operation']}")
            if 'collection' in message_data:
                print(f"[MESSAGE_STRUCTURE] collection: {message_data['collection']}")
            if 'database' in message_data:
                print(f"[MESSAGE_STRUCTURE] database: {message_data['database']}")
            if 'timestamp' in message_data:
                print(f"[MESSAGE_STRUCTURE] timestamp: {message_data['timestamp']}")
            if 'correlation_id' in message_data:
                print(f"[MESSAGE_STRUCTURE] correlation_id: {message_data['correlation_id']}")
            
            # Log document structure
            if 'document' in message_data:
                doc = message_data['document']
                print(f"[MESSAGE_STRUCTURE] document type: {type(doc)}")
                if isinstance(doc, dict):
                    print(f"[MESSAGE_STRUCTURE] document keys ({len(doc.keys())} total): {list(doc.keys())[:15]}...")
                    
                    # Sample specific fields that are problematic
                    if 'subscription' in doc:
                        print(f"[MESSAGE_STRUCTURE] document.subscription exists, type: {type(doc['subscription'])}")
                        if isinstance(doc['subscription'], dict) and 'stripeCustId' in doc['subscription']:
                            stripe_id = doc['subscription']['stripeCustId']
                            print(f"[MESSAGE_STRUCTURE] document.subscription.stripeCustId type: {type(stripe_id)}, value: {stripe_id if not isinstance(stripe_id, list) else f'ARRAY[{len(stripe_id)}]'}")
                    
                    if '_id' in doc:
                        print(f"[MESSAGE_STRUCTURE] document._id: {doc['_id']}")
                        
                elif isinstance(doc, list):
                    print(f"[MESSAGE_STRUCTURE] document is a LIST with {len(doc)} items")
                    if doc:
                        print(f"[MESSAGE_STRUCTURE] First document item type: {type(doc[0])}")
                        if isinstance(doc[0], dict):
                            print(f"[MESSAGE_STRUCTURE] First document item keys: {list(doc[0].keys())[:10]}...")
                            
        elif isinstance(message_data, list):
            print(f"[MESSAGE_STRUCTURE] Message is a LIST with {len(message_data)} items")
            if message_data:
                print(f"[MESSAGE_STRUCTURE] First item type: {type(message_data[0])}")
                if isinstance(message_data[0], dict):
                    print(f"[MESSAGE_STRUCTURE] First item keys: {list(message_data[0].keys())[:10]}...")
        
        print(f"[MESSAGE_STRUCTURE] ============ END MESSAGE ANALYSIS ============")
        # ==================== END MESSAGE STRUCTURE LOGGING ====================
        
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
        
        # ==================== PUBSUB ENVELOPE LOGGING ====================
        print(f"[PUBSUB_ENVELOPE] ============ BEGIN PUBSUB ANALYSIS ============")
        print(f"[PUBSUB_ENVELOPE] Envelope type: {type(envelope)}")
        if isinstance(envelope, dict):
            print(f"[PUBSUB_ENVELOPE] Envelope keys: {list(envelope.keys())}")
            if 'message' in envelope:
                message = envelope['message']
                print(f"[PUBSUB_ENVELOPE] message keys: {list(message.keys()) if isinstance(message, dict) else 'Not a dict'}")
                if isinstance(message, dict) and 'attributes' in message:
                    print(f"[PUBSUB_ENVELOPE] message.attributes: {message['attributes']}")
                if isinstance(message, dict) and 'data' in message:
                    # Check data length before decoding
                    data_str = message['data']
                    print(f"[PUBSUB_ENVELOPE] message.data length: {len(data_str)} chars")
                    print(f"[PUBSUB_ENVELOPE] message.data preview (first 100 chars): {data_str[:100]}...")
        print(f"[PUBSUB_ENVELOPE] ============ END PUBSUB ANALYSIS ============")
        # ==================== END PUBSUB ENVELOPE LOGGING ====================
        
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

        # ==================== DECODED MESSAGE LOGGING ====================
        print(f"[DECODED_MESSAGE] ============ BEGIN DECODED ANALYSIS ============")
        print(f"[DECODED_MESSAGE] Decoded string preview (first 200 chars): {message_data_str[:200]}...")
        
        # Try to parse as JSON to see structure
        try:
            decoded_json = json.loads(message_data_str)
            print(f"[DECODED_MESSAGE] Successfully parsed as JSON")
            print(f"[DECODED_MESSAGE] JSON type: {type(decoded_json)}")
            if isinstance(decoded_json, dict):
                print(f"[DECODED_MESSAGE] JSON keys: {list(decoded_json.keys())}")
                if 'message' in decoded_json:
                    print(f"[DECODED_MESSAGE] Has nested 'message' key - likely CloudEvents wrapper")
                    if isinstance(decoded_json['message'], dict) and 'data' in decoded_json['message']:
                        print(f"[DECODED_MESSAGE] Nested message.data exists - double encoding detected")
        except json.JSONDecodeError as e:
            print(f"[DECODED_MESSAGE] Not valid JSON: {e}")
        
        print(f"[DECODED_MESSAGE] ============ END DECODED ANALYSIS ============")
        # ==================== END DECODED MESSAGE LOGGING ====================

        final_payload_str = message_data_str
        # The ingestor service might wrap its payload inside another message (double-encoding).
        try:
            outer_payload = json.loads(message_data_str)
            if isinstance(outer_payload, dict) and 'message' in outer_payload and 'data' in outer_payload['message']:
                # This is a nested payload. The real data is one level deeper.
                print("[PUBSUB_HANDLER] Detected nested payload, extracting inner data...")
                final_payload_str = base64.b64decode(outer_payload['message']['data']).decode('utf-8')
                print(f"[PUBSUB_HANDLER] Extracted inner payload length: {len(final_payload_str)}")
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
    monitor_status = "enabled" if (schema_monitor and getattr(schema_monitor, 'enabled', False)) else "disabled"
    
    return {
        "status": "healthy",
        "service": "cloud-run-transformer",
        "available_collections": available_collections,
        "project_id": PROJECT_ID,
        "gcs_bucket": GCS_PROCESSED_BUCKET_NAME,
        "schema_monitoring": monitor_status,
        "resilience_mode": "enabled"
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
    
    monitor_status = "enabled" if (schema_monitor and getattr(schema_monitor, 'enabled', False)) else "disabled"
    
    return {
        "service": "cloud-run-transformer",
        "project_id": PROJECT_ID,
        "gcs_processed_bucket": GCS_PROCESSED_BUCKET_NAME,
        "available_collections": available_collections,
        "schemas": schemas_info,
        "schema_monitoring": monitor_status,
        "resilience_features": {
            "auto_fill_missing_fields": True,
            "drop_unexpected_fields": True,
            "lossy_type_casting": True,
            "partial_recovery_mode": True,
            "cloud_logging_integration": monitor_status == "enabled"
        }
    }


if __name__ == "__main__":
    print("[STARTUP] Starting Cloud Run Transformer Service...")
    print(f"[STARTUP] Available collections: {get_available_collections()}")
    print(f"[STARTUP] PROJECT_ID: {PROJECT_ID}")
    print(f"[STARTUP] GCS_PROCESSED_BUCKET_NAME: {GCS_PROCESSED_BUCKET_NAME}")
    print("[STARTUP] Resilience mode: ENABLED - Will continue processing despite schema drift")
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))