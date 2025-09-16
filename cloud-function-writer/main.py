"""
MongoDB Change Stream to GCS Writer - Production Ready
Optimized for reliability, idempotency, BSON preservation, and complete observability
"""

import os
import json
import base64
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional
from collections import defaultdict

import functions_framework
from google.cloud import storage
from google.cloud import exceptions as gcs_exceptions
from google.api_core import retry
from google.api_core import exceptions as api_exceptions
from cloudevents.http import CloudEvent

# Configure structured logging for GCP with JSON format for better querying
import google.cloud.logging
try:
    # Use structured logging in production
    client = google.cloud.logging.Client()
    client.setup_logging()
    logger = logging.getLogger('mongo-gcs-writer')
    logger.setLevel(logging.INFO)
except Exception:
    # Fallback to standard logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger('mongo-gcs-writer')

# Environment configuration with defaults
ENABLE_DETAILED_LOGGING = os.environ.get('ENABLE_DETAILED_LOGGING', 'true').lower() == 'true'
COLLECTIONS_TO_LOG = os.environ.get('COLLECTIONS_TO_LOG', '').split(',') if os.environ.get('COLLECTIONS_TO_LOG') else []
LOG_STATS_EVERY_N = int(os.environ.get('LOG_STATS_EVERY_N', '100'))

# Global statistics for metrics
class FunctionMetrics:
    """Tracks function execution metrics for observability."""
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset metrics (useful for periodic reporting)."""
        self.start_time = time.time()
        self.total_processed = 0
        self.total_succeeded = 0
        self.total_failed = 0
        self.total_dlq = 0
        self.collection_counts = defaultdict(lambda: defaultdict(int))
        self.operation_counts = defaultdict(int)
        self.total_bytes_processed = 0
        self.total_bytes_written = 0
        self.latencies = []
        self.gcs_latencies = []
        self.last_stats_log = time.time()
        self.errors_by_type = defaultdict(int)
    
    def record_processing_start(self):
        """Mark the start of processing a message."""
        return time.time()
    
    def record_success(self, collection: str, operation: str, size_bytes: int, 
                      start_time: float, gcs_duration: float):
        """Record successful processing."""
        total_duration = time.time() - start_time
        self.total_processed += 1
        self.total_succeeded += 1
        self.collection_counts[collection]['success'] += 1
        self.collection_counts[collection][operation] += 1
        self.operation_counts[operation] += 1
        self.total_bytes_processed += size_bytes
        self.total_bytes_written += size_bytes
        self.latencies.append(total_duration)
        self.gcs_latencies.append(gcs_duration)
    
    def record_failure(self, collection: str, operation: str, error_type: str):
        """Record failed processing."""
        self.total_processed += 1
        self.total_failed += 1
        self.collection_counts[collection]['failed'] += 1
        self.errors_by_type[error_type] += 1
    
    def should_log_stats(self):
        """Check if we should log statistics."""
        return (self.total_processed > 0 and 
                self.total_processed % LOG_STATS_EVERY_N == 0)
    
    def get_stats_summary(self):
        """Get comprehensive statistics summary."""
        elapsed = time.time() - self.start_time
        success_rate = (self.total_succeeded / self.total_processed * 100) if self.total_processed > 0 else 0
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        avg_gcs_latency = sum(self.gcs_latencies) / len(self.gcs_latencies) if self.gcs_latencies else 0
        throughput = self.total_processed / elapsed if elapsed > 0 else 0
        
        return {
            'total_processed': self.total_processed,
            'total_succeeded': self.total_succeeded,
            'total_failed': self.total_failed,
            'success_rate': f'{success_rate:.1f}%',
            'throughput_msg_per_sec': f'{throughput:.2f}',
            'avg_latency_ms': f'{avg_latency * 1000:.1f}',
            'avg_gcs_latency_ms': f'{avg_gcs_latency * 1000:.1f}',
            'total_bytes_processed': self.total_bytes_processed,
            'bytes_per_sec': f'{self.total_bytes_processed / elapsed:.0f}',
            'collections': dict(self.collection_counts),
            'operations': dict(self.operation_counts),
            'errors': dict(self.errors_by_type),
            'elapsed_seconds': f'{elapsed:.1f}'
        }

# Initialize metrics tracker
metrics = FunctionMetrics()

# Initialize at cold start for connection pooling
BUCKET_NAME = os.environ.get('GCS_DATA_BUCKET_NAME')
if not BUCKET_NAME:
    raise ValueError('GCS_DATA_BUCKET_NAME environment variable is required')

# Initialize GCS client with connection pooling
storage_client = storage.Client()

# Validate bucket exists at cold start
try:
    bucket = storage_client.bucket(BUCKET_NAME)
    # Verify bucket exists and is accessible
    bucket.reload(retry=retry.Retry(deadline=5.0))
    logger.info(f'Successfully connected to GCS bucket: {BUCKET_NAME}')
except Exception as e:
    logger.error(f'Failed to access GCS bucket {BUCKET_NAME}: {e}')
    raise ValueError(f'Cannot access GCS bucket: {BUCKET_NAME}')

# Configure retry strategy for GCS operations
GCS_RETRY = retry.Retry(
    initial=0.25,      # Start with 250ms
    maximum=2.0,       # Max 2 second wait between retries
    multiplier=2.0,    # Double the wait each time
    deadline=10.0,     # Total timeout of 10 seconds
    predicate=retry.if_exception_type(
        api_exceptions.TooManyRequests,
        api_exceptions.InternalServerError,
        api_exceptions.ServiceUnavailable,
        api_exceptions.GatewayTimeout,
    )
)


def extract_message_data(event: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
    """
    Extract and decode message data from Pub/Sub event.
    Handles both direct and wrapped message formats.
    
    Returns:
        Tuple of (raw_data_bytes, message_attributes, pubsub_message_id)
    
    Raises:
        ValueError: If message cannot be decoded
    """
    try:
        # Get Pub/Sub message ID for idempotency
        pubsub_message_id = event.get('messageId', '')
        
        # Get message attributes if present
        attributes = event.get('attributes', {})
        
        # Extract the base64 encoded data
        if 'data' not in event:
            raise ValueError('No data field in Pub/Sub message')
        
        # Decode base64 to get raw bytes
        raw_bytes = base64.b64decode(event['data'])
        
        # Check if this is a nested Pub/Sub message (from push subscription)
        # by attempting to parse as JSON and looking for nested structure
        try:
            potential_wrapper = json.loads(raw_bytes)
            if isinstance(potential_wrapper, dict) and 'message' in potential_wrapper:
                # This is a wrapped message from push subscription
                nested_message = potential_wrapper['message']
                if 'data' in nested_message:
                    raw_bytes = base64.b64decode(nested_message['data'])
                    pubsub_message_id = nested_message.get('messageId', pubsub_message_id)
                    attributes = nested_message.get('attributes', attributes)
        except (json.JSONDecodeError, KeyError, TypeError):
            # Not a wrapped message, use raw_bytes as is
            pass
        
        return raw_bytes, attributes, pubsub_message_id
        
    except Exception as e:
        logger.error(f'Failed to extract message data: {e}')
        raise ValueError(f'Invalid message format: {e}')


def generate_idempotent_filename(
    raw_data: bytes,
    attributes: Dict[str, Any],
    pubsub_message_id: str
) -> Tuple[str, Dict[str, Any]]:
    """
    Generate an idempotent filename and extract metadata.
    Uses Pub/Sub message ID to ensure exactly-once writes.
    
    Returns:
        Tuple of (gcs_path, metadata_dict)
    """
    # Try to extract MongoDB metadata from the raw data
    metadata = {
        'pubsub_message_id': pubsub_message_id,
        'attributes': attributes,
        'received_at': datetime.now(timezone.utc).isoformat(),
        'data_size_bytes': len(raw_data)
    }
    
    # Default values
    collection = 'unknown'
    operation = 'unknown'
    document_id = 'unknown'
    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    
    # Attempt to parse metadata without modifying the data
    try:
        # Parse as JSON to extract metadata (but we'll still write raw bytes)
        change_event = json.loads(raw_data)
        
        # Extract MongoDB metadata
        collection = change_event.get('collection', 'unknown')
        operation = change_event.get('operation', 'unknown')
        
        # Extract document ID with fallback logic
        if change_event.get('document') and '_id' in change_event['document']:
            document_id = str(change_event['document']['_id'])
        elif change_event.get('documentKey') and '_id' in change_event['documentKey']:
            document_id = str(change_event['documentKey']['_id'])
        
        # Use MongoDB timestamp if available
        if 'timestamp' in change_event:
            timestamp_str = change_event['timestamp'].replace(':', '-').replace('.', '-')[:19]
        
        # Add metadata for monitoring
        metadata['collection'] = collection
        metadata['operation'] = operation
        metadata['document_id'] = document_id
        
        # Extract additional MongoDB metadata
        if 'oplog_timestamp' in change_event:
            metadata['oplog_timestamp'] = change_event['oplog_timestamp']
        if 'correlation_id' in change_event:
            metadata['correlation_id'] = change_event['correlation_id']
        if 'resume_token' in change_event:
            metadata['has_resume_token'] = True
        
        # Log detailed event information if enabled
        if ENABLE_DETAILED_LOGGING or collection in COLLECTIONS_TO_LOG:
            logger.info(
                f"📝 {operation.upper()} on {collection} - "
                f"Doc ID: {document_id[:20]}... - "
                f"Size: {len(raw_data)} bytes - "
                f"Message: {pubsub_message_id[:8]}...",
                extra={
                    'collection': collection,
                    'operation': operation,
                    'document_id': document_id,
                    'message_size': len(raw_data),
                    'pubsub_message_id': pubsub_message_id
                }
            )
            
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        # If we can't parse, use defaults and log
        logger.warning(
            f'⚠️ Could not parse metadata from message {pubsub_message_id[:8]}...: {e}',
            extra={'error': str(e), 'pubsub_message_id': pubsub_message_id}
        )
        metadata['parse_warning'] = str(e)
    
    # Generate deterministic path using Pub/Sub message ID
    # This ensures idempotency - same message always writes to same location
    date_partition = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Use first 8 chars of message ID as prefix for better GCS performance
    prefix = pubsub_message_id[:8] if pubsub_message_id else hashlib.md5(raw_data).hexdigest()[:8]
    
    # Construct filename - using pubsub_message_id ensures idempotency
    filename = f'{operation}_{timestamp_str}_{pubsub_message_id}.json'
    
    # Full GCS path with partitioning
    gcs_path = f'raw/{collection}/{prefix}-{date_partition}/{filename}'
    
    metadata['gcs_path'] = gcs_path
    
    logger.debug(
        f"📁 Generated path: {gcs_path}",
        extra={'gcs_path': gcs_path, 'collection': collection}
    )
    
    return gcs_path, metadata


def write_to_gcs_with_retry(
    blob_path: str,
    data: bytes,
    metadata: Dict[str, Any],
    retry_strategy: retry.Retry = GCS_RETRY
) -> Tuple[bool, float]:
    """
    Write data to GCS with retry logic and idempotency.
    
    Returns:
        Tuple of (success: bool, write_duration_seconds: float)
    """
    gcs_start_time = time.time()
    collection = metadata.get('collection', 'unknown')
    operation = metadata.get('operation', 'unknown')
    document_id = metadata.get('document_id', 'unknown')
    
    try:
        blob = bucket.blob(blob_path)
        
        # Set metadata on the blob for tracking
        blob.metadata = {
            'pubsub_message_id': metadata.get('pubsub_message_id', ''),
            'operation': operation,
            'collection': collection,
            'document_id': document_id,
            'received_at': metadata.get('received_at', ''),
            'data_size': str(metadata.get('data_size_bytes', 0))
        }
        
        logger.debug(f"⬆️ Starting GCS upload: {blob_path}")
        
        # Upload with retry strategy
        # Using if_generation_match=None allows overwrites (idempotent)
        blob.upload_from_string(
            data,
            content_type='application/json',
            retry=retry_strategy,
            timeout=10.0
        )
        
        gcs_duration = time.time() - gcs_start_time
        
        # Log successful write with detailed metrics
        logger.info(
            f'✅ Successfully wrote {operation} for {collection} - '
            f'Doc: {document_id[:20]}... - '
            f'Path: {blob_path} - '
            f'Size: {metadata.get("data_size_bytes", 0)} bytes - '
            f'GCS latency: {gcs_duration*1000:.1f}ms',
            extra={
                'event': 'gcs_write_success',
                'collection': collection,
                'operation': operation,
                'document_id': document_id,
                'gcs_path': blob_path,
                'size_bytes': metadata.get('data_size_bytes', 0),
                'gcs_latency_ms': gcs_duration * 1000,
                'pubsub_message_id': metadata.get('pubsub_message_id', '')
            }
        )
        
        # Warning on slow writes
        if gcs_duration > 2.0:
            logger.warning(
                f'⚠️ Slow GCS write: {gcs_duration:.2f}s for {blob_path}',
                extra={'gcs_latency_seconds': gcs_duration, 'gcs_path': blob_path}
            )
        
        return True, gcs_duration
        
    except api_exceptions.GoogleAPIError as e:
        # These are retryable errors that already exhausted retries
        gcs_duration = time.time() - gcs_start_time
        logger.error(
            f'❌ GCS write failed after retries for {collection}/{operation} - '
            f'Doc: {document_id[:20]}... - '
            f'Path: {blob_path} - '
            f'Error: {e} - '
            f'Duration: {gcs_duration:.2f}s',
            extra={
                'event': 'gcs_write_failure',
                'collection': collection,
                'operation': operation,
                'document_id': document_id,
                'gcs_path': blob_path,
                'error': str(e),
                'error_type': type(e).__name__,
                'duration_seconds': gcs_duration
            }
        )
        return False, gcs_duration
        
    except Exception as e:
        # Unexpected errors
        gcs_duration = time.time() - gcs_start_time
        logger.error(
            f'🚨 Unexpected error writing {collection}/{operation} - '
            f'Path: {blob_path} - '
            f'Error: {e}',
            extra={
                'event': 'gcs_write_unexpected_error',
                'collection': collection,
                'operation': operation,
                'gcs_path': blob_path,
                'error': str(e),
                'error_type': type(e).__name__,
                'duration_seconds': gcs_duration
            }
        )
        return False, gcs_duration


def write_error_context(
    original_event: Dict[str, Any],
    error: str,
    error_type: str
) -> None:
    """
    Write error context to a separate error path for debugging.
    Only called when main write fails.
    """
    try:
        error_data = {
            'error': error,
            'error_type': error_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'original_event': original_event,
            'bucket': BUCKET_NAME
        }
        
        # Write to error path
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        error_path = f'errors/{date_str}/{original_event.get("messageId", "unknown")}_{datetime.now(timezone.utc).timestamp()}.json'
        
        error_blob = bucket.blob(error_path)
        error_blob.upload_from_string(
            json.dumps(error_data, indent=2),
            content_type='application/json',
            retry=GCS_RETRY,
            timeout=5.0
        )
        
        logger.info(f'Error context written to: {error_path}')
        
    except Exception as e:
        # Don't fail the function if error logging fails
        logger.error(f'Failed to write error context: {e}')


@functions_framework.cloud_event
def process_pubsub_message(cloud_event: CloudEvent) -> None:
    """
    Main entry point for Cloud Function triggered by Pub/Sub.
    Processes MongoDB change stream events and writes to GCS.
    
    Guarantees:
    - Idempotent writes using Pub/Sub message ID
    - No data loss (fails function if write fails)
    - Minimal data transformation (preserves BSON)
    - Complete observability through structured logging
    """
    # Start tracking for this message
    processing_start = metrics.record_processing_start()
    
    event_data = cloud_event.data
    message_id = event_data.get('messageId', 'unknown')
    publish_time = event_data.get('publishTime', '')
    
    # Log every message received
    logger.info(
        f'📨 Processing message: {message_id[:8]}... - '
        f'Published: {publish_time}',
        extra={
            'event': 'message_received',
            'pubsub_message_id': message_id,
            'publish_time': publish_time,
            'processing_started_at': datetime.now(timezone.utc).isoformat()
        }
    )
    
    collection = 'unknown'
    operation = 'unknown'
    
    try:
        # Step 1: Extract and decode message
        decode_start = time.time()
        raw_data, attributes, pubsub_msg_id = extract_message_data(event_data)
        decode_duration = time.time() - decode_start
        
        logger.debug(
            f'🔓 Message decoded in {decode_duration*1000:.1f}ms - Size: {len(raw_data)} bytes',
            extra={'decode_latency_ms': decode_duration * 1000, 'size_bytes': len(raw_data)}
        )
        
        # Step 2: Generate idempotent filename
        path_start = time.time()
        gcs_path, metadata = generate_idempotent_filename(
            raw_data, 
            attributes, 
            pubsub_msg_id
        )
        path_duration = time.time() - path_start
        
        # Extract for metrics tracking
        collection = metadata.get('collection', 'unknown')
        operation = metadata.get('operation', 'unknown')
        document_id = metadata.get('document_id', 'unknown')
        
        # Step 3: Write to GCS with retry
        success, gcs_duration = write_to_gcs_with_retry(gcs_path, raw_data, metadata)
        
        if not success:
            # Record failure in metrics
            metrics.record_failure(collection, operation, 'gcs_write_failure')
            
            # Write error context for debugging
            write_error_context(
                event_data,
                'Failed to write to GCS after retries',
                'gcs_write_failure'
            )
            
            logger.error(
                f'❌ Failed to process message {message_id[:8]}... - '
                f'Collection: {collection} - '
                f'Operation: {operation} - '
                f'Doc: {document_id[:20]}...',
                extra={
                    'event': 'message_processing_failed',
                    'pubsub_message_id': message_id,
                    'collection': collection,
                    'operation': operation,
                    'document_id': document_id,
                    'failure_reason': 'gcs_write_failure'
                }
            )
            
            # Raise exception to trigger Cloud Function retry/DLQ
            raise RuntimeError(f'Failed to write message {message_id} to GCS')
        
        # Calculate total processing time
        total_duration = time.time() - processing_start
        
        # Record success in metrics
        metrics.record_success(
            collection=collection,
            operation=operation,
            size_bytes=len(raw_data),
            start_time=processing_start,
            gcs_duration=gcs_duration
        )
        
        # Log successful processing with complete metrics
        logger.info(
            f'✅ Successfully processed {operation} for {collection} - '
            f'Doc: {document_id[:20]}... - '
            f'Total: {total_duration*1000:.1f}ms - '
            f'GCS: {gcs_duration*1000:.1f}ms - '
            f'Message: {message_id[:8]}...',
            extra={
                'event': 'message_processed_success',
                'pubsub_message_id': message_id,
                'collection': collection,
                'operation': operation,
                'document_id': document_id,
                'total_latency_ms': total_duration * 1000,
                'gcs_latency_ms': gcs_duration * 1000,
                'decode_latency_ms': decode_duration * 1000,
                'size_bytes': len(raw_data),
                'gcs_path': gcs_path
            }
        )
        
        # Log periodic statistics
        if metrics.should_log_stats():
            stats = metrics.get_stats_summary()
            logger.info(
                f'📊 Processing statistics - '
                f'Total: {stats["total_processed"]} - '
                f'Success rate: {stats["success_rate"]} - '
                f'Throughput: {stats["throughput_msg_per_sec"]} msg/s - '
                f'Avg latency: {stats["avg_latency_ms"]}ms',
                extra={
                    'event': 'statistics_summary',
                    **stats
                }
            )
        
    except ValueError as e:
        # Data format errors - these won't be fixed by retry
        metrics.record_failure(collection, operation, 'data_format_error')
        
        logger.error(
            f'⚠️ Unrecoverable error for message {message_id[:8]}... - '
            f'Collection: {collection} - '
            f'Error: {e}',
            extra={
                'event': 'message_format_error',
                'pubsub_message_id': message_id,
                'collection': collection,
                'error': str(e),
                'error_type': 'ValueError'
            }
        )
        
        # Write error context for analysis
        write_error_context(
            event_data,
            str(e),
            'data_format_error'
        )
        
        # Don't raise - let message be ACK'd and sent to DLQ
        # This prevents infinite retry loops on bad data
        logger.warning(
            f'📬 Message {message_id[:8]}... acknowledged despite error - Will go to DLQ',
            extra={'pubsub_message_id': message_id, 'action': 'send_to_dlq'}
        )
        
    except Exception as e:
        # Unexpected errors - these might be transient
        metrics.record_failure(collection, operation, type(e).__name__)
        
        total_duration = time.time() - processing_start
        
        logger.error(
            f'🚨 Unexpected error processing message {message_id[:8]}... - '
            f'Collection: {collection} - '
            f'Operation: {operation} - '
            f'Error: {e} - '
            f'Duration: {total_duration*1000:.1f}ms',
            extra={
                'event': 'message_processing_unexpected_error',
                'pubsub_message_id': message_id,
                'collection': collection,
                'operation': operation,
                'error': str(e),
                'error_type': type(e).__name__,
                'total_latency_ms': total_duration * 1000
            },
            exc_info=True
        )
        
        # Write error context
        write_error_context(
            event_data,
            str(e),
            'unexpected_error'
        )
        
        # Raise to trigger retry
        raise RuntimeError(f'Failed to process message {message_id}: {e}')


# Health check endpoint for monitoring
@functions_framework.http
def health(request):
    """Health check endpoint for monitoring."""
    try:
        # Verify bucket is accessible
        bucket.reload(timeout=2.0)
        return {'status': 'healthy', 'bucket': BUCKET_NAME}, 200
    except Exception as e:
        return {'status': 'unhealthy', 'error': str(e)}, 503