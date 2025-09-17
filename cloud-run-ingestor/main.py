#!/usr/bin/env python3
# main.py - Production MongoDB Change Stream Ingestor with Enhanced Monitoring
# Async processing with comprehensive logging, monitoring metrics, and Gunicorn compatibility

import os
import json
import signal
import asyncio
import logging
import time
import sys
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import uuid
from decimal import Decimal

import motor.motor_asyncio
from bson import json_util
from bson import Timestamp
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1
from google.cloud import firestore
from google.cloud import monitoring_v3
import aiohttp
from google.auth import default
from google.auth.transport.requests import Request

# Local imports
from config import Config
from token_manager import TokenManager
from metrics_collector import MetricsCollector

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)

# ============================================================================
# JSON SERIALIZATION HELPER FOR MONITORING
# ============================================================================
def json_serializer(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    if hasattr(obj, '__dict__'):
        return str(obj)
    return str(obj)

# Log service startup
logger.info("="*60)
logger.info("🚀 MongoDB Change Stream Ingestor Starting")
logger.info(f"Version: 2.1.0 (with monitoring)")
logger.info(f"Python: {sys.version}")
logger.info(f"Process ID: {os.getpid()}")
logger.info(f"Environment: {os.environ.get('ENVIRONMENT', 'production')}")
logger.info("="*60)

# Track service start time
SERVICE_START_TIME = time.time()

# Flask app for health checks and manual triggers
app = Flask(__name__)
logger.info("✅ Flask app created")

# Global variables for service management
service: Optional['ChangeStreamIngestionService'] = None
async_loop: Optional[asyncio.AbstractEventLoop] = None
async_thread: Optional[threading.Thread] = None

class AsyncChangeStreamConsumer:
    """Asynchronously consumes MongoDB change streams with resume token management."""
    
    def __init__(self, config: Config, token_manager: TokenManager, metrics: MetricsCollector):
        logger.info("📋 Initializing AsyncChangeStreamConsumer")
        logger.info(f"  Config: Queue={config.QUEUE_MAX_SIZE}, BatchSize={config.CHANGE_STREAM_BATCH_SIZE}")
        logger.info(f"  Oplog Window: {config.OPLOG_WINDOW_HOURS} hours")
        
        self.config = config
        self.token_manager = token_manager
        self.metrics = metrics
        
        # MongoDB async client with timeout
        logger.info(f"🔗 Creating MongoDB client")
        logger.info(f"  URI: {config.MONGO_URI[:30]}...")
        logger.info(f"  Database: {config.MONGO_DB_NAME}")
        
        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                config.MONGO_URI,
                serverSelectionTimeoutMS=10000,  # 10 second timeout
                connectTimeoutMS=10000,
                socketTimeoutMS=10000
            )
            self.db = self.client[config.MONGO_DB_NAME]
            logger.info("✅ MongoDB client created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create MongoDB client: {e}")
            raise
        
        # Internal state
        self.resume_token = None
        self.last_token_save_time = time.time()
        self.last_token_save_count = 0
        self.events_since_checkpoint = 0
        self.is_running = False
        self.change_stream = None
        
        # Event queue for async processing
        self.event_queue = asyncio.Queue(maxsize=config.QUEUE_MAX_SIZE)
        
        # Track collection statistics
        self.collection_stats = {}
        
        # Session tracking
        self.session_start_time = None
        self.session_event_count = 0
        
        logger.info("✅ AsyncChangeStreamConsumer initialized")
    
    async def test_connection(self) -> bool:
        """Test MongoDB connection with detailed logging."""
        logger.info("🔍 Testing MongoDB connection...")
        try:
            start_time = time.time()
            result = await asyncio.wait_for(
                self.client.admin.command('ping'),
                timeout=5.0
            )
            latency = (time.time() - start_time) * 1000
            logger.info(f"✅ MongoDB ping successful in {latency:.1f}ms")
            logger.info(f"  Cluster time: {result.get('$clusterTime', {}).get('clusterTime')}")
            return True
        except asyncio.TimeoutError:
            logger.error("❌ MongoDB connection timeout after 5 seconds")
            return False
        except Exception as e:
            logger.error(f"❌ MongoDB connection test failed: {e}")
            return False
    
    async def start(self):
        """Start consuming change stream."""
        self.is_running = True
        logger.info("="*50)
        logger.info("📊 Starting change stream consumer")
        logger.info(f"  Database: {self.config.MONGO_DB_NAME}")
        logger.info(f"  Collections to log: {self.config.COLLECTIONS_TO_LOG or 'All'}")
        logger.info("="*50)
        
        # Test connection first
        if not await self.test_connection():
            logger.error("❌ Cannot start consumer - MongoDB connection failed")
            self.is_running = False
            return
        
        retry_count = 0
        while self.is_running:
            try:
                retry_count += 1
                if retry_count > 1:
                    logger.info(f"🔄 Retry attempt #{retry_count}")
                
                await self._consume_change_stream()
                
            except Exception as e:
                logger.error(f"❌ Change stream consumer error: {e}", exc_info=True)
                self.metrics.record_error('change_stream_error', str(e))
                
                # Exponential backoff on errors
                wait_time = min(60, 2 ** min(retry_count, 6))
                logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                await asyncio.sleep(wait_time)
                
                if self.metrics.consecutive_errors > 5:
                    logger.critical("🚨 Too many consecutive errors, implementing circuit breaker")
                    await asyncio.sleep(300)  # 5 minute circuit breaker
                    self.metrics.consecutive_errors = 0
                    retry_count = 0
    
    async def _consume_change_stream(self):
        """Main change stream consumption loop with resume logic."""
        logger.info("="*40)
        logger.info("📡 CHANGE STREAM SESSION STARTING")
        
        self.session_start_time = time.time()
        self.session_event_count = 0
        
        # Determine resume point
        resume_token = await self._get_resume_point()
        resume_type = "token" if isinstance(resume_token, dict) else "timestamp" if isinstance(resume_token, Timestamp) else "now"
        logger.info(f"📍 Resume strategy: {resume_type}")
        
        pipeline = [
            {'$match': {'operationType': {'$in': ['insert', 'update', 'delete', 'replace']}}}
        ]
        logger.info(f"🔧 Pipeline configured for operations: insert, update, delete, replace")
        
        try:
            # Configure change stream options
            options = {
                'full_document': 'updateLookup',
                'batch_size': self.config.CHANGE_STREAM_BATCH_SIZE
            }
            
            # Add resume token if available
            if resume_token:
                if isinstance(resume_token, dict) and '_data' in resume_token:
                    options['resume_after'] = resume_token
                    logger.info("📌 Resuming from stored token")
                elif isinstance(resume_token, datetime):
                    options['start_at_operation_time'] = resume_token
                    logger.info(f"📌 Starting from timestamp: {resume_token.isoformat()}")
                else:
                    logger.warning("⚠️ Invalid resume token format, starting from NOW")
            
            # Open change stream
            logger.info("🔌 Opening MongoDB change stream...")
            async with self.db.watch(pipeline, **options) as self.change_stream:
                logger.info("✅ Change stream connected successfully")
                logger.info(f"👂 Listening for changes in database: {self.config.MONGO_DB_NAME}")
                self.metrics.record_connection_established()
                
                # Process events
                async for change in self.change_stream:
                    if not self.is_running:
                        logger.info("🛑 Stop signal received, breaking change stream loop")
                        break
                    
                    self.session_event_count += 1
                    
                    # Log progress every 100 events
                    if self.session_event_count % 100 == 0:
                        elapsed = time.time() - self.session_start_time
                        rate = self.session_event_count / elapsed if elapsed > 0 else 0
                        queue_size = self.event_queue.qsize()
                        logger.info(
                            f"📊 Session progress: {self.session_event_count} events, "
                            f"{rate:.1f} events/sec, Queue: {queue_size}/{self.config.QUEUE_MAX_SIZE}"
                        )
                    
                    # Process the change event
                    await self._process_change(change)
                    
                    # Update resume token
                    self.resume_token = self.change_stream.resume_token
                    self.events_since_checkpoint += 1
                    
                    # Periodic token checkpoint
                    if await self._should_checkpoint():
                        await self._checkpoint_token()
                    
                    # Check queue pressure
                    queue_size = self.event_queue.qsize()
                    if queue_size > self.config.QUEUE_PRESSURE_THRESHOLD:
                        logger.warning(f"⚠️ Queue pressure high: {queue_size}/{self.config.QUEUE_MAX_SIZE}")
                        await asyncio.sleep(0.1)
                        self.metrics.record_backpressure()
                
                logger.info(f"📊 Change stream session ended. Processed {self.session_event_count} events")
                
        except Exception as e:
            logger.error(f"❌ Change stream error: {e}", exc_info=True)
            self.metrics.record_error('change_stream', str(e))
            raise
    
    async def _get_resume_point(self) -> Optional[Any]:
        """Determine resume point using multi-tier strategy."""
        logger.info("🔍 Determining resume point...")
        
        try:
            # Tier 1: Try stored resume token
            stored_token = await self.token_manager.get_resume_token()
            if stored_token:
                logger.info("✅ Found stored resume token")
                return stored_token['token']
            
            # Tier 2: Use timestamp from recent past (with buffer)
            last_checkpoint = await self.token_manager.get_last_checkpoint_time()
            if last_checkpoint:
                buffer_time = last_checkpoint - timedelta(minutes=self.config.RESUME_BUFFER_MINUTES)
                logger.info(f"📅 Using timestamp resume from {buffer_time.isoformat()}")
                return buffer_time
            
            # Tier 3: Start from safe window in the past
            safe_start = datetime.now(timezone.utc) - timedelta(hours=self.config.SAFE_START_HOURS)
            # Convert datetime to MongoDB Timestamp
            timestamp_seconds = int(safe_start.timestamp())
            mongo_timestamp = Timestamp(timestamp_seconds, 1)
            logger.info(f"📅 Starting from safe window: {safe_start.isoformat()} as Timestamp({timestamp_seconds}, 1)")
            return mongo_timestamp
            
        except Exception as e:
            logger.error(f"❌ Error determining resume point: {e}")
            # Tier 4: Last resort - start from NOW
            logger.warning("⚠️ Starting from NOW as last resort")
            return None
    
    async def _process_change(self, change: Dict[str, Any]):
        """Process a single change event with detailed logging and monitoring."""
        try:
            # Extract metadata
            operation = change.get('operationType', 'unknown')
            collection = change.get('ns', {}).get('coll', 'unknown')
            database = change.get('ns', {}).get('db', 'unknown')
            
            # Get document ID
            doc_id = None
            if change.get('documentKey'):
                doc_id = change['documentKey'].get('_id')
            elif change.get('fullDocument'):
                doc_id = change['fullDocument'].get('_id')
            
            # Calculate document size
            doc_size = 0
            if change.get('fullDocument'):
                doc_size = len(json.dumps(change['fullDocument'], default=json_serializer))
            elif change.get('documentKey'):
                doc_size = len(json.dumps(change['documentKey'], default=json_serializer))
            
            # STRUCTURED METRIC LOG - For monitoring
            logger.info(
                "METRIC:change_stream_event",
                extra={
                    'labels': {
                        'component': 'cloud_run_ingestor',
                        'pipeline_stage': 'ingestion',
                        'collection': collection,
                        'operation': operation,
                        'database': database
                    },
                    'jsonPayload': {
                        'event_type': 'change_stream_event',
                        'collection': collection,
                        'operation': operation,
                        'database': database,
                        'document_id': str(doc_id) if doc_id else 'unknown',
                        'document_size_bytes': doc_size,
                        'queue_depth': self.event_queue.qsize(),
                        'queue_capacity': self.config.QUEUE_MAX_SIZE,
                        'timestamp_millis': int(time.time() * 1000),
                        'session_event_count': self.session_event_count
                    }
                }
            )
            
            # Original detailed logging (keep as is for debugging)
            if self.config.ENABLE_DETAILED_LOGGING or collection in self.config.COLLECTIONS_TO_LOG:
                logger.info(f"📝 {operation.upper()} on {collection} - ID: {doc_id}")
            
            # Update collection statistics
            self._update_collection_stats(collection, operation)
            
            # Format the change event
            formatted_event = self._format_change_event(change)
            
            # Add to queue for async publishing
            await self.event_queue.put(formatted_event)
            
            # Record metrics
            self.metrics.record_event_processed(collection, operation)
            
            # Update oplog lag if available
            if change.get('clusterTime'):
                self.metrics.update_oplog_lag(change['clusterTime'])
                
        except Exception as e:
            logger.error(f"❌ Error processing change: {e}", exc_info=True)
            self.metrics.record_error('process_change', str(e))
    
    def _format_change_event(self, change: Dict[str, Any]) -> Dict[str, Any]:
        """Format MongoDB change event for Pub/Sub."""
        operation = change.get('operationType')
        
        # Extract document based on operation type
        document = None
        if operation in ['insert', 'update', 'replace']:
            document = change.get('fullDocument')
        elif operation == 'delete':
            document = change.get('documentKey')
        
        # Create standardized message
        message = {
            'operation': operation,
            'collection': change.get('ns', {}).get('coll'),
            'database': change.get('ns', {}).get('db'),
            'document': document,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'oplog_timestamp': str(change.get('clusterTime')),
            'correlation_id': str(uuid.uuid4()),
            'resume_token': change.get('_id')  # Include for downstream tracking
        }
        
        return message
    
    def _update_collection_stats(self, collection: str, operation: str):
        """Update collection-level statistics."""
        if collection not in self.collection_stats:
            self.collection_stats[collection] = {
                'insert': 0, 'update': 0, 'delete': 0, 'replace': 0, 'total': 0
            }
        
        self.collection_stats[collection][operation] = \
            self.collection_stats[collection].get(operation, 0) + 1
        self.collection_stats[collection]['total'] += 1
    
    async def _should_checkpoint(self) -> bool:
        """Determine if we should checkpoint the resume token."""
        time_elapsed = time.time() - self.last_token_save_time
        
        should_checkpoint = (
            self.events_since_checkpoint >= self.config.TOKEN_CHECKPOINT_EVENTS or
            time_elapsed >= self.config.TOKEN_CHECKPOINT_SECONDS
        )
        
        if should_checkpoint:
            logger.debug(f"💾 Checkpoint triggered: {self.events_since_checkpoint} events, {time_elapsed:.1f}s elapsed")
        
        return should_checkpoint
    
    async def _checkpoint_token(self):
        """Save resume token checkpoint."""
        if self.resume_token:
            try:
                logger.debug(f"💾 Saving checkpoint after {self.events_since_checkpoint} events")
                await self.token_manager.save_resume_token(
                    self.resume_token,
                    self.events_since_checkpoint
                )
                self.last_token_save_time = time.time()
                self.last_token_save_count = self.events_since_checkpoint
                self.events_since_checkpoint = 0
                logger.debug(f"✅ Token checkpoint saved successfully")
            except Exception as e:
                logger.error(f"❌ Failed to checkpoint token: {e}")
                self.metrics.record_error('token_checkpoint', str(e))
    
    async def stop(self):
        """Gracefully stop the consumer."""
        logger.info("🛑 Stopping change stream consumer...")
        self.is_running = False
        
        # Final token checkpoint
        if self.resume_token:
            logger.info("💾 Saving final token checkpoint...")
            await self._checkpoint_token()
        
        if self.change_stream:
            await self.change_stream.close()
        
        # Close MongoDB connection
        self.client.close()
        
        # Log session summary
        if self.session_start_time:
            session_duration = time.time() - self.session_start_time
            logger.info(f"📊 Session summary: {self.session_event_count} events in {session_duration:.1f}s")
        
        logger.info("✅ Change stream consumer stopped")

class AsyncPublisher:
    """Asynchronously publishes events to Pub/Sub with batching and retry logic."""
    
    def __init__(self, config: Config, metrics: MetricsCollector):
        logger.info("📋 Initializing AsyncPublisher")
        logger.info(f"  Workers: {config.PUBLISHER_WORKERS}")
        logger.info(f"  Max concurrent: {config.MAX_CONCURRENT_PUBLISHES}")
        
        self.config = config
        self.metrics = metrics
        
        # Pub/Sub clients
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(config.PROJECT_ID, config.PUBSUB_TOPIC_NAME)
        logger.info(f"  Topic: {config.PUBSUB_TOPIC_NAME}")
        
        self.dlq_topic_path = None
        if config.PUBLISHER_DLQ_TOPIC_NAME:
            self.dlq_topic_path = self.publisher.topic_path(
                config.PROJECT_ID, config.PUBLISHER_DLQ_TOPIC_NAME
            )
            logger.info(f"  DLQ: {config.PUBLISHER_DLQ_TOPIC_NAME}")
        
        # Publishing state
        self.is_running = False
        self.workers = []
        self.publish_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_PUBLISHES)
        
        # Error tracking for circuit breaker
        self.consecutive_dlq = 0
        self.last_dlq_check = time.time()
        self.dlq_window_count = 0
        
        # Performance tracking
        self.total_published = 0
        self.total_failed = 0
        
        logger.info("✅ AsyncPublisher initialized")
    
    async def start(self, event_queue: asyncio.Queue):
        """Start publisher workers."""
        self.is_running = True
        logger.info(f"🚀 Starting {self.config.PUBLISHER_WORKERS} publisher workers")
        
        # Create worker tasks
        for i in range(self.config.PUBLISHER_WORKERS):
            worker = asyncio.create_task(self._worker(event_queue, i))
            self.workers.append(worker)
            logger.info(f"  ✅ Worker {i} started")
    
    async def _worker(self, queue: asyncio.Queue, worker_id: int):
        """Worker coroutine that publishes events from queue."""
        logger.debug(f"👷 Publisher worker {worker_id} running")
        worker_published = 0
        worker_failed = 0
        
        while self.is_running:
            try:
                # Get event from queue with timeout to allow checking is_running
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Check circuit breaker
                if await self._should_circuit_break():
                    logger.warning(f"⚡ Circuit breaker activated for worker {worker_id}")
                    await asyncio.sleep(30)
                    continue
                
                # Publish with semaphore to limit concurrency
                async with self.publish_semaphore:
                    success = await self._publish_event(event)
                    if success:
                        worker_published += 1
                    else:
                        worker_failed += 1
                
                # Log worker progress periodically
                if (worker_published + worker_failed) % 100 == 0:
                    logger.debug(
                        f"👷 Worker {worker_id} progress: "
                        f"{worker_published} published, {worker_failed} failed"
                    )
                
            except Exception as e:
                logger.error(f"❌ Worker {worker_id} error: {e}", exc_info=True)
                self.metrics.record_error(f'worker_{worker_id}', str(e))
        
        logger.info(f"👷 Worker {worker_id} stopped. Published: {worker_published}, Failed: {worker_failed}")
    
    async def _publish_event(self, event: Dict[str, Any]) -> bool:
        """Publish a single event to Pub/Sub with retry logic and monitoring."""
        start_time = time.time()
        collection = event.get('collection', 'unknown')
        operation = event.get('operation', 'unknown')
        correlation_id = event.get('correlation_id', str(uuid.uuid4()))
        
        try:
            # Serialize event
            message_data = json_util.dumps(event).encode('utf-8')
            message_size = len(message_data)
            
            # Add attributes for filtering
            attributes = {
                'operation': operation,
                'collection': collection,
                'database': event.get('database', 'unknown'),
                'correlation_id': correlation_id  # Add for lineage tracking
            }
            
            # Publish with retry
            for attempt in range(self.config.PUBLISH_RETRY_ATTEMPTS):
                try:
                    future = self.publisher.publish(
                        self.topic_path,
                        message_data,
                        **attributes
                    )
                    
                    # Wait for publish confirmation
                    message_id = await asyncio.get_event_loop().run_in_executor(
                        None,
                        future.result,
                        self.config.PUBLISH_TIMEOUT
                    )
                    
                    # Success - record metrics
                    publish_latency = time.time() - start_time
                    self.metrics.record_publish_success(collection, publish_latency)
                    self.total_published += 1
                    
                    # STRUCTURED METRIC LOG - For monitoring
                    logger.info(
                        "METRIC:pubsub_published",
                        extra={
                            'labels': {
                                'component': 'cloud_run_ingestor',
                                'pipeline_stage': 'publish',
                                'collection': collection,
                                'operation': operation
                            },
                            'jsonPayload': {
                                'event_type': 'pubsub_published',
                                'collection': collection,
                                'operation': operation,
                                'message_id': message_id[:8] if message_id else 'unknown',
                                'correlation_id': correlation_id,
                                'message_size_bytes': message_size,
                                'publish_latency_ms': publish_latency * 1000,
                                'attempt': attempt + 1,
                                'timestamp_millis': int(time.time() * 1000)
                            }
                        }
                    )
                    
                    # Reset DLQ counter on success
                    self.consecutive_dlq = 0
                    
                    # Original logging (keep for debugging)
                    if collection in ['customers', 'orders', 'products'] or self.config.ENABLE_DETAILED_LOGGING:
                        logger.debug(f"✉️ Published {operation} for {collection} in {publish_latency:.3f}s")
                    
                    # Warn on slow publishes
                    if publish_latency > 1.0:
                        logger.warning(
                            "METRIC:slow_publish",
                            extra={
                                'labels': {'component': 'cloud_run_ingestor'},
                                'jsonPayload': {
                                    'event_type': 'slow_publish',
                                    'collection': collection,
                                    'latency_seconds': publish_latency
                                }
                            }
                        )
                    
                    return True
                    
                except Exception as e:
                    if attempt < self.config.PUBLISH_RETRY_ATTEMPTS - 1:
                        wait_time = 2 ** attempt
                        logger.debug(f"🔄 Retry {attempt + 1}/{self.config.PUBLISH_RETRY_ATTEMPTS} after {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:
                        raise e
            
        except Exception as e:
            logger.error(
                "METRIC:publish_failed",
                extra={
                    'labels': {
                        'component': 'cloud_run_ingestor',
                        'pipeline_stage': 'publish',
                        'collection': collection,
                        'error_type': type(e).__name__
                    },
                    'jsonPayload': {
                        'event_type': 'publish_failed',
                        'collection': collection,
                        'operation': operation,
                        'correlation_id': correlation_id,
                        'error': str(e),
                        'timestamp_millis': int(time.time() * 1000)
                    }
                }
            )
            self.metrics.record_publish_failure(collection)
            self.total_failed += 1
            
            # Send to DLQ
            await self._send_to_dlq(event, str(e))
            return False
    
    async def _send_to_dlq(self, event: Dict[str, Any], error: str):
        """Send failed message to dead letter queue."""
        if not self.dlq_topic_path:
            logger.error(f"❌ No DLQ configured, dropping message: {event.get('_id')}")
            return
        
        try:
            # Add error context
            event['_error_context'] = {
                'original_topic': self.config.PUBSUB_TOPIC_NAME,
                'error_message': error,
                'error_timestamp': datetime.now(timezone.utc).isoformat(),
                'attempts': self.config.PUBLISH_RETRY_ATTEMPTS
            }
            
            message_data = json_util.dumps(event).encode('utf-8')
            
            future = self.publisher.publish(self.dlq_topic_path, message_data)
            dlq_message_id = await asyncio.get_event_loop().run_in_executor(
                None, future.result, 5.0
            )
            
            logger.warning(f"📬 Sent to DLQ: {dlq_message_id[:8]}... for {event.get('collection')}")
            self.metrics.record_dlq_message(event.get('collection', 'unknown'))
            
            # Track for circuit breaker
            self.consecutive_dlq += 1
            self.dlq_window_count += 1
            
        except Exception as dlq_error:
            logger.critical(f"🚨 Failed to send to DLQ: {dlq_error}")
            logger.critical(f"🚨 DATA LOSS - Message dropped: {event.get('_id')}")
            self.metrics.record_error('dlq_failure', str(dlq_error))
    
    async def _should_circuit_break(self) -> bool:
        """Check if circuit breaker should activate."""
        # Reset window counter if needed
        if time.time() - self.last_dlq_check > self.config.CIRCUIT_BREAKER_WINDOW_SECONDS:
            self.dlq_window_count = 0
            self.last_dlq_check = time.time()
        
        # Circuit break if too many DLQ messages
        should_break = self.dlq_window_count > self.config.CIRCUIT_BREAKER_THRESHOLD
        if should_break:
            logger.warning(
                f"⚡ Circuit breaker check: {self.dlq_window_count} DLQ messages "
                f"in {self.config.CIRCUIT_BREAKER_WINDOW_SECONDS}s window"
            )
        return should_break
    
    async def stop(self):
        """Stop all publisher workers."""
        logger.info(f"🛑 Stopping {len(self.workers)} publisher workers...")
        self.is_running = False
        
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        logger.info(
            f"✅ Publisher stopped. Total published: {self.total_published}, "
            f"Failed: {self.total_failed}"
        )

class ChangeStreamIngestionService:
    """Main service orchestrator for change stream ingestion."""
    
    def __init__(self):
        logger.info("📋 Initializing ChangeStreamIngestionService")
        
        self.config = Config()
        self.metrics = MetricsCollector(self.config)
        self.token_manager = TokenManager(self.config)
        
        self.consumer = AsyncChangeStreamConsumer(
            self.config, self.token_manager, self.metrics
        )
        self.publisher = AsyncPublisher(self.config, self.metrics)
        
        self.shutdown_event = asyncio.Event()
        self.tasks = []
        
        logger.info("✅ ChangeStreamIngestionService initialized")
    
    async def start(self):
        """Start the ingestion service."""
        logger.info("="*60)
        logger.info("🚀 STARTING CHANGE STREAM INGESTION SERVICE")
        logger.info(f"📊 Configuration Summary:")
        logger.info(f"  Project: {self.config.PROJECT_ID}")
        logger.info(f"  Database: {self.config.MONGO_DB_NAME}")
        logger.info(f"  Topic: {self.config.PUBSUB_TOPIC_NAME}")
        logger.info(f"  Workers: {self.config.PUBLISHER_WORKERS}")
        logger.info(f"  Queue Size: {self.config.QUEUE_MAX_SIZE}")
        logger.info(f"  Checkpoint: Every {self.config.TOKEN_CHECKPOINT_EVENTS} events or {self.config.TOKEN_CHECKPOINT_SECONDS}s")
        logger.info(f"  Firestore: {'✅ Enabled' if self.config.ENABLE_FIRESTORE else '❌ Disabled'}")
        logger.info(f"  Metrics Export: {'✅ Enabled' if self.config.ENABLE_METRICS_EXPORT else '❌ Disabled'}")
        logger.info(f"  Detailed Logging: {'✅ Enabled' if self.config.ENABLE_DETAILED_LOGGING else '❌ Disabled'}")
        logger.info("="*60)
        
        try:
            # Initialize components
            logger.info("📦 Initializing TokenManager...")
            await self.token_manager.initialize()
            
            logger.info("📊 Initializing MetricsCollector...")
            await self.metrics.initialize()
            
            # Start publisher workers
            logger.info("🚀 Starting publisher workers...")
            await self.publisher.start(self.consumer.event_queue)
            
            # Start consumer
            logger.info("🚀 Starting change stream consumer...")
            consumer_task = asyncio.create_task(self.consumer.start())
            self.tasks.append(consumer_task)
            
            # Start metrics reporter
            logger.info("📊 Starting metrics reporter...")
            metrics_task = asyncio.create_task(self.metrics.start_reporting())
            self.tasks.append(metrics_task)
            
            logger.info("="*60)
            logger.info("✅ SERVICE READY - Waiting for MongoDB changes")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"❌ Failed to start service: {e}", exc_info=True)
            raise
    
    async def stop(self):
        """Gracefully stop the service."""
        logger.info("="*50)
        logger.info("🛑 Initiating graceful shutdown...")
        
        # Stop consumer first to prevent new events
        await self.consumer.stop()
        
        # Wait for queue to drain
        max_wait = self.config.SHUTDOWN_GRACE_PERIOD
        start_time = time.time()
        initial_queue_size = self.consumer.event_queue.qsize()
        
        if initial_queue_size > 0:
            logger.info(f"⏳ Waiting for {initial_queue_size} queued events to process...")
            
        while not self.consumer.event_queue.empty() and time.time() - start_time < max_wait:
            await asyncio.sleep(1)
            remaining = self.consumer.event_queue.qsize()
            if remaining > 0:
                logger.info(f"  Queue: {remaining} items remaining...")
        
        if self.consumer.event_queue.qsize() > 0:
            logger.warning(f"⚠️ Shutdown timeout - {self.consumer.event_queue.qsize()} events unprocessed")
        
        # Stop publisher
        await self.publisher.stop()
        
        # Stop metrics
        await self.metrics.stop()
        
        # Cancel remaining tasks
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        logger.info("✅ Service stopped gracefully")
        logger.info("="*50)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current service status."""
        uptime = time.time() - self.metrics.start_time
        
        return {
            'service': 'change-stream-ingestion',
            'version': '2.1.0',
            'status': 'running' if self.consumer.is_running else 'stopped',
            'uptime_seconds': uptime,
            'uptime_human': f"{uptime/3600:.1f} hours",
            'queue_size': self.consumer.event_queue.qsize(),
            'queue_capacity': self.config.QUEUE_MAX_SIZE,
            'events_processed': self.metrics.total_events_processed,
            'events_published': self.metrics.total_events_published,
            'events_failed': self.metrics.total_events_failed,
            'publish_success_rate': self.metrics.get_success_rate(),
            'collection_stats': self.consumer.collection_stats,
            'last_checkpoint': self.token_manager.last_checkpoint_time.isoformat() if self.token_manager.last_checkpoint_time else None,
            'consecutive_errors': self.metrics.consecutive_errors,
            'publisher_workers': len(self.publisher.workers),
            'config': {
                'project': self.config.PROJECT_ID,
                'database': self.config.MONGO_DB_NAME,
                'topic': self.config.PUBSUB_TOPIC_NAME,
                'firestore_enabled': self.config.ENABLE_FIRESTORE,
                'metrics_export_enabled': self.config.ENABLE_METRICS_EXPORT
            }
        }

# Flask endpoints
@app.route('/health')
def health_check():
    """Health check endpoint."""
    uptime = time.time() - SERVICE_START_TIME
    
    # During startup grace period
    if uptime < 10:
        return jsonify({
            'status': 'starting',
            'service': 'mongo-ingestor-async',
            'version': '2.1.0',
            'message': 'Service initializing',
            'uptime_seconds': uptime
        }), 200
    
    # Normal operation
    if service and service.consumer.is_running:
        return jsonify({
            'status': 'healthy',
            'service': 'mongo-ingestor-async',
            'version': '2.1.0',
            'uptime_seconds': uptime,
            'queue_depth': service.consumer.event_queue.qsize(),
            'events_processed': service.metrics.total_events_processed
        }), 200
    else:
        return jsonify({
            'status': 'unhealthy',
            'service': 'mongo-ingestor-async',
            'version': '2.1.0',
            'error': 'Service not running',
            'uptime_seconds': uptime
        }), 503

@app.route('/metrics')
def metrics_endpoint():
    """Metrics endpoint for monitoring."""
    if service:
        return jsonify(service.metrics.get_metrics()), 200
    else:
        return jsonify({'error': 'Service not initialized'}), 503

@app.route('/status')
def status_endpoint():
    """Detailed status endpoint."""
    if service:
        return jsonify(service.get_status()), 200
    else:
        return jsonify({'error': 'Service not initialized'}), 503

@app.route('/test-publish', methods=['POST'])
def test_publish():
    """Manual test endpoint for publishing."""
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({'error': 'Invalid JSON payload'}), 400
        
        # Add to queue for publishing
        if service and service.consumer.event_queue:
            asyncio.run_coroutine_threadsafe(
                service.consumer.event_queue.put(data),
                async_loop
            )
            logger.info(f"📮 Test message queued for publishing: {data.get('collection', 'unknown')}")
            return jsonify({'status': 'Message queued for publishing'}), 200
        else:
            return jsonify({'error': 'Service not ready'}), 503
            
    except Exception as e:
        logger.error(f"❌ Test publish error: {e}")
        return jsonify({'error': str(e)}), 500

# Signal handlers
def signal_handler(signum, frame):
    """Handle termination signals."""
    logger.info(f"📡 Received signal {signum}")
    if service and async_loop:
        async_loop.call_soon_threadsafe(
            lambda: asyncio.create_task(service.stop())
        )

# Main async function
async def main():
    """Main async entry point."""
    global service
    
    logger.info("📋 Async main function started")
    
    try:
        # Create service
        service = ChangeStreamIngestionService()
        
        # Setup signal handlers
        #signal.signal(signal.SIGTERM, signal_handler)
        #signal.signal(signal.SIGINT, signal_handler)
        
        # Start service
        await service.start()
        
        # Keep running until shutdown
        logger.info("⏳ Service running - waiting for shutdown signal...")
        await service.shutdown_event.wait()
        
    except Exception as e:
        logger.error(f"❌ Fatal error in main: {e}", exc_info=True)
        raise
    finally:
        logger.info("📋 Async main function completed")

# ============================================================================
def start_async_thread():
    """Start async service in a thread - called when module loads."""
    global async_loop, async_thread
    
    logger.info("🚀 Starting async service thread")
    
    def run_loop():
        global async_loop
        try:
            logger.info("📋 Creating new event loop for async service")
            async_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(async_loop)
            
            logger.info("📋 Starting event loop")
            async_loop.run_until_complete(main())
            
        except Exception as e:
            logger.error(f"❌ Async loop error: {e}", exc_info=True)
        finally:
            logger.info("📋 Event loop completed")
            if async_loop:
                async_loop.close()
    
    async_thread = threading.Thread(target=run_loop, daemon=True, name="AsyncServiceThread")
    async_thread.start()
    logger.info(f"✅ Async service thread started: {async_thread.name}")

# Start the async service immediately when module loads
# This ensures it runs even when Gunicorn imports the module
logger.info("🎬 Module loaded - Starting async service...")
start_async_thread()

# Give the service a moment to initialize
time.sleep(2)

logger.info("✅ Module initialization complete")
logger.info("="*60)

# This block only runs for local testing, not in Gunicorn
if __name__ == '__main__':
    logger.info("🧪 Running in local test mode (not Gunicorn)")
    logger.info("Flask app already running in thread from module load")
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Local test interrupted")