# main.py - Async Cloud Run Ingestor with resilient resume token handling
# This service listens to MongoDB change streams and publishes events to Pub/Sub
# with async processing, comprehensive monitoring, and graceful error recovery.

import os
import json
import signal
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import uuid

import motor.motor_asyncio
from bson import json_util
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1
from google.cloud import firestore
from google.cloud import monitoring_v3
import aiohttp
from google.auth import default
from google.auth.transport.requests import Request

# Local imports (we'll create these modules)
from config import Config
from token_manager import TokenManager
from metrics_collector import MetricsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask app for health checks and manual triggers
app = Flask(__name__)

class AsyncChangeStreamConsumer:
    """Asynchronously consumes MongoDB change streams with resume token management."""
    
    def __init__(self, config: Config, token_manager: TokenManager, metrics: MetricsCollector):
        self.config = config
        self.token_manager = token_manager
        self.metrics = metrics
        
        # MongoDB async client
        self.client = motor.motor_asyncio.AsyncIOMotorClient(config.MONGO_URI)
        self.db = self.client[config.MONGO_DB_NAME]
        
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
        
    async def start(self):
        """Start consuming change stream."""
        self.is_running = True
        logger.info(f"Starting change stream consumer for database '{self.config.MONGO_DB_NAME}'")
        
        while self.is_running:
            try:
                await self._consume_change_stream()
            except Exception as e:
                logger.error(f"Change stream consumer error: {e}")
                self.metrics.record_error('change_stream_error', str(e))
                
                # Exponential backoff on errors
                await asyncio.sleep(min(60, 2 ** self.metrics.consecutive_errors))
                
                if self.metrics.consecutive_errors > 5:
                    logger.critical("Too many consecutive errors, implementing circuit breaker")
                    await asyncio.sleep(300)  # 5 minute circuit breaker
                    self.metrics.consecutive_errors = 0
    
    async def _consume_change_stream(self):
        """Main change stream consumption loop with resume logic."""
        resume_token = await self._get_resume_point()
        
        pipeline = [
            {'$match': {'operationType': {'$in': ['insert', 'update', 'delete', 'replace']}}}
        ]
        
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
                    logger.info("Resuming from stored token")
                elif isinstance(resume_token, datetime):
                    options['start_at_operation_time'] = resume_token
                    logger.info(f"Starting from timestamp: {resume_token}")
                else:
                    logger.warning("Invalid resume token format, starting from NOW")
            
            # Open change stream
            async with self.db.watch(pipeline, **options) as self.change_stream:
                logger.info("Successfully connected to MongoDB change stream")
                self.metrics.record_connection_established()
                
                async for change in self.change_stream:
                    if not self.is_running:
                        break
                    
                    # Process the change event
                    await self._process_change(change)
                    
                    # Update resume token
                    self.resume_token = self.change_stream.resume_token
                    self.events_since_checkpoint += 1
                    
                    # Periodic token checkpoint
                    if await self._should_checkpoint():
                        await self._checkpoint_token()
                    
                    # Check queue pressure
                    if self.event_queue.qsize() > self.config.QUEUE_PRESSURE_THRESHOLD:
                        # Backpressure - slow down consumption
                        await asyncio.sleep(0.1)
                        self.metrics.record_backpressure()
                        
        except Exception as e:
            logger.error(f"Change stream error: {e}")
            self.metrics.record_error('change_stream', str(e))
            raise
    
    async def _get_resume_point(self) -> Optional[Any]:
        """Determine resume point using multi-tier strategy."""
        try:
            # Tier 1: Try stored resume token
            stored_token = await self.token_manager.get_resume_token()
            if stored_token:
                logger.info("Found stored resume token")
                return stored_token['token']
            
            # Tier 2: Use timestamp from recent past (with buffer)
            last_checkpoint = await self.token_manager.get_last_checkpoint_time()
            if last_checkpoint:
                # Go back a bit further to ensure no gaps
                buffer_time = last_checkpoint - timedelta(minutes=self.config.RESUME_BUFFER_MINUTES)
                logger.info(f"Using timestamp resume from {buffer_time}")
                return buffer_time
            
            # Tier 3: Start from safe window in the past
            safe_start = datetime.now(timezone.utc) - timedelta(hours=self.config.SAFE_START_HOURS)
            logger.info(f"Starting from safe window: {safe_start}")
            return safe_start
            
        except Exception as e:
            logger.error(f"Error determining resume point: {e}")
            # Tier 4: Last resort - start from NOW
            logger.warning("Starting from NOW as last resort")
            return None
    
    async def _process_change(self, change: Dict[str, Any]):
        """Process a single change event."""
        try:
            # Extract metadata
            operation = change.get('operationType', 'unknown')
            collection = change.get('ns', {}).get('coll', 'unknown')
            database = change.get('ns', {}).get('db', 'unknown')
            
            # Update collection statistics
            self._update_collection_stats(collection, operation)
            
            # Format the change event
            formatted_event = self._format_change_event(change)
            
            # Add to queue for async publishing
            await self.event_queue.put(formatted_event)
            
            # Record metrics
            self.metrics.record_event_processed(collection, operation)
            
            # Log for specific collections if needed
            if collection in self.config.COLLECTIONS_TO_LOG:
                logger.info(f"Processed {operation} for {collection}: {change.get('_id')}")
                
        except Exception as e:
            logger.error(f"Error processing change: {e}")
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
        
        return (
            self.events_since_checkpoint >= self.config.TOKEN_CHECKPOINT_EVENTS or
            time_elapsed >= self.config.TOKEN_CHECKPOINT_SECONDS
        )
    
    async def _checkpoint_token(self):
        """Save resume token checkpoint."""
        if self.resume_token:
            try:
                await self.token_manager.save_resume_token(
                    self.resume_token,
                    self.events_since_checkpoint
                )
                self.last_token_save_time = time.time()
                self.last_token_save_count = self.events_since_checkpoint
                self.events_since_checkpoint = 0
                logger.debug(f"Checkpointed resume token after {self.last_token_save_count} events")
            except Exception as e:
                logger.error(f"Failed to checkpoint token: {e}")
                self.metrics.record_error('token_checkpoint', str(e))
    
    async def stop(self):
        """Gracefully stop the consumer."""
        logger.info("Stopping change stream consumer...")
        self.is_running = False
        
        # Final token checkpoint
        await self._checkpoint_token()
        
        if self.change_stream:
            await self.change_stream.close()
        
        # Close MongoDB connection
        self.client.close()
        
        logger.info("Change stream consumer stopped")

class AsyncPublisher:
    """Asynchronously publishes events to Pub/Sub with batching and retry logic."""
    
    def __init__(self, config: Config, metrics: MetricsCollector):
        self.config = config
        self.metrics = metrics
        
        # Pub/Sub clients
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(config.PROJECT_ID, config.PUBSUB_TOPIC_NAME)
        self.dlq_topic_path = None
        if config.PUBLISHER_DLQ_TOPIC_NAME:
            self.dlq_topic_path = self.publisher.topic_path(
                config.PROJECT_ID, config.PUBLISHER_DLQ_TOPIC_NAME
            )
        
        # Publishing state
        self.is_running = False
        self.workers = []
        self.publish_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_PUBLISHES)
        
        # Error tracking for circuit breaker
        self.consecutive_dlq = 0
        self.last_dlq_check = time.time()
        self.dlq_window_count = 0
    
    async def start(self, event_queue: asyncio.Queue):
        """Start publisher workers."""
        self.is_running = True
        logger.info(f"Starting {self.config.PUBLISHER_WORKERS} publisher workers")
        
        # Create worker tasks
        for i in range(self.config.PUBLISHER_WORKERS):
            worker = asyncio.create_task(self._worker(event_queue, i))
            self.workers.append(worker)
    
    async def _worker(self, queue: asyncio.Queue, worker_id: int):
        """Worker coroutine that publishes events from queue."""
        logger.info(f"Publisher worker {worker_id} started")
        
        while self.is_running:
            try:
                # Get event from queue with timeout to allow checking is_running
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Check circuit breaker
                if await self._should_circuit_break():
                    logger.warning("Circuit breaker activated, pausing publishing")
                    await asyncio.sleep(30)
                    continue
                
                # Publish with semaphore to limit concurrency
                async with self.publish_semaphore:
                    await self._publish_event(event)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                self.metrics.record_error(f'worker_{worker_id}', str(e))
    
    async def _publish_event(self, event: Dict[str, Any]):
        """Publish a single event to Pub/Sub with retry logic."""
        start_time = time.time()
        
        try:
            # Serialize event
            message_data = json_util.dumps(event).encode('utf-8')
            
            # Add attributes for filtering
            attributes = {
                'operation': event.get('operation', 'unknown'),
                'collection': event.get('collection', 'unknown'),
                'database': event.get('database', 'unknown')
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
                    self.metrics.record_publish_success(
                        event.get('collection', 'unknown'),
                        publish_latency
                    )
                    
                    # Reset DLQ counter on success
                    self.consecutive_dlq = 0
                    
                    logger.debug(f"Published message {message_id} for {event.get('collection')}")
                    return
                    
                except Exception as e:
                    if attempt < self.config.PUBLISH_RETRY_ATTEMPTS - 1:
                        # Exponential backoff
                        await asyncio.sleep(2 ** attempt)
                    else:
                        raise e
            
        except Exception as e:
            logger.error(f"Failed to publish after retries: {e}")
            self.metrics.record_publish_failure(event.get('collection', 'unknown'))
            
            # Send to DLQ
            await self._send_to_dlq(event, str(e))
    
    async def _send_to_dlq(self, event: Dict[str, Any], error: str):
        """Send failed message to dead letter queue."""
        if not self.dlq_topic_path:
            logger.error(f"No DLQ configured, dropping message: {event.get('_id')}")
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
            
            logger.warning(f"Sent to DLQ: {dlq_message_id}")
            self.metrics.record_dlq_message(event.get('collection', 'unknown'))
            
            # Track for circuit breaker
            self.consecutive_dlq += 1
            self.dlq_window_count += 1
            
        except Exception as dlq_error:
            logger.critical(f"Failed to send to DLQ: {dlq_error}")
            logger.critical(f"DATA LOSS - Message dropped: {event.get('_id')}")
            self.metrics.record_error('dlq_failure', str(dlq_error))
    
    async def _should_circuit_break(self) -> bool:
        """Check if circuit breaker should activate."""
        # Reset window counter if needed
        if time.time() - self.last_dlq_check > 300:  # 5 minute window
            self.dlq_window_count = 0
            self.last_dlq_check = time.time()
        
        # Circuit break if too many DLQ messages
        return self.dlq_window_count > self.config.CIRCUIT_BREAKER_THRESHOLD
    
    async def stop(self):
        """Stop all publisher workers."""
        logger.info("Stopping publisher workers...")
        self.is_running = False
        
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        logger.info("Publisher workers stopped")

class ChangeStreamIngestionService:
    """Main service orchestrator for change stream ingestion."""
    
    def __init__(self):
        self.config = Config()
        self.metrics = MetricsCollector(self.config)
        self.token_manager = TokenManager(self.config)
        
        self.consumer = AsyncChangeStreamConsumer(
            self.config, self.token_manager, self.metrics
        )
        self.publisher = AsyncPublisher(self.config, self.metrics)
        
        self.shutdown_event = asyncio.Event()
        self.tasks = []
    
    async def start(self):
        """Start the ingestion service."""
        logger.info("Starting Change Stream Ingestion Service")
        
        # Initialize components
        await self.token_manager.initialize()
        await self.metrics.initialize()
        
        # Start publisher workers
        await self.publisher.start(self.consumer.event_queue)
        
        # Start consumer
        consumer_task = asyncio.create_task(self.consumer.start())
        self.tasks.append(consumer_task)
        
        # Start metrics reporter
        metrics_task = asyncio.create_task(self.metrics.start_reporting())
        self.tasks.append(metrics_task)
        
        logger.info("Service started successfully")
    
    async def stop(self):
        """Gracefully stop the service."""
        logger.info("Initiating graceful shutdown...")
        
        # Stop consumer first to prevent new events
        await self.consumer.stop()
        
        # Wait for queue to drain
        max_wait = 30
        start_time = time.time()
        while not self.consumer.event_queue.empty() and time.time() - start_time < max_wait:
            await asyncio.sleep(1)
            logger.info(f"Waiting for queue to drain: {self.consumer.event_queue.qsize()} items remaining")
        
        # Stop publisher
        await self.publisher.stop()
        
        # Stop metrics
        await self.metrics.stop()
        
        # Cancel remaining tasks
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        logger.info("Service stopped gracefully")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current service status."""
        return {
            'service': 'change-stream-ingestion',
            'status': 'running' if self.consumer.is_running else 'stopped',
            'queue_size': self.consumer.event_queue.qsize(),
            'events_processed': self.metrics.total_events_processed,
            'publish_success_rate': self.metrics.get_success_rate(),
            'collection_stats': self.consumer.collection_stats,
            'last_checkpoint': self.token_manager.last_checkpoint_time,
            'consecutive_errors': self.metrics.consecutive_errors,
            'uptime_seconds': time.time() - self.metrics.start_time
        }

# Global service instance
service: Optional[ChangeStreamIngestionService] = None

# Flask endpoints
@app.route('/health')
def health_check():
    """Health check endpoint."""
    if service and service.consumer.is_running:
        return jsonify({
            'status': 'healthy',
            'service': 'mongo-ingestor-async',
            'uptime': time.time() - service.metrics.start_time,
            'queue_depth': service.consumer.event_queue.qsize()
        }), 200
    else:
        return jsonify({
            'status': 'unhealthy',
            'service': 'mongo-ingestor-async',
            'error': 'Service not running'
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
                asyncio.get_event_loop()
            )
            return jsonify({'status': 'Message queued for publishing'}), 200
        else:
            return jsonify({'error': 'Service not ready'}), 503
            
    except Exception as e:
        logger.error(f"Test publish error: {e}")
        return jsonify({'error': str(e)}), 500

# Signal handlers
def signal_handler(signum, frame):
    """Handle termination signals."""
    logger.info(f"Received signal {signum}")
    if service:
        asyncio.create_task(service.stop())

# Main async function
async def main():
    """Main async entry point."""
    global service
    
    # Create service
    service = ChangeStreamIngestionService()
    
    # Setup signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start service
    await service.start()
    
    # Keep running until shutdown
    await service.shutdown_event.wait()

# Flask app runner in separate thread
def run_flask():
    """Run Flask app in thread."""
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False)

if __name__ == '__main__':
    import threading
    
    # Start Flask in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Run async main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        logger.info("Service terminated")
