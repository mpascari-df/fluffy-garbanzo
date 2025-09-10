# config.py - Configuration management for async Cloud Run Ingestor
# Centralizes all configuration with environment variables and defaults

import os
from typing import List, Optional

class Config:
    """Configuration class with all settings for the ingestion service."""
    
    def __init__(self):
        # === GCP Configuration ===
        self.PROJECT_ID = os.getenv('PROJECT_ID')
        self.REGION = os.getenv('REGION', 'europe-west1')
        
        # === MongoDB Configuration ===
        self.MONGO_URI = os.getenv('MONGO_URI')
        self.MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
        
        # Change stream settings
        self.CHANGE_STREAM_BATCH_SIZE = int(os.getenv('CHANGE_STREAM_BATCH_SIZE', '100'))
        self.OPLOG_WINDOW_HOURS = int(os.getenv('OPLOG_WINDOW_HOURS', '24')) 
        
        # === Pub/Sub Configuration ===
        self.PUBSUB_TOPIC_NAME = os.getenv('PUBSUB_TOPIC_NAME')
        self.PUBLISHER_DLQ_TOPIC_NAME = os.getenv('PUBLISHER_DLQ_TOPIC_NAME')
        
        # Publishing settings
        self.PUBLISHER_WORKERS = int(os.getenv('PUBLISHER_WORKERS', '10'))
        self.MAX_CONCURRENT_PUBLISHES = int(os.getenv('MAX_CONCURRENT_PUBLISHES', '20'))
        self.PUBLISH_TIMEOUT = float(os.getenv('PUBLISH_TIMEOUT', '5.0'))
        self.PUBLISH_RETRY_ATTEMPTS = int(os.getenv('PUBLISH_RETRY_ATTEMPTS', '3'))
        
        # === Queue Configuration ===
        self.QUEUE_MAX_SIZE = int(os.getenv('QUEUE_MAX_SIZE', '10000'))
        self.QUEUE_PRESSURE_THRESHOLD = int(os.getenv('QUEUE_PRESSURE_THRESHOLD', '8000'))
        
        # === Resume Token Configuration ===
        # Firestore collection for token storage
        self.TOKEN_COLLECTION = os.getenv('TOKEN_COLLECTION', 'resume_tokens')
        self.TOKEN_DOCUMENT_ID = os.getenv('TOKEN_DOCUMENT_ID', 'mongo_change_stream')
        
        # Checkpoint settings
        self.TOKEN_CHECKPOINT_EVENTS = int(os.getenv('TOKEN_CHECKPOINT_EVENTS', '1000'))
        self.TOKEN_CHECKPOINT_SECONDS = int(os.getenv('TOKEN_CHECKPOINT_SECONDS', '30'))
        
        # Resume strategy settings
        self.RESUME_BUFFER_MINUTES = int(os.getenv('RESUME_BUFFER_MINUTES', '5'))
        self.SAFE_START_HOURS = int(os.getenv('SAFE_START_HOURS', '2'))
        
        # === Circuit Breaker Configuration ===
        self.CIRCUIT_BREAKER_THRESHOLD = int(os.getenv('CIRCUIT_BREAKER_THRESHOLD', '100'))
        self.CIRCUIT_BREAKER_WINDOW_SECONDS = int(os.getenv('CIRCUIT_BREAKER_WINDOW_SECONDS', '300'))
        
        # === Monitoring Configuration ===
        self.METRICS_REPORTING_INTERVAL = int(os.getenv('METRICS_REPORTING_INTERVAL', '60'))
        self.METRICS_EXPORT_INTERVAL = int(os.getenv('METRICS_EXPORT_INTERVAL', '60'))
        
        # Collections to log (comma-separated)
        collections_to_log = os.getenv('COLLECTIONS_TO_LOG', '')
        self.COLLECTIONS_TO_LOG = [c.strip() for c in collections_to_log.split(',') if c.strip()]
        
        # === Performance Tuning ===
        self.ASYNC_POOL_SIZE = int(os.getenv('ASYNC_POOL_SIZE', '100'))
        self.CONNECTION_POOL_SIZE = int(os.getenv('CONNECTION_POOL_SIZE', '10'))
        
        # === Graceful Shutdown ===
        self.SHUTDOWN_GRACE_PERIOD = int(os.getenv('SHUTDOWN_GRACE_PERIOD', '30'))
        
        # === Feature Flags ===
        self.ENABLE_METRICS_EXPORT = os.getenv('ENABLE_METRICS_EXPORT', 'true').lower() == 'true'
        self.ENABLE_DETAILED_LOGGING = os.getenv('ENABLE_DETAILED_LOGGING', 'false').lower() == 'true'
        self.ENABLE_COLLECTION_STATS = os.getenv('ENABLE_COLLECTION_STATS', 'true').lower() == 'true'
        
        # === Validation ===
        self._validate_config()
    
    def _validate_config(self):
        """Validate required configuration."""
        required_vars = [
            'PROJECT_ID',
            'MONGO_URI',
            'MONGO_DB_NAME',
            'PUBSUB_TOPIC_NAME'
        ]
        
        missing = []
        for var in required_vars:
            if not getattr(self, var):
                missing.append(var)
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def get_summary(self) -> dict:
        """Get configuration summary for logging."""
        return {
            'project_id': self.PROJECT_ID,
            'mongo_db': self.MONGO_DB_NAME,
            'pubsub_topic': self.PUBSUB_TOPIC_NAME,
            'dlq_topic': self.PUBLISHER_DLQ_TOPIC_NAME,
            'publisher_workers': self.PUBLISHER_WORKERS,
            'queue_max_size': self.QUEUE_MAX_SIZE,
            'checkpoint_events': self.TOKEN_CHECKPOINT_EVENTS,
            'checkpoint_seconds': self.TOKEN_CHECKPOINT_SECONDS,
            'oplog_window_hours': self.OPLOG_WINDOW_HOURS,
            'circuit_breaker_threshold': self.CIRCUIT_BREAKER_THRESHOLD,
            'metrics_enabled': self.ENABLE_METRICS_EXPORT
        }
