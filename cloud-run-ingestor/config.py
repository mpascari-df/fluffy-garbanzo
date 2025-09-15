#!/usr/bin/env python3
# config.py - Configuration management for async Cloud Run Ingestor
# Production version with comprehensive logging and validation

import os
import logging
from typing import List, Optional

# Setup logger
logger = logging.getLogger(__name__)

class Config:
    """Configuration class with all settings for the ingestion service."""
    
    def __init__(self):
        logger.info("="*50)
        logger.info("📋 Loading Configuration")
        logger.info("="*50)
        
        # === GCP Configuration ===
        self.PROJECT_ID = os.getenv('PROJECT_ID')
        self.REGION = os.getenv('REGION', 'europe-west1')
        logger.info(f"  🌍 GCP Project: {self.PROJECT_ID}")
        logger.info(f"  🌍 Region: {self.REGION}")
        
        # === MongoDB Configuration ===
        self.MONGO_URI = os.getenv('MONGO_URI')
        self.MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
        
        # Log MongoDB config (safely)
        if self.MONGO_URI:
            # Extract and log connection info safely
            if '@' in self.MONGO_URI:
                # Hide credentials
                uri_parts = self.MONGO_URI.split('@')
                if len(uri_parts) > 1:
                    safe_uri = f"mongodb://***:***@{uri_parts[1][:30]}..."
                else:
                    safe_uri = self.MONGO_URI[:30] + "..."
            else:
                safe_uri = self.MONGO_URI[:50] + "..." if len(self.MONGO_URI) > 50 else self.MONGO_URI
            logger.info(f"  🍃 MongoDB URI: {safe_uri}")
        logger.info(f"  🍃 MongoDB Database: {self.MONGO_DB_NAME}")
        
        # Change stream settings
        self.CHANGE_STREAM_BATCH_SIZE = int(os.getenv('CHANGE_STREAM_BATCH_SIZE', '100'))
        self.OPLOG_WINDOW_HOURS = int(os.getenv('OPLOG_WINDOW_HOURS', '24'))
        logger.info(f"  🍃 Change Stream Batch Size: {self.CHANGE_STREAM_BATCH_SIZE}")
        logger.info(f"  🍃 Oplog Window: {self.OPLOG_WINDOW_HOURS} hours")
        
        # === Pub/Sub Configuration ===
        self.PUBSUB_TOPIC_NAME = os.getenv('PUBSUB_TOPIC_NAME')
        self.PUBLISHER_DLQ_TOPIC_NAME = os.getenv('PUBLISHER_DLQ_TOPIC_NAME')
        logger.info(f"  📬 Pub/Sub Topic: {self.PUBSUB_TOPIC_NAME}")
        logger.info(f"  📬 DLQ Topic: {self.PUBLISHER_DLQ_TOPIC_NAME or 'Not configured'}")
        
        # Publishing settings
        self.PUBLISHER_WORKERS = int(os.getenv('PUBLISHER_WORKERS', '10'))
        self.MAX_CONCURRENT_PUBLISHES = int(os.getenv('MAX_CONCURRENT_PUBLISHES', '20'))
        self.PUBLISH_TIMEOUT = float(os.getenv('PUBLISH_TIMEOUT', '5.0'))
        self.PUBLISH_RETRY_ATTEMPTS = int(os.getenv('PUBLISH_RETRY_ATTEMPTS', '3'))
        logger.info(f"  📬 Publisher Workers: {self.PUBLISHER_WORKERS}")
        logger.info(f"  📬 Max Concurrent Publishes: {self.MAX_CONCURRENT_PUBLISHES}")
        logger.info(f"  📬 Publish Timeout: {self.PUBLISH_TIMEOUT}s")
        logger.info(f"  📬 Retry Attempts: {self.PUBLISH_RETRY_ATTEMPTS}")
        
        # === Queue Configuration ===
        self.QUEUE_MAX_SIZE = int(os.getenv('QUEUE_MAX_SIZE', '10000'))
        self.QUEUE_PRESSURE_THRESHOLD = int(os.getenv('QUEUE_PRESSURE_THRESHOLD', '8000'))
        logger.info(f"  📦 Queue Max Size: {self.QUEUE_MAX_SIZE}")
        logger.info(f"  📦 Queue Pressure Threshold: {self.QUEUE_PRESSURE_THRESHOLD}")
        
        # === Resume Token Configuration ===
        # Firestore collection for token storage
        self.TOKEN_COLLECTION = os.getenv('TOKEN_COLLECTION', 'resume_tokens')
        self.TOKEN_DOCUMENT_ID = os.getenv('TOKEN_DOCUMENT_ID', 'mongo_change_stream')
        
        # Checkpoint settings
        self.TOKEN_CHECKPOINT_EVENTS = int(os.getenv('TOKEN_CHECKPOINT_EVENTS', '1000'))
        self.TOKEN_CHECKPOINT_SECONDS = int(os.getenv('TOKEN_CHECKPOINT_SECONDS', '30'))
        logger.info(f"  💾 Token Collection: {self.TOKEN_COLLECTION}")
        logger.info(f"  💾 Checkpoint Every: {self.TOKEN_CHECKPOINT_EVENTS} events or {self.TOKEN_CHECKPOINT_SECONDS}s")
        
        # Resume strategy settings
        self.RESUME_BUFFER_MINUTES = int(os.getenv('RESUME_BUFFER_MINUTES', '5'))
        self.SAFE_START_HOURS = int(os.getenv('SAFE_START_HOURS', '2'))
        logger.info(f"  💾 Resume Buffer: {self.RESUME_BUFFER_MINUTES} minutes")
        logger.info(f"  💾 Safe Start Window: {self.SAFE_START_HOURS} hours")
        
        # === Circuit Breaker Configuration ===
        self.CIRCUIT_BREAKER_THRESHOLD = int(os.getenv('CIRCUIT_BREAKER_THRESHOLD', '100'))
        self.CIRCUIT_BREAKER_WINDOW_SECONDS = int(os.getenv('CIRCUIT_BREAKER_WINDOW_SECONDS', '300'))
        logger.info(f"  ⚡ Circuit Breaker Threshold: {self.CIRCUIT_BREAKER_THRESHOLD} errors")
        logger.info(f"  ⚡ Circuit Breaker Window: {self.CIRCUIT_BREAKER_WINDOW_SECONDS}s")
        
        # === Monitoring Configuration ===
        self.METRICS_REPORTING_INTERVAL = int(os.getenv('METRICS_REPORTING_INTERVAL', '60'))
        self.METRICS_EXPORT_INTERVAL = int(os.getenv('METRICS_EXPORT_INTERVAL', '60'))
        logger.info(f"  📊 Metrics Reporting Interval: {self.METRICS_REPORTING_INTERVAL}s")
        
        # Collections to log (comma-separated)
        collections_to_log = os.getenv('COLLECTIONS_TO_LOG', '')
        self.COLLECTIONS_TO_LOG = [c.strip() for c in collections_to_log.split(',') if c.strip()]
        if self.COLLECTIONS_TO_LOG:
            logger.info(f"  📊 Collections to Log: {', '.join(self.COLLECTIONS_TO_LOG)}")
        else:
            logger.info(f"  📊 Collections to Log: All collections")
        
        # === Performance Tuning ===
        self.ASYNC_POOL_SIZE = int(os.getenv('ASYNC_POOL_SIZE', '100'))
        self.CONNECTION_POOL_SIZE = int(os.getenv('CONNECTION_POOL_SIZE', '10'))
        logger.info(f"  ⚙️ Async Pool Size: {self.ASYNC_POOL_SIZE}")
        logger.info(f"  ⚙️ Connection Pool Size: {self.CONNECTION_POOL_SIZE}")
        
        # === Graceful Shutdown ===
        self.SHUTDOWN_GRACE_PERIOD = int(os.getenv('SHUTDOWN_GRACE_PERIOD', '30'))
        logger.info(f"  ⏱️ Shutdown Grace Period: {self.SHUTDOWN_GRACE_PERIOD}s")
        
        # === Feature Flags ===
        self.ENABLE_FIRESTORE = os.getenv('ENABLE_FIRESTORE', 'false').lower() == 'true'
        self.ENABLE_METRICS_EXPORT = os.getenv('ENABLE_METRICS_EXPORT', 'true').lower() == 'true'
        self.ENABLE_DETAILED_LOGGING = os.getenv('ENABLE_DETAILED_LOGGING', 'false').lower() == 'true'
        self.ENABLE_COLLECTION_STATS = os.getenv('ENABLE_COLLECTION_STATS', 'true').lower() == 'true'
        
        logger.info("  🎛️ Feature Flags:")
        logger.info(f"    - Firestore: {'✅ Enabled' if self.ENABLE_FIRESTORE else '❌ Disabled'}")
        logger.info(f"    - Metrics Export: {'✅ Enabled' if self.ENABLE_METRICS_EXPORT else '❌ Disabled'}")
        logger.info(f"    - Detailed Logging: {'✅ Enabled' if self.ENABLE_DETAILED_LOGGING else '❌ Disabled'}")
        logger.info(f"    - Collection Stats: {'✅ Enabled' if self.ENABLE_COLLECTION_STATS else '❌ Disabled'}")
        
        # === Environment Detection ===
        self.ENVIRONMENT = os.getenv('ENVIRONMENT', 'production')
        self.DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'
        if self.DEBUG_MODE:
            logger.warning("  ⚠️ DEBUG MODE ENABLED - Extra verbosity active")
        
        # === Validation ===
        logger.info("="*50)
        logger.info("🔍 Validating Configuration")
        try:
            self._validate_config()
            logger.info("✅ Configuration validation passed")
        except ValueError as e:
            logger.error(f"❌ Configuration validation failed: {e}")
            raise
        
        # Log configuration summary
        logger.info("="*50)
        logger.info("✅ Configuration loaded successfully")
        logger.info(f"📊 Summary: {self._get_summary_string()}")
        logger.info("="*50)
    
    def _validate_config(self):
        """Validate required configuration with detailed error reporting."""
        required_vars = [
            ('PROJECT_ID', self.PROJECT_ID, 'GCP Project ID'),
            ('MONGO_URI', self.MONGO_URI, 'MongoDB connection URI'),
            ('MONGO_DB_NAME', self.MONGO_DB_NAME, 'MongoDB database name'),
            ('PUBSUB_TOPIC_NAME', self.PUBSUB_TOPIC_NAME, 'Pub/Sub topic for publishing')
        ]
        
        missing = []
        for var_name, var_value, description in required_vars:
            if not var_value:
                missing.append(f"{var_name} ({description})")
                logger.error(f"  ❌ Missing: {var_name} - {description}")
        
        if missing:
            error_msg = f"Missing required environment variables: {', '.join([m.split(' (')[0] for m in missing])}"
            raise ValueError(error_msg)
        
        # Validate numeric ranges
        validations = [
            (self.PUBLISHER_WORKERS > 0, "PUBLISHER_WORKERS must be > 0"),
            (self.QUEUE_MAX_SIZE > 0, "QUEUE_MAX_SIZE must be > 0"),
            (self.QUEUE_PRESSURE_THRESHOLD < self.QUEUE_MAX_SIZE, 
             "QUEUE_PRESSURE_THRESHOLD must be < QUEUE_MAX_SIZE"),
            (self.PUBLISH_TIMEOUT > 0, "PUBLISH_TIMEOUT must be > 0"),
            (self.TOKEN_CHECKPOINT_EVENTS > 0, "TOKEN_CHECKPOINT_EVENTS must be > 0"),
            (self.TOKEN_CHECKPOINT_SECONDS > 0, "TOKEN_CHECKPOINT_SECONDS must be > 0"),
        ]
        
        for condition, error_message in validations:
            if not condition:
                logger.error(f"  ❌ Validation failed: {error_message}")
                raise ValueError(error_message)
        
        # Warnings for suboptimal configurations
        if self.PUBLISHER_WORKERS > 50:
            logger.warning(f"  ⚠️ High number of publisher workers ({self.PUBLISHER_WORKERS}) may cause resource issues")
        
        if self.QUEUE_MAX_SIZE > 100000:
            logger.warning(f"  ⚠️ Very large queue size ({self.QUEUE_MAX_SIZE}) may cause memory issues")
        
        if not self.PUBLISHER_DLQ_TOPIC_NAME:
            logger.warning("  ⚠️ No DLQ configured - failed messages will be lost")
        
        if not self.ENABLE_FIRESTORE:
            logger.warning("  ⚠️ Firestore disabled - resume tokens will not persist across restarts")
        
        if self.SHUTDOWN_GRACE_PERIOD < 10:
            logger.warning(f"  ⚠️ Short shutdown grace period ({self.SHUTDOWN_GRACE_PERIOD}s) may cause data loss")
    
    def _get_summary_string(self) -> str:
        """Get a one-line configuration summary."""
        return (
            f"Workers={self.PUBLISHER_WORKERS}, "
            f"Queue={self.QUEUE_MAX_SIZE}, "
            f"Firestore={self.ENABLE_FIRESTORE}, "
            f"Metrics={self.ENABLE_METRICS_EXPORT}"
        )
    
    def get_summary(self) -> dict:
        """Get configuration summary for logging and monitoring."""
        return {
            'environment': self.ENVIRONMENT,
            'project_id': self.PROJECT_ID,
            'mongo_db': self.MONGO_DB_NAME,
            'mongo_uri_safe': self.MONGO_URI[:30] + '...' if self.MONGO_URI and len(self.MONGO_URI) > 30 else 'Not set',
            'pubsub_topic': self.PUBSUB_TOPIC_NAME,
            'dlq_topic': self.PUBLISHER_DLQ_TOPIC_NAME,
            'publisher_workers': self.PUBLISHER_WORKERS,
            'max_concurrent_publishes': self.MAX_CONCURRENT_PUBLISHES,
            'queue_max_size': self.QUEUE_MAX_SIZE,
            'queue_pressure_threshold': self.QUEUE_PRESSURE_THRESHOLD,
            'checkpoint_events': self.TOKEN_CHECKPOINT_EVENTS,
            'checkpoint_seconds': self.TOKEN_CHECKPOINT_SECONDS,
            'oplog_window_hours': self.OPLOG_WINDOW_HOURS,
            'resume_buffer_minutes': self.RESUME_BUFFER_MINUTES,
            'safe_start_hours': self.SAFE_START_HOURS,
            'circuit_breaker_threshold': self.CIRCUIT_BREAKER_THRESHOLD,
            'circuit_breaker_window': self.CIRCUIT_BREAKER_WINDOW_SECONDS,
            'shutdown_grace_period': self.SHUTDOWN_GRACE_PERIOD,
            'features': {
                'firestore_enabled': self.ENABLE_FIRESTORE,
                'metrics_enabled': self.ENABLE_METRICS_EXPORT,
                'detailed_logging': self.ENABLE_DETAILED_LOGGING,
                'collection_stats': self.ENABLE_COLLECTION_STATS
            },
            'collections_to_log': self.COLLECTIONS_TO_LOG if self.COLLECTIONS_TO_LOG else 'all',
            'debug_mode': self.DEBUG_MODE
        }
    
    def log_config_change(self, key: str, old_value: any, new_value: any):
        """Log configuration changes at runtime."""
        logger.warning(f"⚙️ Configuration changed: {key} from '{old_value}' to '{new_value}'")
    
    def reload_from_env(self):
        """Reload configuration from environment variables (for hot reload)."""
        logger.info("🔄 Reloading configuration from environment...")
        # Store old values for comparison
        old_summary = self.get_summary()
        
        # Reload by creating new instance
        self.__init__()
        
        # Log changes
        new_summary = self.get_summary()
        for key in old_summary:
            if old_summary[key] != new_summary.get(key):
                logger.info(f"  Changed: {key} = {new_summary.get(key)}")