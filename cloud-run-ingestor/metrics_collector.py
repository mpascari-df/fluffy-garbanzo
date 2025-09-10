# metrics_collector.py - Comprehensive metrics collection and monitoring
# Provides observability for the ingestion pipeline

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from collections import defaultdict, deque
from google.cloud import monitoring_v3
from google.api_core import exceptions

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collects and exports metrics for monitoring the ingestion pipeline."""
    
    def __init__(self, config):
        self.config = config
        self.start_time = time.time()
        
        # Cloud Monitoring client
        self.monitoring_client = None
        self.project_name = None
        
        # Metrics storage
        self.metrics = defaultdict(lambda: 0)
        self.gauges = {}
        self.histograms = defaultdict(list)
        self.rates = defaultdict(lambda: deque(maxlen=100))
        
        # Collection-specific metrics
        self.collection_metrics = defaultdict(lambda: defaultdict(lambda: 0))
        
        # Error tracking
        self.error_counts = defaultdict(lambda: 0)
        self.consecutive_errors = 0
        self.last_error_time = None
        
        # Performance metrics
        self.publish_latencies = deque(maxlen=1000)
        self.processing_rates = deque(maxlen=60)  # Last 60 measurements
        
        # Counters
        self.total_events_processed = 0
        self.total_events_published = 0
        self.total_events_failed = 0
        self.total_dlq_messages = 0
        
        # Rate calculation
        self.last_rate_calc_time = time.time()
        self.last_event_count = 0
        
        # Oplog lag tracking
        self.last_oplog_timestamp = None
        self.estimated_lag_seconds = 0
    
    async def initialize(self):
        """Initialize Cloud Monitoring client."""
        if not self.config.ENABLE_METRICS_EXPORT:
            logger.info("Metrics export disabled")
            return
        
        try:
            self.monitoring_client = monitoring_v3.MetricServiceClient()
            self.project_name = f"projects/{self.config.PROJECT_ID}"
            
            # Test connection
            self.monitoring_client.list_monitored_resource_descriptors(
                name=self.project_name
            )
            
            logger.info("Cloud Monitoring client initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cloud Monitoring: {e}")
            logger.warning("Metrics will be collected locally only")
            self.monitoring_client = None
    
    def record_event_processed(self, collection: str, operation: str):
        """Record a processed event."""
        self.total_events_processed += 1
        self.metrics['events_processed'] += 1
        
        # Collection-specific
        self.collection_metrics[collection]['total'] += 1
        self.collection_metrics[collection][operation] += 1
        
        # Operation-specific
        self.metrics[f'operation_{operation}'] += 1
        
        # Reset consecutive errors on success
        self.consecutive_errors = 0
    
    def record_publish_success(self, collection: str, latency: float):
        """Record successful publish."""
        self.total_events_published += 1
        self.metrics['publish_success'] += 1
        self.collection_metrics[collection]['published'] += 1
        
        # Track latency
        self.publish_latencies.append(latency)
        self.histograms['publish_latency'].append(latency)
    
    def record_publish_failure(self, collection: str):
        """Record failed publish."""
        self.total_events_failed += 1
        self.metrics['publish_failure'] += 1
        self.collection_metrics[collection]['failed'] += 1
    
    def record_dlq_message(self, collection: str):
        """Record DLQ message."""
        self.total_dlq_messages += 1
        self.metrics['dlq_messages'] += 1
        self.collection_metrics[collection]['dlq'] += 1
    
    def record_error(self, error_type: str, error_message: str):
        """Record an error."""
        self.error_counts[error_type] += 1
        self.consecutive_errors += 1
        self.last_error_time = time.time()
        
        # Log if critical
        if self.consecutive_errors > 10:
            logger.critical(f"High consecutive error count: {self.consecutive_errors}")
    
    def record_backpressure(self):
        """Record backpressure event."""
        self.metrics['backpressure_events'] += 1
    
    def record_connection_established(self):
        """Record successful MongoDB connection."""
        self.metrics['connections_established'] += 1
        self.consecutive_errors = 0
    
    def update_oplog_lag(self, oplog_timestamp: Optional[datetime]):
        """Update estimated oplog lag."""
        if oplog_timestamp:
            self.last_oplog_timestamp = oplog_timestamp
            current_time = datetime.now(timezone.utc)
            
            # Handle both datetime and timestamp
            if hasattr(oplog_timestamp, 'as_datetime'):
                oplog_time = oplog_timestamp.as_datetime()
            else:
                oplog_time = oplog_timestamp
            
            self.estimated_lag_seconds = (current_time - oplog_time).total_seconds()
            self.gauges['oplog_lag_seconds'] = self.estimated_lag_seconds
    
    def calculate_rates(self):
        """Calculate processing rates."""
        current_time = time.time()
        time_delta = current_time - self.last_rate_calc_time
        
        if time_delta > 0:
            events_delta = self.total_events_processed - self.last_event_count
            rate = events_delta / time_delta
            
            self.processing_rates.append(rate)
            self.gauges['events_per_second'] = rate
            
            self.last_rate_calc_time = current_time
            self.last_event_count = self.total_events_processed
    
    def get_success_rate(self) -> float:
        """Calculate publish success rate."""
        total_attempts = self.total_events_published + self.total_events_failed
        if total_attempts == 0:
            return 1.0
        return self.total_events_published / total_attempts
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics as dictionary."""
        self.calculate_rates()
        
        # Calculate percentiles for latency
        latencies = list(self.publish_latencies)
        p50 = p95 = p99 = 0
        if latencies:
            latencies.sort()
            p50 = latencies[int(len(latencies) * 0.5)]
            p95 = latencies[int(len(latencies) * 0.95)]
            p99 = latencies[int(len(latencies) * 0.99)]
        
        return {
            'summary': {
                'uptime_seconds': time.time() - self.start_time,
                'total_events_processed': self.total_events_processed,
                'total_events_published': self.total_events_published,
                'total_events_failed': self.total_events_failed,
                'total_dlq_messages': self.total_dlq_messages,
                'success_rate': self.get_success_rate(),
                'consecutive_errors': self.consecutive_errors
            },
            'rates': {
                'events_per_second': self.gauges.get('events_per_second', 0),
                'avg_processing_rate': sum(self.processing_rates) / len(self.processing_rates) if self.processing_rates else 0
            },
            'latency': {
                'publish_p50_ms': p50 * 1000,
                'publish_p95_ms': p95 * 1000,
                'publish_p99_ms': p99 * 1000
            },
            'oplog': {
                'lag_seconds': self.estimated_lag_seconds,
                'last_timestamp': self.last_oplog_timestamp.isoformat() if self.last_oplog_timestamp else None
            },
            'collections': dict(self.collection_metrics),
            'errors': dict(self.error_counts),
            'gauges': dict(self.gauges)
        }
    
    async def export_metrics(self):
        """Export metrics to Cloud Monitoring."""
        if not self.monitoring_client:
            return
        
        try:
            series = []
            now = time.time()
            
            # Create time series for key metrics
            metrics_to_export = [
                ('events_processed', 'custom.googleapis.com/mongo_ingestor/events_processed', 'GAUGE'),
                ('events_per_second', 'custom.googleapis.com/mongo_ingestor/events_per_second', 'GAUGE'),
                ('publish_success_rate', 'custom.googleapis.com/mongo_ingestor/success_rate', 'GAUGE'),
                ('oplog_lag_seconds', 'custom.googleapis.com/mongo_ingestor/oplog_lag', 'GAUGE'),
                ('queue_depth', 'custom.googleapis.com/mongo_ingestor/queue_depth', 'GAUGE'),
                ('dlq_messages', 'custom.googleapis.com/mongo_ingestor/dlq_messages', 'CUMULATIVE')
            ]
            
            for metric_name, metric_type, kind in metrics_to_export:
                value = None
                
                if metric_name == 'publish_success_rate':
                    value = self.get_success_rate()
                elif metric_name == 'events_per_second':
                    value = self.gauges.get('events_per_second', 0)
                elif metric_name == 'oplog_lag_seconds':
                    value = self.estimated_lag_seconds
                elif metric_name == 'queue_depth':
                    value = self.gauges.get('queue_depth', 0)
                elif metric_name == 'dlq_messages':
                    value = self.total_dlq_messages
                elif metric_name == 'events_processed':
                    value = self.total_events_processed
                
                if value is not None:
                    series.append(self._create_time_series(
                        metric_type, value, kind
                    ))
            
            # Write time series
            if series:
                self.monitoring_client.create_time_series(
                    name=self.project_name,
                    time_series=series
                )
                logger.debug(f"Exported {len(series)} metrics to Cloud Monitoring")
                
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
    
    def _create_time_series(self, metric_type: str, value: float, kind: str):
        """Create a time series for Cloud Monitoring."""
        series = monitoring_v3.TimeSeries()
        series.metric.type = metric_type
        
        # Add labels
        series.metric.labels['service'] = 'mongo-ingestor'
        series.metric.labels['database'] = self.config.MONGO_DB_NAME
        
        # Set resource
        series.resource.type = 'global'
        
        # Add point
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": seconds, "nanos": nanos}}
        )
        
        if kind == 'CUMULATIVE':
            interval.start_time.seconds = int(self.start_time)
        
        point = monitoring_v3.Point({
            "interval": interval,
            "value": {"double_value": value}
        })
        
        series.points = [point]
        return series
    
    async def start_reporting(self):
        """Start periodic metrics reporting."""
        while True:
            try:
                await asyncio.sleep(self.config.METRICS_REPORTING_INTERVAL)
                
                # Calculate rates
                self.calculate_rates()
                
                # Export to Cloud Monitoring
                if self.config.ENABLE_METRICS_EXPORT:
                    await self.export_metrics()
                
                # Log summary
                if self.config.ENABLE_DETAILED_LOGGING:
                    metrics = self.get_metrics()
                    logger.info(f"Metrics summary: {metrics['summary']}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics reporting: {e}")
    
    async def stop(self):
        """Stop metrics collection and do final export."""
        try:
            # Final metrics export
            if self.config.ENABLE_METRICS_EXPORT:
                await self.export_metrics()
            
            # Log final summary
            metrics = self.get_metrics()
            logger.info(f"Final metrics summary: {metrics['summary']}")
            
        except Exception as e:
            logger.error(f"Error during metrics shutdown: {e}")
