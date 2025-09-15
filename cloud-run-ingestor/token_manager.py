# token_manager.py - Resume token persistence manager using Firestore
# Provides reliable token storage with fallback strategies

import asyncio
import logging
import time
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient
import json
from bson import json_util

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logger = logging.getLogger(__name__)

class TokenManager:
    """Manages resume token persistence in Firestore for change stream recovery."""
    
    def __init__(self, config):
        logger.info("="*50)
        logger.info("📋 Initializing TokenManager")
        logger.info(f"  Collection: {config.TOKEN_COLLECTION}")
        logger.info(f"  Document ID: {config.TOKEN_DOCUMENT_ID}")
        logger.info(f"  Checkpoint Events: {config.TOKEN_CHECKPOINT_EVENTS}")
        logger.info(f"  Checkpoint Seconds: {config.TOKEN_CHECKPOINT_SECONDS}")
        logger.info(f"  Resume Buffer: {config.RESUME_BUFFER_MINUTES} minutes")
        logger.info(f"  Firestore Enabled: {'✅ Yes' if config.ENABLE_FIRESTORE else '❌ No'}")
        logger.info("="*50)
        
        self.config = config
        self.db: Optional[AsyncClient] = None
        self.collection = None
        self.document_id = config.TOKEN_DOCUMENT_ID
        
        # Cache
        self.last_saved_token = None
        self.last_checkpoint_time = None
        self.save_in_progress = False
        
        # Statistics
        self.total_saves = 0
        self.failed_saves = 0
        self.consecutive_failures = 0
        self.last_save_duration = 0
        self.total_save_time = 0
        
        # Performance tracking
        self.save_latencies = []
        self.max_latency = 0
        
        logger.info("✅ TokenManager instance created")
    
    async def initialize(self):
        """Initialize Firestore connection."""
        logger.info("🔗 Initializing Firestore connection...")
        
        if not self.config.ENABLE_FIRESTORE:
            logger.warning("⚠️ Firestore disabled by configuration - token persistence unavailable")
            logger.warning("  Will rely on timestamp-based recovery only")
            self.db = None
            return
        
        start_time = time.time()
        
        try:
            # Use async Firestore client
            logger.info(f"  Project: {self.config.PROJECT_ID}")
            logger.info(f"  Creating async Firestore client...")
            
            self.db = firestore.AsyncClient(project=self.config.PROJECT_ID)
            self.collection = self.db.collection(self.config.TOKEN_COLLECTION)
            
            logger.info(f"  Testing connection to collection: {self.config.TOKEN_COLLECTION}")
            
            # Test connection by attempting to read
            doc = await self.collection.document(self.document_id).get()
            
            connection_time = (time.time() - start_time) * 1000
            
            if doc.exists:
                token_data = doc.to_dict()
                token_age = 'unknown'
                
                if token_data and 'timestamp' in token_data:
                    age_delta = datetime.now(timezone.utc) - token_data['timestamp']
                    token_age = f"{age_delta.total_seconds() / 3600:.1f} hours"
                
                logger.info(f"✅ Firestore connected in {connection_time:.1f}ms")
                logger.info(f"📌 Existing token found:")
                logger.info(f"    Document ID: {doc.id}")
                logger.info(f"    Token age: {token_age}")
                logger.info(f"    Save count: {token_data.get('save_count', 'unknown')}")
                logger.info(f"    Total events: {token_data.get('total_events', 'unknown')}")
            else:
                logger.info(f"✅ Firestore connected in {connection_time:.1f}ms")
                logger.info("📌 No existing token found - will create new checkpoint on first save")
            
            # Reset failure counter on successful connection
            self.consecutive_failures = 0
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize Firestore: {e}", exc_info=True)
            logger.warning("⚠️ Token persistence disabled - will rely on timestamp recovery")
            logger.warning(f"  Error details: {str(e)}")
            self.db = None
            self.consecutive_failures += 1
    
    async def save_resume_token(self, token: Any, events_processed: int = 0) -> bool:
        """
        Save resume token to Firestore with comprehensive logging.
        
        Args:
            token: The MongoDB resume token
            events_processed: Number of events processed since last save
            
        Returns:
            bool: Success status
        """
        if not self.db:
            if self.total_saves == 0:  # Only log once
                logger.debug("💾 Token save skipped - Firestore not available")
            return False
        
        if self.save_in_progress:
            logger.warning("⚠️ Token save already in progress, skipping concurrent save")
            return False
        
        self.save_in_progress = True
        start_time = time.time()
        
        try:
            # Serialize token for size calculation
            serialized_token = self._serialize_token(token)
            token_size = len(serialized_token)
            
            # Log save initiation periodically
            if self.total_saves % 10 == 0 or self.config.ENABLE_DETAILED_LOGGING:
                logger.debug(f"💾 Saving resume token (Save #{self.total_saves + 1})")
                logger.debug(f"  Events since checkpoint: {events_processed}")
                logger.debug(f"  Token size: {token_size} bytes")
            
            # Prepare token data
            token_data = {
                'token': serialized_token,
                'timestamp': datetime.now(timezone.utc),
                'events_since_checkpoint': events_processed,
                'total_events': self.total_saves * self.config.TOKEN_CHECKPOINT_EVENTS + events_processed,
                'save_count': self.total_saves + 1,
                'last_error': None,
                'consecutive_failures': 0,
                'token_size_bytes': token_size,
                'save_duration_ms': None,  # Will be updated after save
                'metadata': {
                    'service_version': '2.0.0',
                    'oplog_window_hours': self.config.OPLOG_WINDOW_HOURS,
                    'project_id': self.config.PROJECT_ID,
                    'database': self.config.MONGO_DB_NAME,
                    'checkpoint_config': {
                        'events': self.config.TOKEN_CHECKPOINT_EVENTS,
                        'seconds': self.config.TOKEN_CHECKPOINT_SECONDS
                    }
                }
            }
            
            # Save to Firestore with merge to preserve other fields
            await self.collection.document(self.document_id).set(
                token_data,
                merge=True
            )
            
            # Calculate save duration
            save_duration = (time.time() - start_time) * 1000
            self.last_save_duration = save_duration
            self.total_save_time += save_duration
            self.save_latencies.append(save_duration)
            
            # Track max latency
            if save_duration > self.max_latency:
                self.max_latency = save_duration
            
            # Update save duration in Firestore if it was slow
            if save_duration > 1000:  # More than 1 second
                logger.warning(f"⚠️ Slow token save detected: {save_duration:.1f}ms")
                await self.collection.document(self.document_id).update({
                    'save_duration_ms': save_duration,
                    'slow_save_warning': True
                })
            
            # Update cache
            self.last_saved_token = token
            self.last_checkpoint_time = token_data['timestamp']
            self.total_saves += 1
            self.consecutive_failures = 0
            
            # Log success with metrics
            if self.total_saves % 10 == 0:  # Every 10 saves
                avg_latency = sum(self.save_latencies[-10:]) / min(10, len(self.save_latencies))
                logger.info(
                    f"📊 Token checkpoint stats - "
                    f"Saves: {self.total_saves}, "
                    f"Failed: {self.failed_saves}, "
                    f"Success rate: {(self.total_saves / (self.total_saves + self.failed_saves)) * 100:.1f}%, "
                    f"Avg latency: {avg_latency:.1f}ms, "
                    f"Max latency: {self.max_latency:.1f}ms"
                )
            
            # Verbose logging for important milestones
            if self.total_saves == 1:
                logger.info(f"✅ First token checkpoint saved successfully in {save_duration:.1f}ms")
            elif self.total_saves % 100 == 0:
                logger.info(
                    f"✅ Milestone: {self.total_saves} tokens saved, "
                    f"Total time in saves: {self.total_save_time/1000:.1f}s"
                )
            elif self.config.ENABLE_DETAILED_LOGGING:
                logger.debug(f"✅ Token saved in {save_duration:.1f}ms (Save #{self.total_saves})")
            
            return True
            
        except Exception as e:
            save_duration = (time.time() - start_time) * 1000
            self.failed_saves += 1
            self.consecutive_failures += 1
            
            logger.error(
                f"❌ Failed to save resume token after {save_duration:.1f}ms: {e}",
                exc_info=True
            )
            logger.error(
                f"  Stats: Failed saves: {self.failed_saves}, "
                f"Consecutive failures: {self.consecutive_failures}"
            )
            
            # Critical alert on multiple consecutive failures
            if self.consecutive_failures >= 3:
                logger.critical(
                    f"🚨 CRITICAL: {self.consecutive_failures} consecutive token save failures! "
                    f"Token persistence may be compromised. "
                    f"Last successful save: {self.last_checkpoint_time}"
                )
            
            # Try to save error state
            try:
                await self._save_error_state(str(e), save_duration)
            except Exception as error_save_exception:
                logger.error(f"❌ Could not save error state: {error_save_exception}")
                
            return False
            
        finally:
            self.save_in_progress = False
    
    async def get_resume_token(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve the last saved resume token with detailed logging.
        
        Returns:
            Dict containing token and metadata, or None if not found
        """
        logger.info("🔍 Retrieving resume token from Firestore...")
        
        if not self.db:
            logger.warning("⚠️ Firestore not available, cannot retrieve token")
            logger.info("  Will fall back to timestamp-based recovery")
            return None
        
        start_time = time.time()
        
        try:
            doc = await self.collection.document(self.document_id).get()
            retrieval_time = (time.time() - start_time) * 1000
            
            if not doc.exists:
                logger.info(f"📭 No saved resume token found (checked in {retrieval_time:.1f}ms)")
                logger.info("  Will use timestamp-based recovery strategy")
                return None
            
            data = doc.to_dict()
            
            # Validate and log token details
            if 'timestamp' in data:
                token_timestamp = data['timestamp']
                token_age = datetime.now(timezone.utc) - token_timestamp
                token_age_hours = token_age.total_seconds() / 3600
                
                logger.info(f"📌 Resume token found in {retrieval_time:.1f}ms:")
                logger.info(f"  Token age: {token_age_hours:.1f} hours")
                logger.info(f"  Saved at: {token_timestamp.isoformat()}")
                logger.info(f"  Save count: {data.get('save_count', 'unknown')}")
                logger.info(f"  Total events: {data.get('total_events', 'unknown')}")
                logger.info(f"  Token size: {data.get('token_size_bytes', 'unknown')} bytes")
                
                # Check token validity
                if token_age_hours > self.config.OPLOG_WINDOW_HOURS:
                    logger.warning(
                        f"⚠️ Token age ({token_age_hours:.1f}h) exceeds "
                        f"oplog window ({self.config.OPLOG_WINDOW_HOURS}h)"
                    )
                    logger.warning("  Token may be invalid - MongoDB might not have this resume point")
                    data['potentially_invalid'] = True
                    
                    # Check if we should use it anyway
                    if token_age_hours > self.config.OPLOG_WINDOW_HOURS * 2:
                        logger.error(
                            f"❌ Token too old ({token_age_hours:.1f}h) - "
                            f"exceeds 2x oplog window, discarding"
                        )
                        return None
                else:
                    logger.info(f"✅ Token is within oplog window - safe to use")
            else:
                logger.warning("⚠️ Token found but missing timestamp - treating as potentially invalid")
            
            # Check for previous errors
            if data.get('last_error'):
                logger.warning(f"⚠️ Previous error recorded: {data['last_error']}")
                if data.get('error_timestamp'):
                    logger.warning(f"  Error time: {data['error_timestamp']}")
            
            # Deserialize token
            if 'token' in data:
                original_token = data['token']
                data['token'] = self._deserialize_token(original_token)
                logger.debug(f"✅ Token deserialized successfully")
            else:
                logger.error("❌ Token document exists but missing 'token' field")
                return None
            
            return data
            
        except Exception as e:
            retrieval_time = (time.time() - start_time) * 1000
            logger.error(
                f"❌ Failed to retrieve resume token after {retrieval_time:.1f}ms: {e}",
                exc_info=True
            )
            return None
    
    async def get_last_checkpoint_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the last checkpoint with logging.
        
        Returns:
            datetime of last checkpoint or None
        """
        logger.debug("🔍 Getting last checkpoint time...")
        
        # Check cache first
        if self.last_checkpoint_time:
            age = (datetime.now(timezone.utc) - self.last_checkpoint_time).total_seconds()
            logger.debug(f"📌 Using cached checkpoint time: {self.last_checkpoint_time.isoformat()} (age: {age:.1f}s)")
            return self.last_checkpoint_time
        
        # Try to get from Firestore
        token_data = await self.get_resume_token()
        if token_data and 'timestamp' in token_data:
            checkpoint_time = token_data['timestamp']
            logger.info(f"📌 Last checkpoint time from Firestore: {checkpoint_time.isoformat()}")
            return checkpoint_time
        
        logger.info("📭 No checkpoint time available")
        return None
    
    async def clear_token(self):
        """Clear the saved resume token (use with caution)."""
        logger.warning("⚠️ CLEARING RESUME TOKEN - This action cannot be undone!")
        
        if not self.db:
            logger.error("❌ Cannot clear token - Firestore not available")
            return
        
        try:
            # First, backup the token data
            logger.info("💾 Backing up token before deletion...")
            doc = await self.collection.document(self.document_id).get()
            
            if doc.exists:
                backup_data = doc.to_dict()
                backup_data['deleted_at'] = datetime.now(timezone.utc)
                backup_data['deleted_by'] = 'manual_clear'
                
                # Save backup
                backup_id = f"{self.document_id}_backup_{int(time.time())}"
                await self.collection.document(backup_id).set(backup_data)
                logger.info(f"📋 Token backed up to: {backup_id}")
            
            # Delete the token
            await self.collection.document(self.document_id).delete()
            
            # Clear cache
            self.last_saved_token = None
            self.last_checkpoint_time = None
            
            logger.info("✅ Resume token cleared successfully")
            logger.info("  New changes will start from configured resume strategy")
            
        except Exception as e:
            logger.error(f"❌ Failed to clear resume token: {e}", exc_info=True)
    
    async def _save_error_state(self, error_message: str, duration_ms: float = 0):
        """Save error information for debugging."""
        if not self.db:
            return
        
        try:
            error_data = {
                'last_error': error_message[:500],  # Truncate long errors
                'error_timestamp': datetime.now(timezone.utc),
                'failed_save_count': self.failed_saves,
                'consecutive_failures': self.consecutive_failures,
                'operation_duration_ms': duration_ms,
                'last_successful_save': self.last_checkpoint_time,
                'total_successful_saves': self.total_saves
            }
            
            await self.collection.document(f"{self.document_id}_error").set(
                error_data,
                merge=True
            )
            
            logger.debug(f"📋 Error state recorded in Firestore")
            
        except Exception as e:
            logger.error(f"❌ Failed to save error state: {e}")
    
    def _serialize_token(self, token: Any) -> str:
        """Serialize token for storage with logging."""
        try:
            # Use json_util for BSON types
            serialized = json_util.dumps(token)
            
            if self.config.ENABLE_DETAILED_LOGGING:
                logger.debug(
                    f"📦 Token serialized - "
                    f"Type: {type(token).__name__}, "
                    f"Size: {len(serialized)} bytes"
                )
            
            return serialized
            
        except Exception as e:
            logger.error(
                f"❌ Failed to serialize token - "
                f"Type: {type(token).__name__}, "
                f"Error: {e}"
            )
            # Fallback to string representation
            logger.warning("⚠️ Using fallback string serialization")
            return str(token)
    
    def _deserialize_token(self, token_str: str) -> Any:
        """Deserialize token from storage with logging."""
        try:
            deserialized = json_util.loads(token_str)
            
            if self.config.ENABLE_DETAILED_LOGGING:
                logger.debug(
                    f"📦 Token deserialized - "
                    f"Result type: {type(deserialized).__name__}"
                )
            
            return deserialized
            
        except Exception as e:
            logger.warning(
                f"⚠️ Failed to deserialize as JSON - "
                f"Error: {e}, "
                f"Returning as string"
            )
            # If it's not JSON, return as-is
            return token_str
    
    def get_stats(self) -> Dict[str, Any]:
        """Get token manager statistics with detailed metrics."""
        stats = {
            'total_saves': self.total_saves,
            'failed_saves': self.failed_saves,
            'success_rate': (self.total_saves / (self.total_saves + self.failed_saves) * 100) if (self.total_saves + self.failed_saves) > 0 else 100,
            'consecutive_failures': self.consecutive_failures,
            'last_checkpoint': self.last_checkpoint_time.isoformat() if self.last_checkpoint_time else None,
            'firestore_available': self.db is not None,
            'save_in_progress': self.save_in_progress,
            'performance': {
                'last_save_duration_ms': self.last_save_duration,
                'avg_save_duration_ms': sum(self.save_latencies[-10:]) / min(10, len(self.save_latencies)) if self.save_latencies else 0,
                'max_save_duration_ms': self.max_latency,
                'total_save_time_ms': self.total_save_time
            }
        }
        
        # Log stats periodically
        if self.total_saves > 0 and self.total_saves % 50 == 0:
            logger.info(f"📊 TokenManager Stats: {stats}")
        
        return stats