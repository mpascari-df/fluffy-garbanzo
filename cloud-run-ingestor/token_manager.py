# token_manager.py - Resume token persistence manager using Firestore
# Provides reliable token storage with fallback strategies

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient
import json
from bson import json_util

logger = logging.getLogger(__name__)

class TokenManager:
    """Manages resume token persistence in Firestore for change stream recovery."""
    
    def __init__(self, config):
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
    
    async def initialize(self):
        """Initialize Firestore connection."""
        try:
            # Use async Firestore client
            self.db = firestore.AsyncClient(project=self.config.PROJECT_ID)
            self.collection = self.db.collection(self.config.TOKEN_COLLECTION)
            
            # Test connection by attempting to read
            doc = await self.collection.document(self.document_id).get()
            if doc.exists:
                logger.info(f"Connected to Firestore. Existing token found: {doc.id}")
            else:
                logger.info("Connected to Firestore. No existing token found.")
                
        except Exception as e:
            logger.error(f"Failed to initialize Firestore: {e}")
            logger.warning("Token persistence disabled - will rely on timestamp recovery")
            self.db = None
    
    async def save_resume_token(self, token: Any, events_processed: int = 0) -> bool:
        """
        Save resume token to Firestore.
        
        Args:
            token: The MongoDB resume token
            events_processed: Number of events processed since last save
            
        Returns:
            bool: Success status
        """
        if not self.db or self.save_in_progress:
            return False
        
        self.save_in_progress = True
        
        try:
            # Prepare token data
            token_data = {
                'token': self._serialize_token(token),
                'timestamp': datetime.now(timezone.utc),
                'events_since_checkpoint': events_processed,
                'total_events': self.total_saves * self.config.TOKEN_CHECKPOINT_EVENTS + events_processed,
                'save_count': self.total_saves + 1,
                'last_error': None,
                'metadata': {
                    'service_version': '2.0.0',
                    'oplog_window_hours': self.config.OPLOG_WINDOW_HOURS,
                    'project_id': self.config.PROJECT_ID,
                    'database': self.config.MONGO_DB_NAME
                }
            }
            
            # Save to Firestore with merge to preserve other fields
            await self.collection.document(self.document_id).set(
                token_data,
                merge=True
            )
            
            # Update cache
            self.last_saved_token = token
            self.last_checkpoint_time = token_data['timestamp']
            self.total_saves += 1
            
            logger.debug(f"Token saved successfully. Total saves: {self.total_saves}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save resume token: {e}")
            self.failed_saves += 1
            
            # Try to save error state
            try:
                await self._save_error_state(str(e))
            except:
                pass
                
            return False
            
        finally:
            self.save_in_progress = False
    
    async def get_resume_token(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve the last saved resume token.
        
        Returns:
            Dict containing token and metadata, or None if not found
        """
        if not self.db:
            logger.warning("Firestore not available, cannot retrieve token")
            return None
        
        try:
            doc = await self.collection.document(self.document_id).get()
            
            if not doc.exists:
                logger.info("No saved resume token found")
                return None
            
            data = doc.to_dict()
            
            # Validate token age
            if 'timestamp' in data:
                token_age_hours = (
                    datetime.now(timezone.utc) - data['timestamp']
                ).total_seconds() / 3600
                
                if token_age_hours > self.config.OPLOG_WINDOW_HOURS:
                    logger.warning(
                        f"Saved token is {token_age_hours:.1f} hours old, "
                        f"exceeds oplog window of {self.config.OPLOG_WINDOW_HOURS} hours"
                    )
                    # Token might be invalid, but return it anyway for attempt
                    data['potentially_invalid'] = True
            
            # Deserialize token
            if 'token' in data:
                data['token'] = self._deserialize_token(data['token'])
            
            logger.info(f"Retrieved resume token from {data.get('timestamp')}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to retrieve resume token: {e}")
            return None
    
    async def get_last_checkpoint_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the last checkpoint.
        
        Returns:
            datetime of last checkpoint or None
        """
        if self.last_checkpoint_time:
            return self.last_checkpoint_time
        
        token_data = await self.get_resume_token()
        if token_data and 'timestamp' in token_data:
            return token_data['timestamp']
        
        return None
    
    async def clear_token(self):
        """Clear the saved resume token (use with caution)."""
        if not self.db:
            return
        
        try:
            await self.collection.document(self.document_id).delete()
            logger.info("Resume token cleared")
            self.last_saved_token = None
            self.last_checkpoint_time = None
        except Exception as e:
            logger.error(f"Failed to clear resume token: {e}")
    
    async def _save_error_state(self, error_message: str):
        """Save error information for debugging."""
        if not self.db:
            return
        
        error_data = {
            'last_error': error_message,
            'error_timestamp': datetime.now(timezone.utc),
            'failed_save_count': self.failed_saves
        }
        
        await self.collection.document(f"{self.document_id}_error").set(
            error_data,
            merge=True
        )
    
    def _serialize_token(self, token: Any) -> str:
        """Serialize token for storage."""
        try:
            # Use json_util for BSON types
            return json_util.dumps(token)
        except Exception as e:
            logger.error(f"Failed to serialize token: {e}")
            # Fallback to string representation
            return str(token)
    
    def _deserialize_token(self, token_str: str) -> Any:
        """Deserialize token from storage."""
        try:
            return json_util.loads(token_str)
        except Exception:
            # If it's not JSON, return as-is
            return token_str
    
    def get_stats(self) -> Dict[str, Any]:
        """Get token manager statistics."""
        return {
            'total_saves': self.total_saves,
            'failed_saves': self.failed_saves,
            'last_checkpoint': self.last_checkpoint_time.isoformat() if self.last_checkpoint_time else None,
            'firestore_available': self.db is not None,
            'save_in_progress': self.save_in_progress
        }
