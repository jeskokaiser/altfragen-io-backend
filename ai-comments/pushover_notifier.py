"""
Pushover notification module for error alerts.

Sends push notifications via Pushover API when critical errors occur.
Requires PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN environment variables.
"""

import os
import logging
from typing import Optional
import httpx

logger = logging.getLogger(__name__)


class PushoverNotifier:
    """Handles sending notifications via Pushover API."""
    
    def __init__(self):
        self.user_key = os.getenv("PUSHOVER_USER_KEY")
        self.api_token = os.getenv("PUSHOVER_API_TOKEN")
        self.enabled = bool(self.user_key and self.api_token)
        
        if not self.enabled:
            logger.warning(
                "Pushover notifications disabled: PUSHOVER_USER_KEY or PUSHOVER_API_TOKEN not set"
            )
    
    async def send_notification(
        self,
        title: str,
        message: str,
        priority: int = 0,
        sound: Optional[str] = None,
    ) -> bool:
        """
        Send a notification via Pushover.
        
        Args:
            title: Notification title
            message: Notification message
            priority: 0=normal, 1=high, 2=emergency
            sound: Optional sound name (default: device default)
        
        Returns:
            True if notification was sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            data = {
                "token": self.api_token,
                "user": self.user_key,
                "title": title,
                "message": message,
                "priority": priority,
            }
            
            if sound:
                data["sound"] = sound
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    "https://api.pushover.net/1/messages.json",
                    data=data,
                )
                
                if response.is_success:
                    logger.debug(f"Pushover notification sent: {title}")
                    return True
                else:
                    logger.error(
                        f"Pushover API error: {response.status_code} - {response.text}"
                    )
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to send Pushover notification: {e}", exc_info=True)
            return False
    
    async def notify_error(
        self,
        context: str,
        error: Exception,
        details: Optional[str] = None,
    ) -> bool:
        """
        Send an error notification.
        
        Args:
            context: Context where the error occurred (e.g., "Submit Process", "Consume Process")
            error: The exception that occurred
            details: Optional additional details
        
        Returns:
            True if notification was sent successfully, False otherwise
        """
        error_message = str(error)
        error_type = type(error).__name__
        
        message = f"Error: {error_type}\n{error_message}"
        if details:
            message += f"\n\nDetails: {details}"
        
        return await self.send_notification(
            title=f"AI Commentary Error: {context}",
            message=message,
            priority=0,  # Normal priority for errors
        )
    
    async def notify_critical(
        self,
        context: str,
        message: str,
        details: Optional[str] = None,
    ) -> bool:
        """
        Send a critical notification (e.g., batch job failures, API outages).
        
        Args:
            context: Context of the critical issue
            message: Critical message
            details: Optional additional details
        
        Returns:
            True if notification was sent successfully, False otherwise
        """
        full_message = message
        if details:
            full_message += f"\n\nDetails: {details}"
        
        return await self.send_notification(
            title=f"AI Commentary Critical: {context}",
            message=full_message,
            priority=1,  # High priority for critical errors
        )
    
    async def notify_warning(
        self,
        context: str,
        message: str,
    ) -> bool:
        """
        Send a warning notification (e.g., batch job status issues, partial failures).
        
        Args:
            context: Context of the warning
            message: Warning message
        
        Returns:
            True if notification was sent successfully, False otherwise
        """
        return await self.send_notification(
            title=f"AI Commentary Warning: {context}",
            message=message,
            priority=0,  # Normal priority
        )


# Global instance
_notifier: Optional[PushoverNotifier] = None


def get_notifier() -> PushoverNotifier:
    """Get or create the global PushoverNotifier instance."""
    global _notifier
    if _notifier is None:
        _notifier = PushoverNotifier()
    return _notifier

