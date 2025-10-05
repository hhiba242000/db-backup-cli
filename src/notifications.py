import os
from typing import Optional
from slack_sdk.webhook import WebhookClient
from datetime import datetime


class SlackNotifier:
    """Send backup notifications to Slack"""
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        self.enabled = os.getenv('SLACK_ENABLED', 'false').lower() == 'true'
        
        if self.enabled and not self.webhook_url:
            print("Warning: Slack notifications enabled but SLACK_WEBHOOK_URL not set")
            self.enabled = False
    
    def send_backup_success(self, database: str, file_path: str, size_mb: float, 
                           duration: float, compression_ratio: Optional[float] = None):
        """Send success notification"""
        if not self.enabled:
            return
        
        fields = [
            {
                "title": "Database",
                "value": database,
                "short": True
            },
            {
                "title": "Size",
                "value": f"{size_mb:.2f} MB",
                "short": True
            },
            {
                "title": "Duration",
                "value": f"{duration:.2f} seconds",
                "short": True
            },
            {
                "title": "File",
                "value": file_path,
                "short": False
            }
        ]
        
        if compression_ratio:
            fields.append({
                "title": "Compression Ratio",
                "value": f"{compression_ratio:.1f}x",
                "short": True
            })
        
        message = {
            "attachments": [
                {
                    "color": "good",
                    "title": "Backup Completed Successfully",
                    "fields": fields,
                    "footer": "Database Backup CLI",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        self._send(message)
    
    def send_backup_failure(self, database: str, error: str):
        """Send failure notification"""
        if not self.enabled:
            return
        
        message = {
            "attachments": [
                {
                    "color": "danger",
                    "title": "Backup Failed",
                    "fields": [
                        {
                            "title": "Database",
                            "value": database,
                            "short": True
                        },
                        {
                            "title": "Error",
                            "value": error,
                            "short": False
                        }
                    ],
                    "footer": "Database Backup CLI",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        self._send(message)
    
    def send_restore_success(self, database: str, backup_file: str):
        """Send restore success notification"""
        if not self.enabled:
            return
        
        message = {
            "attachments": [
                {
                    "color": "good",
                    "title": "Database Restored Successfully",
                    "fields": [
                        {
                            "title": "Database",
                            "value": database,
                            "short": True
                        },
                        {
                            "title": "From Backup",
                            "value": backup_file,
                            "short": False
                        }
                    ],
                    "footer": "Database Backup CLI",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        self._send(message)
    
    def send_restore_failure(self, database: str, backup_file: str, error: str):
        """Send restore failure notification"""
        if not self.enabled:
            return
        
        message = {
            "attachments": [
                {
                    "color": "danger",
                    "title": "Database Restore Failed",
                    "fields": [
                        {
                            "title": "Database",
                            "value": database,
                            "short": True
                        },
                        {
                            "title": "Backup File",
                            "value": backup_file,
                            "short": False
                        },
                        {
                            "title": "Error",
                            "value": error,
                            "short": False
                        }
                    ],
                    "footer": "Database Backup CLI",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        self._send(message)
    
    def _send(self, message: dict):
        """Internal method to send message to Slack"""
        try:
            webhook = WebhookClient(self.webhook_url)
            response = webhook.send(
                text="Database Backup Notification",
                attachments=message.get("attachments")
            )
            
            if response.status_code != 200:
                print(f"Failed to send Slack notification: {response.status_code}")
        except Exception as e:
            print(f"Error sending Slack notification: {e}")