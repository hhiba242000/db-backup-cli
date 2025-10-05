import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Optional

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration management for database backups"""
    
    @staticmethod
    def get_database_config() -> Dict[str, Optional[str]]:
        """
        Get database configuration from environment variables.
        
        Returns:
            Dictionary with database connection parameters
        """
        return {
            'type': os.getenv('DB_TYPE'),
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT')) if os.getenv('DB_PORT') else None,
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME')
        }
    
    @staticmethod
    def get_backup_dir() -> str:
        """Get backup directory from environment or use default"""
        return os.getenv('BACKUP_DIR', 'backups')
    
    @staticmethod
    def validate_config() -> bool:
        """Check if required config values are present"""
        config = Config.get_database_config()
        required = ['type', 'host', 'user', 'password', 'database']
        
        missing = [key for key in required if not config.get(key)]
        
        if missing:
            print(f"Missing required configuration: {', '.join(missing)}")
            print("Please set these in your .env file or pass as command-line arguments")
            return False
        
        return True