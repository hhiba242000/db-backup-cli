import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Optional

load_dotenv()


class Config:
    """Configuration management with support for multiple database profiles"""
    
    @staticmethod
    def get_database_config(db_type: Optional[str] = None) -> Dict[str, Optional[str]]:
        """
        Get database configuration based on database type.
        
        Reads environment variables like POSTGRES_HOST, MYSQL_HOST, etc.
        Falls back to generic DB_* variables if type-specific ones don't exist.
        """
        if not db_type:
            db_type = os.getenv('DB_TYPE', 'postgres')
        
        # Try type-specific config first (e.g., POSTGRES_HOST)
        prefix = db_type.upper()
        
        config = {
            'type': db_type,
            'host': os.getenv(f'{prefix}_HOST') or os.getenv('DB_HOST'),
            'port': os.getenv(f'{prefix}_PORT') or os.getenv('DB_PORT'),
            'user': os.getenv(f'{prefix}_USER') or os.getenv('DB_USER'),
            'password': os.getenv(f'{prefix}_PASSWORD') or os.getenv('DB_PASSWORD'),
            'database': os.getenv(f'{prefix}_DATABASE') or os.getenv('DB_NAME')
        }
        
        # Convert port to int if present
        if config['port']:
            config['port'] = int(config['port'])
        
        return config
    
    @staticmethod
    def get_backup_dir() -> str:
        """Get backup directory from environment or use default"""
        return os.getenv('BACKUP_DIR', 'backups')
    
    @staticmethod
    def validate_config(db_type: str) -> bool:
        """Check if required config values are present for db_type"""
        config = Config.get_database_config(db_type)
        required = ['type', 'host', 'user', 'password', 'database']
        
        missing = [key for key in required if not config.get(key)]
        
        if missing:
            print(f"Missing required configuration for {db_type}: {', '.join(missing)}")
            return False
        
        return True