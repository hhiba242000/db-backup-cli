# src/adapters/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class BackupResult:
    """
    A container for backup results.
    
    Think of this as a structured way to return multiple pieces of 
    information from the backup function instead of just True/False.
    """
    success: bool                          # Did it work?
    file_path: str                         # Where is the backup file?
    size_bytes: int                        # How big is it?
    duration_seconds: float                # How long did it take?
    timestamp: datetime                     # When was it created?
    error_message: Optional[str] = None    # If failed, why?
    
    def size_mb(self) -> float:
        """Convert bytes to megabytes for easier reading"""
        return self.size_bytes / (1024 * 1024)
    
    def size_gb(self) -> float:
        """Convert bytes to gigabytes"""
        return self.size_bytes / (1024 * 1024 * 1024)


class DatabaseAdapter(ABC):
    """
    Abstract Base Class (ABC) - This is like a contract.
    
    Any class that inherits from this MUST implement all methods
    marked with @abstractmethod. This ensures consistency across
    different database types.
    
    ABC stands for "Abstract Base Class" - it's a Python feature
    that helps enforce interface contracts.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the adapter with connection parameters.
        
        Args:
            connection_params: Dictionary containing host, port, user, password, etc.
        """
        self.connection_params = connection_params
        self.connection = None
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test if we can connect to the database.
        
        This is important - we should always verify credentials
        before attempting a backup that might take hours!
        
        Returns:
            True if connection succeeds, False otherwise
        """
        pass
    
    @abstractmethod
    def backup(self, output_path: str, backup_type: str = "full") -> BackupResult:
        """
        Perform a backup operation.
        
        Args:
            output_path: Where to save the backup file
            backup_type: "full", "incremental", or "differential"
            
        Returns:
            BackupResult object with all the details
        """
        pass
    
    @abstractmethod
    def restore(self, backup_path: str, target_db: Optional[str] = None) -> bool:
        """
        Restore database from a backup file.
        
        Args:
            backup_path: Path to the backup file
            target_db: Optional different database name to restore to
            
        Returns:
            True if restore succeeds, False otherwise
        """
        pass
    
    @abstractmethod
    def get_database_size(self) -> int:
        """
        Get the current size of the database in bytes.
        
        This helps users estimate backup size and duration.
        
        Returns:
            Database size in bytes
        """
        pass
    
    def validate_params(self) -> bool:
        """
        Validate that required connection parameters are present.
        Not abstract - child classes can use this or override it.
        """
        required = ['host', 'user', 'password', 'database']
        return all(param in self.connection_params for param in required)