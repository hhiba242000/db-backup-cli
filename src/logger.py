# src/logger.py
import logging
from pathlib import Path
from datetime import datetime
import json


class BackupLogger:
    """Centralized logging for backup operations"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger("db_backup")
        self.logger.setLevel(logging.INFO)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup file and console handlers"""
        
        # File handler - detailed logs
        log_file = self.log_dir / f"backup_{datetime.now().strftime('%Y%m')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Console handler - errors only
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def log_backup_start(self, database: str, db_type: str, host: str):
        """Log the start of a backup operation"""
        self.logger.info(
            f"BACKUP_START | Database: {database} | Type: {db_type} | Host: {host}"
        )
    
    def log_backup_success(self, database: str, file_path: str, size_mb: float, 
                          duration: float, compression_ratio: float = None):
        """Log successful backup completion"""
        msg = (
            f"BACKUP_SUCCESS | Database: {database} | "
            f"File: {file_path} | Size: {size_mb:.2f}MB | "
            f"Duration: {duration:.2f}s"
        )
        if compression_ratio:
            msg += f" | Compression: {compression_ratio:.1f}x"
        
        self.logger.info(msg)
    
    def log_backup_failure(self, database: str, error: str):
        """Log backup failure"""
        self.logger.error(
            f"BACKUP_FAILED | Database: {database} | Error: {error}"
        )
    
    def log_restore_start(self, database: str, backup_file: str):
        """Log the start of a restore operation"""
        self.logger.info(
            f"RESTORE_START | Database: {database} | From: {backup_file}"
        )
    
    def log_restore_success(self, database: str, backup_file: str):
        """Log successful restore"""
        self.logger.info(
            f"RESTORE_SUCCESS | Database: {database} | From: {backup_file}"
        )
    
    def log_restore_failure(self, database: str, backup_file: str, error: str):
        """Log restore failure"""
        self.logger.error(
            f"RESTORE_FAILED | Database: {database} | From: {backup_file} | Error: {error}"
        )
    
    def log_connection_test(self, database: str, host: str, success: bool):
        """Log connection test result"""
        status = "SUCCESS" if success else "FAILED"
        level = logging.INFO if success else logging.WARNING
        self.logger.log(
            level,
            f"CONNECTION_TEST | Database: {database} | Host: {host} | Status: {status}"
        )


class BackupMetadata:
    """Store structured backup metadata in JSON format"""
    
    def __init__(self, metadata_dir: str = "logs"):
        self.metadata_dir = Path(metadata_dir)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file = self.metadata_dir / "backup_metadata.json"
        
        # Load existing metadata or create new
        self.metadata = self._load_metadata()
    
    def _load_metadata(self):
        """Load existing metadata from file"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {"backups": []}
        return {"backups": []}
    
    def _save_metadata(self):
        """Save metadata to file"""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
    
    def add_backup_record(self, record: dict):
        """Add a backup record to metadata"""
        self.metadata["backups"].append(record)
        self._save_metadata()
    
    def get_recent_backups(self, database: str = None, limit: int = 10):
        """Get recent backups, optionally filtered by database"""
        backups = self.metadata["backups"]
        
        if database:
            backups = [b for b in backups if b.get("database") == database]
        
        # Return most recent first
        return sorted(backups, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]
    
    def get_backup_stats(self):
        """Get statistics about backups"""
        backups = self.metadata["backups"]
        
        if not backups:
            return {
                "total_backups": 0,
                "successful": 0,
                "failed": 0,
                "total_size_mb": 0
            }
        
        successful = [b for b in backups if b.get("success")]
        failed = [b for b in backups if not b.get("success")]
        
        total_size = sum(b.get("size_bytes", 0) for b in successful) / (1024 * 1024)
        
        return {
            "total_backups": len(backups),
            "successful": len(successful),
            "failed": len(failed),
            "total_size_mb": total_size,
            "databases": list(set(b.get("database") for b in backups if b.get("database")))
        }