# src/adapters/mongodb.py
import subprocess
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import time
from pathlib import Path
from datetime import datetime
from typing import Optional
import shutil
from .base import DatabaseAdapter, BackupResult


class MongoDBAdapter(DatabaseAdapter):
    """
    MongoDB-specific implementation of backup/restore operations.
    
    Uses mongodump and mongorestore tools.
    """
    
    def __init__(self, connection_params):
        super().__init__(connection_params)
        if 'port' not in self.connection_params:
            self.connection_params['port'] = 27017
    
    def _get_connection_string(self) -> str:
        """Build MongoDB connection string"""
        return (
            f"mongodb://{self.connection_params['user']}:"
            f"{self.connection_params['password']}@"
            f"{self.connection_params['host']}:"
            f"{self.connection_params['port']}/"
            f"{self.connection_params['database']}"
            f"?authSource={self.connection_params['database']}"
        )
    
    def test_connection(self) -> bool:
        """Test MongoDB connection"""
        try:
            client = MongoClient(
                self._get_connection_string(),
                serverSelectionTimeoutMS=10000
            )
            
            # Trigger connection
            client.server_info()
            client.close()
            return True
            
        except ConnectionFailure as e:
            print(f"Connection error: {e}")
            return False
        except OperationFailure as e:
            print(f"Authentication error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False
    
    def backup(self, output_path: str, backup_type: str = "full", format: str = "archive") -> BackupResult:
        """
        Create backup using mongodump.
        
        MongoDB backups create a directory with BSON files.
        We'll create an archive file for easier management.
        """
        print(f"Starting MongoDB backup...")
        start_time = time.time()
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # mongodump command
        mongodump_path = 'mongodump'
        
        # Build mongodump command
        # MongoDB uses --archive for single-file output
        cmd = [
            mongodump_path,
            f"--uri={self._get_connection_string()}",
            f"--archive={output_path}",
            '--gzip'  # Compress the archive
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            duration = time.time() - start_time
            file_size = output_file.stat().st_size
            
            print(f"Backup completed in {duration:.2f} seconds")
            
            return BackupResult(
                success=True,
                file_path=str(output_path),
                size_bytes=file_size,
                duration_seconds=duration,
                timestamp=datetime.now()
            )
            
        except subprocess.CalledProcessError as e:
            duration = time.time() - start_time
            error_msg = f"mongodump failed: {e.stderr}"
            print(f"{error_msg}")
            
            return BackupResult(
                success=False,
                file_path="",
                size_bytes=0,
                duration_seconds=duration,
                timestamp=datetime.now(),
                error_message=error_msg
            )
        
        except FileNotFoundError:
            return BackupResult(
                success=False,
                file_path="",
                size_bytes=0,
                duration_seconds=0,
                timestamp=datetime.now(),
                error_message="mongodump not found. Please install MongoDB Database Tools."
            )
    
    def restore(self, backup_path: str, target_db: Optional[str] = None) -> bool:
        """
        Restore database from backup using mongorestore.
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return False
        
        db_name = target_db or self.connection_params['database']
        
        print(f"Restoring to database '{db_name}'...")
        print("This will overwrite existing data!")
        
        mongorestore_path = 'mongorestore'
        
        # Build connection string for target database
        restore_uri = (
            f"mongodb://{self.connection_params['user']}:"
            f"{self.connection_params['password']}@"
            f"{self.connection_params['host']}:"
            f"{self.connection_params['port']}/"
            f"{db_name}"
            f"?authSource={self.connection_params['database']}"
        )
        
        cmd = [
            mongorestore_path,
            f"--uri={restore_uri}",
            f"--archive={backup_path}",
            '--gzip',
            '--drop'  # Drop collections before restoring
        ]
        
        try:
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            print(f"Database restored successfully!")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Restore failed: {e.stderr}")
            return False
    
    def get_database_size(self) -> int:
        """Get MongoDB database size"""
        try:
            client = MongoClient(self._get_connection_string())
            db = client[self.connection_params['database']]
            
            stats = db.command("dbStats")
            size = stats.get('dataSize', 0)
            
            client.close()
            return int(size)
            
        except Exception as e:
            print(f"Could not get database size: {e}")
            return 0
    
    def restore_selective(self, backup_path: str, target_db: str, collections: list) -> bool:
        """
        Restore specific collections from backup.
        
        MongoDB uses collections instead of tables.
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return False
        
        print(f"Restoring collections: {', '.join(collections)} to database '{target_db}'...")
        
        mongorestore_path = 'mongorestore'
        
        restore_uri = (
            f"mongodb://{self.connection_params['user']}:"
            f"{self.connection_params['password']}@"
            f"{self.connection_params['host']}:"
            f"{self.connection_params['port']}/"
            f"{target_db}"
            f"?authSource={self.connection_params['database']}"
        )
        
        # MongoDB allows specifying collections with --nsInclude
        # Format: database.collection
        ns_include = [f"{self.connection_params['database']}.{col}" for col in collections]
        
        cmd = [
            mongorestore_path,
            f"--uri={restore_uri}",
            f"--archive={backup_path}",
            '--gzip',
            '--drop'
        ]
        
        # Add namespace filters
        for ns in ns_include:
            cmd.extend(['--nsInclude', ns])
        
        try:
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            print(f"Collections restored successfully!")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Restore failed: {e.stderr}")
            return False
    
    def list_tables_in_backup(self, backup_path: str) -> list:
        """
        List collections in a MongoDB backup.
        
        MongoDB calls them collections, not tables.
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return []
        
        mongorestore_path = 'mongorestore'
        
        cmd = [
            mongorestore_path,
            f"--archive={backup_path}",
            '--gzip',
            '--dryRun'  # Don't actually restore, just show what would happen
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse output to find collection names
            collections = []
            for line in result.stdout.split('\n'):
                # Look for lines mentioning collections
                if 'restoring' in line.lower() and '.' in line:
                    # Extract collection name (format: database.collection)
                    parts = line.split()
                    for part in parts:
                        if '.' in part and not part.startswith('--'):
                            collection = part.split('.')[-1]
                            if collection and collection not in collections:
                                collections.append(collection)
            
            return collections
            
        except subprocess.CalledProcessError as e:
            print(f"Failed to list collections: {e.stderr}")
            return []