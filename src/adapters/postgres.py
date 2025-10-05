# src/adapters/postgres.py
import subprocess
import psycopg 
from psycopg import OperationalError
import time
from pathlib import Path
from datetime import datetime
from typing import Optional
from .base import DatabaseAdapter, BackupResult


class PostgreSQLAdapter(DatabaseAdapter):
    """
    PostgreSQL-specific implementation of backup/restore operations.
    
    This uses two approaches:
    1. psycopg3 library for connection testing and queries
    2. pg_dump/pg_restore command-line tools for actual backups
    """
    
    def __init__(self, connection_params):
        super().__init__(connection_params)
        if 'port' not in self.connection_params:
            self.connection_params['port'] = 5432
    
    def _get_connection_string(self) -> str:
        """Build PostgreSQL connection string for psycopg3"""
        return (
            f"host={self.connection_params['host']} "
            f"port={self.connection_params['port']} "
            f"user={self.connection_params['user']} "
            f"password={self.connection_params['password']} "
            f"dbname={self.connection_params['database']}"
        )
    
    def test_connection(self) -> bool:
        """
        Try to connect to PostgreSQL using psycopg3.
        
        This is a quick check before we start a potentially long backup.
        """
        try:
            # psycopg3 uses a connection string approach
            conn = psycopg.connect(
                self._get_connection_string(),
                connect_timeout=10
            )
            
            # If we got here, connection worked!
            conn.close()
            return True
            
        except OperationalError as e:
            print(f"❌ Connection error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    def backup(self, output_path: str, backup_type: str = "full") -> BackupResult:
        """
        Create a backup using pg_dump.
        
        pg_dump is a command-line utility that comes with PostgreSQL.
        We call it using Python's subprocess module.
        """
        print(f"🔄 Starting PostgreSQL backup...")
        start_time = time.time()
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Use full path to pg_dump for Homebrew installation
        pg_dump_path = '/opt/homebrew/opt/postgresql@15/bin/pg_dump'
        
        # Check if pg_dump exists at this path
        if not Path(pg_dump_path).exists():
            # Fallback to system pg_dump
            pg_dump_path = 'pg_dump'
        
        # Build the pg_dump command
        cmd = [
            pg_dump_path,
            f"--host={self.connection_params['host']}",
            f"--port={self.connection_params['port']}",
            f"--username={self.connection_params['user']}",
            f"--dbname={self.connection_params['database']}",
            f"--file={output_path}",
            '--format=custom',
            '--verbose',
            '--no-password'
        ]
        
        # Pass password via environment variable
        env = {
            'PGPASSWORD': self.connection_params['password']
        }
        
        try:
            # Run the command
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Calculate metrics
            duration = time.time() - start_time
            file_size = output_file.stat().st_size
            
            print(f"✅ Backup completed in {duration:.2f} seconds")
            
            return BackupResult(
                success=True,
                file_path=str(output_path),
                size_bytes=file_size,
                duration_seconds=duration,
                timestamp=datetime.now()
            )
            
        except subprocess.CalledProcessError as e:
            duration = time.time() - start_time
            error_msg = f"pg_dump failed: {e.stderr}"
            print(f"❌ {error_msg}")
            
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
                error_message="pg_dump not found. Is PostgreSQL installed?"
            )
    
    def restore(self, backup_path: str, target_db: Optional[str] = None) -> bool:
        """
        Restore a database from backup using pg_restore.
        
        WARNING: This will overwrite the existing database!
        """
        if not Path(backup_path).exists():
            print(f"❌ Backup file not found: {backup_path}")
            return False
        
        db_name = target_db or self.connection_params['database']
        
        print(f"🔄 Restoring to database '{db_name}'...")
        print("⚠️  This will overwrite existing data!")
        
        # Use full path to pg_restore (same as pg_dump)
        pg_restore_path = '/opt/homebrew/opt/postgresql@15/bin/pg_restore'
        
        # Check if pg_restore exists at this path
        if not Path(pg_restore_path).exists():
            # Fallback to system pg_restore
            pg_restore_path = 'pg_restore'
        
        cmd = [
            pg_restore_path,
            f"--host={self.connection_params['host']}",
            f"--port={self.connection_params['port']}",
            f"--username={self.connection_params['user']}",
            f"--dbname={db_name}",
            '--verbose',
            '--clean',
            '--if-exists',
            backup_path
        ]
        
        env = {'PGPASSWORD': self.connection_params['password']}
        
        try:
            subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
            print(f"✅ Database restored successfully!")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Restore failed: {e.stderr}")
            return False
    
    def get_database_size(self) -> int:
        """
        Query PostgreSQL for the database size using psycopg3.
        """
        try:
            # psycopg3 connection
            conn = psycopg.connect(self._get_connection_string())
            
            # Execute query - psycopg3 uses context managers
            with conn.cursor() as cursor:
                query = f"SELECT pg_database_size('{self.connection_params['database']}')"
                cursor.execute(query)
                size = cursor.fetchone()[0]
            
            conn.close()
            return size
            
        except Exception as e:
            print(f"❌ Could not get database size: {e}")
            return 0
        

    def restore_selective(self, backup_path: str, target_db: str, tables: list) -> bool:
        """
        Restore specific tables from backup using pg_restore.
        
        Args:
            backup_path: Path to backup file
            target_db: Target database name
            tables: List of table names to restore
        
        Returns:
            True if successful, False otherwise
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return False
        
        print(f"Restoring tables: {', '.join(tables)} to database '{target_db}'...")
        print("This will overwrite existing data in these tables!")
        
        pg_restore_path = '/opt/homebrew/opt/postgresql@15/bin/pg_restore'
        
        if not Path(pg_restore_path).exists():
            pg_restore_path = 'pg_restore'
        
        # Build command with table selections
        cmd = [
            pg_restore_path,
            f"--host={self.connection_params['host']}",
            f"--port={self.connection_params['port']}",
            f"--username={self.connection_params['user']}",
            f"--dbname={target_db}",
            '--verbose',
            '--clean',
            '--if-exists',
        ]
        
        # Add each table
        for table in tables:
            cmd.extend(['--table', table])
        
        cmd.append(backup_path)
        
        env = {'PGPASSWORD': self.connection_params['password']}
        
        try:
            subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
            print(f"Tables restored successfully!")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Restore failed: {e.stderr}")
            return False


    def list_tables_in_backup(self, backup_path: str) -> list:
        """
        List all tables available in a backup file.
        
        Args:
            backup_path: Path to backup file
        
        Returns:
            List of table names
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return []
        
        pg_restore_path = '/opt/homebrew/opt/postgresql@15/bin/pg_restore'
        
        if not Path(pg_restore_path).exists():
            pg_restore_path = 'pg_restore'
        
        cmd = [
            pg_restore_path,
            '--list',
            backup_path
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Parse the output to extract table names
            tables = []
            for line in result.stdout.split('\n'):
                # Look for lines that contain "TABLE DATA"
                if 'TABLE DATA' in line:
                    # Extract table name
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part == 'public' and i + 1 < len(parts):
                            table_name = parts[i + 1]
                            if table_name not in tables:
                                tables.append(table_name)
            
            return tables
            
        except subprocess.CalledProcessError as e:
            print(f"Failed to list tables: {e.stderr}")
            return []