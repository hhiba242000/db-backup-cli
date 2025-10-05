# src/adapters/mysql.py
import subprocess
import mysql.connector
from mysql.connector import Error
import time
from pathlib import Path
from datetime import datetime
from typing import Optional
from .base import DatabaseAdapter, BackupResult


class MySQLAdapter(DatabaseAdapter):
    """
    MySQL-specific implementation of backup/restore operations.
    
    Uses mysqldump and mysql command-line tools.
    """
    
    def __init__(self, connection_params):
        super().__init__(connection_params)
        if 'port' not in self.connection_params:
            self.connection_params['port'] = 3306
    
    def test_connection(self) -> bool:
        """Test MySQL connection"""
        try:
            conn = mysql.connector.connect(
                host=self.connection_params['host'],
                port=self.connection_params['port'],
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                database=self.connection_params['database'],
                connection_timeout=30,
                use_pure=True
            )
            
            conn.close()
            return True
            
        except Error as e:
            print(f"Connection error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False
    
    def backup(self, output_path: str, backup_type: str = "full", format: str = "sql") -> BackupResult:
        """
        Create backup using mysqldump.
        
        MySQL doesn't have a binary format like PostgreSQL, so we use SQL format.
        """
        print(f"Starting MySQL backup...")
        start_time = time.time()
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Use full path to mysqldump
        mysqldump_path = '/opt/homebrew/opt/mysql-client/bin/mysqldump'
        
        if not Path(mysqldump_path).exists():
            mysqldump_path = 'mysqldump'
        
        # Build mysqldump command
        cmd = [
            mysqldump_path,
            f"--host={self.connection_params['host']}",
            f"--port={self.connection_params['port']}",
            f"--user={self.connection_params['user']}",
            f"--password={self.connection_params['password']}",
            '--protocol=TCP',
            '--single-transaction',  # Consistent snapshot without locking
            '--routines',  # Include stored procedures
            '--triggers',  # Include triggers
            '--events',  # Include events
            self.connection_params['database'],
            f"--result-file={output_path}"
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
            error_msg = f"mysqldump failed: {e.stderr}"
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
                error_message=f"mysqldump not found at {mysqldump_path}. Please check installation."
            )
    
    def restore(self, backup_path: str, target_db: Optional[str] = None) -> bool:
        """
        Restore database from backup using mysql command.
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return False
        
        db_name = target_db or self.connection_params['database']
        
        print(f"Restoring to database '{db_name}'...")
        print("This will overwrite existing data!")
        
        mysql_path = '/opt/homebrew/opt/mysql-client/bin/mysql'
        
        if not Path(mysql_path).exists():
            mysql_path = 'mysql'
        
        cmd = [
            mysql_path,
            f"--host={self.connection_params['host']}",
            f"--port={self.connection_params['port']}",
            f"--user={self.connection_params['user']}",
            f"--password={self.connection_params['password']}",
            '--protocol=TCP',
            db_name
        ]
        
        try:
            with open(backup_path, 'r') as f:
                subprocess.run(
                    cmd,
                    stdin=f,
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
        """Query MySQL for database size"""
        try:
            conn = mysql.connector.connect(
                host=self.connection_params['host'],
                port=self.connection_params['port'],
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                database=self.connection_params['database'],
                use_pure=True
            )
            
            cursor = conn.cursor()
            query = """
                SELECT SUM(data_length + index_length) 
                FROM information_schema.tables 
                WHERE table_schema = %s
            """
            cursor.execute(query, (self.connection_params['database'],))
            size = cursor.fetchone()[0] or 0
            
            cursor.close()
            conn.close()
            
            return int(size)
            
        except Exception as e:
            print(f"Could not get database size: {e}")
            return 0
    
    def restore_selective(self, backup_path: str, target_db: str, tables: list) -> bool:
        """
        Restore specific tables from backup.
        
        For MySQL, we need to extract specific tables from the SQL dump.
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return False
        
        print(f"Restoring tables: {', '.join(tables)} to database '{target_db}'...")
        
        mysql_path = '/opt/homebrew/opt/mysql-client/bin/mysql'
        
        if not Path(mysql_path).exists():
            mysql_path = 'mysql'
        
        # For MySQL, we need to filter the SQL file to only include specific tables
        # This is more complex than PostgreSQL's --table option
        # For now, we'll use a simpler approach with grep
        
        try:
            # Create temporary filtered SQL file
            temp_file = Path(backup_path).parent / "temp_restore.sql"
            
            # Use sed/awk to extract specific tables (simplified approach)
            # In production, you'd want a more robust SQL parser
            with open(backup_path, 'r') as infile:
                with open(temp_file, 'w') as outfile:
                    include = False
                    for line in infile:
                        # Check for table creation/data
                        for table in tables:
                            if f"Table structure for table `{table}`" in line:
                                include = True
                            elif "Table structure for table" in line and table not in line:
                                include = False
                        
                        if include or line.startswith('--') or line.startswith('/*'):
                            outfile.write(line)
            
            # Restore from filtered file
            cmd = [
                mysql_path,
                f"--host={self.connection_params['host']}",
                f"--port={self.connection_params['port']}",
                f"--user={self.connection_params['user']}",
                f"--password={self.connection_params['password']}",
                target_db
            ]
            
            with open(temp_file, 'r') as f:
                subprocess.run(
                    cmd,
                    stdin=f,
                    capture_output=True,
                    text=True,
                    check=True
                )
            
            # Clean up temp file
            temp_file.unlink()
            
            print(f"Tables restored successfully!")
            return True
            
        except Exception as e:
            print(f"Restore failed: {e}")
            if temp_file.exists():
                temp_file.unlink()
            return False
    
    def list_tables_in_backup(self, backup_path: str) -> list:
        """
        List tables in a MySQL backup file.
        
        Parse the SQL dump to find table names.
        """
        if not Path(backup_path).exists():
            print(f"Backup file not found: {backup_path}")
            return []
        
        tables = []
        
        try:
            with open(backup_path, 'r') as f:
                for line in f:
                    # Look for CREATE TABLE statements
                    if line.startswith('CREATE TABLE'):
                        # Extract table name between backticks
                        start = line.find('`') + 1
                        end = line.find('`', start)
                        if start > 0 and end > start:
                            table_name = line[start:end]
                            if table_name not in tables:
                                tables.append(table_name)
            
            return tables
            
        except Exception as e:
            print(f"Failed to list tables: {e}")
            return []