# src/verification.py
import hashlib
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
import json


class BackupVerifier:
    """Verify backup file integrity and validity"""
    
    def __init__(self):
        self.verification_dir = Path("logs/verifications")
        self.verification_dir.mkdir(parents=True, exist_ok=True)
    
    def verify_file_integrity(self, backup_path: str) -> Tuple[bool, str]:
        """
        Level 1: Basic file integrity checks.
        
        Args:
            backup_path: Path to backup file
        
        Returns:
            (success, message) tuple
        """
        backup_file = Path(backup_path)
        
        # Check file exists
        if not backup_file.exists():
            return False, f"File does not exist: {backup_path}"
        
        # Check file is readable
        if not backup_file.is_file():
            return False, f"Not a regular file: {backup_path}"
        
        try:
            with open(backup_file, 'rb') as f:
                f.read(1)
        except PermissionError:
            return False, f"File is not readable: {backup_path}"
        except Exception as e:
            return False, f"Error reading file: {e}"
        
        # Check file size is reasonable
        size = backup_file.stat().st_size
        if size < 100:  # Less than 100 bytes is suspicious
            return False, f"File is too small ({size} bytes), likely corrupted"
        
        return True, f"File integrity OK ({size} bytes)"
    
    def calculate_checksum(self, backup_path: str) -> Optional[str]:
        """
        Calculate SHA256 checksum of backup file.
        
        Args:
            backup_path: Path to backup file
        
        Returns:
            Checksum string or None if failed
        """
        try:
            sha256_hash = hashlib.sha256()
            with open(backup_path, "rb") as f:
                # Read in chunks to handle large files
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception as e:
            print(f"Error calculating checksum: {e}")
            return None
    
    def verify_backup_format(self, backup_path: str, db_type: str = 'postgres') -> Tuple[bool, str]:
        """
        Level 2: Verify backup file format is valid.
        
        Args:
            backup_path: Path to backup file
            db_type: Database type (postgres, mysql, etc.)
        
        Returns:
            (success, message) tuple
        """
        if db_type == 'postgres':
            return self._verify_postgres_format(backup_path)
        else:
            return False, f"Unsupported database type: {db_type}"
    
    def _verify_postgres_format(self, backup_path: str) -> Tuple[bool, str]:
        """
        Verify PostgreSQL backup format using pg_restore --list.
        """
        pg_restore_path = '/opt/homebrew/opt/postgresql@15/bin/pg_restore'
        
        if not Path(pg_restore_path).exists():
            pg_restore_path = 'pg_restore'
        
        cmd = [pg_restore_path, '--list', backup_path]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )
            
            # Count objects in backup
            lines = result.stdout.split('\n')
            table_count = sum(1 for line in lines if 'TABLE DATA' in line)
            
            if table_count == 0:
                return False, "No tables found in backup (possibly empty or corrupted)"
            
            return True, f"Valid PostgreSQL backup format ({table_count} tables)"
            
        except subprocess.TimeoutExpired:
            return False, "Verification timed out (file may be corrupted)"
        except subprocess.CalledProcessError as e:
            return False, f"Invalid backup format: {e.stderr}"
        except Exception as e:
            return False, f"Error verifying format: {e}"
    
    def verify_full(self, backup_path: str, db_type: str = 'postgres') -> Dict:
        """
        Run full verification suite.
        
        Args:
            backup_path: Path to backup file
            db_type: Database type
        
        Returns:
            Dictionary with verification results
        """
        results = {
            'backup_path': backup_path,
            'timestamp': datetime.now().isoformat(),
            'db_type': db_type,
            'checks': {}
        }
        
        # Level 1: File integrity
        integrity_ok, integrity_msg = self.verify_file_integrity(backup_path)
        results['checks']['file_integrity'] = {
            'passed': integrity_ok,
            'message': integrity_msg
        }
        
        if not integrity_ok:
            results['overall_status'] = 'FAILED'
            results['summary'] = 'File integrity check failed'
            return results
        
        # Calculate checksum
        checksum = self.calculate_checksum(backup_path)
        if checksum:
            results['checksum'] = checksum
        
        # Level 2: Format validation
        format_ok, format_msg = self.verify_backup_format(backup_path, db_type)
        results['checks']['format_validation'] = {
            'passed': format_ok,
            'message': format_msg
        }
        
        if not format_ok:
            results['overall_status'] = 'FAILED'
            results['summary'] = 'Format validation failed'
            return results
        
        # All checks passed
        results['overall_status'] = 'PASSED'
        results['summary'] = 'All verification checks passed'
        
        # Save verification report
        self._save_verification_report(results)
        
        return results
    
    def _save_verification_report(self, results: Dict):
        """Save verification report to file"""
        try:
            backup_name = Path(results['backup_path']).stem
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = self.verification_dir / f"{backup_name}_verification_{timestamp}.json"
            
            with open(report_file, 'w') as f:
                json.dump(results, f, indent=2)
                
        except Exception as e:
            print(f"Warning: Could not save verification report: {e}")
    
    def get_verification_history(self, backup_path: Optional[str] = None) -> list:
        """
        Get verification history for a backup or all backups.
        
        Args:
            backup_path: Optional specific backup to filter by
        
        Returns:
            List of verification results
        """
        history = []
        
        for report_file in self.verification_dir.glob("*.json"):
            try:
                with open(report_file, 'r') as f:
                    report = json.load(f)
                    
                if backup_path is None or report.get('backup_path') == backup_path:
                    history.append(report)
                    
            except Exception as e:
                print(f"Error reading report {report_file}: {e}")
        
        # Sort by timestamp, newest first
        history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return history