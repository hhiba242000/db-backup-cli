# src/retention.py
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List
import json


class RetentionPolicy:
    """Manage backup file retention and cleanup"""
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = Path(backup_dir)
    
    def apply_policy(self, keep_daily: int = 7, keep_weekly: int = 4, 
                     keep_monthly: int = 12, dry_run: bool = False) -> Dict:
        """
        Apply retention policy to backups.
        
        Args:
            keep_daily: Number of daily backups to keep (default: 7)
            keep_weekly: Number of weekly backups to keep (default: 4)
            keep_monthly: Number of monthly backups to keep (default: 12)
            dry_run: If True, don't delete files, just show what would be deleted
        
        Returns:
            Dictionary with deletion results
        """
        results = {
            'files_checked': 0,
            'files_kept': 0,
            'files_deleted': 0,
            'space_freed_mb': 0,
            'deleted_files': []
        }
        
        # Get all backup files
        backup_files = []
        for ext in ['*.dump', '*.sql', '*.archive']:
            backup_files.extend(self.backup_dir.glob(ext))
        
        results['files_checked'] = len(backup_files)
        
        # Group by database and type
        grouped = self._group_backups(backup_files)
        
        now = datetime.now()
        
        for group_key, files in grouped.items():
            # Sort by timestamp (newest first)
            files.sort(key=lambda x: x['timestamp'], reverse=True)
            
            keep_files = set()
            
            # Daily retention (last N days)
            daily_cutoff = now - timedelta(days=keep_daily)
            for f in files:
                if f['timestamp'] >= daily_cutoff:
                    keep_files.add(f['path'])
            
            # Weekly retention (one per week for N weeks)
            weekly_cutoff = now - timedelta(weeks=keep_weekly)
            weekly_buckets = {}
            for f in files:
                if f['timestamp'] < daily_cutoff and f['timestamp'] >= weekly_cutoff:
                    week_key = f['timestamp'].strftime('%Y-W%W')
                    if week_key not in weekly_buckets:
                        weekly_buckets[week_key] = f
                        keep_files.add(f['path'])
            
            # Monthly retention (one per month for N months)
            monthly_cutoff = now - timedelta(days=30 * keep_monthly)
            monthly_buckets = {}
            for f in files:
                if f['timestamp'] < weekly_cutoff and f['timestamp'] >= monthly_cutoff:
                    month_key = f['timestamp'].strftime('%Y-%m')
                    if month_key not in monthly_buckets:
                        monthly_buckets[month_key] = f
                        keep_files.add(f['path'])
            
            # Delete files not in keep list
            for f in files:
                if f['path'] not in keep_files:
                    file_size_mb = f['path'].stat().st_size / (1024 * 1024)
                    results['space_freed_mb'] += file_size_mb
                    results['deleted_files'].append(str(f['path']))
                    
                    if not dry_run:
                        f['path'].unlink()
                    
                    results['files_deleted'] += 1
        
        results['files_kept'] = results['files_checked'] - results['files_deleted']
        
        return results
    
    def _group_backups(self, backup_files: List[Path]) -> Dict:
        """Group backup files by database and type"""
        grouped = {}
        
        for file_path in backup_files:
            # Parse filename: database_dbtype_backup_timestamp.ext
            parts = file_path.stem.split('_')
            
            if len(parts) >= 4:
                database = parts[0]
                db_type = parts[1]
                timestamp_str = parts[3]
                
                try:
                    # Parse timestamp
                    timestamp = datetime.strptime(timestamp_str, '%Y%m%d')
                except ValueError:
                    # Try with time included
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
                    except ValueError:
                        continue
                
                group_key = f"{database}_{db_type}"
                
                if group_key not in grouped:
                    grouped[group_key] = []
                
                grouped[group_key].append({
                    'path': file_path,
                    'timestamp': timestamp,
                    'database': database,
                    'db_type': db_type
                })
        
        return grouped
    
    def get_retention_stats(self) -> Dict:
        """Get statistics about current backups"""
        backup_files = []
        for ext in ['*.dump', '*.sql', '*.archive']:
            backup_files.extend(self.backup_dir.glob(ext))
        
        total_size = sum(f.stat().st_size for f in backup_files)
        
        # Group by age
        now = datetime.now()
        by_age = {
            'last_day': 0,
            'last_week': 0,
            'last_month': 0,
            'older': 0
        }
        
        for f in backup_files:
            age = now - datetime.fromtimestamp(f.stat().st_mtime)
            if age.days < 1:
                by_age['last_day'] += 1
            elif age.days < 7:
                by_age['last_week'] += 1
            elif age.days < 30:
                by_age['last_month'] += 1
            else:
                by_age['older'] += 1
        
        return {
            'total_backups': len(backup_files),
            'total_size_mb': total_size / (1024 * 1024),
            'total_size_gb': total_size / (1024 * 1024 * 1024),
            'by_age': by_age
        }