# src/cli.py
import click
from pathlib import Path
import sys
from datetime import datetime
from .adapters.postgres import PostgreSQLAdapter
from .config import Config
from .logger import BackupLogger, BackupMetadata
from .notifications import SlackNotifier
from .verification import BackupVerifier

verifier = BackupVerifier()
slack_notifier = SlackNotifier()
logger = BackupLogger()
metadata_store = BackupMetadata()

@click.group()
@click.version_option(version='0.1.0')
def cli():
    """
    Database Backup CLI
    
    A powerful tool for backing up and restoring databases.
    """
    pass


@cli.command()
@click.option('--db-type', 
              type=click.Choice(['postgres'], case_sensitive=False),
              default=None,
              help='Type of database (default: from .env)')
@click.option('--host', 
              default=None,
              help='Database host (default: from .env)')
@click.option('--port', 
              type=int,
              default=None,
              help='Database port (default: from .env)')
@click.option('--user', 
              default=None,
              help='Database username (default: from .env)')
@click.option('--password', 
              default=None,
              help='Database password (default: from .env)')
@click.option('--database', 
              default=None,
              help='Database name (default: from .env)')
@click.option('--output', 
              type=click.Path(),
              default=None,
              help='Output file path')
@click.option('--backup-type',
              type=click.Choice(['full', 'incremental', 'differential']),
              default='full',
              help='Type of backup')
@click.option('--output-dir',
              type=click.Path(),
              default=None,
              help='Directory to store backups (default: from .env)')
def backup(db_type, host, port, user, password, database, output, backup_type, output_dir):
    """Backup a database"""
    
    # Load config from .env
    config = Config.get_database_config()
    backup_dir = Config.get_backup_dir()
    
    # Use CLI arguments if provided, otherwise use config
    db_type = db_type or config.get('type')
    host = host or config.get('host')
    port = port or config.get('port')
    user = user or config.get('user')
    password = password or config.get('password')
    database = database or config.get('database')
    output_dir = output_dir or backup_dir
    
    # Validate we have all required parameters
    if not all([db_type, host, user, password, database]):
        click.echo("Error: Missing required parameters!", err=True)
        sys.exit(1)
    
    # Log backup start
    logger.log_backup_start(database, db_type, host)
    
    # Auto-generate output filename if not provided
    if output is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{database}_{db_type}_backup_{timestamp}.dump"
        output = Path(output_dir) / filename
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        click.echo(f"Auto-generated filename: {output}\n")
    
    # Display what we're about to do
    click.echo("=" * 60)
    click.echo("DATABASE BACKUP TOOL")
    click.echo("=" * 60)
    click.echo(f"Database Type: {db_type.upper()}")
    click.echo(f"Host:          {host}")
    click.echo(f"Port:          {port or '(default)'}")
    click.echo(f"User:          {user}")
    click.echo(f"Database:      {database}")
    click.echo(f"Output:        {output}")
    click.echo(f"Backup Type:   {backup_type}")
    click.echo("=" * 60 + "\n")
    
    connection_params = {
        'host': host,
        'user': user,
        'password': password,
        'database': database
    }
    
    if port:
        connection_params['port'] = port
    
    if db_type == 'postgres':
        adapter = PostgreSQLAdapter(connection_params)
    else:
        click.echo(f"Unsupported database type: {db_type}", err=True)
        sys.exit(1)
    
    # Test connection
    click.echo("Testing database connection...")
    connection_ok = adapter.test_connection()
    logger.log_connection_test(database, host, connection_ok)
    
    if not connection_ok:
        click.echo("Failed to connect to database!", err=True)
        logger.log_backup_failure(database, "Connection failed")
        sys.exit(1)
    
    click.echo("Connection successful!\n")
    
    # Get database size
    click.echo("Checking database size...")
    db_size = adapter.get_database_size()
    if db_size > 0:
        size_mb = db_size / (1024 * 1024)
        size_gb = db_size / (1024 * 1024 * 1024)
        
        if size_gb >= 1:
            click.echo(f"   Database size: {size_gb:.2f} GB")
        else:
            click.echo(f"   Database size: {size_mb:.2f} MB")
    click.echo()
    
    # Perform backup
    click.echo("Starting backup operation...")
    result = adapter.backup(str(output), backup_type)
    
    # Show results
    click.echo()
    if result.success:
        # Calculate compression ratio
        compression_ratio = None
        if db_size > 0 and result.size_bytes > 0:
            compression_ratio = db_size / result.size_bytes
        
        # Log success
        logger.log_backup_success(
            database, 
            result.file_path, 
            result.size_mb(), 
            result.duration_seconds,
            compression_ratio
        )
        slack_notifier.send_backup_success(
            database, 
            result.file_path, 
            result.size_mb(), 
            result.duration_seconds,
            compression_ratio
        )

        click.echo("\nVerifying backup...")
        verification_results = verifier.verify_full(result.file_path, db_type)
        
        if verification_results['overall_status'] == 'PASSED':
            click.echo("Backup verification: PASSED")
        else:
            click.echo(f"Warning: Backup verification FAILED - {verification_results['summary']}", err=True)
    

        # Save metadata
        metadata_store.add_backup_record({
            "timestamp": result.timestamp.isoformat(),
            "database": database,
            "db_type": db_type,
            "host": host,
            "file_path": result.file_path,
            "size_bytes": result.size_bytes,
            "duration_seconds": result.duration_seconds,
            "compression_ratio": compression_ratio,
            "backup_type": backup_type,
            "success": True
        })
        
        click.echo("=" * 60)
        click.echo("BACKUP COMPLETED SUCCESSFULLY!")
        click.echo("=" * 60)
        click.echo(f"File:     {result.file_path}")
        click.echo(f"Size:     {result.size_mb():.2f} MB")
        
        if compression_ratio:
            click.echo(f"Compression: {compression_ratio:.1f}x (saved {(1 - 1/compression_ratio)*100:.1f}%)")
        
        click.echo(f"Duration: {result.duration_seconds:.2f} seconds")
        click.echo(f"Time:     {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        click.echo("=" * 60)
    else:
        # Log failure
        logger.log_backup_failure(database, result.error_message)
        slack_notifier.send_backup_failure(database, result.error_message)

        # Save metadata
        metadata_store.add_backup_record({
            "timestamp": result.timestamp.isoformat(),
            "database": database,
            "db_type": db_type,
            "host": host,
            "error": result.error_message,
            "success": False
        })
        
        click.echo("=" * 60)
        click.echo("BACKUP FAILED!")
        click.echo("=" * 60)
        click.echo(f"Error: {result.error_message}")
        click.echo("=" * 60)
        sys.exit(1)

@cli.command()
@click.option('--db-type', 
              type=click.Choice(['postgres'], case_sensitive=False),
              default=None)
@click.option('--host', default=None)
@click.option('--port', type=int, default=None)
@click.option('--user', default=None)
@click.option('--password', default=None)
@click.option('--database', 
              default=None,
              help='Target database name')
@click.option('--input', 
              'backup_file',
              type=click.Path(exists=True),
              required=True,
              help='Backup file to restore from')
@click.option('--confirm',
              is_flag=True,
              help='Skip confirmation prompt')
def restore(db_type, host, port, user, password, database, backup_file, confirm):
    """Restore a database from backup"""
    
    # Load config from .env
    config = Config.get_database_config()
    
    # Use CLI arguments if provided, otherwise use config
    db_type = db_type or config.get('type')
    host = host or config.get('host')
    port = port or config.get('port')
    user = user or config.get('user')
    password = password or config.get('password')
    database = database or config.get('database')
    
    if not all([db_type, host, user, password, database]):
        click.echo("Error: Missing required parameters!", err=True)
        sys.exit(1)
    
    # Log restore start
    logger.log_restore_start(database, backup_file)
    
    click.echo("\n" + "=" * 60)
    click.echo("DATABASE RESTORE TOOL")
    click.echo("=" * 60)
    click.echo(f"WARNING: This will OVERWRITE database '{database}'!")
    click.echo("=" * 60 + "\n")
    
    if not confirm:
        click.confirm(
            f"Are you sure you want to restore to '{database}'?",
            abort=True
        )
    
    connection_params = {
        'host': host,
        'user': user,
        'password': password,
        'database': database
    }
    
    if port:
        connection_params['port'] = port
    
    if db_type == 'postgres':
        adapter = PostgreSQLAdapter(connection_params)
    else:
        click.echo(f"Unsupported database type: {db_type}", err=True)
        sys.exit(1)
    
    click.echo("Testing database connection...")
    if not adapter.test_connection():
        click.echo("Failed to connect to database!", err=True)
        logger.log_restore_failure(database, backup_file, "Connection failed")
        sys.exit(1)
    
    click.echo("Connection successful!\n")
    
    success = adapter.restore(backup_file, database)
    
    if success:
        logger.log_restore_success(database, backup_file)
        slack_notifier.send_restore_success(database, backup_file)
        click.echo("\nDatabase restored successfully!")
    else:
        logger.log_restore_failure(database, backup_file, "Restore operation failed")
        slack_notifier.send_restore_failure(database, backup_file, "Restore operation failed")
        click.echo("\nRestore failed!")
        sys.exit(1)


@cli.command()
@click.option('--database', default=None, help='Filter by database name')
@click.option('--limit', default=10, help='Number of recent backups to show')
def history(database, limit):
    """View backup history"""
    
    recent = metadata_store.get_recent_backups(database, limit)
    
    if not recent:
        click.echo("No backup history found.")
        return
    
    click.echo("\n" + "=" * 80)
    click.echo("BACKUP HISTORY")
    click.echo("=" * 80)
    
    for backup in recent:
        status = "SUCCESS" if backup.get("success") else "FAILED"
        status_color = "green" if backup.get("success") else "red"
        
        click.echo(f"\nTimestamp: {backup.get('timestamp')}")
        click.echo(f"Database:  {backup.get('database')}")
        click.echo(f"Status:    {click.style(status, fg=status_color)}")
        
        if backup.get("success"):
            size_mb = backup.get("size_bytes", 0) / (1024 * 1024)
            click.echo(f"File:      {backup.get('file_path')}")
            click.echo(f"Size:      {size_mb:.2f} MB")
            click.echo(f"Duration:  {backup.get('duration_seconds', 0):.2f}s")
            
            if backup.get("compression_ratio"):
                click.echo(f"Compression: {backup.get('compression_ratio'):.1f}x")
        else:
            click.echo(f"Error:     {backup.get('error')}")
        
        click.echo("-" * 80)


@cli.command()
def stats():
    """Show backup statistics"""
    
    stats = metadata_store.get_backup_stats()
    
    click.echo("\n" + "=" * 60)
    click.echo("BACKUP STATISTICS")
    click.echo("=" * 60)
    click.echo(f"Total Backups:     {stats['total_backups']}")
    click.echo(f"Successful:        {stats['successful']}")
    click.echo(f"Failed:            {stats['failed']}")
    click.echo(f"Total Storage:     {stats['total_size_mb']:.2f} MB")
    click.echo(f"Databases Backed Up: {', '.join(stats['databases'])}")
    click.echo("=" * 60 + "\n")

    
@cli.command()
@click.argument('backup_file', type=click.Path(exists=True))
def list_tables(backup_file):
    """
    List all tables in a backup file
    
    Example: python3 -m src.cli list-tables backups/testdb_postgres_backup_20251002_220000.dump
    """
    
    # Load config
    config = Config.get_database_config()
    db_type = config.get('type')
    
    if db_type == 'postgres':
        # Create a temporary adapter just for listing
        temp_params = {
            'host': 'localhost',
            'port': 5432,
            'user': 'temp',
            'password': 'temp',
            'database': 'temp'
        }
        adapter = PostgreSQLAdapter(temp_params)
    else:
        click.echo("Unsupported database type", err=True)
        sys.exit(1)
    
    click.echo(f"\nTables in backup: {backup_file}")
    click.echo("=" * 60)
    
    tables = adapter.list_tables_in_backup(backup_file)
    
    if tables:
        for i, table in enumerate(tables, 1):
            click.echo(f"{i}. {table}")
        click.echo("=" * 60)
        click.echo(f"Total: {len(tables)} tables\n")
    else:
        click.echo("No tables found or unable to read backup file\n")


@cli.command()
@click.option('--db-type', 
              type=click.Choice(['postgres'], case_sensitive=False),
              default=None)
@click.option('--host', default=None)
@click.option('--port', type=int, default=None)
@click.option('--user', default=None)
@click.option('--password', default=None)
@click.option('--database', 
              default=None,
              help='Target database name')
@click.option('--input', 
              'backup_file',
              type=click.Path(exists=True),
              required=True,
              help='Backup file to restore from')
@click.option('--tables',
              required=True,
              help='Comma-separated list of tables to restore (e.g., users,orders)')
@click.option('--confirm',
              is_flag=True,
              help='Skip confirmation prompt')
def restore_tables(db_type, host, port, user, password, database, backup_file, tables, confirm):
    """
    Restore specific tables from a backup
    
    Example: 
    python3 -m src.cli restore-tables --input backup.dump --tables users,orders
    """
    
    # Load config
    config = Config.get_database_config()
    
    # Use CLI arguments if provided, otherwise use config
    db_type = db_type or config.get('type')
    host = host or config.get('host')
    port = port or config.get('port')
    user = user or config.get('user')
    password = password or config.get('password')
    database = database or config.get('database')
    
    if not all([db_type, host, user, password, database]):
        click.echo("Error: Missing required parameters!", err=True)
        sys.exit(1)
    
    # Parse tables
    table_list = [t.strip() for t in tables.split(',')]
    
    click.echo("\n" + "=" * 60)
    click.echo("SELECTIVE TABLE RESTORE")
    click.echo("=" * 60)
    click.echo(f"WARNING: This will OVERWRITE these tables in '{database}':")
    for table in table_list:
        click.echo(f"  - {table}")
    click.echo("=" * 60 + "\n")
    
    if not confirm:
        click.confirm(
            f"Are you sure you want to restore these tables?",
            abort=True
        )
    
    connection_params = {
        'host': host,
        'user': user,
        'password': password,
        'database': database
    }
    
    if port:
        connection_params['port'] = port
    
    if db_type == 'postgres':
        adapter = PostgreSQLAdapter(connection_params)
    else:
        click.echo(f"Unsupported database type: {db_type}", err=True)
        sys.exit(1)
    
    click.echo("Testing database connection...")
    if not adapter.test_connection():
        click.echo("Failed to connect to database!", err=True)
        sys.exit(1)
    
    click.echo("Connection successful!\n")
    
    success = adapter.restore_selective(backup_file, database, table_list)
    
    if success:
        click.echo(f"\nTables restored successfully!")
    else:
        click.echo("\nRestore failed!")
        sys.exit(1)


@cli.command()
@click.argument('backup_file', type=click.Path(exists=True))
@click.option('--db-type', 
              type=click.Choice(['postgres'], case_sensitive=False),
              default='postgres',
              help='Database type')
def verify(backup_file, db_type):
    """
    Verify a backup file's integrity and validity
    
    Example: python3 -m src.cli verify backups/testdb_postgres_backup_20251002_220000.dump
    """
    
    click.echo(f"\nVerifying backup: {backup_file}")
    click.echo("=" * 60)
    
    results = verifier.verify_full(backup_file, db_type)
    
    # Display results
    click.echo(f"\nDatabase Type: {results['db_type']}")
    click.echo(f"Verification Time: {results['timestamp']}")
    
    if 'checksum' in results:
        click.echo(f"SHA256 Checksum: {results['checksum']}")
    
    click.echo("\nVerification Checks:")
    click.echo("-" * 60)
    
    for check_name, check_result in results['checks'].items():
        status = "PASS" if check_result['passed'] else "FAIL"
        status_color = "green" if check_result['passed'] else "red"
        
        click.echo(f"\n{check_name.replace('_', ' ').title()}:")
        click.echo(f"  Status: {click.style(status, fg=status_color)}")
        click.echo(f"  {check_result['message']}")
    
    click.echo("\n" + "=" * 60)
    overall_color = "green" if results['overall_status'] == 'PASSED' else "red"
    click.echo(f"Overall Status: {click.style(results['overall_status'], fg=overall_color)}")
    click.echo(f"Summary: {results['summary']}")
    click.echo("=" * 60 + "\n")
    
    # Exit with error code if verification failed
    if results['overall_status'] != 'PASSED':
        sys.exit(1)


@cli.command()
@click.option('--backup-file', 
              type=click.Path(exists=True),
              help='Show history for specific backup file')
def verify_history(backup_file):
    """
    Show verification history
    
    Example: python3 -m src.cli verify-history
    """
    
    history = verifier.get_verification_history(backup_file)
    
    if not history:
        click.echo("No verification history found")
        return
    
    click.echo("\nBackup Verification History")
    click.echo("=" * 80)
    
    for report in history:
        status_color = "green" if report['overall_status'] == 'PASSED' else "red"
        
        click.echo(f"\nBackup: {report['backup_path']}")
        click.echo(f"Verified: {report['timestamp']}")
        click.echo(f"Status: {click.style(report['overall_status'], fg=status_color)}")
        click.echo(f"Summary: {report['summary']}")
        
        if 'checksum' in report:
            click.echo(f"Checksum: {report['checksum'][:16]}...")
        
        click.echo("-" * 80)

if __name__ == '__main__':
    cli()