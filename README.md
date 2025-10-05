# Database Backup CLI

A comprehensive command-line tool for backing up and restoring PostgreSQL, MySQL, and MongoDB databases.

## Features

- **Multi-Database Support**: PostgreSQL, MySQL, MongoDB
- **Full & Selective Restore**: Restore entire databases or specific tables/collections
- **Backup Verification**: Automated integrity and format validation
- **Configuration Management**: Profile-based config with .env support
- **Retention Policies**: Automatic cleanup of old backups (daily/weekly/monthly)
- **Slack Notifications**: Real-time alerts for backup operations
- **Comprehensive Logging**: File-based logs with metadata tracking
- **Backup History**: Query past backups and verification reports

## Installation

### Prerequisites

```bash
# Python 3.8+
python --version

# Docker (for testing)
docker --version

# Database client tools
brew install postgresql@15 mysql-client mongodb-database-tools
```

### Setup

```bash
# Clone or create project directory
mkdir db-backup-cli
cd db-backup-cli

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure .env
cp .env.example .env
# Edit .env with your database credentials
```

## Configuration

Create a `.env` file with your database configurations:

```bash
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword
POSTGRES_DATABASE=mydb

# MySQL
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=myuser
MYSQL_PASSWORD=mypassword
MYSQL_DATABASE=mydb

# MongoDB
MONGODB_HOST=127.0.0.1
MONGODB_PORT=27017
MONGODB_USER=myuser
MONGODB_PASSWORD=mypassword
MONGODB_DATABASE=mydb

# Backup settings
BACKUP_DIR=backups

# Slack notifications (optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_ENABLED=true

# AWS S3 (optional)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_S3_BUCKET=your-bucket
S3_UPLOAD_ENABLED=false
```

## Usage

### Basic Backup Operations

```bash
# Backup a database (uses .env configuration)
python3 -m src.cli backup --db-type postgres
python3 -m src.cli backup --db-type mysql
python3 -m src.cli backup --db-type mongodb

# Override configuration
python3 -m src.cli backup --db-type postgres --host prod.example.com --user admin

# Manual output path
python3 -m src.cli backup --db-type postgres --output /path/to/backup.dump
```

### Restore Operations

```bash
# Full restore
python3 -m src.cli restore --db-type postgres --input backups/backup.dump

# Restore to different database
python3 -m src.cli restore --db-type postgres --database other_db --input backups/backup.dump

# Skip confirmation (use carefully)
python3 -m src.cli restore --db-type postgres --input backups/backup.dump --confirm
```

### Selective Restore

```bash
# List tables/collections in backup
python3 -m src.cli list-tables backups/backup.dump --db-type postgres

# Restore specific tables
python3 -m src.cli restore-tables --db-type postgres --input backup.dump --tables users,orders

# Restore specific MongoDB collections
python3 -m src.cli restore-tables --db-type mongodb --input backup.archive --tables users,sessions
```

### Backup All Databases

```bash
# Backup all configured databases
python3 -m src.cli backup-all

# Backup specific databases
python3 -m src.cli backup-all --databases postgres,mysql

# Backup all and apply retention policy
python3 -m src.cli backup-all --apply-retention
```

### Retention Management

```bash
# View current backup statistics
python3 -m src.cli retention-stats

# Clean up old backups (dry run)
python3 -m src.cli cleanup --dry-run

# Apply retention policy
python3 -m src.cli cleanup --keep-daily 7 --keep-weekly 4 --keep-monthly 12

# Retention policy:
# - Keep last 7 daily backups
# - Keep 1 backup per week for last 4 weeks
# - Keep 1 backup per month for last 12 months
```

### Verification

```bash
# Verify backup integrity
python3 -m src.cli verify backups/backup.dump --db-type postgres

# View verification history
python3 -m src.cli verify-history

# View history for specific backup
python3 -m src.cli verify-history --backup-file backups/backup.dump
```

### Monitoring & History

```bash
# View backup history
python3 -m src.cli history

# Filter by database
python3 -m src.cli history --database mydb --limit 20

# View statistics
python3 -m src.cli stats

# List S3 backups
python3 -m src.cli s3-list
```

## Automated Backups

### Cron Setup (Linux/Mac)

Create `backup_scheduled.sh`:

```bash
#!/bin/bash
cd /path/to/db-backup-cli
source venv/bin/activate
python3 -m src.cli backup-all --apply-retention
```

Make executable and add to crontab:

```bash
chmod +x backup_scheduled.sh

# Edit crontab
crontab -e

# Add line (runs daily at 2 AM):
0 2 * * * /path/to/backup_scheduled.sh >> /var/log/backups.log 2>&1
```

### Common Cron Schedules

```bash
0 2 * * *       # Daily at 2 AM
0 2 * * 0       # Weekly on Sunday at 2 AM
0 2 1 * *       # Monthly on 1st at 2 AM
*/30 * * * *    # Every 30 minutes
0 */6 * * *     # Every 6 hours
```

## Architecture

```
db-backup-cli/
├── src/
│   ├── adapters/          # Database-specific implementations
│   │   ├── base.py        # Abstract base class
│   │   ├── postgres.py    # PostgreSQL adapter
│   │   ├── mysql.py       # MySQL adapter
│   │   └── mongodb.py     # MongoDB adapter
│   ├── cli.py             # Command-line interface
│   ├── config.py          # Configuration management
│   ├── logger.py          # Logging system
│   ├── notifications.py   # Slack notifications
│   ├── verification.py    # Backup verification
│   └── retention.py       # Retention policies
├── backups/               # Local backup storage
├── logs/                  # Log files and metadata
├── .env                   # Configuration (not in git)
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Troubleshooting

### Connection Issues

```bash
# Test database connection manually
docker exec -it postgres-test psql -U user -d database

# Check if port is accessible
nc -zv localhost 5432

# Verify credentials in .env
```

### pg_dump/mysqldump not found

```bash
# Add to PATH (Mac)
echo 'export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Verify
pg_dump --version
mysqldump --version
```

### Permission Denied

```bash
# Check file permissions
ls -la backups/

# Fix permissions
chmod 755 backups/
```

## Best Practices

1. **Test restores regularly** - Backups are worthless if you can't restore
2. **Use retention policies** - Don't fill your disk
3. **Enable verification** - Catch corrupted backups early
4. **Monitor Slack alerts** - Know when backups fail
6. **Schedule backups** - Automate with cron
7. **Keep logs** - Review periodically for issues


## Support

For issues or questions:
- Check logs in `logs/`
- Review verification reports in `logs/verifications/`
- Test with `--dry-run` flags
- Verify database connectivity first
