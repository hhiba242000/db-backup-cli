#!/bin/bash

cd /path/to/db-backup-cli
source venv/bin/activate

# Single command does everything
python3 -m src.cli backup-all

# Cleanup old backups
find backups/ -mtime +30 -delete