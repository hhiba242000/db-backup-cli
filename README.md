# DB Backup CLI

A flexible and extensible command-line database backup tool that supports multiple database types and storage backends.

## Features

- **Multiple Database Support**: Currently supports PostgreSQL, with extensible architecture for other databases
- **Compression**: Built-in gzip compression with configurable compression levels
- **Flexible Storage**: Local file system storage with pluggable architecture for cloud storage
- **Configuration Management**: YAML configuration files and environment variable support
- **Retention Policies**: Automatic cleanup of old backup files
- **Comprehensive Logging**: Detailed backup process information
- **Cross-Platform**: Works on Linux, macOS, and Windows

## Installation

### From Source

```bash
git clone https://github.com/yourusername/db-backup-cli.git
cd db-backup-cli
pip install -e .
```

### Using pip (when published)

```bash
pip install db-backup-cli
```

## Quick Start

### Basic Usage

Create a backup with minimal configuration:

```bash
db-backup --database-url "postgresql://user:password@localhost:5432/mydb" --output-dir ./backups
```

### Using Configuration File

Create a configuration file `backup_config.yaml`:

```yaml
database_url: "postgresql://user:password@localhost:5432/mydb"
database_type: "postgresql"
output_dir: "./backups"
enable_compression: true
compression_level: 6
backup_name_template: "{database}_{timestamp}.sql"
retention_days: 30
verbose: true
storage_type: "local"
```

Run the backup:

```bash
db-backup --config backup_config.yaml
```

## Configuration Options

### Command Line Arguments

- `--config, -c`: Configuration file path (default: `backup_config.yaml`)
- `--database-url`: Database connection URL
- `--output-dir`: Output directory for backups (default: `./backups`)
- `--compress`: Enable compression
- `--verbose, -v`: Verbose output

### Configuration File Options

```yaml
# Database settings
database_url: "postgresql://user:password@host:port/database"
database_type: "postgresql"  # Currently only postgresql is supported

# Output settings
output_dir: "./backups"
backup_name_template: "{database}_{timestamp}.sql"

# Compression settings
enable_compression: true
compression_level: 6  # 1-9, where 9 is maximum compression

# Retention settings
retention_days: 30  # Delete backups older than 30 days (0 = no cleanup)

# General settings
verbose: false

# Storage settings
storage_type: "local"  # Currently only local is supported
storage_config: {}     # Additional storage-specific configuration
```

### Environment Variables

You can also configure the tool using environment variables:

- `DATABASE_URL`: Database connection URL
- `DATABASE_TYPE`: Database type
- `BACKUP_OUTPUT_DIR`: Output directory
- `BACKUP_COMPRESSION`: Enable compression (true/false)
- `BACKUP_COMPRESSION_LEVEL`: Compression level (1-9)
- `BACKUP_RETENTION_DAYS`: Retention period in days
- `BACKUP_VERBOSE`: Verbose output (true/false)
- `STORAGE_TYPE`: Storage type

## Database Support

### PostgreSQL

For PostgreSQL databases, the tool uses `pg_dump` which must be installed and available in your PATH.

**Connection URL Format:**
```
postgresql://username:password@hostname:port/database_name
```

**Required Dependencies:**
- PostgreSQL client tools (`pg_dump`)
- `psycopg2-binary` Python package

## Architecture

The project follows a modular architecture:

```
src/
├── cli.py              # CLI entry point and argument parsing
├── config.py           # Configuration management
├── adapters/           # Database adapters
│   ├── base.py         # Abstract base class for database adapters
│   └── postgres.py     # PostgreSQL implementation
├── backup/
│   ├── engine.py       # Main backup orchestration
│   └── compressor.py   # Compression logic
└── storage/
    └── local.py        # Local file system storage
```

### Extending the Tool

#### Adding New Database Support

1. Create a new adapter in `src/adapters/` that inherits from `DatabaseAdapter`
2. Implement all abstract methods
3. Register the adapter in the backup engine

#### Adding New Storage Backends

1. Create a new storage handler in `src/storage/`
2. Implement the storage interface
3. Update the configuration to support the new storage type

## Development

### Setting up Development Environment

```bash
git clone https://github.com/yourusername/db-backup-cli.git
cd db-backup-cli
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
black src/
flake8 src/
mypy src/
```

## Troubleshooting

### Common Issues

1. **`pg_dump` not found**: Install PostgreSQL client tools
2. **Permission denied**: Check write permissions for output directory
3. **Connection failed**: Verify database URL and network connectivity

### Debug Mode

Run with verbose output to see detailed information:

```bash
db-backup --verbose --config backup_config.yaml
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

### v0.1.0 (Initial Release)
- PostgreSQL backup support
- Local file system storage
- Gzip compression
- Configuration file and environment variable support
- Automatic retention cleanup
- Cross-platform compatibility

Personal-Note:
dont forget to export:
 export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"