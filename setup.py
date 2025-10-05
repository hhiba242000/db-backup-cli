"""
Setup configuration for db-backup-cli package.
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

# Read requirements
requirements = []
with open('requirements.txt') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
            # Extract just the package name, ignoring version specifiers
            pkg = line.split('>=')[0].split('==')[0].split('<')[0].split('>')[0]
            requirements.append(line)

setup(
    name="db-backup-cli",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A flexible command-line database backup tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/db-backup-cli",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Utilities",
    ],
    python_requires=">=3.8",
    install_requires=[
        "psycopg2-binary>=2.9.0",
        "PyYAML>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "db-backup=src.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)