# Repository Cleanup Guide

This guide explains how to use the `cleanup.py` script to maintain a clean development environment for JaySoft:DBMocker.

## Overview

The cleanup script removes files and directories that should be ignored according to `.gitignore` but may still be present in your working directory. This includes:

- Python cache files (`__pycache__/`, `*.pyc`)
- Build artifacts (`*.egg-info/`, `dist/`, `build/`)
- Demo databases (`*.db`, `*.sqlite`)
- Configuration files with sensitive data (`*_config.yaml`)
- Log files (`*.log`)
- OS-generated files (`.DS_Store`, `Thumbs.db`)
- Editor temporary files (`*.swp`, `.vscode/`)
- Test coverage reports (`.coverage`, `htmlcov/`)

## Usage

### Basic Usage

```bash
# Interactive cleanup (asks for confirmation)
python cleanup.py

# See what would be removed without actually deleting
python cleanup.py --dry-run

# Remove files without confirmation
python cleanup.py --force

# Show detailed information during cleanup
python cleanup.py --verbose --dry-run
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be removed without actually deleting files |
| `--force` | Skip confirmation prompt and remove files immediately |
| `--verbose` | Show detailed information about each operation |
| `--help` | Show help message and exit |

## Examples

### Check what needs cleaning
```bash
python cleanup.py --dry-run
```

### Clean up after development
```bash
python cleanup.py
```

### Automated cleanup (for CI/CD)
```bash
python cleanup.py --force
```

### Detailed cleanup analysis
```bash
python cleanup.py --verbose --dry-run
```

## When to Use

### During Development
- After running tests (removes `.pytest_cache/`, `.coverage`)
- After building packages (removes `dist/`, `*.egg-info/`)
- When switching branches
- Before committing code

### CI/CD Pipelines
- Before deployment
- After testing phases
- In cleanup stages

### Team Workflows
- Before sharing development environments
- When onboarding new developers
- Regular maintenance schedules

## What Gets Removed

### Python Files
- `__pycache__/` directories
- `*.pyc`, `*.pyo`, `*.pyd` files
- `*$py.class` files

### Build/Distribution
- `build/`, `dist/` directories
- `*.egg-info/` directories
- `*.egg` files
- `.eggs/`, `develop-eggs/` directories

### Database Files
- `*.db`, `*.sqlite`, `*.sqlite3` files
- `demo.db`, `sample.db`, `test.db`

### Configuration Files
- `*_config.yaml`, `*_config.yml`
- `config_local.*`
- `.env.local`

### Log Files
- `*.log` files
- `logs/` directory

### OS Files
- `.DS_Store` (macOS)
- `Thumbs.db` (Windows)
- `._*` (macOS resource forks)

### Editor Files
- `.vscode/` directory
- `*.swp`, `*.swo` (Vim)
- `*.sublime-*` (Sublime Text)

### Test/Coverage
- `.coverage`, `.coverage.*`
- `htmlcov/` directory
- `.pytest_cache/`

## Safety Features

### Confirmation Prompt
By default, the script asks for confirmation before removing files:
```
⚠️  This action cannot be undone!
Do you want to proceed with cleanup? (y/N):
```

### Essential File Protection
The script never removes essential files:
- `.gitignore`
- `cleanup.py` (itself)
- `README.md`
- `pyproject.toml`
- `requirements.txt`

### Git Directory Protection
Files in the `.git` directory are never touched.

### Dry Run Mode
Always test with `--dry-run` first to see what would be removed.

## Output Examples

### Clean Repository
```bash
$ python cleanup.py --dry-run
🧹 JaySoft:DBMocker Repository Cleanup
==================================================

✅ Repository is already clean! No files to remove.
```

### Files Found for Cleanup
```bash
$ python cleanup.py --dry-run
🧹 JaySoft:DBMocker Repository Cleanup
==================================================

📋 Found 15 items to remove (2.3 MB):

📁 Directories to remove:
   • dbmocker/__pycache__ (1.8 MB)
   • dbmocker/core/__pycache__ (0.3 MB)
   • dbmocker.egg-info (0.1 MB)

📄 Files to remove:
   • demo.db (0.1 MB)
   • gringotts_config.yaml (0.5 KB)
   • order_config.yaml (0.3 KB)
   ... and 9 more files

🔍 DRY RUN MODE - No files will be actually removed
```

### Successful Cleanup
```bash
$ python cleanup.py --force
🧹 JaySoft:DBMocker Repository Cleanup
==================================================

📋 Found 15 items to remove (2.3 MB):
...

🗑️  Removing files...

✅ Cleanup completed!
   • Removed: 15/15 items
   • Space freed: 2.3 MB
   • Repository is now clean and .gitignore compliant! 🎉
```

## Integration

### Pre-commit Hook
Add to `.git/hooks/pre-commit`:
```bash
#!/bin/bash
python cleanup.py --force
```

### Makefile
```makefile
clean:
	python cleanup.py --force

check-clean:
	python cleanup.py --dry-run
```

### CI/CD Pipeline
```yaml
- name: Clean repository
  run: python cleanup.py --force
```

## Troubleshooting

### Permission Errors
If you get permission errors, ensure you have write access to the files and directories being removed.

### Git Not Found
The script requires `git` to be available in your PATH. Install git if not present.

### Script Not Executable
Make the script executable:
```bash
chmod +x cleanup.py
```

### Large Files Warning
For very large files/directories, the script may take some time. Use `--verbose` to see progress.

## Best Practices

1. **Always use `--dry-run` first** to see what will be removed
2. **Commit important changes** before running cleanup
3. **Use `--force` in automated scripts** to avoid prompts
4. **Run cleanup regularly** to maintain a clean environment
5. **Review .gitignore** periodically to ensure it covers all needed patterns

## Contributing

If you find files that should be cleaned but aren't, please:

1. Update the `patterns_to_remove` list in `cleanup.py`
2. Add the pattern to `.gitignore` if needed
3. Test with `--dry-run`
4. Submit a pull request

The cleanup script helps maintain professional code quality and ensures consistent development environments across the team.
