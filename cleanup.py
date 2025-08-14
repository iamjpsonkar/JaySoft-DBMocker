#!/usr/bin/env python3
"""
JaySoft-DBMocker Repository Cleanup Script

This script removes all files and directories that should be ignored according to .gitignore
but may still be present in the working directory. Useful for maintaining a clean development
environment and ensuring .gitignore compliance.

Usage:
    python cleanup.py [--dry-run] [--force]

Options:
    --dry-run    Show what would be removed without actually deleting
    --force      Skip confirmation prompt and remove files immediately
    --verbose    Show detailed information about each operation
"""

import os
import sys
import shutil
import argparse
import subprocess
from pathlib import Path
from typing import List, Set


class RepositoryCleanup:
    """Handles cleanup of gitignore-compliant files from the repository."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.repo_root = Path(__file__).parent
        self.removed_count = 0
        self.removed_size = 0
        
        # Files and patterns to remove based on .gitignore
        self.patterns_to_remove = [
            # Python cache files
            "__pycache__",
            "*.pyc", 
            "*.pyo",
            "*.pyd",
            "*$py.class",
            
            # Build/distribution files
            "build/",
            "dist/",
            "*.egg-info/",
            "*.egg",
            "develop-eggs/",
            ".eggs/",
            
            # Database files
            "*.db",
            "*.sqlite",
            "*.sqlite3",
            "demo.db",
            "sample.db", 
            "test.db",
            
            # Configuration files
            "*_config.yaml",
            "*_config.yml",
            "config_local.*",
            ".env.local",
            
            # Log files
            "*.log",
            "logs/",
            
            # OS generated files
            ".DS_Store",
            ".DS_Store?",
            "._*",
            ".Spotlight-V100",
            ".Trashes",
            "ehthumbs.db",
            "Thumbs.db",
            
            # Editor files
            "*.swp",
            "*.swo", 
            "*~",
            ".vscode/",
            "*.sublime-*",
            
            # Backup files
            "*.bak",
            "*.backup",
            "*.tmp",
            
            # Test/coverage reports
            ".coverage",
            ".coverage.*",
            "htmlcov/",
            ".pytest_cache/",
            ".cache",
            "coverage.xml",
            "*.cover",
            
            # Environment files
            ".env",
            ".venv/",
            "env/",
            "venv/",
            "ENV/",
            
            # IDE files
            ".idea/",
            ".spyderproject",
            ".spyproject",
            ".ropeproject",
        ]
    
    def log(self, message: str, level: str = "INFO"):
        """Log message with optional verbosity control."""
        if self.verbose or level in ["WARNING", "ERROR"]:
            prefix = f"[{level}]" if level != "INFO" else ""
            print(f"{prefix} {message}")
    
    def get_ignored_files(self) -> Set[Path]:
        """Get list of files that are currently ignored by git."""
        ignored_files = set()
        
        try:
            # Use git status --ignored to find ignored files
            result = subprocess.run(
                ["git", "status", "--ignored", "--porcelain"],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                check=True
            )
            
            for line in result.stdout.strip().split('\n'):
                if line.startswith('!!'):
                    # Remove the '!! ' prefix and get the file path
                    file_path = line[3:]
                    ignored_files.add(self.repo_root / file_path)
                    
        except subprocess.CalledProcessError:
            self.log("Warning: Could not get git ignored files", "WARNING")
        
        return ignored_files
    
    def find_files_to_remove(self) -> List[Path]:
        """Find all files that match the cleanup patterns."""
        files_to_remove = []
        
        # Start with git-ignored files
        git_ignored = self.get_ignored_files()
        files_to_remove.extend(git_ignored)
        
        # Also manually search for pattern matches
        for pattern in self.patterns_to_remove:
            if pattern.endswith('/'):
                # Directory pattern
                dir_name = pattern.rstrip('/')
                for path in self.repo_root.rglob(dir_name):
                    if path.is_dir() and path not in files_to_remove:
                        files_to_remove.append(path)
            elif '*' in pattern:
                # Glob pattern
                for path in self.repo_root.rglob(pattern):
                    if path not in files_to_remove:
                        files_to_remove.append(path)
            else:
                # Exact filename
                for path in self.repo_root.rglob(pattern):
                    if path not in files_to_remove:
                        files_to_remove.append(path)
        
        # Filter out files that are in .git directory or are essential
        essential_files = {'.gitignore', 'cleanup.py', 'README.md', 'pyproject.toml', 'requirements.txt'}
        
        filtered_files = []
        for path in files_to_remove:
            # Skip if in .git directory
            if '.git' in path.parts:
                continue
            # Skip essential files
            if path.name in essential_files:
                continue
            # Skip if file doesn't exist (may have been removed already)
            if not path.exists():
                continue
            
            filtered_files.append(path)
        
        return sorted(set(filtered_files))
    
    def get_file_size(self, path: Path) -> int:
        """Get size of file or directory in bytes."""
        if path.is_file():
            return path.stat().st_size
        elif path.is_dir():
            total_size = 0
            try:
                for file_path in path.rglob('*'):
                    if file_path.is_file():
                        total_size += file_path.stat().st_size
            except (OSError, PermissionError):
                pass
            return total_size
        return 0
    
    def format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    def remove_file(self, path: Path, dry_run: bool = False) -> bool:
        """Remove a file or directory."""
        try:
            size = self.get_file_size(path)
            
            if dry_run:
                file_type = "directory" if path.is_dir() else "file"
                self.log(f"Would remove {file_type}: {path.relative_to(self.repo_root)} ({self.format_size(size)})")
                return True
            else:
                if path.is_dir():
                    shutil.rmtree(path)
                    self.log(f"Removed directory: {path.relative_to(self.repo_root)} ({self.format_size(size)})")
                else:
                    path.unlink()
                    self.log(f"Removed file: {path.relative_to(self.repo_root)} ({self.format_size(size)})")
                
                self.removed_count += 1
                self.removed_size += size
                return True
                
        except (OSError, PermissionError) as e:
            self.log(f"Error removing {path}: {e}", "ERROR")
            return False
    
    def cleanup(self, dry_run: bool = False, force: bool = False) -> bool:
        """Perform the cleanup operation."""
        print("🧹 JaySoft-DBMocker Repository Cleanup")
        print("=" * 50)
        print()
        
        # Find files to remove
        files_to_remove = self.find_files_to_remove()
        
        if not files_to_remove:
            print("✅ Repository is already clean! No files to remove.")
            return True
        
        # Calculate total size
        total_size = sum(self.get_file_size(path) for path in files_to_remove)
        
        print(f"📋 Found {len(files_to_remove)} items to remove ({self.format_size(total_size)}):")
        print()
        
        # Group files by type for better display
        directories = [p for p in files_to_remove if p.is_dir()]
        files = [p for p in files_to_remove if p.is_file()]
        
        if directories:
            print("📁 Directories to remove:")
            for directory in directories[:10]:  # Show first 10
                dir_size = self.get_file_size(directory)
                print(f"   • {directory.relative_to(self.repo_root)} ({self.format_size(dir_size)})")
            if len(directories) > 10:
                print(f"   ... and {len(directories) - 10} more directories")
            print()
        
        if files:
            print("📄 Files to remove:")
            for file_path in files[:15]:  # Show first 15
                file_size = self.get_file_size(file_path)
                print(f"   • {file_path.relative_to(self.repo_root)} ({self.format_size(file_size)})")
            if len(files) > 15:
                print(f"   ... and {len(files) - 15} more files")
            print()
        
        if dry_run:
            print("🔍 DRY RUN MODE - No files will be actually removed")
            return True
        
        # Confirmation prompt
        if not force:
            print("⚠️  This action cannot be undone!")
            response = input("Do you want to proceed with cleanup? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("❌ Cleanup cancelled by user")
                return False
        
        print()
        print("🗑️  Removing files...")
        print()
        
        # Remove files
        success_count = 0
        for path in files_to_remove:
            if self.remove_file(path, dry_run=False):
                success_count += 1
        
        print()
        print("✅ Cleanup completed!")
        print(f"   • Removed: {success_count}/{len(files_to_remove)} items")
        print(f"   • Space freed: {self.format_size(self.removed_size)}")
        
        if success_count == len(files_to_remove):
            print("   • Repository is now clean and .gitignore compliant! 🎉")
        
        return True


def main():
    """Main entry point for the cleanup script."""
    parser = argparse.ArgumentParser(
        description="Clean up JaySoft-DBMocker repository by removing gitignore-compliant files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cleanup.py                    # Interactive cleanup
  python cleanup.py --dry-run          # See what would be removed
  python cleanup.py --force            # Remove without confirmation
  python cleanup.py --verbose --dry-run # Detailed dry run
        """
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Show what would be removed without actually deleting files'
    )
    
    parser.add_argument(
        '--force', 
        action='store_true',
        help='Skip confirmation prompt and remove files immediately'
    )
    
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='Show detailed information about each operation'
    )
    
    args = parser.parse_args()
    
    # Ensure we're in a git repository
    if not Path('.git').exists():
        print("❌ Error: This script must be run from the root of a git repository")
        sys.exit(1)
    
    # Create cleanup instance and run
    cleanup = RepositoryCleanup(verbose=args.verbose)
    
    try:
        success = cleanup.cleanup(dry_run=args.dry_run, force=args.force)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n❌ Cleanup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
