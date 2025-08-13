#!/usr/bin/env python3
"""
Quick setup script for JaySoft:DBMocker demo.
This script sets up a demo environment and runs basic examples.
"""

import sys
import subprocess
import os
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stdout:
            print(f"   Output: {e.stdout}")
        if e.stderr:
            print(f"   Error: {e.stderr}")
        return False


def check_python_version():
    """Check if Python version is supported."""
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} is supported")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} is not supported. Please use Python 3.8+")
        return False


def install_dependencies():
    """Install required dependencies."""
    print("\n📦 Installing Dependencies")
    print("=" * 50)
    
    # Install package in development mode
    if not run_command("pip install -e .", "Installing JaySoft:DBMocker"):
        return False
    
    # Install development dependencies
    if not run_command("pip install pytest pytest-cov black flake8 mypy", "Installing development tools"):
        print("⚠️  Development tools installation failed, but core package should work")
    
    return True


def create_demo_database():
    """Create demo database."""
    print("\n🗄️  Creating Demo Database")
    print("=" * 50)
    
    if run_command("python examples/demo_setup.py", "Creating SQLite demo database"):
        return True
    else:
        print("❌ Failed to create demo database")
        return False


def run_tests():
    """Run the test suite."""
    print("\n🧪 Running Tests")
    print("=" * 50)
    
    if run_command("python -m pytest tests/ -v", "Running test suite"):
        return True
    else:
        print("⚠️  Some tests failed, but the package might still work")
        return False


def run_cli_demo():
    """Run CLI demonstration."""
    print("\n💻 CLI Demonstration")
    print("=" * 50)
    
    print("Testing CLI help...")
    if not run_command("python -m dbmocker.cli --help", "Checking CLI help"):
        return False
    
    print("\nAnalyzing demo database...")
    if not run_command(
        "python -m dbmocker.cli analyze --driver sqlite --database demo.db --host '' --port 0 --username '' --password '' --output schema_analysis.json",
        "Analyzing demo database schema"
    ):
        return False
    
    print("\nGenerating sample data...")
    if not run_command(
        "python -m dbmocker.cli generate --driver sqlite --database demo.db --host '' --port 0 --username '' --password '' --rows 100 --dry-run",
        "Generating sample data (dry run)"
    ):
        return False
    
    return True


def run_examples():
    """Run the usage examples."""
    print("\n📋 Running Usage Examples")
    print("=" * 50)
    
    if run_command("python examples/basic_usage.py", "Running usage examples"):
        return True
    else:
        print("⚠️  Some examples failed")
        return False


def main():
    """Main setup function."""
    print("🚀 JaySoft:DBMocker Demo Setup")
    print("=" * 70)
    print("This script will set up JaySoft:DBMocker and run a complete demonstration.")
    print()
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print("❌ Please run this script from the DBMocker root directory")
        sys.exit(1)
    
    print(f"📁 Working directory: {os.getcwd()}")
    
    # Install dependencies
    if not install_dependencies():
        print("❌ Failed to install dependencies")
        sys.exit(1)
    
    # Create demo database
    if not create_demo_database():
        print("❌ Failed to create demo database")
        sys.exit(1)
    
    # Run tests
    print("\n" + "=" * 70)
    choice = input("🧪 Run tests? This will take a few minutes. (y/N): ").strip().lower()
    if choice in ['y', 'yes']:
        run_tests()
    else:
        print("⏭️  Skipping tests")
    
    # Run CLI demo
    if not run_cli_demo():
        print("❌ CLI demonstration failed")
    
    # Run examples
    if not run_examples():
        print("❌ Examples failed")
    
    # Final summary
    print("\n" + "=" * 70)
    print("🎉 JaySoft:DBMocker Demo Setup Complete!")
    print()
    print("📋 What was installed:")
    print("   ✅ JaySoft:DBMocker package with all dependencies")
    print("   ✅ Demo SQLite database with sample schema")
    print("   ✅ Configuration examples")
    print()
    print("🎯 Next steps:")
    print("   • Try the CLI: python -m dbmocker.cli --help")
    print("   • Launch the GUI: python -m dbmocker.cli gui")
    print("   • Edit examples/config_example.yaml for custom settings")
    print("   • Read README.md for detailed documentation")
    print("   • Visit: https://github.com/iamjpsonkar/JaySoft-DBMocker")
    print()
    print("📊 Demo files created:")
    print("   • demo.db - SQLite database with e-commerce schema")
    print("   • schema_analysis.json - Database analysis results")
    print("   • examples/ - Usage examples and configuration")
    print()
    
    if Path("demo.db").exists():
        size = Path("demo.db").stat().st_size
        print(f"💾 Demo database size: {size:,} bytes")
    
    print("\n🎮 Quick commands to try:")
    print("   python -m dbmocker.cli analyze --driver sqlite --database demo.db --host '' --port 0 --username '' --password ''")
    print("   python -m dbmocker.cli generate --driver sqlite --database demo.db --host '' --port 0 --username '' --password '' --rows 1000")
    print("   python examples/basic_usage.py")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
