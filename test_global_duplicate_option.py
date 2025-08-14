#!/usr/bin/env python3
"""
Test Global Duplicate Option Functionality
Demonstrates the new global duplicate control options with constraint awareness.
"""

import os
import sys
import sqlite3
import subprocess
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def activate_conda_environment():
    """Activate conda environment if available."""
    try:
        # Check if conda is available
        result = subprocess.run(['conda', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Conda is available")
            # Try to activate MyVenv environment
            try:
                subprocess.run(['conda', 'activate', 'MyVenv'], check=True, shell=True)
                print("✅ Conda environment 'MyVenv' activated")
                return True
            except subprocess.CalledProcessError:
                print("⚠️  Conda environment 'MyVenv' not found, using current environment")
                return False
        else:
            print("⚠️  Conda not available, using current Python environment")
            return False
    except FileNotFoundError:
        print("⚠️  Conda not found, using current Python environment")
        return False

def create_test_database():
    """Create a test database with various constraint scenarios."""
    db_path = Path(__file__).parent / "test_global_duplicates.db"
    
    # Remove existing database
    if db_path.exists():
        db_path.unlink()
    
    # Create database with various constraint types
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Create tables with different constraint scenarios
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE,
            status VARCHAR(20),  -- No constraints - can duplicate
            age INTEGER,         -- No constraints - can duplicate
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            order_status VARCHAR(20),  -- No constraints - can duplicate
            priority VARCHAR(10),      -- No constraints - can duplicate
            amount DECIMAL(10,2),
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(100) NOT NULL,
            category VARCHAR(50),      -- No constraints - can duplicate
            rating DECIMAL(3,2),       -- No constraints - can duplicate
            price DECIMAL(10,2),
            description TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Test database created: {db_path}")
    return str(db_path)

def run_test_case(test_name, command_args, description):
    """Run a test case and show results."""
    print(f"\n" + "="*60)
    print(f"🧪 {test_name}")
    print(f"📝 {description}")
    print("="*60)
    
    # Build command
    base_cmd = [
        "python", "-m", "dbmocker.cli", "generate",
        "--driver", "sqlite",
        "--host", "localhost",
        "--port", "1",
        "--username", "",
        "--password", "",
        "--rows", "25",  # Small number for demonstration
        "--batch-size", "10",
        "--dry-run"  # Use dry-run to show configuration
    ]
    
    full_cmd = base_cmd + command_args
    
    print(f"💻 Command: {' '.join(full_cmd)}")
    print()
    
    # Run command
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, cwd=project_root)
        
        if result.returncode == 0:
            print("✅ Command executed successfully!")
            print("\n📊 Output:")
            print(result.stdout)
        else:
            print("❌ Command failed!")
            print("\n🚨 Error output:")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ Exception occurred: {e}")

def inspect_database_content(db_path):
    """Inspect database content to show actual duplicate behavior."""
    print(f"\n" + "="*60)
    print("🔍 DATABASE CONTENT INSPECTION")
    print("="*60)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check users table
        print("\n👥 USERS TABLE:")
        cursor.execute("SELECT status, COUNT(*) as count FROM users GROUP BY status ORDER BY count DESC")
        status_counts = cursor.fetchall()
        print("Status distribution:")
        for status, count in status_counts:
            print(f"  {status}: {count} records")
        
        # Check orders table
        print("\n📦 ORDERS TABLE:")
        cursor.execute("SELECT order_status, COUNT(*) as count FROM orders GROUP BY order_status ORDER BY count DESC")
        order_status_counts = cursor.fetchall()
        print("Order status distribution:")
        for status, count in order_status_counts:
            print(f"  {status}: {count} records")
            
        cursor.execute("SELECT priority, COUNT(*) as count FROM orders GROUP BY priority ORDER BY count DESC")
        priority_counts = cursor.fetchall()
        print("Priority distribution:")
        for priority, count in priority_counts:
            print(f"  {priority}: {count} records")
        
        # Check products table
        print("\n🛍️  PRODUCTS TABLE:")
        cursor.execute("SELECT category, COUNT(*) as count FROM products GROUP BY category ORDER BY count DESC")
        category_counts = cursor.fetchall()
        print("Category distribution:")
        for category, count in category_counts:
            print(f"  {category}: {count} records")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error inspecting database: {e}")

def main():
    """Main test function."""
    print("🚀 Testing Global Duplicate Option Functionality")
    print("===============================================")
    
    # Activate conda environment
    activate_conda_environment()
    
    # Create test database
    db_path = create_test_database()
    
    # Test Case 1: Generate New Only (Default)
    run_test_case(
        "Test 1: Generate New Only (Default)",
        [
            "--database", db_path,
            "--generate-new-only"
        ],
        "Default behavior - all values should be unique where constraints allow"
    )
    
    # Test Case 2: Allow Duplicates Globally
    run_test_case(
        "Test 2: Allow Duplicates Globally",
        [
            "--database", db_path,
            "--duplicate-allowed",
            "--global-duplicate-mode", "allow_duplicates"
        ],
        "Allow duplicates for columns without constraints - should see repeated values"
    )
    
    # Test Case 3: Smart Duplicates Globally
    run_test_case(
        "Test 3: Smart Duplicates Globally",
        [
            "--database", db_path,
            "--duplicate-allowed",
            "--global-duplicate-mode", "smart_duplicates",
            "--global-duplicate-probability", "0.7",
            "--global-max-duplicate-values", "5"
        ],
        "Smart duplicates with 70% probability and max 5 unique values per column"
    )
    
    # Test Case 4: Actual Data Generation with Duplicates (not dry-run)
    print(f"\n" + "="*60)
    print("🧪 Test 4: Actual Data Generation with Duplicates")
    print("📝 Generate real data to inspect duplicate behavior")
    print("="*60)
    
    actual_cmd = [
        "python", "-m", "dbmocker.cli", "generate",
        "--driver", "sqlite",
        "--host", "localhost",
        "--port", "1",
        "--database", db_path,
        "--username", "",
        "--password", "",
        "--rows", "20",
        "--batch-size", "5",
        "--duplicate-allowed",
        "--global-duplicate-mode", "allow_duplicates",
        "--truncate"  # Clear existing data
    ]
    
    print(f"💻 Command: {' '.join(actual_cmd)}")
    print()
    
    try:
        result = subprocess.run(actual_cmd, capture_output=True, text=True, cwd=project_root)
        
        if result.returncode == 0:
            print("✅ Data generation completed successfully!")
            print("\n📊 Output:")
            print(result.stdout)
            
            # Inspect the generated data
            inspect_database_content(db_path)
            
        else:
            print("❌ Data generation failed!")
            print("\n🚨 Error output:")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
    
    print(f"\n✅ Test completed! Database saved at: {db_path}")
    print("\n📋 Summary:")
    print("  • Test 1: Demonstrates generate-new-only mode (default)")
    print("  • Test 2: Shows global duplicate allowance for constraint-free columns")
    print("  • Test 3: Demonstrates smart duplicate mode with controlled variety")
    print("  • Test 4: Shows actual data generation with duplicate inspection")

if __name__ == "__main__":
    main()
