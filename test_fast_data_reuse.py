#!/usr/bin/env python3
"""
Test script for Fast Data Reuse feature
Demonstrates ultra-fast insertion of millions of records by reusing existing data.
"""

import os
import sys
import sqlite3
import time
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.enhanced_models import (
    EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy,
    create_high_performance_config
)
from dbmocker.core.ultra_fast_processor import create_ultra_fast_processor
from dbmocker.core.fast_data_reuse import create_fast_data_reuser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def print_header(title: str):
    """Print formatted header."""
    print(f"\n{'='*60}")
    print(f"🚀 {title}")
    print(f"{'='*60}")

def print_section(title: str):
    """Print formatted section."""
    print(f"\n{'-'*40}")
    print(f"📊 {title}")
    print(f"{'-'*40}")

def create_test_database_with_existing_data():
    """Create test database with some existing data."""
    db_path = project_root / "fast_reuse_test.db"
    
    # Remove existing database
    if db_path.exists():
        db_path.unlink()
    
    print_section("Creating Test Database with Existing Data")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a realistic user table
    cursor.execute("""
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            age INTEGER CHECK(age >= 18 AND age <= 120),
            country VARCHAR(100),
            city VARCHAR(100),
            occupation VARCHAR(100),
            salary DECIMAL(10,2),
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            profile_data JSON
        )
    """)
    
    # Create orders table with foreign key
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(user_id),
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) DEFAULT 'pending',
            total_amount DECIMAL(10,2) NOT NULL,
            shipping_country VARCHAR(100),
            shipping_city VARCHAR(100),
            payment_method VARCHAR(50),
            notes TEXT
        )
    """)
    
    # Insert some existing data to reuse
    existing_users = [
        ('john_doe', 'john@example.com', 'John', 'Doe', 25, 'USA', 'New York', 'Engineer', 75000.00, 1, '{"skills": ["Python", "SQL"]}'),
        ('jane_smith', 'jane@example.com', 'Jane', 'Smith', 30, 'Canada', 'Toronto', 'Designer', 65000.00, 1, '{"skills": ["Design", "UX"]}'),
        ('bob_wilson', 'bob@example.com', 'Bob', 'Wilson', 35, 'UK', 'London', 'Manager', 85000.00, 1, '{"skills": ["Management", "Strategy"]}'),
        ('alice_brown', 'alice@example.com', 'Alice', 'Brown', 28, 'Australia', 'Sydney', 'Developer', 70000.00, 1, '{"skills": ["JavaScript", "React"]}'),
        ('charlie_davis', 'charlie@example.com', 'Charlie', 'Davis', 32, 'Germany', 'Berlin', 'Analyst', 60000.00, 1, '{"skills": ["Analytics", "Excel"]}'),
        ('diana_miller', 'diana@example.com', 'Diana', 'Miller', 27, 'France', 'Paris', 'Marketing', 55000.00, 1, '{"skills": ["Marketing", "Social Media"]}'),
        ('frank_garcia', 'frank@example.com', 'Frank', 'Garcia', 29, 'Spain', 'Madrid', 'Sales', 50000.00, 1, '{"skills": ["Sales", "Communication"]}'),
        ('grace_martinez', 'grace@example.com', 'Grace', 'Martinez', 26, 'Mexico', 'Mexico City', 'HR', 45000.00, 1, '{"skills": ["HR", "Recruitment"]}'),
        ('henry_lopez', 'henry@example.com', 'Henry', 'Lopez', 31, 'Brazil', 'São Paulo', 'Finance', 65000.00, 1, '{"skills": ["Finance", "Accounting"]}'),
        ('isabel_gonzalez', 'isabel@example.com', 'Isabel', 'Gonzalez', 33, 'Argentina', 'Buenos Aires', 'Operations', 58000.00, 1, '{"skills": ["Operations", "Logistics"]}')
    ]
    
    cursor.executemany("""
        INSERT INTO users (username, email, first_name, last_name, age, country, city, occupation, salary, is_active, profile_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, existing_users)
    
    # Insert some existing orders
    existing_orders = [
        (1, 'completed', 299.99, 'USA', 'New York', 'credit_card', 'First order'),
        (2, 'completed', 149.50, 'Canada', 'Toronto', 'paypal', 'Quick delivery'),
        (3, 'pending', 599.00, 'UK', 'London', 'credit_card', 'Large order'),
        (1, 'completed', 89.99, 'USA', 'New York', 'debit_card', 'Repeat customer'),
        (4, 'shipped', 199.99, 'Australia', 'Sydney', 'credit_card', 'International shipping'),
    ]
    
    cursor.executemany("""
        INSERT INTO orders (user_id, status, total_amount, shipping_country, shipping_city, payment_method, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, existing_orders)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Test database created: {db_path}")
    print(f"📊 Initial data: {len(existing_users)} users, {len(existing_orders)} orders")
    
    return str(db_path)

def test_fast_data_reuse_basic():
    """Test basic fast data reuse functionality."""
    print_header("Fast Data Reuse - Basic Test")
    
    db_path = create_test_database_with_existing_data()
    
    # Connect to database
    db_config = DatabaseConfig(
        host="",
        port=0,
        database=db_path,
        username="",
        password="",
        driver="sqlite"
    )
    
    with DatabaseConnection(db_config) as db_conn:
        db_conn.connect()
        
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema("test_db")
        
        # Create fast data reuser
        fast_reuser = create_fast_data_reuser(db_conn, schema, sample_size=10000, fast_mode=True)
        
        # Test basic functionality
        print_section("Testing Basic Fast Data Reuse")
        
        # Prepare tables
        for table_name in ['users', 'orders']:
            print(f"🔧 Preparing {table_name} for fast data reuse...")
            success = fast_reuser.prepare_table_for_fast_insertion(table_name)
            
            if success:
                stats = fast_reuser.get_reuse_statistics(table_name)
                print(f"✅ {table_name} prepared:")
                print(f"   📊 Existing rows: {stats['total_existing_rows']}")
                print(f"   📝 Sampled rows: {stats['sampled_rows']}")
                print(f"   🔄 Reusable rows: {stats['reusable_rows']}")
                print(f"   📈 Reuse ratio: {stats['reuse_ratio']:.1%}")
            else:
                print(f"❌ {table_name} could not be prepared")

def test_fast_data_reuse_performance():
    """Test fast data reuse performance with different scales."""
    print_header("Fast Data Reuse - Performance Test")
    
    db_path = project_root / "fast_reuse_test.db"
    
    # Connect to database
    db_config = DatabaseConfig(
        host="",
        port=0,
        database=str(db_path),
        username="",
        password="",
        driver="sqlite"
    )
    
    with DatabaseConnection(db_config) as db_conn:
        db_conn.connect()
        
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema("test_db")
        
        # Create fast data reuser
        fast_reuser = create_fast_data_reuser(db_conn, schema, sample_size=10000, fast_mode=True)
        
        # Performance tests with different scales
        test_cases = [
            {'rows': 10000, 'label': 'Small (10K)'},
            {'rows': 50000, 'label': 'Medium (50K)'},
            {'rows': 200000, 'label': 'Large (200K)'},
            {'rows': 1000000, 'label': 'Very Large (1M)'}
        ]
        
        print_section("Performance Comparison Tests")
        
        for test_case in test_cases:
            rows = test_case['rows']
            label = test_case['label']
            
            print(f"\n🎯 Testing {label} - {rows:,} rows")
            
            # Progress callback for every 1000 records
            progress_count = 0
            def progress_callback(table, current, total):
                nonlocal progress_count
                if current % 1000 == 0 and current != progress_count:
                    progress_count = current
                    percentage = (current / total) * 100
                    print(f"  📊 Progress: {current:,}/{total:,} ({percentage:.1f}%)")
            
            try:
                start_time = time.time()
                
                # Fast insert using data reuse
                result = fast_reuser.fast_insert_millions('orders', rows, progress_callback)
                
                end_time = time.time()
                total_time = end_time - start_time
                rate = result['rows_inserted'] / total_time if total_time > 0 else 0
                
                print(f"  ✅ Completed: {result['rows_inserted']:,} rows")
                print(f"  ⏱️  Time: {total_time:.2f}s")
                print(f"  🚄 Rate: {rate:,.0f} rows/s")
                print(f"  📝 Method: {result['method']}")
                
                # Performance assessment
                if rate > 100000:
                    print(f"  🚀 EXCELLENT: >100K rows/s")
                elif rate > 50000:
                    print(f"  ✅ VERY GOOD: >50K rows/s")
                elif rate > 25000:
                    print(f"  👍 GOOD: >25K rows/s")
                else:
                    print(f"  ⚠️  MODERATE: Consider optimization")
                    
            except Exception as e:
                print(f"  ❌ Failed: {e}")

def test_ultra_fast_processor_integration():
    """Test integration with UltraFastProcessor."""
    print_header("Ultra-Fast Processor Integration Test")
    
    db_path = project_root / "fast_reuse_test.db"
    
    # Connect to database
    db_config = DatabaseConfig(
        host="",
        port=0,
        database=str(db_path),
        username="",
        password="",
        driver="sqlite"
    )
    
    with DatabaseConnection(db_config) as db_conn:
        db_conn.connect()
        
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema("test_db")
        
        print_section("Testing Ultra-Fast Processor with Fast Data Reuse")
        
        # Create configuration with fast data reuse enabled
        config = create_high_performance_config(
            target_tables={'orders': 500000},
            performance_mode=PerformanceMode.ULTRA_HIGH,
            enable_duplicates=True,
            duplicate_strategy=DuplicateStrategy.FAST_DATA_REUSE,
            seed=42
        )
        
        # Override fast data reuse settings
        config.duplicates.enable_fast_data_reuse = True
        config.duplicates.data_reuse_sample_size = 10000
        config.duplicates.data_reuse_probability = 0.95
        config.duplicates.fast_insertion_mode = True
        config.duplicates.progress_update_interval = 1000
        
        print(f"🔧 Configuration:")
        print(f"   Performance Mode: {config.performance.performance_mode}")
        print(f"   Duplicate Strategy: {config.duplicates.global_duplicate_strategy}")
        print(f"   Fast Data Reuse: {config.duplicates.enable_fast_data_reuse}")
        print(f"   Sample Size: {config.duplicates.data_reuse_sample_size:,}")
        print(f"   Reuse Probability: {config.duplicates.data_reuse_probability:.1%}")
        
        # Create ultra-fast processor
        processor = create_ultra_fast_processor(schema, config, db_conn)
        
        # Progress tracking
        def progress_callback(table, current, total):
            if current % 5000 == 0:  # Update every 5K for this test
                percentage = (current / total) * 100
                elapsed = time.time() - start_time
                rate = current / elapsed if elapsed > 0 else 0
                print(f"  📈 Progress: {current:,}/{total:,} ({percentage:.1f}%) | Rate: {rate:,.0f} rows/s")
        
        # Process 500K records
        target_rows = 500000
        print(f"\n🚀 Processing {target_rows:,} records with integrated fast data reuse...")
        
        start_time = time.time()
        report = processor.process_millions_of_records('orders', target_rows, progress_callback)
        total_time = time.time() - start_time
        
        print(f"\n🎉 Integration test completed!")
        print(f"📊 Results:")
        print(f"   📈 Rows generated: {report.total_rows_generated:,}")
        print(f"   ⏱️  Total time: {total_time:.2f}s")
        print(f"   🚄 Average rate: {report.average_rows_per_second:,.0f} rows/s")

def test_constraint_respect():
    """Test that constraints are properly respected."""
    print_header("Constraint Respect Test")
    
    db_path = project_root / "fast_reuse_test.db"
    
    # Connect to database
    db_config = DatabaseConfig(
        host="",
        port=0,
        database=str(db_path),
        username="",
        password="",
        driver="sqlite"
    )
    
    with DatabaseConnection(db_config) as db_conn:
        db_conn.connect()
        
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema("test_db")
        
        print_section("Testing Constraint Respect")
        
        # Create fast data reuser
        fast_reuser = create_fast_data_reuser(db_conn, schema, sample_size=1000, fast_mode=True)
        
        # Check what columns can be safely reused
        fast_reuser.prepare_table_for_fast_insertion('users')
        stats = fast_reuser.get_reuse_statistics('users')
        
        print(f"📊 Users table analysis:")
        print(f"   Total columns: {len(schema.get_table('users').columns)}")
        print(f"   Reusable columns: {len(stats.get('unique_values_count', {}))}")
        print(f"   Columns with data: {list(stats.get('unique_values_count', {}).keys())}")
        
        # Generate a small batch to verify constraints
        print(f"\n🧪 Testing constraint compliance with 1000 records...")
        
        try:
            result = fast_reuser.fast_insert_millions('users', 1000)
            print(f"✅ Successfully inserted {result['rows_inserted']} rows")
            print(f"   No constraint violations detected")
            
            # Check for any duplicate primary keys or unique values
            unique_violations = db_conn.execute_query("""
                SELECT user_id, COUNT(*) as count 
                FROM users 
                GROUP BY user_id 
                HAVING COUNT(*) > 1
            """)
            
            if unique_violations:
                print(f"❌ Found {len(unique_violations)} primary key violations!")
            else:
                print(f"✅ No primary key violations found")
                
            email_violations = db_conn.execute_query("""
                SELECT email, COUNT(*) as count 
                FROM users 
                GROUP BY email 
                HAVING COUNT(*) > 1
            """)
            
            if email_violations:
                print(f"❌ Found {len(email_violations)} email uniqueness violations!")
            else:
                print(f"✅ No email uniqueness violations found")
                
        except Exception as e:
            print(f"❌ Constraint test failed: {e}")

def main():
    """Main test function."""
    print_header("DBMocker Fast Data Reuse Test Suite")
    
    print("🎯 This test suite demonstrates:")
    print("   • Ultra-fast data insertion using existing data")
    print("   • Constraint-aware data reuse")
    print("   • Performance scaling to millions of records")
    print("   • Integration with UltraFastProcessor")
    print("   • Progress tracking every 1000 records")
    
    # Run test suite
    try:
        test_fast_data_reuse_basic()
        test_constraint_respect()
        test_fast_data_reuse_performance()
        test_ultra_fast_processor_integration()
        
        print_header("Test Suite Completed Successfully!")
        
        # Final performance summary
        db_path = project_root / "fast_reuse_test.db"
        if db_path.exists():
            size_mb = db_path.stat().st_size / (1024 * 1024)
            print(f"📊 Final database size: {size_mb:.1f}MB")
            print(f"📁 Database location: {db_path}")
            
            # Get final row counts
            db_config = DatabaseConfig(host="", port=0, database=str(db_path), username="", password="", driver="sqlite")
            with DatabaseConnection(db_config) as db_conn:
                db_conn.connect()
                
                user_count = db_conn.execute_query("SELECT COUNT(*) FROM users")[0][0]
                order_count = db_conn.execute_query("SELECT COUNT(*) FROM orders")[0][0]
                
                print(f"📈 Final counts: {user_count:,} users, {order_count:,} orders")
        
        print(f"\n🧹 To clean up: rm {db_path}")
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
