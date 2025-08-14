#!/usr/bin/env python3
"""Test script to demonstrate new multi-threading/multi-processing and duplicate features."""

import sys
import time
import tempfile
import sqlite3
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.models import GenerationConfig, TableGenerationConfig, ColumnGenerationConfig
from dbmocker.core.parallel_generator import ParallelDataGenerator, ParallelDataInserter


def create_test_database():
    """Create a test SQLite database with multiple tables."""
    # Create temporary database
    db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = db_file.name
    db_file.close()
    
    # Create tables
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) NOT NULL,
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Orders table  
    cursor.execute('''
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
    
    # Products table
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            category VARCHAR(50),
            in_stock BOOLEAN DEFAULT 1
        )
    ''')
    
    conn.commit()
    conn.close()
    
    return db_path


def test_standard_generation(db_path, rows=10000):
    """Test standard generation performance."""
    print(f"\n🔄 Testing STANDARD generation with {rows:,} rows...")
    
    config = DatabaseConfig(
        host="localhost",
        port=1,  # SQLite doesn't use port but validation requires it > 0
        database=db_path,
        username="",
        password="",
        driver="sqlite"
    )
    
    with DatabaseConnection(config) as db_conn:
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        # Create standard generator
        from dbmocker.core.generator import DataGenerator
        gen_config = GenerationConfig(batch_size=1000, max_workers=1)
        generator = DataGenerator(schema, gen_config, db_conn)
        
        start_time = time.time()
        
        # Generate data for users
        users_data = generator.generate_data_for_table('users', rows)
        
        # Generate data for orders  
        orders_data = generator.generate_data_for_table('orders', rows * 2)
        
        # Generate data for products
        products_data = generator.generate_data_for_table('products', rows // 2)
        
        generation_time = time.time() - start_time
        total_rows = len(users_data) + len(orders_data) + len(products_data)
        
        print(f"  ✅ Generated {total_rows:,} rows in {generation_time:.2f}s")
        print(f"  📊 Performance: {total_rows/generation_time:,.0f} rows/second")
        
        return generation_time


def test_parallel_generation(db_path, rows=10000):
    """Test parallel generation performance."""
    print(f"\n🚀 Testing PARALLEL generation with {rows:,} rows...")
    
    config = DatabaseConfig(
        host="localhost",
        port=1,  # SQLite doesn't use port but validation requires it > 0
        database=db_path,
        username="",
        password="",
        driver="sqlite"
    )
    
    with DatabaseConnection(config) as db_conn:
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        # Create parallel generator
        gen_config = GenerationConfig(
            batch_size=2000,
            max_workers=4,
            enable_multiprocessing=True if rows >= 50000 else False,
            max_processes=2,
            rows_per_process=25000
        )
        
        generator = ParallelDataGenerator(schema, gen_config, db_conn)
        
        start_time = time.time()
        
        # Generate data for users
        users_data = generator.generate_data_for_table_parallel('users', rows)
        
        # Generate data for orders
        orders_data = generator.generate_data_for_table_parallel('orders', rows * 2)
        
        # Generate data for products
        products_data = generator.generate_data_for_table_parallel('products', rows // 2)
        
        generation_time = time.time() - start_time
        total_rows = len(users_data) + len(orders_data) + len(products_data)
        
        print(f"  ✅ Generated {total_rows:,} rows in {generation_time:.2f}s")
        print(f"  📊 Performance: {total_rows/generation_time:,.0f} rows/second")
        
        return generation_time


def test_duplicate_generation(db_path, rows=5000):
    """Test duplicate value generation."""
    print(f"\n🔄 Testing DUPLICATE generation with {rows:,} rows...")
    
    config = DatabaseConfig(
        host="localhost",
        port=1,  # SQLite doesn't use port but validation requires it > 0
        database=db_path,
        username="",
        password="",
        driver="sqlite"
    )
    
    with DatabaseConnection(config) as db_conn:
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        # Configure duplicate generation for status columns
        gen_config = GenerationConfig(
            batch_size=1000,
            max_workers=2,
            table_configs={
                'users': TableGenerationConfig(
                    rows_to_generate=rows,
                    column_configs={
                        'status': ColumnGenerationConfig(
                            duplicate_mode="allow_duplicates",
                            duplicate_value="premium"  # All users will have premium status
                        )
                    }
                ),
                'orders': TableGenerationConfig(
                    rows_to_generate=rows,
                    column_configs={
                        'status': ColumnGenerationConfig(
                            duplicate_mode="allow_duplicates",
                            duplicate_value="completed"  # All orders will be completed
                        )
                    }
                )
            }
        )
        
        generator = ParallelDataGenerator(schema, gen_config, db_conn)
        
        start_time = time.time()
        
        # Generate data with duplicates
        users_data = generator.generate_data_for_table_parallel('users', rows)
        orders_data = generator.generate_data_for_table_parallel('orders', rows)
        
        generation_time = time.time() - start_time
        
        # Verify duplicates
        user_statuses = [row['status'] for row in users_data]
        order_statuses = [row['status'] for row in orders_data]
        
        unique_user_statuses = set(user_statuses)
        unique_order_statuses = set(order_statuses)
        
        print(f"  ✅ Generated {len(users_data):,} users and {len(orders_data):,} orders in {generation_time:.2f}s")
        print(f"  🔄 User statuses: {unique_user_statuses} (should be {'premium'})")
        print(f"  🔄 Order statuses: {unique_order_statuses} (should be {'completed'})")
        
        # Verify all values are duplicates as expected
        assert len(unique_user_statuses) == 1 and 'premium' in unique_user_statuses
        assert len(unique_order_statuses) == 1 and 'completed' in unique_order_statuses
        
        print("  ✅ Duplicate generation working correctly!")


def main():
    """Run performance tests."""
    print("🚀 DBMocker Performance Test Suite")
    print("=" * 50)
    
    # Create test database
    db_path = create_test_database()
    print(f"📁 Created test database: {db_path}")
    
    try:
        # Test with smaller dataset first
        print("\n📊 SMALL DATASET TESTS (10K rows)")
        print("-" * 40)
        
        standard_time = test_standard_generation(db_path, 10000)
        parallel_time = test_parallel_generation(db_path, 10000)
        
        speedup = standard_time / parallel_time if parallel_time > 0 else 1
        print(f"\n⚡ Parallel speedup: {speedup:.2f}x faster")
        
        # Test duplicate generation
        test_duplicate_generation(db_path, 5000)
        
        # Test with larger dataset if requested
        if len(sys.argv) > 1 and sys.argv[1] == "large":
            print("\n📊 LARGE DATASET TESTS (100K rows)")
            print("-" * 40)
            
            standard_time_large = test_standard_generation(db_path, 100000)
            parallel_time_large = test_parallel_generation(db_path, 100000)
            
            speedup_large = standard_time_large / parallel_time_large if parallel_time_large > 0 else 1
            print(f"\n⚡ Large dataset parallel speedup: {speedup_large:.2f}x faster")
        
        print("\n🎉 All tests completed successfully!")
        print("\n💡 Usage Tips:")
        print("  • Use multiprocessing for >100K rows per table")
        print("  • Use multithreading for 10K-100K rows per table") 
        print("  • Use duplicate mode for testing edge cases")
        print("  • CLI: dbmocker high-performance --help")
        print("  • GUI: Advanced tab → Performance Settings")
        
    finally:
        # Cleanup
        Path(db_path).unlink(missing_ok=True)
        print(f"\n🧹 Cleaned up test database")


if __name__ == "__main__":
    main()
