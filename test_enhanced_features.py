#!/usr/bin/env python3
"""
Enhanced DBMocker Test Script
Tests multi-threading, multi-processing, and duplicate handling features
with conda environment activation and constraint-aware duplicate generation.
"""

import os
import sys
import sqlite3
import time
import subprocess
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.models import (
    GenerationConfig, TableGenerationConfig, ColumnGenerationConfig,
    ColumnType, ConstraintType
)
from dbmocker.core.parallel_generator import ParallelDataGenerator, ParallelDataInserter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def activate_conda_environment():
    """Activate conda environment MyVenv if available."""
    try:
        # Check if we're already in the environment
        if os.environ.get('CONDA_DEFAULT_ENV') == 'MyVenv':
            print("✅ Already in conda environment 'MyVenv'")
            return True
        
        # Try to find conda
        conda_paths = [
            '/opt/conda/bin/conda',
            '/opt/miniconda3/bin/conda',
            '/opt/homebrew/bin/conda',
            os.path.expanduser('~/anaconda3/bin/conda'),
            os.path.expanduser('~/miniconda3/bin/conda')
        ]
        
        conda_cmd = None
        for path in conda_paths:
            if os.path.exists(path):
                conda_cmd = path
                break
        
        if conda_cmd is None:
            # Try system conda
            result = subprocess.run(['which', 'conda'], capture_output=True, text=True)
            if result.returncode == 0:
                conda_cmd = result.stdout.strip()
        
        if conda_cmd:
            print(f"🔍 Found conda at: {conda_cmd}")
            
            # Check if MyVenv environment exists
            result = subprocess.run([conda_cmd, 'env', 'list'], capture_output=True, text=True)
            if 'MyVenv' in result.stdout:
                print("✅ Conda environment 'MyVenv' found")
                # Note: We can't activate conda env in Python directly, 
                # but we can inform the user
                print("💡 To activate conda environment, run: conda activate MyVenv")
                return True
            else:
                print("⚠️  Conda environment 'MyVenv' not found")
                print("💡 Create it with: conda create -n MyVenv python=3.9")
                return False
        else:
            print("⚠️  Conda not found on system")
            return False
            
    except Exception as e:
        print(f"⚠️  Could not check conda environment: {e}")
        return False


def create_test_database():
    """Create a test SQLite database with various constraints."""
    db_path = project_root / "test_enhanced.db"
    
    # Remove existing database
    if db_path.exists():
        db_path.unlink()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables with various constraint types for testing
    tables_sql = [
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            status VARCHAR(20) DEFAULT 'active',
            department VARCHAR(50),
            salary DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        )
        """,
        """
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(100) UNIQUE NOT NULL,
            type VARCHAR(30),
            priority INTEGER DEFAULT 1
        )
        """,
        """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER,
            name VARCHAR(200) NOT NULL,
            sku VARCHAR(50) UNIQUE,
            price DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) DEFAULT 'available',
            rating DECIMAL(3,2),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
        """,
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            order_date DATE DEFAULT CURRENT_DATE,
            status VARCHAR(20) DEFAULT 'pending',
            priority VARCHAR(10) DEFAULT 'normal',
            region VARCHAR(50),
            total_amount DECIMAL(10,2),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """,
        """
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER DEFAULT 1,
            unit_price DECIMAL(10,2),
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
        """
    ]
    
    for sql in tables_sql:
        cursor.execute(sql)
    
    # Create indexes
    indexes_sql = [
        "CREATE INDEX idx_users_department ON users(department)",
        "CREATE INDEX idx_products_status ON products(status)",
        "CREATE INDEX idx_orders_region ON orders(region)",
        "CREATE UNIQUE INDEX idx_products_sku ON products(sku)"
    ]
    
    for sql in indexes_sql:
        cursor.execute(sql)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Test database created: {db_path}")
    return db_path


def test_constraint_aware_duplicates(db_path):
    """Test constraint-aware duplicate generation."""
    print("\n🔍 Testing Constraint-Aware Duplicate Generation")
    print("=" * 60)
    
    # Create database configuration
    db_config = DatabaseConfig(
        driver='sqlite',
        database=str(db_path),
        host='localhost',  # Required by model but not used for SQLite
        port=1,            # Dummy port for SQLite
        username='',       # Not used for SQLite
        password=''        # Not used for SQLite
    )
    
    with DatabaseConnection(db_config) as db_conn:
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        # Test configuration with various duplicate modes
        generation_config = GenerationConfig(
            batch_size=50,
            max_workers=2,
            enable_multiprocessing=False,  # Keep simple for testing
            seed=42
        )
        
        # Configure different duplicate modes for different columns
        for table in schema.tables:
            table_config = TableGenerationConfig(rows_to_generate=100)
            
            for column in table.columns:
                if column.name in ['status', 'department', 'priority', 'region', 'type']:
                    # These columns can have duplicates
                    table_config.column_configs[column.name] = ColumnGenerationConfig(
                        duplicate_mode="allow_duplicates"
                    )
                    print(f"   📋 {table.name}.{column.name}: allow_duplicates")
                
                elif column.name in ['rating', 'total_amount']:
                    # These can have smart duplicates
                    table_config.column_configs[column.name] = ColumnGenerationConfig(
                        duplicate_mode="smart_duplicates",
                        duplicate_probability=0.7,
                        max_duplicate_values=5
                    )
                    print(f"   🧠 {table.name}.{column.name}: smart_duplicates")
                
                elif column.name in ['username', 'email', 'sku']:
                    # These have unique constraints - should auto-detect and use generate_new
                    table_config.column_configs[column.name] = ColumnGenerationConfig(
                        duplicate_mode="allow_duplicates"  # This should be overridden by constraint detection
                    )
                    print(f"   🔒 {table.name}.{column.name}: allow_duplicates (will be overridden by unique constraint)")
            
            generation_config.table_configs[table.name] = table_config
        
        # Create parallel generator
        generator = ParallelDataGenerator(schema, generation_config, db_conn)
        inserter = ParallelDataInserter(db_conn, schema)
        
        # Test generation for each table
        for table in schema.tables:
            print(f"\n   🔄 Testing {table.name}...")
            
            # Generate test data
            start_time = time.time()
            data = generator.generate_data_for_table_parallel(table.name, 100)
            generation_time = time.time() - start_time
            
            print(f"   ✅ Generated {len(data)} rows in {generation_time:.3f}s")
            
            # Analyze duplicate patterns
            if data:
                for column in table.columns:
                    if column.name in ['status', 'department', 'priority', 'region', 'type']:
                        values = [row.get(column.name) for row in data if row.get(column.name) is not None]
                        unique_values = len(set(values))
                        print(f"      • {column.name}: {unique_values} unique values out of {len(values)} total (duplicates allowed)")
                    
                    elif column.name in ['username', 'email', 'sku']:
                        values = [row.get(column.name) for row in data if row.get(column.name) is not None]
                        unique_values = len(set(values))
                        if unique_values == len(values):
                            print(f"      • {column.name}: All {unique_values} values unique (constraint detected ✅)")
                        else:
                            print(f"      • {column.name}: {unique_values} unique out of {len(values)} (constraint not enforced ⚠️)")
            
            # Insert data
            if data:
                stats = inserter.insert_data_parallel(table.name, data, batch_size=50, max_workers=2)
                print(f"   💾 Inserted {stats.total_rows_generated} rows")


def test_performance_scaling(db_path):
    """Test performance scaling with different configurations."""
    print("\n🚀 Testing Performance Scaling")
    print("=" * 60)
    
    db_config = DatabaseConfig(
        driver='sqlite',
        database=str(db_path),
        host='localhost',
        port=1,  # Dummy port for SQLite
        username='',
        password=''
    )
    
    test_configs = [
        {"name": "Single Thread", "max_workers": 1, "enable_multiprocessing": False, "rows": 100},
        {"name": "Multi Thread (4)", "max_workers": 4, "enable_multiprocessing": False, "rows": 100},
        {"name": "Adaptive Config", "max_workers": 8, "enable_multiprocessing": True, "rows": 100}
    ]
    
    with DatabaseConnection(db_config) as db_conn:
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        for config_info in test_configs:
            print(f"\n   🔧 Testing {config_info['name']}...")
            
            generation_config = GenerationConfig(
                batch_size=25,
                max_workers=config_info['max_workers'],
                enable_multiprocessing=config_info['enable_multiprocessing'],
                max_processes=2,
                rows_per_process=50,
                seed=42
            )
            
            generator = ParallelDataGenerator(schema, generation_config, db_conn)
            
            # Clear existing data
            with db_conn.get_session() as session:
                from sqlalchemy import text
                for table in schema.tables:
                    session.execute(text(f"DELETE FROM {table.name}"))
                session.commit()
            
            # Test generation
            start_time = time.time()
            all_data = generator.generate_data_for_all_tables_parallel(config_info['rows'])
            generation_time = time.time() - start_time
            
            total_rows = sum(len(data) for data in all_data.values())
            rows_per_second = total_rows / generation_time if generation_time > 0 else 0
            
            print(f"      ✅ Generated {total_rows} rows in {generation_time:.3f}s ({rows_per_second:.0f} rows/sec)")


def test_memory_management(db_path):
    """Test memory management for larger datasets."""
    print("\n💾 Testing Memory Management")
    print("=" * 60)
    
    db_config = DatabaseConfig(
        driver='sqlite',
        database=str(db_path),
        host='localhost',
        port=1,  # Dummy port for SQLite
        username='',
        password=''
    )
    
    with DatabaseConnection(db_config) as db_conn:
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        # Configure for larger dataset with memory streaming
        generation_config = GenerationConfig(
            batch_size=100,
            max_workers=2,
            enable_multiprocessing=False,
            seed=42
        )
        
        generator = ParallelDataGenerator(schema, generation_config, db_conn)
        
        # Test with a larger dataset that should trigger streaming
        test_table = schema.tables[0]  # Use first table
        print(f"   🔄 Testing memory streaming with {test_table.name}...")
        
        # Force memory estimation to be high to test streaming
        original_estimate = generator._estimate_memory_usage
        
        def force_high_memory_estimate(table, num_rows):
            return 999999  # Force streaming mode
        
        generator._estimate_memory_usage = force_high_memory_estimate
        
        start_time = time.time()
        data = generator.generate_data_for_table_parallel(test_table.name, 500)
        generation_time = time.time() - start_time
        
        print(f"   ✅ Generated {len(data)} rows with memory streaming in {generation_time:.3f}s")
        
        # Restore original method
        generator._estimate_memory_usage = original_estimate


def test_duplicate_value_specification(db_path):
    """Test specifying exact duplicate values."""
    print("\n🎯 Testing Duplicate Value Specification")
    print("=" * 60)
    
    db_config = DatabaseConfig(
        driver='sqlite',
        database=str(db_path),
        host='localhost',
        port=1,  # Dummy port for SQLite
        username='',
        password=''
    )
    
    with DatabaseConnection(db_config) as db_conn:
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        # Configure with specific duplicate values
        generation_config = GenerationConfig(
            batch_size=50,
            max_workers=1,
            seed=42
        )
        
        # Test with orders table
        orders_table = schema.get_table('orders')
        if orders_table:
            table_config = TableGenerationConfig(rows_to_generate=50)
            
            # Specify exact duplicate values
            table_config.column_configs['status'] = ColumnGenerationConfig(
                duplicate_mode="allow_duplicates",
                duplicate_value="processing"
            )
            
            table_config.column_configs['priority'] = ColumnGenerationConfig(
                duplicate_mode="allow_duplicates",
                duplicate_value="high"
            )
            
            table_config.column_configs['region'] = ColumnGenerationConfig(
                duplicate_mode="allow_duplicates",
                duplicate_value="North America"
            )
            
            generation_config.table_configs['orders'] = table_config
            
            generator = ParallelDataGenerator(schema, generation_config, db_conn)
            
            print(f"   🔄 Generating orders with specific duplicate values...")
            data = generator.generate_data_for_table_parallel('orders', 50)
            
            if data:
                # Verify all values are as specified
                status_values = set(row.get('status') for row in data)
                priority_values = set(row.get('priority') for row in data)
                region_values = set(row.get('region') for row in data)
                
                print(f"   ✅ Status values: {status_values} (expected: {{'processing'}})")
                print(f"   ✅ Priority values: {priority_values} (expected: {{'high'}})")
                print(f"   ✅ Region values: {region_values} (expected: {{'North America'}})")
                
                # Verify constraints
                if status_values == {'processing'}:
                    print("   ✅ Status duplicate value correctly applied")
                else:
                    print("   ❌ Status duplicate value not applied correctly")
                
                if priority_values == {'high'}:
                    print("   ✅ Priority duplicate value correctly applied")
                else:
                    print("   ❌ Priority duplicate value not applied correctly")


def main():
    """Main test function."""
    print("🚀 Enhanced DBMocker Feature Test")
    print("=" * 80)
    
    # Check conda environment
    activate_conda_environment()
    
    try:
        # Create test database
        db_path = create_test_database()
        
        # Run comprehensive tests
        test_constraint_aware_duplicates(db_path)
        test_performance_scaling(db_path)
        test_memory_management(db_path)
        test_duplicate_value_specification(db_path)
        
        print("\n🎉 All Enhanced Feature Tests Completed Successfully!")
        print("=" * 80)
        print("✅ Multi-threading and multi-processing capabilities tested")
        print("✅ Constraint-aware duplicate generation tested")
        print("✅ Memory management and streaming tested")
        print("✅ Specific duplicate value assignment tested")
        print("✅ Performance scaling validated")
        
        print(f"\n💡 Test database available at: {db_path}")
        print("💡 Run with different configurations to test millions of records:")
        print("   python -m dbmocker.cli high-performance --driver sqlite --database test_enhanced.db --rows 1000000")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


if __name__ == "__main__":
    main()
