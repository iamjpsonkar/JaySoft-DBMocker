#!/usr/bin/env python3
"""
Integration test to verify fast data reuse is working correctly
through both CLI and programmatic interfaces.
"""

import os
import sys
import sqlite3
import subprocess
import time
import tempfile
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def print_header(title: str):
    """Print formatted header."""
    print(f"\n{'='*60}")
    print(f"🧪 {title}")
    print(f"{'='*60}")

def create_test_db_with_data():
    """Create a test database with existing data for reuse."""
    # Create temporary database
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(db_fd)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create test table
    cursor.execute("""
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE,
            email VARCHAR(100) UNIQUE,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            age INTEGER,
            country VARCHAR(50),
            occupation VARCHAR(100),
            active BOOLEAN DEFAULT 1
        )
    """)
    
    # Insert some initial data
    test_data = [
        ('john_doe', 'john@test.com', 'John', 'Doe', 25, 'USA', 'Engineer', 1),
        ('jane_smith', 'jane@test.com', 'Jane', 'Smith', 30, 'Canada', 'Designer', 1),
        ('bob_wilson', 'bob@test.com', 'Bob', 'Wilson', 35, 'UK', 'Manager', 1),
        ('alice_brown', 'alice@test.com', 'Alice', 'Brown', 28, 'Australia', 'Developer', 1),
        ('charlie_davis', 'charlie@test.com', 'Charlie', 'Davis', 32, 'Germany', 'Analyst', 1),
    ]
    
    cursor.executemany("""
        INSERT INTO test_users (username, email, first_name, last_name, age, country, occupation, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, test_data)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Created test database: {db_path}")
    print(f"📊 Initial data: {len(test_data)} users")
    
    return db_path

def test_cli_fast_data_reuse():
    """Test fast data reuse through CLI."""
    print_header("CLI Fast Data Reuse Test")
    
    db_path = create_test_db_with_data()
    
    try:
        # Test CLI command with fast data reuse
        cmd = [
            sys.executable, "-m", "dbmocker.enhanced_cli", "generate",
            "--driver", "sqlite",
            "--database", db_path,
            "--host", "",
            "--port", "0", 
            "--username", "",
            "--password", "",
            "--table", "test_users",
            "--rows", "50000",  # Generate 50K rows
            "--duplicate-strategy", "fast_data_reuse",
            "--sample-size", "5000",
            "--reuse-probability", "0.95",
            "--progress-interval", "1000",
            "--performance-mode", "ultra_high"
        ]
        
        print("🚀 Running CLI command:")
        print(f"   {' '.join(cmd)}")
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        if result.returncode == 0:
            print("✅ CLI execution successful!")
            print("📊 Output:")
            for line in result.stdout.split('\n'):
                if line.strip():
                    print(f"   {line}")
            
            # Check final row count
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM test_users")
            final_count = cursor.fetchone()[0]
            conn.close()
            
            print(f"📈 Final row count: {final_count:,}")
            print(f"⏱️  Total time: {end_time - start_time:.2f}s")
            
            if final_count >= 50000:
                print("🎉 Fast data reuse via CLI PASSED!")
                return True
            else:
                print("❌ Expected at least 50000 rows, got {final_count}")
                return False
        else:
            print("❌ CLI execution failed!")
            print("Error output:")
            for line in result.stderr.split('\n'):
                if line.strip():
                    print(f"   {line}")
            return False
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
    
    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_programmatic_fast_data_reuse():
    """Test fast data reuse programmatically."""
    print_header("Programmatic Fast Data Reuse Test")
    
    db_path = create_test_db_with_data()
    
    try:
        from dbmocker.core.database import DatabaseConnection, DatabaseConfig
        from dbmocker.core.analyzer import SchemaAnalyzer
        from dbmocker.core.enhanced_models import (
            EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy,
            create_high_performance_config
        )
        from dbmocker.core.ultra_fast_processor import create_ultra_fast_processor
        
        # Configure database connection
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
            
            # Create configuration with fast data reuse
            config = create_high_performance_config(
                target_tables={'test_users': 25000},
                performance_mode=PerformanceMode.ULTRA_HIGH,
                enable_duplicates=True,
                duplicate_strategy=DuplicateStrategy.FAST_DATA_REUSE,
                sample_size=5000,
                reuse_probability=0.95,
                progress_interval=1000
            )
            
            print("🔧 Configuration:")
            print(f"   Performance Mode: {config.performance.performance_mode}")
            print(f"   Duplicate Strategy: {config.duplicates.global_duplicate_strategy}")
            print(f"   Fast Data Reuse: {config.duplicates.enable_fast_data_reuse}")
            print(f"   Sample Size: {config.duplicates.data_reuse_sample_size:,}")
            print(f"   Reuse Probability: {config.duplicates.data_reuse_probability:.1%}")
            
            # Create processor
            processor = create_ultra_fast_processor(schema, config, db_conn)
            
            # Progress tracking
            progress_count = 0
            def progress_callback(table, current, total):
                nonlocal progress_count
                if current % 1000 == 0 and current != progress_count:
                    progress_count = current
                    percentage = (current / total) * 100
                    print(f"  📊 Progress: {current:,}/{total:,} ({percentage:.1f}%)")
            
            print("\n🚀 Starting programmatic fast data reuse...")
            start_time = time.time()
            
            # Process records
            report = processor.process_millions_of_records('test_users', 25000, progress_callback)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            print(f"\n🎉 Programmatic test completed!")
            print(f"📊 Results:")
            print(f"   Rows generated: {report.total_rows_generated:,}")
            print(f"   Total time: {total_time:.2f}s")
            print(f"   Average rate: {report.average_rows_per_second:,.0f} rows/s")
            
            # Verify final count
            final_count = db_conn.execute_query("SELECT COUNT(*) FROM test_users")[0][0]
            print(f"   Final row count: {final_count:,}")
            
            if report.total_rows_generated >= 25000 and final_count >= 25000:
                print("🎉 Programmatic fast data reuse PASSED!")
                return True
            else:
                print(f"❌ Expected at least 25000 rows generated and in DB")
                return False
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_performance_comparison():
    """Compare performance with and without fast data reuse."""
    print_header("Performance Comparison Test")
    
    # Test without fast data reuse
    print("\n📊 Testing WITHOUT fast data reuse...")
    db_path1 = create_test_db_with_data()
    
    try:
        from dbmocker.core.database import DatabaseConnection, DatabaseConfig
        from dbmocker.core.analyzer import SchemaAnalyzer
        from dbmocker.core.enhanced_models import (
            create_high_performance_config, PerformanceMode, DuplicateStrategy
        )
        from dbmocker.core.ultra_fast_processor import create_ultra_fast_processor
        
        db_config = DatabaseConfig(host="", port=0, database=db_path1, username="", password="", driver="sqlite")
        
        with DatabaseConnection(db_config) as db_conn:
            db_conn.connect()
            analyzer = SchemaAnalyzer(db_conn)
            schema = analyzer.analyze_schema("test_db")
            
            # Configuration without fast data reuse
            config1 = create_high_performance_config(
                target_tables={'test_users': 10000},
                performance_mode=PerformanceMode.HIGH_SPEED,
                enable_duplicates=True,
                duplicate_strategy=DuplicateStrategy.SMART_DUPLICATES
            )
            
            processor1 = create_ultra_fast_processor(schema, config1, db_conn)
            
            start_time = time.time()
            report1 = processor1.process_millions_of_records('test_users', 10000)
            time1 = time.time() - start_time
            
            print(f"   ⏱️  Time: {time1:.2f}s")
            print(f"   🚄 Rate: {report1.average_rows_per_second:,.0f} rows/s")
    
    finally:
        if os.path.exists(db_path1):
            os.unlink(db_path1)
    
    # Test with fast data reuse
    print("\n📊 Testing WITH fast data reuse...")
    db_path2 = create_test_db_with_data()
    
    try:
        db_config = DatabaseConfig(host="", port=0, database=db_path2, username="", password="", driver="sqlite")
        
        with DatabaseConnection(db_config) as db_conn:
            db_conn.connect()
            analyzer = SchemaAnalyzer(db_conn)
            schema = analyzer.analyze_schema("test_db")
            
            # Configuration with fast data reuse
            config2 = create_high_performance_config(
                target_tables={'test_users': 10000},
                performance_mode=PerformanceMode.ULTRA_HIGH,
                enable_duplicates=True,
                duplicate_strategy=DuplicateStrategy.FAST_DATA_REUSE,
                sample_size=5000,
                reuse_probability=0.95
            )
            
            processor2 = create_ultra_fast_processor(schema, config2, db_conn)
            
            start_time = time.time()
            report2 = processor2.process_millions_of_records('test_users', 10000)
            time2 = time.time() - start_time
            
            print(f"   ⏱️  Time: {time2:.2f}s")
            print(f"   🚄 Rate: {report2.average_rows_per_second:,.0f} rows/s")
            
            # Compare results
            print(f"\n🏁 Performance Comparison:")
            if time2 < time1:
                speedup = time1 / time2
                print(f"   🚀 Fast data reuse is {speedup:.1f}x FASTER!")
            else:
                print(f"   ⚠️  Fast data reuse took {time2:.2f}s vs {time1:.2f}s")
            
            rate_improvement = (report2.average_rows_per_second / report1.average_rows_per_second) if report1.average_rows_per_second > 0 else 1
            print(f"   📈 Rate improvement: {rate_improvement:.1f}x")
    
    finally:
        if os.path.exists(db_path2):
            os.unlink(db_path2)

def main():
    """Run all integration tests."""
    print_header("DBMocker Fast Data Reuse Integration Tests")
    
    print("🎯 Testing fast data reuse integration:")
    print("   • CLI interface with fast data reuse")
    print("   • Programmatic interface with fast data reuse")
    print("   • Performance comparison")
    print("   • Progress tracking every 1000 records")
    
    tests = [
        ("CLI Fast Data Reuse", test_cli_fast_data_reuse),
        ("Programmatic Fast Data Reuse", test_programmatic_fast_data_reuse),
        ("Performance Comparison", test_performance_comparison),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} ERROR: {e}")
    
    print_header("Test Results")
    print(f"📊 Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests PASSED! Fast data reuse is working correctly.")
    else:
        print("⚠️  Some tests failed. Check implementation.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
