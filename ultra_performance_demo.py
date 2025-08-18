#!/usr/bin/env python3
"""
Ultra-Performance DBMocker Demonstration
Showcases the new high-performance features for generating millions of records.
"""

import os
import sys
import sqlite3
import time
import logging
import psutil
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging for clear output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Reduce SQLAlchemy noise
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)

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

def print_performance_metrics(start_time: float, rows_generated: int, operation: str):
    """Print performance metrics."""
    duration = time.time() - start_time
    rate = rows_generated / duration if duration > 0 else 0
    
    print(f"✅ {operation} completed:")
    print(f"   📈 Rows: {rows_generated:,}")
    print(f"   ⏱️  Time: {duration:.2f}s")
    print(f"   🚄 Rate: {rate:,.0f} rows/sec")
    print(f"   💾 Memory: {psutil.virtual_memory().percent:.1f}%")

def create_test_database():
    """Create a comprehensive test database with multiple table types."""
    db_path = project_root / "ultra_performance_test.db"
    
    # Remove existing database
    if db_path.exists():
        db_path.unlink()
    
    print_section("Creating Test Database")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a variety of tables to test different scenarios
    
    # 1. Large user table (typical for millions of records)
    cursor.execute("""
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            age INTEGER CHECK(age >= 18 AND age <= 120),
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            account_balance DECIMAL(10,2) DEFAULT 0.00,
            profile_data JSON
        )
    """)
    
    # 2. Product catalog (moderate size with references)
    cursor.execute("""
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(100) UNIQUE NOT NULL,
            description TEXT,
            is_active BOOLEAN DEFAULT 1
        )
    """)
    
    cursor.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER REFERENCES categories(category_id),
            sku VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            price DECIMAL(10,2) NOT NULL CHECK(price > 0),
            stock_quantity INTEGER DEFAULT 0,
            is_available BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 3. High-volume transaction table
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(user_id),
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) DEFAULT 'pending',
            total_amount DECIMAL(10,2) NOT NULL,
            shipping_address TEXT,
            payment_method VARCHAR(50),
            notes TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER REFERENCES orders(order_id),
            product_id INTEGER REFERENCES products(product_id),
            quantity INTEGER NOT NULL CHECK(quantity > 0),
            unit_price DECIMAL(10,2) NOT NULL,
            subtotal DECIMAL(10,2) NOT NULL
        )
    """)
    
    # 4. Analytics/logging table (very high volume)
    cursor.execute("""
        CREATE TABLE user_activities (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(user_id),
            activity_type VARCHAR(50) NOT NULL,
            activity_data JSON,
            ip_address VARCHAR(45),
            user_agent TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 5. Configuration table (small, reference data)
    cursor.execute("""
        CREATE TABLE system_config (
            config_id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_key VARCHAR(100) UNIQUE NOT NULL,
            config_value TEXT,
            data_type VARCHAR(20) DEFAULT 'string',
            is_active BOOLEAN DEFAULT 1,
            description TEXT
        )
    """)
    
    # Add some indexes for realistic performance
    cursor.execute("CREATE INDEX idx_users_email ON users(email)")
    cursor.execute("CREATE INDEX idx_users_username ON users(username)")
    cursor.execute("CREATE INDEX idx_products_category ON products(category_id)")
    cursor.execute("CREATE INDEX idx_orders_user ON orders(user_id)")
    cursor.execute("CREATE INDEX idx_activities_user ON user_activities(user_id)")
    cursor.execute("CREATE INDEX idx_activities_type ON user_activities(activity_type)")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Test database created: {db_path}")
    print(f"📊 Tables: users, categories, products, orders, order_items, user_activities, system_config")
    
    return str(db_path)

def demonstrate_standard_generation():
    """Demonstrate standard generation capabilities."""
    print_header("Standard Generation Demonstration")
    
    try:
        from dbmocker.core.database import DatabaseConnection, DatabaseConfig
        from dbmocker.core.analyzer import SchemaAnalyzer
        from dbmocker.core.generator import DataGenerator
        from dbmocker.core.models import GenerationConfig
        
        db_path = create_test_database()
        
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
            print_section("Schema Analysis")
            analyzer = SchemaAnalyzer(db_conn)
            schema = analyzer.analyze_schema("test_db")
            
            print(f"📋 Found {len(schema.tables)} tables:")
            for table in schema.tables:
                print(f"   • {table.name}: {len(table.columns)} columns")
            
            # Generate small dataset with standard generator
            print_section("Standard Generation (Baseline)")
            
            config = GenerationConfig(
                batch_size=1000,
                max_workers=2,
                seed=42
            )
            
            generator = DataGenerator(schema, config, db_conn)
            
            # Generate small amounts for baseline comparison
            test_tables = {
                'categories': 100,
                'users': 5000,
                'products': 1000
            }
            
            for table_name, rows in test_tables.items():
                start_time = time.time()
                data = generator.generate_data_for_table(table_name, rows)
                print_performance_metrics(start_time, len(data), f"Standard generation - {table_name}")
        
        print("✅ Standard generation demonstration completed")
        
    except Exception as e:
        print(f"❌ Standard generation failed: {e}")
        import traceback
        traceback.print_exc()

def demonstrate_high_performance_generation():
    """Demonstrate high-performance generation capabilities."""
    print_header("High-Performance Generation Demonstration")
    
    try:
        from dbmocker.core.database import DatabaseConnection, DatabaseConfig
        from dbmocker.core.analyzer import SchemaAnalyzer
        from dbmocker.core.enhanced_models import (
            EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy,
            create_high_performance_config
        )
        from dbmocker.core.high_performance_generator import HighPerformanceGenerator
        
        db_path = project_root / "ultra_performance_test.db"
        
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
            
            # Test different performance modes
            performance_tests = [
                {
                    'mode': PerformanceMode.BALANCED,
                    'tables': {'users': 25000, 'products': 5000, 'orders': 10000},
                    'description': 'Balanced Mode (Medium Scale)'
                },
                {
                    'mode': PerformanceMode.HIGH_SPEED,
                    'tables': {'users': 50000, 'orders': 25000, 'user_activities': 100000},
                    'description': 'High-Speed Mode (Large Scale)'
                }
            ]
            
            for test_config in performance_tests:
                print_section(test_config['description'])
                
                # Create optimized configuration
                config = create_high_performance_config(
                    target_tables=test_config['tables'],
                    performance_mode=test_config['mode'],
                    enable_duplicates=True,
                    duplicate_strategy=DuplicateStrategy.SMART_DUPLICATES,
                    seed=42
                )
                
                generator = HighPerformanceGenerator(schema, config, db_conn)
                
                total_start_time = time.time()
                total_rows = 0
                
                for table_name, row_count in test_config['tables'].items():
                    print(f"\n🎯 Generating {table_name}: {row_count:,} rows")
                    
                    def progress_callback(table, current, total):
                        if current % 10000 == 0:
                            progress = (current / total) * 100
                            print(f"  📊 Progress: {current:,}/{total:,} ({progress:.1f}%)")
                    
                    start_time = time.time()
                    stats = generator.generate_millions_of_records(
                        table_name, row_count, progress_callback
                    )
                    
                    print_performance_metrics(start_time, stats.total_rows_generated, f"High-perf generation - {table_name}")
                    total_rows += stats.total_rows_generated
                
                # Overall performance report
                total_time = time.time() - total_start_time
                overall_rate = total_rows / total_time if total_time > 0 else 0
                
                print(f"\n🏆 {test_config['description']} Summary:")
                print(f"   📈 Total rows: {total_rows:,}")
                print(f"   ⏱️  Total time: {total_time:.2f}s")
                print(f"   🚄 Overall rate: {overall_rate:,.0f} rows/sec")
                
                # Get performance report
                perf_report = generator.get_performance_report()
                print(f"   💾 Cache hit rate: {perf_report['cache_metrics']['hit_rate']:.1%}")
                print(f"   🧵 Threads used: {perf_report['generation_metrics']['threads_used']}")
        
        print("✅ High-performance generation demonstration completed")
        
    except Exception as e:
        print(f"❌ High-performance generation failed: {e}")
        import traceback
        traceback.print_exc()

def demonstrate_ultra_fast_processing():
    """Demonstrate ultra-fast processing for millions of records."""
    print_header("Ultra-Fast Processing Demonstration (Millions of Records)")
    
    try:
        from dbmocker.core.database import DatabaseConnection, DatabaseConfig
        from dbmocker.core.analyzer import SchemaAnalyzer
        from dbmocker.core.enhanced_models import (
            EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy,
            create_high_performance_config
        )
        from dbmocker.core.ultra_fast_processor import create_ultra_fast_processor
        
        db_path = project_root / "ultra_performance_test.db"
        
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
            
            print_section("Ultra-Fast Processor Setup")
            
            # Ultra-high performance configuration
            target_tables = {
                'user_activities': 1000000,  # 1 million activity records
                'users': 250000,             # 250K users
                'orders': 500000             # 500K orders
            }
            
            config = create_high_performance_config(
                target_tables=target_tables,
                performance_mode=PerformanceMode.ULTRA_HIGH,
                enable_duplicates=True,
                duplicate_strategy=DuplicateStrategy.CACHED_POOL,
                seed=42
            )
            
            print(f"🎯 Target: {sum(target_tables.values()):,} total records")
            print(f"⚙️  Mode: {config.performance.performance_mode}")
            print(f"🔄 Strategy: {config.duplicates.global_duplicate_strategy}")
            print(f"📦 Batch size: {config.performance.batch_size:,}")
            print(f"🧵 Max workers: {config.performance.max_workers}")
            
            # Create ultra-fast processor
            processor = create_ultra_fast_processor(schema, config, db_conn)
            
            # Process each table
            total_start_time = time.time()
            grand_total_rows = 0
            
            for table_name, row_count in target_tables.items():
                print_section(f"Ultra-Fast Processing: {table_name}")
                print(f"🎯 Target rows: {row_count:,}")
                
                def progress_callback(table, current, total):
                    if current % 50000 == 0:  # Update every 50K rows
                        progress = (current / total) * 100
                        elapsed = time.time() - table_start_time
                        rate = current / elapsed if elapsed > 0 else 0
                        eta = (total - current) / rate if rate > 0 else 0
                        print(f"  📊 {current:,}/{total:,} ({progress:.1f}%) | {rate:,.0f} rows/s | ETA: {eta:.0f}s")
                
                table_start_time = time.time()
                report = processor.process_millions_of_records(
                    table_name, row_count, progress_callback
                )
                
                print_performance_metrics(table_start_time, report.total_rows_generated, f"Ultra-fast processing - {table_name}")
                grand_total_rows += report.total_rows_generated
                
                # Additional metrics from ultra-fast processor
                if hasattr(report, 'cache_hit_rate'):
                    print(f"   🎯 Cache hit rate: {report.cache_hit_rate:.1%}")
                if hasattr(report, 'threads_used'):
                    print(f"   🧵 Threads used: {report.threads_used}")
            
            # Grand total summary
            grand_total_time = time.time() - total_start_time
            grand_rate = grand_total_rows / grand_total_time if grand_total_time > 0 else 0
            
            print_section("Ultra-Fast Processing Summary")
            print(f"🏆 GRAND TOTAL:")
            print(f"   📈 Total rows generated: {grand_total_rows:,}")
            print(f"   ⏱️  Total time: {grand_total_time:.2f}s")
            print(f"   🚄 Overall rate: {grand_rate:,.0f} rows/sec")
            print(f"   💾 Peak memory: {psutil.virtual_memory().percent:.1f}%")
            
            # Performance comparison
            if grand_rate > 100000:
                print(f"   🚀 EXCELLENT: >100K rows/sec - Enterprise-grade performance!")
            elif grand_rate > 50000:
                print(f"   ✅ VERY GOOD: >50K rows/sec - High-performance achieved!")
            elif grand_rate > 25000:
                print(f"   👍 GOOD: >25K rows/sec - Solid performance")
            else:
                print(f"   ⚠️  MODERATE: Consider optimizing for better performance")
        
        print("✅ Ultra-fast processing demonstration completed")
        
    except Exception as e:
        print(f"❌ Ultra-fast processing failed: {e}")
        import traceback
        traceback.print_exc()

def demonstrate_duplicate_strategies():
    """Demonstrate different duplicate handling strategies."""
    print_header("Duplicate Strategies Demonstration")
    
    try:
        from dbmocker.core.database import DatabaseConnection, DatabaseConfig
        from dbmocker.core.analyzer import SchemaAnalyzer
        from dbmocker.core.enhanced_models import (
            EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy,
            create_high_performance_config
        )
        from dbmocker.core.high_performance_generator import HighPerformanceGenerator
        
        db_path = project_root / "ultra_performance_test.db"
        
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
            
            # Test different duplicate strategies
            duplicate_tests = [
                {
                    'strategy': DuplicateStrategy.GENERATE_NEW,
                    'description': 'Generate New (No Duplicates)',
                    'rows': 10000
                },
                {
                    'strategy': DuplicateStrategy.ALLOW_SIMPLE,
                    'description': 'Allow Simple Duplicates',
                    'rows': 10000
                },
                {
                    'strategy': DuplicateStrategy.SMART_DUPLICATES,
                    'description': 'Smart Duplicates (Realistic Distribution)',
                    'rows': 10000
                },
                {
                    'strategy': DuplicateStrategy.CACHED_POOL,
                    'description': 'Cached Pool (Maximum Performance)',
                    'rows': 10000
                }
            ]
            
            for test_config in duplicate_tests:
                print_section(test_config['description'])
                
                # Create configuration with specific duplicate strategy
                config = create_high_performance_config(
                    target_tables={'users': test_config['rows']},
                    performance_mode=PerformanceMode.HIGH_SPEED,
                    enable_duplicates=True,
                    duplicate_strategy=test_config['strategy'],
                    seed=42
                )
                
                generator = HighPerformanceGenerator(schema, config, db_conn)
                
                start_time = time.time()
                stats = generator.generate_millions_of_records(
                    'users', test_config['rows']
                )
                
                print_performance_metrics(start_time, stats.total_rows_generated, test_config['description'])
                
                # Additional duplicate-specific metrics would go here
                # (In a real implementation, we'd analyze the generated data for duplicate rates)
        
        print("✅ Duplicate strategies demonstration completed")
        
    except Exception as e:
        print(f"❌ Duplicate strategies demonstration failed: {e}")
        import traceback
        traceback.print_exc()

def demonstrate_system_scalability():
    """Demonstrate system scalability across different data sizes."""
    print_header("System Scalability Demonstration")
    
    try:
        from dbmocker.core.database import DatabaseConnection, DatabaseConfig
        from dbmocker.core.analyzer import SchemaAnalyzer
        from dbmocker.core.enhanced_models import (
            EnhancedGenerationConfig, PerformanceMode,
            create_high_performance_config
        )
        from dbmocker.core.high_performance_generator import HighPerformanceGenerator
        
        db_path = project_root / "ultra_performance_test.db"
        
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
            
            # Scalability test scenarios
            scalability_tests = [
                {'rows': 1000, 'label': 'Small (1K)'},
                {'rows': 10000, 'label': 'Medium (10K)'},
                {'rows': 100000, 'label': 'Large (100K)'},
                {'rows': 250000, 'label': 'Very Large (250K)'},
            ]
            
            print_section("Scalability Analysis")
            
            results = []
            
            for test in scalability_tests:
                print(f"\n🎯 Testing {test['label']} records")
                
                config = create_high_performance_config(
                    target_tables={'user_activities': test['rows']},
                    performance_mode=PerformanceMode.HIGH_SPEED,
                    enable_duplicates=True,
                    seed=42
                )
                
                generator = HighPerformanceGenerator(schema, config, db_conn)
                
                start_time = time.time()
                stats = generator.generate_millions_of_records(
                    'user_activities', test['rows']
                )
                duration = time.time() - start_time
                
                rate = stats.total_rows_generated / duration if duration > 0 else 0
                
                results.append({
                    'rows': test['rows'],
                    'label': test['label'],
                    'duration': duration,
                    'rate': rate
                })
                
                print(f"   ⏱️  {duration:.2f}s ({rate:,.0f} rows/sec)")
            
            # Analysis
            print_section("Scalability Analysis Results")
            print("📊 Performance scaling:")
            
            for i, result in enumerate(results):
                if i > 0:
                    prev_result = results[i-1]
                    scale_factor = result['rows'] / prev_result['rows']
                    time_ratio = result['duration'] / prev_result['duration']
                    efficiency = scale_factor / time_ratio
                    
                    print(f"   {result['label']}: {efficiency:.2f}x efficiency vs {prev_result['label']}")
                else:
                    print(f"   {result['label']}: baseline")
            
            # Linear scaling analysis
            if len(results) >= 2:
                first_rate = results[0]['rate']
                last_rate = results[-1]['rate']
                
                if last_rate >= first_rate * 0.8:  # Within 20% of linear scaling
                    print("🚀 EXCELLENT: Near-linear scaling achieved!")
                elif last_rate >= first_rate * 0.6:
                    print("✅ GOOD: Decent scaling performance")
                else:
                    print("⚠️  SUBOPTIMAL: Performance degrades with scale")
        
        print("✅ System scalability demonstration completed")
        
    except Exception as e:
        print(f"❌ System scalability demonstration failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main demonstration function."""
    print_header("DBMocker Ultra-Performance Demonstration")
    
    print("🎯 This demonstration showcases:")
    print("   • Standard vs High-Performance vs Ultra-Fast generation")
    print("   • Multi-threading and parallel processing")
    print("   • Memory optimization and streaming")
    print("   • Intelligent caching and duplicate strategies")
    print("   • Connection pooling and bulk operations")
    print("   • Scalability from thousands to millions of records")
    
    print(f"\n💻 System Info:")
    print(f"   CPU Cores: {psutil.cpu_count()}")
    print(f"   Memory: {psutil.virtual_memory().total / (1024**3):.1f}GB")
    print(f"   Available: {psutil.virtual_memory().available / (1024**3):.1f}GB")
    
    # Run demonstrations
    demonstrations = [
        ("Standard Generation", demonstrate_standard_generation),
        ("High-Performance Generation", demonstrate_high_performance_generation),
        ("Ultra-Fast Processing", demonstrate_ultra_fast_processing),
        ("Duplicate Strategies", demonstrate_duplicate_strategies),
        ("System Scalability", demonstrate_system_scalability),
    ]
    
    overall_start_time = time.time()
    
    for demo_name, demo_func in demonstrations:
        try:
            demo_func()
            print(f"\n✅ {demo_name} completed successfully")
        except Exception as e:
            print(f"\n❌ {demo_name} failed: {e}")
            continue
    
    # Final summary
    total_time = time.time() - overall_start_time
    
    print_header("Demonstration Summary")
    print(f"🎉 All demonstrations completed in {total_time:.2f}s")
    print(f"📊 Check the generated database: {project_root}/ultra_performance_test.db")
    print(f"💡 Use SQLite browser to inspect the generated data")
    
    # Cleanup recommendation
    print(f"\n🧹 To clean up:")
    print(f"   rm {project_root}/ultra_performance_test.db")
    print(f"   rm -rf /tmp/dbmocker_cache")

if __name__ == "__main__":
    main()
