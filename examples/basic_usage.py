"""Basic usage examples for JaySoft-DBMocker."""

import os
import sys
from pathlib import Path

# Add the parent directory to sys.path to import dbmocker
sys.path.insert(0, str(Path(__file__).parent.parent))

from dbmocker.core.database import create_database_connection
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.generator import DataGenerator
from dbmocker.core.inserter import DataInserter
from dbmocker.core.models import (
    GenerationConfig, TableGenerationConfig, ColumnGenerationConfig
)


def example_1_basic_usage():
    """Example 1: Basic usage with SQLite demo database."""
    print("🔍 Example 1: Basic Usage")
    print("=" * 50)
    
    # Create demo database first
    from demo_setup import create_demo_database
    db_path = "demo.db"
    create_demo_database(db_path)
    
    # Connect to the demo database
    db_conn = create_database_connection(
        host="",  # Not used for SQLite
        port=0,   # Not used for SQLite
        database=db_path,
        username="",  # Not used for SQLite
        password="",  # Not used for SQLite
        driver="sqlite"
    )
    
    with db_conn:
        # Analyze the schema
        print("\n🔍 Analyzing database schema...")
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema(analyze_data_patterns=True)
        
        print(f"📊 Found {len(schema.tables)} tables:")
        for table in schema.tables:
            print(f"   • {table.name}: {table.row_count} rows, {len(table.columns)} columns")
        
        # Generate data with basic configuration
        print("\n🎲 Generating mock data...")
        config = GenerationConfig(
            seed=42,  # For reproducible results
            batch_size=100
        )
        
        generator = DataGenerator(schema, config)
        inserter = DataInserter(db_conn, schema)
        
        # Generate data for users table
        print("Generating data for 'users' table...")
        user_data = generator.generate_data_for_table("users", 50)
        print(f"Generated {len(user_data)} user records")
        
        # Show sample data
        print("\nSample generated user:")
        if user_data:
            sample = user_data[0]
            for key, value in sample.items():
                print(f"   {key}: {value}")
        
        print(f"\n✅ Example 1 completed!")


def example_2_custom_configuration():
    """Example 2: Custom configuration with specific column rules."""
    print("\n🎯 Example 2: Custom Configuration")
    print("=" * 50)
    
    db_path = "demo.db"
    
    # Connect to database
    db_conn = create_database_connection(
        host="", port=0, database=db_path, username="", password="", driver="sqlite"
    )
    
    with db_conn:
        # Analyze schema
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema(analyze_data_patterns=True)
        
        # Create custom configuration
        config = GenerationConfig(
            seed=123,
            batch_size=50,
            table_configs={
                "users": TableGenerationConfig(
                    rows_to_generate=25,
                    column_configs={
                        "email": ColumnGenerationConfig(
                            generator_function="email",
                            null_probability=0.0
                        ),
                        "first_name": ColumnGenerationConfig(
                            generator_function="name",
                            null_probability=0.02
                        ),
                        "age": ColumnGenerationConfig(
                            min_value=18,
                            max_value=65,
                            null_probability=0.1
                        ),
                        "country": ColumnGenerationConfig(
                            possible_values=["United States", "Canada", "United Kingdom", "Germany", "France"],
                            null_probability=0.05
                        ),
                        "gender": ColumnGenerationConfig(
                            weighted_values={
                                "male": 0.45,
                                "female": 0.45,
                                "other": 0.10
                            }
                        )
                    }
                ),
                "products": TableGenerationConfig(
                    rows_to_generate=100,
                    column_configs={
                        "name": ColumnGenerationConfig(
                            generator_function="lorem",
                            max_length=50
                        ),
                        "price": ColumnGenerationConfig(
                            min_value=9.99,
                            max_value=999.99
                        ),
                        "rating": ColumnGenerationConfig(
                            min_value=1.0,
                            max_value=5.0
                        ),
                        "status": ColumnGenerationConfig(
                            weighted_values={
                                "active": 0.85,
                                "inactive": 0.10,
                                "discontinued": 0.05
                            }
                        )
                    }
                )
            }
        )
        
        generator = DataGenerator(schema, config)
        inserter = DataInserter(db_conn, schema)
        
        # Truncate existing data
        print("\n🗑️ Truncating existing data...")
        for table_name in ["users", "products"]:
            try:
                inserter.truncate_table(table_name)
                print(f"   Truncated {table_name}")
            except Exception as e:
                print(f"   Warning: Could not truncate {table_name}: {e}")
        
        # Generate and insert data
        print("\n🎲 Generating custom data...")
        
        for table_name in ["users", "products"]:
            table_config = config.table_configs.get(table_name)
            if table_config:
                rows_to_generate = table_config.rows_to_generate
                print(f"\nGenerating {rows_to_generate} rows for '{table_name}'...")
                
                data = generator.generate_data_for_table(table_name, rows_to_generate)
                stats = inserter.insert_data(table_name, data, batch_size=config.batch_size)
                
                print(f"   ✅ Inserted {stats.total_rows_generated} rows in {stats.total_time_seconds:.2f}s")
        
        print(f"\n✅ Example 2 completed!")


def example_3_foreign_key_handling():
    """Example 3: Demonstrating foreign key relationship handling."""
    print("\n🔗 Example 3: Foreign Key Relationships")
    print("=" * 50)
    
    db_path = "demo.db"
    
    db_conn = create_database_connection(
        host="", port=0, database=db_path, username="", password="", driver="sqlite"
    )
    
    with db_conn:
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema(analyze_data_patterns=True)
        
        # Show table dependencies
        dependencies = schema.get_table_dependencies()
        print("\n📊 Table Dependencies:")
        for table, deps in dependencies.items():
            if deps:
                print(f"   {table} depends on: {', '.join(deps)}")
            else:
                print(f"   {table} has no dependencies")
        
        # Configure generation with proper order
        config = GenerationConfig(
            seed=456,
            batch_size=20,
            preserve_existing_data=True,
            reuse_existing_values=0.7,  # 70% chance to reuse existing FK values
            table_configs={
                "categories": TableGenerationConfig(rows_to_generate=5),
                "users": TableGenerationConfig(rows_to_generate=20),
                "products": TableGenerationConfig(rows_to_generate=30),
                "orders": TableGenerationConfig(rows_to_generate=50),
                "order_items": TableGenerationConfig(rows_to_generate=100)
            }
        )
        
        generator = DataGenerator(schema, config)
        inserter = DataInserter(db_conn, schema)
        
        # Generate data in dependency order
        generation_order = ["categories", "users", "products", "orders", "order_items"]
        
        print("\n🎲 Generating data with FK relationships...")
        
        total_stats = {"generated": 0, "inserted": 0, "time": 0.0}
        
        for table_name in generation_order:
            if table_name in config.table_configs:
                table_config = config.table_configs[table_name]
                rows = table_config.rows_to_generate
                
                print(f"\n   Processing {table_name} ({rows} rows)...")
                
                # Truncate table
                try:
                    inserter.truncate_table(table_name)
                except Exception as e:
                    print(f"     Warning: {e}")
                
                # Generate and insert
                data = generator.generate_data_for_table(table_name, rows)
                stats = inserter.insert_data(table_name, data)
                
                total_stats["generated"] += len(data)
                total_stats["inserted"] += stats.total_rows_generated
                total_stats["time"] += stats.total_time_seconds
                
                print(f"     ✅ {stats.total_rows_generated} rows in {stats.total_time_seconds:.2f}s")
        
        # Verify data integrity
        print("\n🔍 Verifying data integrity...")
        integrity_report = inserter.verify_data_integrity(generation_order)
        
        violations = integrity_report.get('foreign_key_violations', [])
        if violations:
            print(f"   ⚠️ Found {len(violations)} FK violations")
            for violation in violations[:3]:  # Show first 3
                print(f"     • {violation}")
        else:
            print(f"   ✅ No FK violations found")
        
        print(f"\n📊 Total Summary:")
        print(f"   Generated: {total_stats['generated']} rows")
        print(f"   Inserted: {total_stats['inserted']} rows")
        print(f"   Time: {total_stats['time']:.2f} seconds")
        
        print(f"\n✅ Example 3 completed!")


def example_4_performance_testing():
    """Example 4: Performance testing with larger datasets."""
    print("\n⚡ Example 4: Performance Testing")
    print("=" * 50)
    
    db_path = "demo.db"
    
    db_conn = create_database_connection(
        host="", port=0, database=db_path, username="", password="", driver="sqlite"
    )
    
    with db_conn:
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema(analyze_data_patterns=False)  # Skip patterns for speed
        
        # Performance-optimized configuration
        config = GenerationConfig(
            seed=789,
            batch_size=1000,  # Larger batches for better performance
            max_workers=2,
            table_configs={
                "users": TableGenerationConfig(rows_to_generate=1000),
                "categories": TableGenerationConfig(rows_to_generate=20),
                "products": TableGenerationConfig(rows_to_generate=500),
                "orders": TableGenerationConfig(rows_to_generate=2000),
                "order_items": TableGenerationConfig(rows_to_generate=5000)
            }
        )
        
        generator = DataGenerator(schema, config)
        inserter = DataInserter(db_conn, schema)
        
        print("\n🏃 Performance test with larger dataset...")
        print(f"Configuration:")
        print(f"   Batch size: {config.batch_size}")
        print(f"   Max workers: {config.max_workers}")
        
        import time
        start_time = time.time()
        
        # Process tables in dependency order
        processing_order = ["categories", "users", "products", "orders", "order_items"]
        
        for table_name in processing_order:
            if table_name in config.table_configs:
                table_config = config.table_configs[table_name]
                rows = table_config.rows_to_generate
                
                print(f"\n   {table_name}: Generating {rows} rows...")
                
                table_start = time.time()
                data = generator.generate_data_for_table(table_name, rows)
                generation_time = time.time() - table_start
                
                print(f"     Generation: {generation_time:.2f}s ({len(data)/generation_time:.0f} rows/s)")
                
                insert_start = time.time()
                inserter.truncate_table(table_name)
                stats = inserter.insert_data(table_name, data, batch_size=config.batch_size)
                insert_time = time.time() - insert_start
                
                print(f"     Insertion: {insert_time:.2f}s ({stats.total_rows_generated/insert_time:.0f} rows/s)")
        
        total_time = time.time() - start_time
        total_rows = sum(config.table_configs[t].rows_to_generate for t in processing_order if t in config.table_configs)
        
        print(f"\n📊 Performance Summary:")
        print(f"   Total rows: {total_rows:,}")
        print(f"   Total time: {total_time:.2f} seconds")
        print(f"   Average rate: {total_rows/total_time:.0f} rows/second")
        
        print(f"\n✅ Example 4 completed!")


def cleanup():
    """Clean up demo database."""
    db_path = "demo.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"\n🧹 Cleaned up demo database: {db_path}")


if __name__ == "__main__":
    print("🚀 JaySoft-DBMocker Usage Examples")
    print("=" * 70)
    
    try:
        # Run all examples
        example_1_basic_usage()
        example_2_custom_configuration()
        example_3_foreign_key_handling()
        example_4_performance_testing()
        
        print("\n" + "=" * 70)
        print("🎉 All examples completed successfully!")
        print("\nNext steps:")
        print("• Try the CLI: dbmocker --help")
        print("• Launch the GUI: dbmocker gui")
        print("• Check out the documentation in README.md")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Examples interrupted by user")
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup()
