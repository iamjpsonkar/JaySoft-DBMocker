#!/usr/bin/env python3
"""
JaySoft-DBMocker Selective Table Generation Example

This example demonstrates the enhanced selective table generation where:
- You can select specific tables for data generation
- Foreign keys automatically use existing data from unselected tables
- Bulk selection operations (Select All, Clear All, Smart Selection)
- FK dependency analysis and validation

Scenario:
- E-commerce database with 8 tables
- Select only 3 tables for generation: orders, order_items, reviews
- These tables have FKs to unselected tables: users, products, categories
- System automatically uses existing data from unselected tables for FKs
"""

import os
import sys
import sqlite3
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.models import GenerationConfig, TableGenerationConfig
from dbmocker.core.smart_generator import DependencyAwareGenerator
from dbmocker.core.inserter import DataInserter


def create_ecommerce_database():
    """Create an e-commerce database with existing reference data."""
    db_path = "selective_demo.db"
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🏗️  Creating e-commerce database with reference data...")
    
    # Create tables
    cursor.execute("""
    CREATE TABLE categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL UNIQUE,
        description TEXT,
        is_active BOOLEAN DEFAULT 1
    )
    """)
    
    cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username VARCHAR(50) NOT NULL UNIQUE,
        email VARCHAR(100) NOT NULL UNIQUE,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(200) NOT NULL,
        price REAL NOT NULL,
        category_id INTEGER NOT NULL,
        stock_quantity INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT 1,
        FOREIGN KEY (category_id) REFERENCES categories(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        total_amount REAL NOT NULL,
        status VARCHAR(20) DEFAULT 'pending',
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 1,
        unit_price REAL NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        rating INTEGER CHECK (rating >= 1 AND rating <= 5),
        comment TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE coupons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code VARCHAR(20) NOT NULL UNIQUE,
        discount_percent REAL,
        min_order_amount REAL,
        is_active BOOLEAN DEFAULT 1
    )
    """)
    
    cursor.execute("""
    CREATE TABLE user_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        address_line1 VARCHAR(200),
        city VARCHAR(100),
        country VARCHAR(100),
        is_default BOOLEAN DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)
    
    print("✅ Database schema created!")
    
    # Insert reference data that we want to keep
    print("📊 Inserting reference data...")
    
    # Categories (will NOT be selected for generation)
    categories = [
        (1, "Electronics", "Electronic devices and gadgets", 1),
        (2, "Books", "Physical and digital books", 1),
        (3, "Clothing", "Fashion and apparel", 1),
        (4, "Home", "Home and garden items", 1),
        (5, "Sports", "Sports and fitness equipment", 1)
    ]
    cursor.executemany("INSERT INTO categories (id, name, description, is_active) VALUES (?, ?, ?, ?)", categories)
    
    # Users (will NOT be selected for generation)
    users = [
        (1, "alice_smith", "alice@example.com", "Alice", "Smith"),
        (2, "bob_jones", "bob@example.com", "Bob", "Jones"), 
        (3, "carol_davis", "carol@example.com", "Carol", "Davis"),
        (4, "david_wilson", "david@example.com", "David", "Wilson"),
        (5, "eve_brown", "eve@example.com", "Eve", "Brown"),
        (6, "frank_miller", "frank@example.com", "Frank", "Miller"),
        (7, "grace_garcia", "grace@example.com", "Grace", "Garcia"),
        (8, "henry_rodriguez", "henry@example.com", "Henry", "Rodriguez")
    ]
    cursor.executemany("INSERT INTO users (id, username, email, first_name, last_name) VALUES (?, ?, ?, ?, ?)", users)
    
    # Products (will NOT be selected for generation)
    products = [
        (1, "iPhone 15", 999.99, 1, 50, 1),
        (2, "MacBook Pro", 1999.99, 1, 25, 1),
        (3, "Python Programming", 49.99, 2, 100, 1),
        (4, "JavaScript Guide", 39.99, 2, 75, 1),
        (5, "Cotton T-Shirt", 19.99, 3, 200, 1),
        (6, "Denim Jeans", 79.99, 3, 150, 1),
        (7, "Coffee Maker", 129.99, 4, 30, 1),
        (8, "Garden Tools Set", 89.99, 4, 40, 1),
        (9, "Running Shoes", 149.99, 5, 60, 1),
        (10, "Yoga Mat", 29.99, 5, 80, 1),
        (11, "Wireless Earbuds", 199.99, 1, 45, 1),
        (12, "Cookbook Collection", 69.99, 2, 90, 1)
    ]
    cursor.executemany("INSERT INTO products (id, name, price, category_id, stock_quantity, is_active) VALUES (?, ?, ?, ?, ?, ?)", products)
    
    # Coupons (will NOT be selected for generation)
    coupons = [
        (1, "SAVE10", 10.0, 50.0, 1),
        (2, "WELCOME20", 20.0, 100.0, 1),
        (3, "BULK15", 15.0, 200.0, 1)
    ]
    cursor.executemany("INSERT INTO coupons (id, code, discount_percent, min_order_amount, is_active) VALUES (?, ?, ?, ?, ?)", coupons)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Reference database created: {db_path}")
    print(f"📈 Reference data:")
    print(f"   - Categories: {len(categories)} rows")
    print(f"   - Users: {len(users)} rows") 
    print(f"   - Products: {len(products)} rows")
    print(f"   - Coupons: {len(coupons)} rows")
    
    return db_path


def demonstrate_selective_generation():
    """Demonstrate selective table generation with FK dependency handling."""
    print("\n" + "="*70)
    print("🎯 JaySoft-DBMocker Selective Table Generation")
    print("="*70)
    
    # Create database with reference data
    db_path = create_ecommerce_database()
    
    try:
        # Connect to database
        db_config = DatabaseConfig(
            host="localhost",
            port=3306,  # Dummy port for SQLite
            database=db_path,
            username="",
            password="",
            driver="sqlite"
        )
        
        with DatabaseConnection(db_config) as db_conn:
            print("\n🔍 Analyzing database schema...")
            analyzer = SchemaAnalyzer(db_conn)
            schema = analyzer.analyze_schema(analyze_data_patterns=True)
            
            print(f"✅ Found {len(schema.tables)} tables")
            
            # Show all tables and their current data
            print("\n📊 Current Database State:")
            for table in schema.tables:
                result = db_conn.execute_query(f'SELECT COUNT(*) FROM `{table.name}`')
                count = result[0][0] if result else 0
                fk_count = len(table.foreign_keys)
                fk_info = f" (has {fk_count} FK)" if fk_count > 0 else ""
                print(f"   {table.name}: {count} rows{fk_info}")
            
            print("\n🎯 Selective Generation Configuration:")
            print("   📋 SELECTED for generation: orders, order_items, reviews")
            print("   🔄 UNSELECTED (use existing): categories, users, products, coupons, user_addresses")
            print("   🔗 FK relationships will automatically use existing data from unselected tables")
            
            # Configure selective generation
            generation_config = GenerationConfig(
                batch_size=50,
                seed=67890,  # For reproducible results
                preserve_existing_data=True,
                reuse_existing_values=1.0,  # Always use existing FK values when available
                prefer_existing_fk_values=True
            )
            
            # Configure specific table selection
            generation_config.table_configs = {
                # SELECTED TABLES (will generate new data)
                "orders": TableGenerationConfig(rows_to_generate=30, use_existing_data=False),
                "order_items": TableGenerationConfig(rows_to_generate=80, use_existing_data=False), 
                "reviews": TableGenerationConfig(rows_to_generate=50, use_existing_data=False),
                
                # UNSELECTED TABLES (will use existing data - not generated)
                "categories": TableGenerationConfig(rows_to_generate=0, use_existing_data=True),
                "users": TableGenerationConfig(rows_to_generate=0, use_existing_data=True),
                "products": TableGenerationConfig(rows_to_generate=0, use_existing_data=True),
                "coupons": TableGenerationConfig(rows_to_generate=0, use_existing_data=True),
                "user_addresses": TableGenerationConfig(rows_to_generate=0, use_existing_data=True)
            }
            
            # Create smart generator
            print("\n🧠 Creating smart generator with FK dependency analysis...")
            smart_generator = DependencyAwareGenerator(schema, generation_config, db_conn)
            
            # Analyze FK dependencies
            print("\n🔗 Analyzing FK Dependencies:")
            fk_dependencies = smart_generator.analyze_fk_dependencies_for_selection()
            
            if fk_dependencies:
                print("   Selected tables with FK references to unselected tables:")
                for selected_table, referenced_tables in fk_dependencies.items():
                    print(f"     • {selected_table} → {', '.join(referenced_tables)}")
                print("   ✅ Will automatically use existing data from referenced tables")
            else:
                print("   No FK dependencies between selected and unselected tables")
            
            # Validate FK integrity
            print("\n🔍 Validating FK Integrity:")
            validation_results = smart_generator.validate_fk_integrity_for_selection()
            
            for selected_table, referenced_validations in validation_results.items():
                print(f"   {selected_table}:")
                for ref_table, has_data in referenced_validations.items():
                    status = "✅ Has data" if has_data else "❌ No data"
                    print(f"     → {ref_table}: {status}")
            
            # Generate data
            print("\n🎲 Starting selective data generation...")
            generated_data = smart_generator.generate_data_for_all_tables()
            
            # Show generation results
            print("\n📊 Generation Results:")
            selected_count = 0
            unselected_count = 0
            total_generated_rows = 0
            
            for table_name, data in generated_data.items():
                table_config = generation_config.table_configs.get(table_name)
                if table_config and table_config.use_existing_data:
                    print(f"   {table_name}: Using existing data (unselected)")
                    unselected_count += 1
                else:
                    print(f"   {table_name}: Generated {len(data)} new rows (selected)")
                    selected_count += 1
                    total_generated_rows += len(data)
            
            # Insert generated data
            print("\n💾 Inserting generated data...")
            inserter = DataInserter(db_conn, schema)
            
            total_inserted = 0
            for table_name, data in generated_data.items():
                if data:  # Only insert if there's new data
                    try:
                        # Convert DECIMAL to float for SQLite compatibility
                        for row in data:
                            for key, value in row.items():
                                if hasattr(value, '__class__') and 'Decimal' in str(type(value)):
                                    row[key] = float(value)
                        
                        inserted_count = inserter.insert_data(table_name, data)
                        total_inserted += inserted_count
                        print(f"   ✅ {table_name}: Inserted {inserted_count} rows")
                    except Exception as e:
                        print(f"   ❌ {table_name}: Failed to insert - {e}")
            
            print(f"\n🎉 Selective Generation Complete!")
            print(f"   📊 Selected tables: {selected_count}")
            print(f"   🔄 Unselected tables: {unselected_count}")
            print(f"   📈 Total new rows: {total_inserted}")
            
            # Verify FK integrity after generation
            print("\n🔍 Verifying FK Integrity After Generation...")
            
            # Check orders reference existing users
            result = db_conn.execute_query("SELECT COUNT(*) FROM orders WHERE user_id NOT IN (SELECT id FROM users)")
            invalid_user_fks = result[0][0] if result else 0
            
            # Check order_items reference existing orders and products
            result = db_conn.execute_query("SELECT COUNT(*) FROM order_items WHERE order_id NOT IN (SELECT id FROM orders)")
            invalid_order_fks = result[0][0] if result else 0
            result = db_conn.execute_query("SELECT COUNT(*) FROM order_items WHERE product_id NOT IN (SELECT id FROM products)")
            invalid_product_fks = result[0][0] if result else 0
            
            # Check reviews reference existing users and products
            result = db_conn.execute_query("SELECT COUNT(*) FROM reviews WHERE user_id NOT IN (SELECT id FROM users)")
            invalid_review_user_fks = result[0][0] if result else 0
            result = db_conn.execute_query("SELECT COUNT(*) FROM reviews WHERE product_id NOT IN (SELECT id FROM products)")
            invalid_review_product_fks = result[0][0] if result else 0
            
            # Report integrity results
            integrity_checks = [
                ("Orders → Users", invalid_user_fks),
                ("Order Items → Orders", invalid_order_fks),
                ("Order Items → Products", invalid_product_fks),
                ("Reviews → Users", invalid_review_user_fks),
                ("Reviews → Products", invalid_review_product_fks)
            ]
            
            all_passed = True
            for check_name, invalid_count in integrity_checks:
                if invalid_count == 0:
                    print(f"   ✅ {check_name}: PASS")
                else:
                    print(f"   ❌ {check_name}: FAIL ({invalid_count} invalid references)")
                    all_passed = False
            
            if all_passed:
                print("\n✅ All FK integrity checks passed!")
            else:
                print("\n❌ Some FK integrity issues detected!")
            
            # Show final database state
            print("\n📈 Final Database State:")
            for table in schema.tables:
                result = db_conn.execute_query(f'SELECT COUNT(*) FROM `{table.name}`')
                count = result[0][0] if result else 0
                table_config = generation_config.table_configs.get(table.name)
                status = "unselected" if (table_config and table_config.use_existing_data) else "selected"
                print(f"   {table.name}: {count} rows ({status})")
            
            print(f"\n✅ Selective generation demonstration completed!")
            print(f"   Database: {db_path}")
            print(f"   🎯 Key Achievement: FK integrity maintained while selectively generating data")
    
    except Exception as e:
        print(f"❌ Error during selective generation: {e}")
        raise


if __name__ == "__main__":
    demonstrate_selective_generation()
