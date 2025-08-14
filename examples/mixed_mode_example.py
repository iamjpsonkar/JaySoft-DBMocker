#!/usr/bin/env python3
"""
JaySoft-DBMocker Mixed Mode Example

This example demonstrates how to use JaySoft-DBMocker in mixed mode where:
- Some tables use existing data (no new generation)
- Some tables generate new data
- Foreign key relationships work correctly between mixed modes

Scenario:
- 10 tables total
- 5 tables have foreign keys
- 3 FK tables use existing data (categories, users, products)
- 2 FK tables generate new data (orders, order_items)
- 5 tables without FK generate new data (reviews, wishlists, etc.)
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


def create_sample_database():
    """Create a sample database with existing data to demonstrate mixed mode."""
    db_path = "mixed_mode_demo.db"
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🏗️  Creating sample database with existing data...")
    
    # Create tables
    cursor.execute("""
    CREATE TABLE categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL UNIQUE,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username VARCHAR(50) NOT NULL UNIQUE,
        email VARCHAR(100) NOT NULL UNIQUE,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(200) NOT NULL,
        description TEXT,
        price DECIMAL(10, 2) NOT NULL,
        category_id INTEGER NOT NULL,
        stock_quantity INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (category_id) REFERENCES categories(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        total_amount DECIMAL(10, 2) NOT NULL,
        status VARCHAR(20) DEFAULT 'pending',
        shipping_address TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 1,
        unit_price DECIMAL(10, 2) NOT NULL,
        total_price DECIMAL(10, 2) NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    """)
    
    # Non-FK tables
    cursor.execute("""
    CREATE TABLE reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        rating INTEGER CHECK (rating >= 1 AND rating <= 5),
        comment TEXT,
        is_verified BOOLEAN DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE wishlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name VARCHAR(100) NOT NULL,
        is_public BOOLEAN DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE coupons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code VARCHAR(20) NOT NULL UNIQUE,
        discount_percentage DECIMAL(5, 2),
        discount_amount DECIMAL(10, 2),
        min_order_amount DECIMAL(10, 2),
        max_uses INTEGER,
        current_uses INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT 1,
        expires_at DATETIME
    )
    """)
    
    cursor.execute("""
    CREATE TABLE shipping_methods (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        cost DECIMAL(10, 2) NOT NULL,
        estimated_days INTEGER,
        is_active BOOLEAN DEFAULT 1
    )
    """)
    
    cursor.execute("""
    CREATE TABLE payment_methods (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        card_type VARCHAR(20),
        last_four_digits VARCHAR(4),
        expiry_month INTEGER,
        expiry_year INTEGER,
        is_default BOOLEAN DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)
    
    print("✅ Database schema created!")
    
    # Insert existing data for tables we want to keep
    print("📊 Inserting existing data...")
    
    # Categories - KEEP EXISTING
    categories = [
        (1, "Electronics", "Electronic devices and gadgets"),
        (2, "Books", "Physical and digital books"),
        (3, "Clothing", "Men's and women's clothing"),
        (4, "Home & Garden", "Home improvement and gardening"),
        (5, "Sports", "Sports equipment and apparel")
    ]
    cursor.executemany("INSERT INTO categories (id, name, description) VALUES (?, ?, ?)", categories)
    
    # Users - KEEP EXISTING  
    users = [
        (1, "john_doe", "john@example.com", "John", "Doe", 1),
        (2, "jane_smith", "jane@example.com", "Jane", "Smith", 1),
        (3, "bob_wilson", "bob@example.com", "Bob", "Wilson", 1),
        (4, "alice_brown", "alice@example.com", "Alice", "Brown", 0),
        (5, "charlie_davis", "charlie@example.com", "Charlie", "Davis", 1),
        (6, "diana_miller", "diana@example.com", "Diana", "Miller", 1),
        (7, "eve_garcia", "eve@example.com", "Eve", "Garcia", 1),
        (8, "frank_rodriguez", "frank@example.com", "Frank", "Rodriguez", 0)
    ]
    cursor.executemany("INSERT INTO users (id, username, email, first_name, last_name, is_active) VALUES (?, ?, ?, ?, ?, ?)", users)
    
    # Products - KEEP EXISTING
    products = [
        (1, "iPhone 15", "Latest Apple smartphone", 999.99, 1, 50, 1),
        (2, "Samsung Galaxy S24", "Android flagship phone", 899.99, 1, 30, 1),
        (3, "Python Programming Book", "Learn Python programming", 49.99, 2, 100, 1),
        (4, "Men's T-Shirt", "Comfortable cotton t-shirt", 19.99, 3, 200, 1),
        (5, "Women's Jeans", "Stylish denim jeans", 79.99, 3, 75, 1),
        (6, "Garden Shovel", "Heavy-duty gardening tool", 29.99, 4, 25, 1),
        (7, "Basketball", "Official size basketball", 24.99, 5, 40, 1),
        (8, "Laptop Stand", "Ergonomic laptop stand", 39.99, 1, 60, 1),
        (9, "Cookbook", "Delicious recipes", 34.99, 2, 80, 1),
        (10, "Running Shoes", "Comfortable running shoes", 129.99, 5, 45, 1)
    ]
    cursor.executemany("INSERT INTO products (id, name, description, price, category_id, stock_quantity, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)", products)
    
    # Add some reviews for existing data relationship
    reviews = [
        (1, 1, 5, "Excellent phone!", 1),
        (2, 1, 4, "Good value for money", 1),
        (1, 3, 5, "Great book for beginners", 1),
        (3, 7, 4, "Nice basketball", 1),
        (2, 10, 5, "Love these shoes!", 1)
    ]
    cursor.executemany("INSERT INTO reviews (user_id, product_id, rating, comment, is_verified) VALUES (?, ?, ?, ?, ?)", reviews)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Sample database created: {db_path}")
    print(f"📈 Existing data:")
    print(f"   - Categories: {len(categories)} rows")
    print(f"   - Users: {len(users)} rows")
    print(f"   - Products: {len(products)} rows")
    print(f"   - Reviews: {len(reviews)} rows")
    
    return db_path


def demonstrate_mixed_mode_generation():
    """Demonstrate mixed mode data generation."""
    print("\n" + "="*60)
    print("🎲 JaySoft-DBMocker Mixed Mode Demonstration")
    print("="*60)
    
    # Create sample database with existing data
    db_path = create_sample_database()
    
    try:
        # Connect to database
        db_config = DatabaseConfig(
            host="localhost",
            port=3306,  # Dummy port, not used for SQLite
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
            
            # Show table dependencies
            dependencies = schema.get_table_dependencies()
            print("\n📊 Table Dependencies:")
            for table, deps in dependencies.items():
                if deps:
                    print(f"   {table} depends on: {', '.join(deps)}")
                else:
                    print(f"   {table} has no dependencies")
            
            # Configure mixed mode generation
            print("\n⚙️  Configuring Mixed Mode Generation:")
            print("   🔄 Tables using existing data: categories, users, products")
            print("   🆕 Tables generating new data: orders, order_items, wishlists, coupons, shipping_methods, payment_methods")
            
            generation_config = GenerationConfig(
                batch_size=100,
                seed=12345,  # For reproducible results
                preserve_existing_data=True,
                reuse_existing_values=0.9,  # High probability to reuse existing FK values
                use_existing_tables=["categories", "users", "products"],  # Mixed mode!
                prefer_existing_fk_values=True
            )
            
            # Configure table-specific settings
            generation_config.table_configs = {
                # Tables using existing data (will be skipped for generation)
                "categories": TableGenerationConfig(rows_to_generate=0, use_existing_data=True),
                "users": TableGenerationConfig(rows_to_generate=0, use_existing_data=True), 
                "products": TableGenerationConfig(rows_to_generate=0, use_existing_data=True),
                
                # Tables generating new data
                "orders": TableGenerationConfig(rows_to_generate=25),
                "order_items": TableGenerationConfig(rows_to_generate=75),
                "reviews": TableGenerationConfig(rows_to_generate=30),  # Reviews can use existing users/products
                "wishlists": TableGenerationConfig(rows_to_generate=15),
                "coupons": TableGenerationConfig(rows_to_generate=10),
                "shipping_methods": TableGenerationConfig(rows_to_generate=5),
                "payment_methods": TableGenerationConfig(rows_to_generate=20)
            }
            
            # Create smart generator with mixed mode support
            print("\n🧠 Creating smart dependency-aware generator...")
            smart_generator = DependencyAwareGenerator(schema, generation_config, db_conn)
            
            # Generate data
            print("\n🎲 Starting mixed mode data generation...")
            generated_data = smart_generator.generate_data_for_all_tables()
            
            # Show generation results
            print("\n📊 Generation Results:")
            for table_name, data in generated_data.items():
                if table_name in generation_config.use_existing_tables:
                    print(f"   {table_name}: Using existing data (no generation)")
                else:
                    print(f"   {table_name}: Generated {len(data)} new rows")
            
            # Insert generated data
            print("\n💾 Inserting generated data...")
            inserter = DataInserter(db_conn, schema)
            
            total_inserted = 0
            for table_name, data in generated_data.items():
                if data:  # Only insert if there's new data to insert
                    try:
                        inserted_count = inserter.insert_data(table_name, data)
                        total_inserted += inserted_count
                        print(f"   ✅ {table_name}: Inserted {inserted_count} rows")
                    except Exception as e:
                        print(f"   ❌ {table_name}: Failed to insert - {e}")
            
            print(f"\n🎉 Mixed Mode Generation Complete!")
            print(f"   📊 Total new rows inserted: {total_inserted}")
            print(f"   🔄 Tables using existing data: {len(generation_config.use_existing_tables)}")
            print(f"   🆕 Tables with new data: {len([t for t in generated_data.keys() if generated_data[t]])}")
            
            # Verify FK integrity
            print("\n🔍 Verifying Foreign Key Integrity...")
            
            # Check orders reference existing users
            result = db_conn.execute_query("SELECT COUNT(*) FROM orders WHERE user_id NOT IN (SELECT id FROM users)")
            invalid_fks = result[0][0] if result else 0
            if invalid_fks == 0:
                print("   ✅ Orders -> Users FK integrity: PASS")
            else:
                print(f"   ❌ Orders -> Users FK integrity: FAIL ({invalid_fks} invalid references)")
            
            # Check order_items reference existing orders and products
            result = db_conn.execute_query("SELECT COUNT(*) FROM order_items WHERE order_id NOT IN (SELECT id FROM orders)")
            invalid_order_fks = result[0][0] if result else 0
            result = db_conn.execute_query("SELECT COUNT(*) FROM order_items WHERE product_id NOT IN (SELECT id FROM products)")
            invalid_product_fks = result[0][0] if result else 0
            
            if invalid_order_fks == 0 and invalid_product_fks == 0:
                print("   ✅ Order Items FK integrity: PASS")
            else:
                print(f"   ❌ Order Items FK integrity: FAIL (orders: {invalid_order_fks}, products: {invalid_product_fks})")
            
            # Show final row counts
            print("\n📈 Final Database State:")
            for table in schema.tables:
                result = db_conn.execute_query(f'SELECT COUNT(*) FROM `{table.name}`')
                count = result[0][0] if result else 0
                status = "existing" if table.name in generation_config.use_existing_tables else "mixed"
                print(f"   {table.name}: {count} rows ({status})")
            
            print(f"\n✅ Mixed mode demonstration completed successfully!")
            print(f"   Database: {db_path}")
    
    except Exception as e:
        print(f"❌ Error during mixed mode generation: {e}")
        raise


if __name__ == "__main__":
    demonstrate_mixed_mode_generation()
