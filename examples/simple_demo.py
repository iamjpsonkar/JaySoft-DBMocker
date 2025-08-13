"""Simple demo setup for JaySoft:DBMocker."""

import sqlite3
import os


def create_simple_demo_db(db_path: str = "simple_demo.db"):
    """Create a simple demo database."""
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Simple users table
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Simple products table
    cursor.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(200) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            category VARCHAR(100),
            in_stock BOOLEAN DEFAULT 1
        )
    """)
    
    # Simple orders table with FK
    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            order_date DATE DEFAULT CURRENT_DATE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    # Insert sample data
    sample_users = [
        ('John Doe', 'john@example.com', 25),
        ('Jane Smith', 'jane@example.com', 30),
        ('Bob Wilson', 'bob@example.com', 35)
    ]
    
    for user in sample_users:
        cursor.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", user)
    
    sample_products = [
        ('Laptop', 999.99, 'Electronics'),
        ('Book', 19.99, 'Books'),
        ('Coffee Mug', 9.99, 'Home')
    ]
    
    for product in sample_products:
        cursor.execute("INSERT INTO products (name, price, category) VALUES (?, ?, ?)", product)
    
    sample_orders = [
        (1, 999.99, 'delivered'),
        (2, 29.98, 'shipped'),
        (1, 9.99, 'pending')
    ]
    
    for order in sample_orders:
        cursor.execute("INSERT INTO orders (user_id, total_amount, status) VALUES (?, ?, ?)", order)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Simple demo database created: {db_path}")
    print("📊 Contains:")
    print("   • 3 users")
    print("   • 3 products") 
    print("   • 3 orders")
    print("   • 1 foreign key relationship (orders -> users)")


if __name__ == "__main__":
    create_simple_demo_db()
