"""Demo setup script for JaySoft:DBMocker - creates a sample database with realistic schema."""

import sqlite3
import os
from pathlib import Path


def create_demo_database(db_path: str = "demo.db"):
    """Create a demo SQLite database with realistic e-commerce schema."""
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables with realistic e-commerce schema
    
    # Users table
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            phone VARCHAR(20),
            date_of_birth DATE,
            age INTEGER CHECK (age >= 13 AND age <= 120),
            gender VARCHAR(10) CHECK (gender IN ('male', 'female', 'other')),
            country VARCHAR(100),
            city VARCHAR(100),
            postal_code VARCHAR(20),
            address TEXT,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    
    # Categories table
    cursor.execute("""
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(100) UNIQUE NOT NULL,
            description TEXT,
            parent_id INTEGER,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_id) REFERENCES categories(id)
        )
    """)
    
    # Products table
    cursor.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            sku VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
            cost DECIMAL(10,2) CHECK (cost >= 0),
            weight DECIMAL(8,3),
            dimensions VARCHAR(50),
            stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
            reorder_level INTEGER DEFAULT 0,
            status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'discontinued')),
            brand VARCHAR(100),
            model VARCHAR(100),
            color VARCHAR(50),
            size VARCHAR(20),
            material VARCHAR(100),
            warranty_months INTEGER,
            rating DECIMAL(3,2) CHECK (rating >= 0 AND rating <= 5),
            review_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
    """)
    
    # Orders table
    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            order_number VARCHAR(50) UNIQUE NOT NULL,
            order_date DATE NOT NULL,
            order_time TIME,
            status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'returned')),
            payment_method VARCHAR(50),
            payment_status VARCHAR(20) DEFAULT 'pending' CHECK (payment_status IN ('pending', 'paid', 'failed', 'refunded')),
            subtotal DECIMAL(12,2) NOT NULL CHECK (subtotal >= 0),
            tax_amount DECIMAL(10,2) DEFAULT 0 CHECK (tax_amount >= 0),
            shipping_cost DECIMAL(8,2) DEFAULT 0 CHECK (shipping_cost >= 0),
            discount_amount DECIMAL(10,2) DEFAULT 0 CHECK (discount_amount >= 0),
            total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
            currency VARCHAR(3) DEFAULT 'USD',
            shipping_address TEXT,
            billing_address TEXT,
            notes TEXT,
            estimated_delivery DATE,
            actual_delivery DATE,
            tracking_number VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    # Order items table
    cursor.execute("""
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
            discount_percent DECIMAL(5,2) DEFAULT 0 CHECK (discount_percent >= 0 AND discount_percent <= 100),
            discount_amount DECIMAL(10,2) DEFAULT 0 CHECK (discount_amount >= 0),
            total_price DECIMAL(12,2) NOT NULL CHECK (total_price >= 0),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (order_id, product_id),
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    
    # Reviews table
    cursor.execute("""
        CREATE TABLE reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            order_id INTEGER,
            rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
            title VARCHAR(200),
            comment TEXT,
            is_verified BOOLEAN DEFAULT 0,
            helpful_count INTEGER DEFAULT 0,
            total_votes INTEGER DEFAULT 0,
            status VARCHAR(20) DEFAULT 'published' CHECK (status IN ('pending', 'published', 'hidden', 'spam')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            UNIQUE (user_id, product_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (product_id) REFERENCES products(id),
            FOREIGN KEY (order_id) REFERENCES orders(id)
        )
    """)
    
    # Wishlists table
    cursor.execute("""
        CREATE TABLE wishlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name VARCHAR(100) DEFAULT 'My Wishlist',
            description TEXT,
            is_public BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    # Wishlist items table
    cursor.execute("""
        CREATE TABLE wishlist_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wishlist_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes VARCHAR(500),
            priority INTEGER DEFAULT 0 CHECK (priority >= 0 AND priority <= 10),
            UNIQUE (wishlist_id, product_id),
            FOREIGN KEY (wishlist_id) REFERENCES wishlists(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    
    # Coupons table
    cursor.execute("""
        CREATE TABLE coupons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            discount_type VARCHAR(20) NOT NULL CHECK (discount_type IN ('percentage', 'fixed_amount', 'free_shipping')),
            discount_value DECIMAL(10,2) NOT NULL CHECK (discount_value >= 0),
            minimum_order_amount DECIMAL(10,2) DEFAULT 0,
            maximum_discount_amount DECIMAL(10,2),
            usage_limit INTEGER,
            usage_count INTEGER DEFAULT 0,
            user_limit INTEGER DEFAULT 1,
            valid_from DATE,
            valid_until DATE,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Shopping cart table
    cursor.execute("""
        CREATE TABLE shopping_cart (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            UNIQUE (user_id, product_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        )
    """)
    
    # Inventory log table
    cursor.execute("""
        CREATE TABLE inventory_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            change_type VARCHAR(20) NOT NULL CHECK (change_type IN ('restock', 'sale', 'return', 'adjustment', 'damage')),
            quantity_change INTEGER NOT NULL,
            old_quantity INTEGER NOT NULL,
            new_quantity INTEGER NOT NULL,
            reference_id INTEGER,
            reference_type VARCHAR(50),
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER,
            FOREIGN KEY (product_id) REFERENCES products(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        )
    """)
    
    # Insert some sample data to establish patterns
    
    # Sample users
    sample_users = [
        ('john_doe', 'john.doe@email.com', 'John', 'Doe', '+1-555-0101', '1985-06-15', 38, 'male', 'United States', 'New York', '10001', '123 Main St'),
        ('jane_smith', 'jane.smith@gmail.com', 'Jane', 'Smith', '+1-555-0102', '1990-03-22', 33, 'female', 'United States', 'Los Angeles', '90210', '456 Oak Ave'),
        ('mike_wilson', 'mike.wilson@outlook.com', 'Mike', 'Wilson', '+44-20-7946-0958', '1988-11-08', 35, 'male', 'United Kingdom', 'London', 'SW1A 1AA', '789 High St'),
        ('sara_johnson', 'sara.j@company.com', 'Sara', 'Johnson', '+33-1-42-96-12-34', '1992-09-14', 31, 'female', 'France', 'Paris', '75001', '321 Rue de la Paix'),
        ('alex_brown', 'alex.brown@university.edu', 'Alex', 'Brown', '+49-30-12345678', '1995-01-30', 28, 'other', 'Germany', 'Berlin', '10115', '654 Hauptstraße')
    ]
    
    for user in sample_users:
        cursor.execute("""
            INSERT INTO users (username, email, first_name, last_name, phone, date_of_birth, age, gender, country, city, postal_code, address, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, user)
    
    # Sample categories
    sample_categories = [
        ('Electronics', 'Electronic devices and gadgets'),
        ('Computers', 'Desktop and laptop computers, accessories'),
        ('Smartphones', 'Mobile phones and accessories'),
        ('Clothing', 'Fashion and apparel'),
        ('Books', 'Physical and digital books'),
        ('Home & Garden', 'Home improvement and gardening supplies'),
        ('Sports & Outdoors', 'Sports equipment and outdoor gear'),
        ('Health & Beauty', 'Health and beauty products'),
        ('Toys & Games', 'Toys for children and adults'),
        ('Automotive', 'Car parts and accessories')
    ]
    
    for category in sample_categories:
        cursor.execute("INSERT INTO categories (name, description) VALUES (?, ?)", category)
    
    # Sample products
    sample_products = [
        (1, 'LAPTOP-001', 'Dell XPS 13 Laptop', 'High-performance ultrabook with 13-inch display', 999.99, 750.00, 1.2, '30.2x19.9x1.4 cm', 25, 5, 'active', 'Dell', 'XPS 13', 'Silver', '13"'),
        (1, 'PHONE-001', 'iPhone 14 Pro', 'Latest Apple smartphone with advanced camera', 1099.99, 800.00, 0.2, '14.7x7.2x0.8 cm', 15, 3, 'active', 'Apple', 'iPhone 14 Pro', 'Space Black', '128GB'),
        (2, 'MOUSE-001', 'Logitech MX Master 3', 'Wireless ergonomic mouse for productivity', 99.99, 60.00, 0.14, '12.6x8.4x5.1 cm', 50, 10, 'active', 'Logitech', 'MX Master 3', 'Black', 'Wireless'),
        (4, 'SHIRT-001', 'Cotton T-Shirt', 'Comfortable 100% cotton t-shirt', 19.99, 8.00, 0.2, 'Various', 100, 20, 'active', 'Generic', 'Basic Tee', 'Blue', 'L'),
        (5, 'BOOK-001', 'Python Programming Guide', 'Comprehensive guide to Python programming', 49.99, 25.00, 0.8, '23x18x3 cm', 30, 5, 'active', 'TechBooks', 'Pro Guide', 'N/A', 'Paperback')
    ]
    
    for product in sample_products:
        cursor.execute("""
            INSERT INTO products (category_id, sku, name, description, price, cost, weight, dimensions, stock_quantity, reorder_level, status, brand, model, color, size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, product)
    
    # Sample orders
    sample_orders = [
        (1, 'ORD-2024-001', '2024-01-15', 'delivered', 'credit_card', 'paid', 1099.99, 87.99, 9.99, 0.00, 1197.97, 'USD'),
        (2, 'ORD-2024-002', '2024-01-16', 'shipped', 'paypal', 'paid', 119.98, 9.60, 5.99, 10.00, 125.57, 'USD'),
        (3, 'ORD-2024-003', '2024-01-17', 'processing', 'credit_card', 'paid', 49.99, 4.00, 0.00, 5.00, 48.99, 'USD'),
        (1, 'ORD-2024-004', '2024-01-18', 'pending', 'bank_transfer', 'pending', 999.99, 79.99, 0.00, 50.00, 1029.98, 'USD')
    ]
    
    for order in sample_orders:
        cursor.execute("""
            INSERT INTO orders (user_id, order_number, order_date, status, payment_method, payment_status, subtotal, tax_amount, shipping_cost, discount_amount, total_amount, currency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, order)
    
    # Sample order items
    sample_order_items = [
        (1, 2, 1, 1099.99, 0.00, 0.00, 1099.99),
        (2, 3, 1, 99.99, 0.00, 0.00, 99.99),
        (2, 4, 1, 19.99, 0.00, 0.00, 19.99),
        (3, 5, 1, 49.99, 10.00, 5.00, 44.99),
        (4, 1, 1, 999.99, 0.00, 0.00, 999.99)
    ]
    
    for item in sample_order_items:
        cursor.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_percent, discount_amount, total_price)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, item)
    
    # Sample reviews
    sample_reviews = [
        (1, 2, 1, 5, 'Excellent phone!', 'Great camera quality and performance. Highly recommended.', 1, 15, 18),
        (2, 3, 2, 4, 'Good mouse', 'Comfortable to use, but a bit pricey.', 1, 8, 12),
        (3, 5, 3, 5, 'Perfect for learning', 'Very well written and easy to follow examples.', 1, 22, 25)
    ]
    
    for review in sample_reviews:
        cursor.execute("""
            INSERT INTO reviews (user_id, product_id, order_id, rating, title, comment, is_verified, helpful_count, total_votes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, review)
    
    # Sample coupons
    sample_coupons = [
        ('WELCOME10', 'Welcome Discount', '10% off for new customers', 'percentage', 10.00, 50.00, 50.00, None, 100, 0, 1, '2024-01-01', '2024-12-31'),
        ('FREESHIP', 'Free Shipping', 'Free shipping on orders over $75', 'free_shipping', 0.00, 75.00, None, None, None, 0, 1, '2024-01-01', '2024-12-31'),
        ('SAVE25', 'Save $25', '$25 off orders over $200', 'fixed_amount', 25.00, 200.00, 25.00, None, 50, 0, 1, '2024-01-01', '2024-06-30')
    ]
    
    for coupon in sample_coupons:
        cursor.execute("""
            INSERT INTO coupons (code, name, description, discount_type, discount_value, minimum_order_amount, maximum_discount_amount, usage_limit, usage_count, user_limit, valid_from, valid_until)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, coupon)
    
    # Create indexes for better performance
    indexes = [
        "CREATE INDEX idx_users_email ON users(email)",
        "CREATE INDEX idx_users_username ON users(username)",
        "CREATE INDEX idx_products_category ON products(category_id)",
        "CREATE INDEX idx_products_sku ON products(sku)",
        "CREATE INDEX idx_orders_user ON orders(user_id)",
        "CREATE INDEX idx_orders_date ON orders(order_date)",
        "CREATE INDEX idx_order_items_order ON order_items(order_id)",
        "CREATE INDEX idx_order_items_product ON order_items(product_id)",
        "CREATE INDEX idx_reviews_product ON reviews(product_id)",
        "CREATE INDEX idx_reviews_user ON reviews(user_id)"
    ]
    
    for index in indexes:
        cursor.execute(index)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Demo database created successfully: {db_path}")
    print(f"📊 Database contains:")
    print(f"   • Users: 5 sample records")
    print(f"   • Categories: 10 sample records")
    print(f"   • Products: 5 sample records")
    print(f"   • Orders: 4 sample records")
    print(f"   • Order Items: 5 sample records")
    print(f"   • Reviews: 3 sample records")
    print(f"   • Coupons: 3 sample records")
    print(f"   • 10 optimized indexes")
    print(f"\n🚀 Ready for DBMocker testing!")
    print(f"\nTry: dbmocker analyze --driver sqlite --database {db_path} --host '' --port 0 --username '' --password ''")


if __name__ == "__main__":
    create_demo_database()
