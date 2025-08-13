"""Test configuration and fixtures for DBMocker tests."""

import pytest
import tempfile
import os
from unittest.mock import Mock

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.models import (
    DatabaseSchema, TableInfo, ColumnInfo, ConstraintInfo,
    ColumnType, ConstraintType
)


@pytest.fixture
def temp_db_file():
    """Create a temporary database file for SQLite testing."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def mock_db_config():
    """Create a mock database configuration for testing."""
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="test_db",
        username="test_user",
        password="test_pass",
        driver="postgresql"
    )


@pytest.fixture
def mock_db_connection(mock_db_config):
    """Create a mock database connection for testing."""
    connection = Mock(spec=DatabaseConnection)
    connection.config = mock_db_config
    connection.test_connection.return_value = True
    return connection


@pytest.fixture
def sample_schema():
    """Create a sample database schema for testing."""
    # Users table
    users_table = TableInfo(
        name="users",
        columns=[
            ColumnInfo(
                name="id",
                data_type=ColumnType.INTEGER,
                is_nullable=False,
                is_auto_increment=True
            ),
            ColumnInfo(
                name="username",
                data_type=ColumnType.VARCHAR,
                max_length=50,
                is_nullable=False
            ),
            ColumnInfo(
                name="email",
                data_type=ColumnType.VARCHAR,
                max_length=255,
                is_nullable=False,
                detected_pattern="email"
            ),
            ColumnInfo(
                name="first_name",
                data_type=ColumnType.VARCHAR,
                max_length=100,
                is_nullable=True
            ),
            ColumnInfo(
                name="last_name",
                data_type=ColumnType.VARCHAR,
                max_length=100,
                is_nullable=True
            ),
            ColumnInfo(
                name="age",
                data_type=ColumnType.INTEGER,
                is_nullable=True,
                min_value=18,
                max_value=120
            ),
            ColumnInfo(
                name="is_active",
                data_type=ColumnType.BOOLEAN,
                is_nullable=False,
                default_value=True
            ),
            ColumnInfo(
                name="created_at",
                data_type=ColumnType.TIMESTAMP,
                is_nullable=False
            ),
            ColumnInfo(
                name="updated_at",
                data_type=ColumnType.TIMESTAMP,
                is_nullable=True
            )
        ],
        constraints=[
            ConstraintInfo(
                name="users_pkey",
                type=ConstraintType.PRIMARY_KEY,
                columns=["id"]
            ),
            ConstraintInfo(
                name="users_username_unique",
                type=ConstraintType.UNIQUE,
                columns=["username"]
            ),
            ConstraintInfo(
                name="users_email_unique",
                type=ConstraintType.UNIQUE,
                columns=["email"]
            )
        ],
        row_count=5000
    )
    
    # Categories table
    categories_table = TableInfo(
        name="categories",
        columns=[
            ColumnInfo(
                name="id",
                data_type=ColumnType.INTEGER,
                is_nullable=False,
                is_auto_increment=True
            ),
            ColumnInfo(
                name="name",
                data_type=ColumnType.VARCHAR,
                max_length=100,
                is_nullable=False
            ),
            ColumnInfo(
                name="description",
                data_type=ColumnType.TEXT,
                is_nullable=True
            )
        ],
        constraints=[
            ConstraintInfo(
                name="categories_pkey",
                type=ConstraintType.PRIMARY_KEY,
                columns=["id"]
            )
        ],
        row_count=50
    )
    
    # Products table
    products_table = TableInfo(
        name="products",
        columns=[
            ColumnInfo(
                name="id",
                data_type=ColumnType.INTEGER,
                is_nullable=False,
                is_auto_increment=True
            ),
            ColumnInfo(
                name="category_id",
                data_type=ColumnType.INTEGER,
                is_nullable=False
            ),
            ColumnInfo(
                name="name",
                data_type=ColumnType.VARCHAR,
                max_length=200,
                is_nullable=False
            ),
            ColumnInfo(
                name="description",
                data_type=ColumnType.TEXT,
                is_nullable=True
            ),
            ColumnInfo(
                name="price",
                data_type=ColumnType.DECIMAL,
                precision=10,
                scale=2,
                is_nullable=False
            ),
            ColumnInfo(
                name="stock_quantity",
                data_type=ColumnType.INTEGER,
                is_nullable=False,
                default_value=0
            ),
            ColumnInfo(
                name="status",
                data_type=ColumnType.ENUM,
                enum_values=["active", "inactive", "discontinued"],
                is_nullable=False,
                default_value="active"
            ),
            ColumnInfo(
                name="created_at",
                data_type=ColumnType.TIMESTAMP,
                is_nullable=False
            )
        ],
        constraints=[
            ConstraintInfo(
                name="products_pkey",
                type=ConstraintType.PRIMARY_KEY,
                columns=["id"]
            ),
            ConstraintInfo(
                name="products_category_fk",
                type=ConstraintType.FOREIGN_KEY,
                columns=["category_id"],
                referenced_table="categories",
                referenced_columns=["id"]
            )
        ],
        row_count=1000
    )
    
    products_table.foreign_keys = [
        ConstraintInfo(
            name="products_category_fk",
            type=ConstraintType.FOREIGN_KEY,
            columns=["category_id"],
            referenced_table="categories",
            referenced_columns=["id"]
        )
    ]
    
    # Orders table
    orders_table = TableInfo(
        name="orders",
        columns=[
            ColumnInfo(
                name="id",
                data_type=ColumnType.INTEGER,
                is_nullable=False,
                is_auto_increment=True
            ),
            ColumnInfo(
                name="user_id",
                data_type=ColumnType.INTEGER,
                is_nullable=False
            ),
            ColumnInfo(
                name="order_date",
                data_type=ColumnType.DATE,
                is_nullable=False
            ),
            ColumnInfo(
                name="total_amount",
                data_type=ColumnType.DECIMAL,
                precision=12,
                scale=2,
                is_nullable=False
            ),
            ColumnInfo(
                name="status",
                data_type=ColumnType.ENUM,
                enum_values=["pending", "confirmed", "shipped", "delivered", "cancelled"],
                is_nullable=False,
                default_value="pending"
            ),
            ColumnInfo(
                name="shipping_address",
                data_type=ColumnType.TEXT,
                is_nullable=True
            ),
            ColumnInfo(
                name="notes",
                data_type=ColumnType.TEXT,
                is_nullable=True
            )
        ],
        constraints=[
            ConstraintInfo(
                name="orders_pkey",
                type=ConstraintType.PRIMARY_KEY,
                columns=["id"]
            ),
            ConstraintInfo(
                name="orders_user_fk",
                type=ConstraintType.FOREIGN_KEY,
                columns=["user_id"],
                referenced_table="users",
                referenced_columns=["id"]
            )
        ],
        row_count=10000
    )
    
    orders_table.foreign_keys = [
        ConstraintInfo(
            name="orders_user_fk",
            type=ConstraintType.FOREIGN_KEY,
            columns=["user_id"],
            referenced_table="users",
            referenced_columns=["id"]
        )
    ]
    
    # Order items table
    order_items_table = TableInfo(
        name="order_items",
        columns=[
            ColumnInfo(
                name="id",
                data_type=ColumnType.INTEGER,
                is_nullable=False,
                is_auto_increment=True
            ),
            ColumnInfo(
                name="order_id",
                data_type=ColumnType.INTEGER,
                is_nullable=False
            ),
            ColumnInfo(
                name="product_id",
                data_type=ColumnType.INTEGER,
                is_nullable=False
            ),
            ColumnInfo(
                name="quantity",
                data_type=ColumnType.INTEGER,
                is_nullable=False,
                default_value=1
            ),
            ColumnInfo(
                name="unit_price",
                data_type=ColumnType.DECIMAL,
                precision=10,
                scale=2,
                is_nullable=False
            ),
            ColumnInfo(
                name="total_price",
                data_type=ColumnType.DECIMAL,
                precision=12,
                scale=2,
                is_nullable=False
            )
        ],
        constraints=[
            ConstraintInfo(
                name="order_items_pkey",
                type=ConstraintType.PRIMARY_KEY,
                columns=["id"]
            ),
            ConstraintInfo(
                name="order_items_order_fk",
                type=ConstraintType.FOREIGN_KEY,
                columns=["order_id"],
                referenced_table="orders",
                referenced_columns=["id"]
            ),
            ConstraintInfo(
                name="order_items_product_fk",
                type=ConstraintType.FOREIGN_KEY,
                columns=["product_id"],
                referenced_table="products",
                referenced_columns=["id"]
            ),
            ConstraintInfo(
                name="order_items_unique",
                type=ConstraintType.UNIQUE,
                columns=["order_id", "product_id"]
            )
        ],
        row_count=25000
    )
    
    order_items_table.foreign_keys = [
        ConstraintInfo(
            name="order_items_order_fk",
            type=ConstraintType.FOREIGN_KEY,
            columns=["order_id"],
            referenced_table="orders",
            referenced_columns=["id"]
        ),
        ConstraintInfo(
            name="order_items_product_fk",
            type=ConstraintType.FOREIGN_KEY,
            columns=["product_id"],
            referenced_table="products",
            referenced_columns=["id"]
        )
    ]
    
    return DatabaseSchema(
        database_name="sample_ecommerce_db",
        tables=[users_table, categories_table, products_table, orders_table, order_items_table]
    )
