"""Tests for data generation functionality."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, date
from decimal import Decimal

from dbmocker.core.generator import DataGenerator
from dbmocker.core.models import (
    DatabaseSchema, TableInfo, ColumnInfo, ConstraintInfo,
    ColumnType, ConstraintType, GenerationConfig, TableGenerationConfig,
    ColumnGenerationConfig
)


class TestDataGenerator:
    """Test DataGenerator class."""
    
    def create_sample_schema(self) -> DatabaseSchema:
        """Create a sample database schema for testing."""
        # Create users table
        users_table = TableInfo(
            name="users",
            columns=[
                ColumnInfo(name="id", data_type=ColumnType.INTEGER, is_nullable=False),
                ColumnInfo(name="name", data_type=ColumnType.VARCHAR, max_length=100, is_nullable=False),
                ColumnInfo(name="email", data_type=ColumnType.VARCHAR, max_length=255, is_nullable=False,
                          detected_pattern="email"),
                ColumnInfo(name="age", data_type=ColumnType.INTEGER, is_nullable=True,
                          min_value=18, max_value=100),
                ColumnInfo(name="created_at", data_type=ColumnType.TIMESTAMP, is_nullable=False)
            ],
            constraints=[
                ConstraintInfo(name="users_pkey", type=ConstraintType.PRIMARY_KEY, columns=["id"]),
                ConstraintInfo(name="users_email_unique", type=ConstraintType.UNIQUE, columns=["email"])
            ]
        )
        
        # Create orders table
        orders_table = TableInfo(
            name="orders",
            columns=[
                ColumnInfo(name="id", data_type=ColumnType.INTEGER, is_nullable=False),
                ColumnInfo(name="user_id", data_type=ColumnType.INTEGER, is_nullable=False),
                ColumnInfo(name="total", data_type=ColumnType.DECIMAL, precision=10, scale=2, is_nullable=False),
                ColumnInfo(name="status", data_type=ColumnType.ENUM, 
                          enum_values=["pending", "confirmed", "shipped", "delivered"], is_nullable=False)
            ],
            constraints=[
                ConstraintInfo(name="orders_pkey", type=ConstraintType.PRIMARY_KEY, columns=["id"]),
                ConstraintInfo(name="orders_user_fk", type=ConstraintType.FOREIGN_KEY, 
                             columns=["user_id"], referenced_table="users", referenced_columns=["id"])
            ]
        )
        
        orders_table.foreign_keys = [
            ConstraintInfo(name="orders_user_fk", type=ConstraintType.FOREIGN_KEY,
                         columns=["user_id"], referenced_table="users", referenced_columns=["id"])
        ]
        
        return DatabaseSchema(
            database_name="test_db",
            tables=[users_table, orders_table]
        )
    
    def test_generator_initialization(self):
        """Test DataGenerator initialization."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        
        generator = DataGenerator(schema, config)
        
        assert generator.schema == schema
        assert generator.config == config
        assert generator.faker is not None
    
    def test_generate_integer(self):
        """Test integer value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_int", data_type=ColumnType.INTEGER)
        value = generator._generate_integer(column, None)
        
        assert isinstance(value, int)
        assert 1 <= value <= 2147483647
    
    def test_generate_integer_with_config(self):
        """Test integer generation with custom configuration."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_int", data_type=ColumnType.INTEGER)
        column_config = ColumnGenerationConfig(min_value=10, max_value=20)
        
        value = generator._generate_integer(column, column_config)
        
        assert isinstance(value, int)
        assert 10 <= value <= 20
    
    def test_generate_varchar(self):
        """Test varchar value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_varchar", data_type=ColumnType.VARCHAR, max_length=50)
        value = generator._generate_varchar(column, None)
        
        assert isinstance(value, str)
        assert len(value) <= 50
    
    def test_generate_decimal(self):
        """Test decimal value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_decimal", data_type=ColumnType.DECIMAL, precision=10, scale=2)
        value = generator._generate_decimal(column, None)
        
        assert isinstance(value, Decimal)
    
    def test_generate_boolean(self):
        """Test boolean value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_bool", data_type=ColumnType.BOOLEAN)
        value = generator._generate_boolean(column, None)
        
        assert isinstance(value, bool)
    
    def test_generate_date(self):
        """Test date value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_date", data_type=ColumnType.DATE)
        value = generator._generate_date(column, None)
        
        assert isinstance(value, date)
    
    def test_generate_datetime(self):
        """Test datetime value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_datetime", data_type=ColumnType.DATETIME)
        value = generator._generate_datetime(column, None)
        
        assert isinstance(value, datetime)
    
    def test_generate_enum(self):
        """Test enum value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_enum", data_type=ColumnType.ENUM, 
                           enum_values=["option1", "option2", "option3"])
        value = generator._generate_enum(column, None)
        
        assert value in ["option1", "option2", "option3"]
    
    def test_generate_from_pattern_email(self):
        """Test pattern-based generation for email."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_email", data_type=ColumnType.VARCHAR, 
                           detected_pattern="email")
        value = generator._generate_from_pattern(column, None)
        
        assert isinstance(value, str)
        assert "@" in value
    
    def test_generate_possible_values(self):
        """Test generation from possible values list."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_choice", data_type=ColumnType.VARCHAR)
        column_config = ColumnGenerationConfig(possible_values=["A", "B", "C"])
        
        value = generator._generate_varchar(column, column_config)
        
        assert value in ["A", "B", "C"]
    
    def test_null_generation(self):
        """Test null value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_nullable", data_type=ColumnType.VARCHAR, is_nullable=True)
        column_config = ColumnGenerationConfig(null_probability=1.0)  # Always null
        
        value = generator._generate_column_value(column, TableGenerationConfig(
            column_configs={"test_nullable": column_config}
        ))
        
        assert value is None
    
    def test_is_foreign_key_column(self):
        """Test foreign key column detection."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        orders_table = schema.get_table("orders")
        
        assert generator._is_foreign_key_column(orders_table, "user_id") is True
        assert generator._is_foreign_key_column(orders_table, "total") is False
    
    def test_generate_row_basic(self):
        """Test basic row generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        users_table = schema.get_table("users")
        table_config = TableGenerationConfig()
        
        row = generator._generate_row(users_table, table_config)
        
        assert "id" in row
        assert "name" in row
        assert "email" in row
        assert "age" in row
        assert "created_at" in row
        
        assert isinstance(row["id"], int)
        assert isinstance(row["name"], str)
        assert isinstance(row["email"], str)
        assert "@" in row["email"]  # Should be email pattern
    
    def test_generate_data_for_table(self):
        """Test data generation for entire table."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        users_data = generator.generate_data_for_table("users", 5)
        
        assert len(users_data) == 5
        assert all("id" in row for row in users_data)
        assert all("email" in row for row in users_data)
        assert all("@" in row["email"] for row in users_data)
    
    def test_generate_data_for_table_not_found(self):
        """Test data generation for non-existent table."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        with pytest.raises(ValueError, match="Table nonexistent not found"):
            generator.generate_data_for_table("nonexistent", 5)
    
    def test_custom_generator(self):
        """Test custom generator function."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        column = ColumnInfo(name="test_custom", data_type=ColumnType.VARCHAR)
        
        # Test built-in custom generator
        value = generator._apply_custom_generator("name", column)
        assert isinstance(value, str)
        assert len(value) > 0
        
        # Test non-existent custom generator
        value = generator._apply_custom_generator("nonexistent", column)
        assert isinstance(value, str)  # Should fall back to default generation
    
    def test_cache_generated_values(self):
        """Test value caching for FK references."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        row = {"id": 1, "name": "Test User"}
        generator._cache_generated_values("users", row)
        
        assert "users" in generator._generated_values
        assert "id" in generator._generated_values["users"]
        assert 1 in generator._generated_values["users"]["id"]
    
    @patch('dbmocker.core.generator.random.choice')
    def test_foreign_key_generation(self, mock_choice):
        """Test foreign key value generation."""
        schema = self.create_sample_schema()
        config = GenerationConfig(seed=42)
        generator = DataGenerator(schema, config)
        
        # Mock cached values
        generator._generated_values["users"] = {"id": [1, 2, 3]}
        mock_choice.return_value = 2
        
        orders_table = schema.get_table("orders")
        user_id_column = orders_table.get_column("user_id")
        
        value = generator._generate_foreign_key_value(orders_table, user_id_column)
        
        assert value == 2
        mock_choice.assert_called_once_with([1, 2, 3])
