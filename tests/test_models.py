"""Tests for data models."""

import pytest
from dbmocker.core.models import (
    ColumnInfo, TableInfo, DatabaseSchema, ConstraintInfo,
    ColumnType, ConstraintType, GenerationConfig, 
    TableGenerationConfig, ColumnGenerationConfig
)


class TestColumnInfo:
    """Test ColumnInfo data class."""
    
    def test_column_creation(self):
        """Test basic column creation."""
        column = ColumnInfo(
            name="test_column",
            data_type=ColumnType.VARCHAR,
            max_length=100,
            is_nullable=True
        )
        
        assert column.name == "test_column"
        assert column.data_type == ColumnType.VARCHAR
        assert column.max_length == 100
        assert column.is_nullable is True
        assert column.default_value is None
        assert column.is_auto_increment is False
    
    def test_column_with_pattern(self):
        """Test column with detected pattern."""
        column = ColumnInfo(
            name="email",
            data_type=ColumnType.VARCHAR,
            detected_pattern="email",
            sample_values=["test@example.com", "user@domain.org"]
        )
        
        assert column.detected_pattern == "email"
        assert len(column.sample_values) == 2
        assert "test@example.com" in column.sample_values


class TestConstraintInfo:
    """Test ConstraintInfo data class."""
    
    def test_primary_key_constraint(self):
        """Test primary key constraint."""
        constraint = ConstraintInfo(
            name="users_pkey",
            type=ConstraintType.PRIMARY_KEY,
            columns=["id"]
        )
        
        assert constraint.name == "users_pkey"
        assert constraint.type == ConstraintType.PRIMARY_KEY
        assert constraint.columns == ["id"]
        assert constraint.referenced_table is None
    
    def test_foreign_key_constraint(self):
        """Test foreign key constraint."""
        constraint = ConstraintInfo(
            name="orders_user_fk",
            type=ConstraintType.FOREIGN_KEY,
            columns=["user_id"],
            referenced_table="users",
            referenced_columns=["id"],
            on_delete="CASCADE"
        )
        
        assert constraint.type == ConstraintType.FOREIGN_KEY
        assert constraint.referenced_table == "users"
        assert constraint.referenced_columns == ["id"]
        assert constraint.on_delete == "CASCADE"


class TestTableInfo:
    """Test TableInfo data class."""
    
    def create_sample_table(self) -> TableInfo:
        """Create a sample table for testing."""
        columns = [
            ColumnInfo(name="id", data_type=ColumnType.INTEGER, is_nullable=False),
            ColumnInfo(name="name", data_type=ColumnType.VARCHAR, max_length=100),
            ColumnInfo(name="email", data_type=ColumnType.VARCHAR, max_length=255)
        ]
        
        constraints = [
            ConstraintInfo(name="users_pkey", type=ConstraintType.PRIMARY_KEY, columns=["id"]),
            ConstraintInfo(name="users_email_unique", type=ConstraintType.UNIQUE, columns=["email"])
        ]
        
        return TableInfo(
            name="users",
            columns=columns,
            constraints=constraints,
            row_count=1000
        )
    
    def test_table_creation(self):
        """Test basic table creation."""
        table = self.create_sample_table()
        
        assert table.name == "users"
        assert len(table.columns) == 3
        assert len(table.constraints) == 2
        assert table.row_count == 1000
    
    def test_get_column(self):
        """Test getting column by name."""
        table = self.create_sample_table()
        
        column = table.get_column("name")
        assert column is not None
        assert column.name == "name"
        assert column.data_type == ColumnType.VARCHAR
        
        # Test non-existent column
        column = table.get_column("nonexistent")
        assert column is None
    
    def test_get_primary_key_columns(self):
        """Test getting primary key columns."""
        table = self.create_sample_table()
        pk_columns = table.get_primary_key_columns()
        
        assert pk_columns == ["id"]
    
    def test_get_foreign_key_columns(self):
        """Test getting foreign key columns."""
        table = self.create_sample_table()
        
        # Add a foreign key constraint
        fk_constraint = ConstraintInfo(
            name="users_dept_fk",
            type=ConstraintType.FOREIGN_KEY,
            columns=["dept_id"],
            referenced_table="departments",
            referenced_columns=["id"]
        )
        table.constraints.append(fk_constraint)
        
        fk_columns = table.get_foreign_key_columns()
        assert "dept_id" in fk_columns


class TestDatabaseSchema:
    """Test DatabaseSchema data class."""
    
    def create_sample_schema(self) -> DatabaseSchema:
        """Create a sample schema for testing."""
        users_table = TableInfo(
            name="users",
            columns=[
                ColumnInfo(name="id", data_type=ColumnType.INTEGER),
                ColumnInfo(name="name", data_type=ColumnType.VARCHAR)
            ]
        )
        
        orders_table = TableInfo(
            name="orders",
            columns=[
                ColumnInfo(name="id", data_type=ColumnType.INTEGER),
                ColumnInfo(name="user_id", data_type=ColumnType.INTEGER)
            ],
            foreign_keys=[
                ConstraintInfo(
                    name="orders_user_fk",
                    type=ConstraintType.FOREIGN_KEY,
                    columns=["user_id"],
                    referenced_table="users",
                    referenced_columns=["id"]
                )
            ]
        )
        
        return DatabaseSchema(
            database_name="test_db",
            tables=[users_table, orders_table]
        )
    
    def test_schema_creation(self):
        """Test basic schema creation."""
        schema = self.create_sample_schema()
        
        assert schema.database_name == "test_db"
        assert len(schema.tables) == 2
        assert len(schema.views) == 0
    
    def test_get_table(self):
        """Test getting table by name."""
        schema = self.create_sample_schema()
        
        table = schema.get_table("users")
        assert table is not None
        assert table.name == "users"
        
        # Test non-existent table
        table = schema.get_table("nonexistent")
        assert table is None
    
    def test_get_table_dependencies(self):
        """Test getting table dependencies."""
        schema = self.create_sample_schema()
        dependencies = schema.get_table_dependencies()
        
        assert "users" in dependencies
        assert "orders" in dependencies
        assert dependencies["users"] == []  # No dependencies
        assert dependencies["orders"] == ["users"]  # Depends on users


class TestGenerationConfig:
    """Test GenerationConfig model."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = GenerationConfig()
        
        assert config.batch_size == 1000
        assert config.max_workers == 4
        assert config.truncate_existing is False
        assert config.seed is None
        assert len(config.table_configs) == 0
        assert config.include_tables is None
        assert len(config.exclude_tables) == 0
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = GenerationConfig(
            batch_size=500,
            max_workers=2,
            truncate_existing=True,
            seed=42,
            include_tables=["users", "orders"],
            exclude_tables=["logs"]
        )
        
        assert config.batch_size == 500
        assert config.max_workers == 2
        assert config.truncate_existing is True
        assert config.seed == 42
        assert config.include_tables == ["users", "orders"]
        assert config.exclude_tables == ["logs"]


class TestTableGenerationConfig:
    """Test TableGenerationConfig model."""
    
    def test_default_table_config(self):
        """Test default table configuration."""
        config = TableGenerationConfig()
        
        assert config.rows_to_generate == 1000
        assert len(config.column_configs) == 0
        assert len(config.custom_generators) == 0
    
    def test_custom_table_config(self):
        """Test custom table configuration."""
        column_config = ColumnGenerationConfig(
            min_value=1,
            max_value=100,
            null_probability=0.1
        )
        
        config = TableGenerationConfig(
            rows_to_generate=5000,
            column_configs={"age": column_config}
        )
        
        assert config.rows_to_generate == 5000
        assert "age" in config.column_configs
        assert config.column_configs["age"].min_value == 1


class TestColumnGenerationConfig:
    """Test ColumnGenerationConfig model."""
    
    def test_default_column_config(self):
        """Test default column configuration."""
        config = ColumnGenerationConfig()
        
        assert config.min_value is None
        assert config.max_value is None
        assert config.min_length is None
        assert config.max_length is None
        assert config.pattern is None
        assert config.possible_values is None
        assert config.null_probability == 0.0
    
    def test_custom_column_config(self):
        """Test custom column configuration."""
        config = ColumnGenerationConfig(
            min_value=10,
            max_value=100,
            min_length=5,
            max_length=50,
            pattern=r"\d{3}-\d{3}-\d{4}",
            possible_values=["A", "B", "C"],
            null_probability=0.2,
            generator_function="custom_gen"
        )
        
        assert config.min_value == 10
        assert config.max_value == 100
        assert config.min_length == 5
        assert config.max_length == 50
        assert config.pattern == r"\d{3}-\d{3}-\d{4}"
        assert config.possible_values == ["A", "B", "C"]
        assert config.null_probability == 0.2
        assert config.generator_function == "custom_gen"
