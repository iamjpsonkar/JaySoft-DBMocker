"""Data models for schema representation and configuration."""

from typing import List, Dict, Any, Optional, Union
from enum import Enum
from dataclasses import dataclass, field
from pydantic import BaseModel, Field


class ColumnType(Enum):
    """Enumeration of supported column data types."""
    INTEGER = "integer"
    BIGINT = "bigint"
    SMALLINT = "smallint"
    DECIMAL = "decimal"
    FLOAT = "float"
    DOUBLE = "double"
    VARCHAR = "varchar"
    TEXT = "text"
    CHAR = "char"
    BOOLEAN = "boolean"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    JSON = "json"
    JSONB = "jsonb"  # PostgreSQL binary JSON
    BLOB = "blob"
    UUID = "uuid"
    ENUM = "enum"
    
    # Network and address types
    INET = "inet"  # IP address
    CIDR = "cidr"  # Network address
    MACADDR = "macaddr"  # MAC address
    
    # Spatial/Geometry types
    GEOMETRY = "geometry"
    POINT = "point"
    POLYGON = "polygon"
    
    # Array types
    ARRAY = "array"
    
    # Financial types
    MONEY = "money"
    
    # Binary types
    BYTEA = "bytea"  # PostgreSQL binary data
    VARBINARY = "varbinary"  # MySQL binary
    
    # XML type
    XML = "xml"


class ConstraintType(Enum):
    """Enumeration of database constraint types."""
    PRIMARY_KEY = "primary_key"
    FOREIGN_KEY = "foreign_key"
    UNIQUE = "unique"
    CHECK = "check"
    NOT_NULL = "not_null"
    DEFAULT = "default"


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: ColumnType
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    default_value: Optional[Any] = None
    is_auto_increment: bool = False
    enum_values: Optional[List[str]] = None
    comment: Optional[str] = None
    
    # Pattern detection from existing data
    detected_pattern: Optional[str] = None
    sample_values: List[Any] = field(default_factory=list)
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    avg_length: Optional[float] = None


@dataclass
class ConstraintInfo:
    """Information about database constraints."""
    name: str
    type: ConstraintType
    columns: List[str]
    referenced_table: Optional[str] = None
    referenced_columns: Optional[List[str]] = None
    check_condition: Optional[str] = None
    on_delete: Optional[str] = None
    on_update: Optional[str] = None


@dataclass
class IndexInfo:
    """Information about database indexes."""
    name: str
    columns: List[str]
    is_unique: bool = False
    is_primary: bool = False
    index_type: Optional[str] = None


@dataclass
class TableInfo:
    """Complete information about a database table."""
    name: str
    schema: Optional[str] = None
    columns: List[ColumnInfo] = field(default_factory=list)
    constraints: List[ConstraintInfo] = field(default_factory=list)
    indexes: List[IndexInfo] = field(default_factory=list)
    row_count: int = 0
    comment: Optional[str] = None
    
    # Relationships
    foreign_keys: List[ConstraintInfo] = field(default_factory=list)
    referenced_by: List[ConstraintInfo] = field(default_factory=list)
    
    def get_column(self, name: str) -> Optional[ColumnInfo]:
        """Get column by name."""
        for column in self.columns:
            if column.name == name:
                return column
        return None
    
    def get_primary_key_columns(self) -> List[str]:
        """Get primary key column names."""
        for constraint in self.constraints:
            if constraint.type == ConstraintType.PRIMARY_KEY:
                return constraint.columns
        return []
    
    def get_foreign_key_columns(self) -> List[str]:
        """Get foreign key column names."""
        fk_columns = []
        for constraint in self.constraints:
            if constraint.type == ConstraintType.FOREIGN_KEY:
                fk_columns.extend(constraint.columns)
        return fk_columns


@dataclass
class DatabaseSchema:
    """Complete database schema information."""
    database_name: str
    tables: List[TableInfo] = field(default_factory=list)
    views: List[str] = field(default_factory=list)
    functions: List[str] = field(default_factory=list)
    procedures: List[str] = field(default_factory=list)
    table_patterns: Dict[str, Any] = field(default_factory=dict)  # For pattern analysis results
    
    def get_table(self, name: str) -> Optional[TableInfo]:
        """Get table by name."""
        for table in self.tables:
            if table.name == name:
                return table
        return None
    
    def get_table_dependencies(self) -> Dict[str, List[str]]:
        """Get table dependency graph based on foreign keys."""
        dependencies = {}
        
        for table in self.tables:
            deps = []
            for fk in table.foreign_keys:
                if fk.referenced_table and fk.referenced_table != table.name:
                    deps.append(fk.referenced_table)
            dependencies[table.name] = deps
        
        return dependencies


class GenerationConfig(BaseModel):
    """Configuration for data generation."""
    
    # Global settings
    batch_size: int = Field(default=1000, description="Batch size for bulk inserts")
    max_workers: int = Field(default=4, description="Number of worker threads")
    truncate_existing: bool = Field(default=False, description="Truncate existing data")
    seed: Optional[int] = Field(default=None, description="Random seed for reproducible data")
    
    # Table-specific settings
    table_configs: Dict[str, "TableGenerationConfig"] = Field(
        default_factory=dict, description="Per-table generation configuration"
    )
    
    # Inclusion/exclusion
    include_tables: Optional[List[str]] = Field(
        default=None, description="Tables to include (None = all)"
    )
    exclude_tables: List[str] = Field(
        default_factory=list, description="Tables to exclude"
    )
    
    # Data relationships
    preserve_existing_data: bool = Field(
        default=True, description="Preserve existing data for FK references"
    )
    reuse_existing_values: float = Field(
        default=0.3, description="Probability of reusing existing values"
    )
    
    # Pattern analysis options (NEW FEATURE)
    analyze_existing_data: bool = Field(
        default=False, description="Analyze existing data for realistic generation patterns"
    )
    pattern_sample_size: int = Field(
        default=1000, description="Sample size for existing data pattern analysis"
    )


class TableGenerationConfig(BaseModel):
    """Configuration for generating data for a specific table."""
    
    rows_to_generate: int = Field(default=1000, description="Number of rows to generate")
    
    # Column-specific overrides
    column_configs: Dict[str, "ColumnGenerationConfig"] = Field(
        default_factory=dict, description="Per-column generation configuration"
    )
    
    # Custom generators
    custom_generators: Dict[str, str] = Field(
        default_factory=dict, description="Custom generator functions for columns"
    )


class ColumnGenerationConfig(BaseModel):
    """Configuration for generating data for a specific column."""
    
    # Value constraints
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    
    # Pattern constraints
    pattern: Optional[str] = None
    format_string: Optional[str] = None
    
    # Value sets
    possible_values: Optional[List[Any]] = None
    weighted_values: Optional[Dict[Any, float]] = None
    
    # Null probability
    null_probability: float = Field(default=0.0, description="Probability of generating NULL")
    
    # Custom generator
    generator_function: Optional[str] = None


# Update forward references
GenerationConfig.model_rebuild()
TableGenerationConfig.model_rebuild()


@dataclass
class GenerationStats:
    """Statistics from data generation process."""
    tables_processed: int = 0
    total_rows_generated: int = 0
    total_time_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    table_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
