"""Database specification analyzer using DESCRIBE statements."""

import logging
import re
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum

from .database import DatabaseConnection

logger = logging.getLogger(__name__)


class MySQLDataType(Enum):
    """MySQL data types with size information."""
    TINYINT = "tinyint"
    SMALLINT = "smallint"
    MEDIUMINT = "mediumint"
    INT = "int"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    FLOAT = "float"
    DOUBLE = "double"
    CHAR = "char"
    VARCHAR = "varchar"
    TINYTEXT = "tinytext"
    TEXT = "text"
    MEDIUMTEXT = "mediumtext"
    LONGTEXT = "longtext"
    BINARY = "binary"
    VARBINARY = "varbinary"
    TINYBLOB = "tinyblob"
    BLOB = "blob"
    MEDIUMBLOB = "mediumblob"
    LONGBLOB = "longblob"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    YEAR = "year"
    JSON = "json"
    ENUM = "enum"
    SET = "set"


@dataclass
class ColumnSpec:
    """Detailed column specification from DESCRIBE."""
    name: str
    data_type: str
    base_type: MySQLDataType
    max_length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    is_nullable: bool
    default_value: Optional[str]
    is_auto_increment: bool
    is_primary_key: bool
    is_unique: bool
    enum_values: Optional[List[str]]
    comment: Optional[str]
    
    # Derived constraints
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    character_set: Optional[str] = None


@dataclass
class TableSpec:
    """Complete table specification."""
    name: str
    columns: List[ColumnSpec]
    primary_keys: List[str]
    unique_constraints: List[List[str]]
    foreign_keys: List[Dict[str, Any]]
    check_constraints: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]
    row_count: int
    comment: Optional[str] = None


class DatabaseSpecAnalyzer:
    """Analyzes database using DESCRIBE and INFORMATION_SCHEMA for exact specifications."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.database_name = db_connection.config.database
        
    def analyze_all_tables(self, include_tables: Optional[List[str]] = None, 
                          exclude_tables: Optional[List[str]] = None) -> Dict[str, TableSpec]:
        """Analyze all tables in the database for exact specifications."""
        logger.info("Starting database specification analysis using DESCRIBE")
        
        # Get all tables
        tables = self._get_all_tables()
        
        # Apply filtering to match schema analysis
        if include_tables:
            tables = [t for t in tables if t in include_tables]
        if exclude_tables:
            tables = [t for t in tables if t not in exclude_tables]
        
        # Filter out common system/migration tables (same as SchemaAnalyzer)
        system_tables = {
            'alembic_version',  # Alembic migration table
            'django_migrations',  # Django migration table
            'schema_migrations',  # Rails migration table
            'flyway_schema_history',  # Flyway migration table
            'information_schema',  # MySQL system schema
            'performance_schema',  # MySQL performance schema
            'mysql',  # MySQL system database
            'sys'  # MySQL/SQL Server system schema
        }
        
        # Auto-exclude system tables unless explicitly included
        if not include_tables:  # Only auto-exclude if no specific tables requested
            tables = [t for t in tables if t not in system_tables]
            
        logger.info(f"Found {len(tables)} tables to analyze with DESCRIBE: {tables}")
        
        table_specs = {}
        
        for table_name in tables:
            logger.info(f"Analyzing table specifications: {table_name}")
            spec = self._analyze_table_spec(table_name)
            if spec:
                table_specs[table_name] = spec
        
        logger.info(f"Analyzed {len(table_specs)} table specifications")
        return table_specs
    
    def _get_all_tables(self) -> List[str]:
        """Get all table names from the database using multiple methods for consistency."""
        table_names = []
        
        # Method 1: Database-specific optimal query
        try:
            if self.db_connection.config.driver == "mysql":
                result = self.db_connection.execute_query("SHOW TABLES")
                table_names = [row[0] for row in result] if result else []
            elif self.db_connection.config.driver == "postgresql":
                result = self.db_connection.execute_query(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                )
                table_names = [row[0] for row in result] if result else []
            elif self.db_connection.config.driver == "sqlite":
                result = self.db_connection.execute_query(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                table_names = [row[0] for row in result] if result else []
            
            logger.debug(f"DatabaseSpecAnalyzer found tables: {table_names}")
            
        except Exception as e:
            logger.warning(f"Failed to get table names: {e}")
            
        return table_names
    
    def _analyze_table_spec(self, table_name: str) -> Optional[TableSpec]:
        """Analyze complete specification for a single table."""
        try:
            # Get basic column information from DESCRIBE
            columns = self._get_column_specs(table_name)
            
            # Get additional constraint information
            foreign_keys = self._get_foreign_keys(table_name)
            check_constraints = self._get_check_constraints(table_name)
            indexes = self._get_indexes(table_name)
            unique_constraints = self._get_unique_constraints(table_name)
            
            # Get row count
            row_count = self._get_row_count(table_name)
            
            # Extract primary keys
            primary_keys = [col.name for col in columns if col.is_primary_key]
            
            return TableSpec(
                name=table_name,
                columns=columns,
                primary_keys=primary_keys,
                unique_constraints=unique_constraints,
                foreign_keys=foreign_keys,
                check_constraints=check_constraints,
                indexes=indexes,
                row_count=row_count
            )
            
        except Exception as e:
            logger.error(f"Failed to analyze table {table_name}: {e}")
            logger.debug(f"Table {table_name} analysis error details:", exc_info=True)
            return None
    
    def _get_column_specs(self, table_name: str) -> List[ColumnSpec]:
        """Get detailed column specifications using DESCRIBE."""
        quoted_table = self.db_connection.quote_identifier(table_name)
        result = self.db_connection.execute_query(f"DESCRIBE {quoted_table}")
        
        columns = []
        for row in result:
            try:
                field, type_str, null, key, default, extra = row
                
                # Parse the type string (e.g., "varchar(50)", "int(11)", "enum('a','b')")
                base_type, max_length, precision, scale, enum_values = self._parse_type_string(type_str)
                
                # Determine constraints
                is_nullable = null.upper() == 'YES'
                is_primary_key = 'PRI' in key
                is_unique = 'UNI' in key
                is_auto_increment = 'auto_increment' in extra.lower()
                
                # Handle default value - convert to string if it's not already
                if default is not None:
                    if isinstance(default, (tuple, list)):
                        # Handle complex default values (might be tuples or expressions)
                        default_str = str(default[0]) if default else None
                    else:
                        default_str = str(default).strip() if hasattr(default, 'strip') else str(default)
                else:
                    default_str = None
                
                # Calculate value ranges for numeric types
                min_value, max_value = self._get_numeric_range(base_type, max_length, precision, scale)
                
                column_spec = ColumnSpec(
                    name=field,
                    data_type=type_str,
                    base_type=base_type,
                    max_length=max_length,
                    precision=precision,
                    scale=scale,
                    is_nullable=is_nullable,
                    default_value=default_str,
                    is_auto_increment=is_auto_increment,
                    is_primary_key=is_primary_key,
                    is_unique=is_unique,
                    enum_values=enum_values,
                    comment=None,
                    min_value=min_value,
                    max_value=max_value
                )
                
                columns.append(column_spec)
                
            except Exception as e:
                logger.warning(f"Failed to parse column {field if 'field' in locals() else '?'} in table {table_name}: {e}")
                continue
        
        return columns
    
    def _parse_type_string(self, type_str: str) -> Tuple[MySQLDataType, Optional[int], Optional[int], Optional[int], Optional[List[str]]]:
        """Parse MySQL type string into components."""
        type_str = type_str.lower()
        
        # Handle ENUM types: enum('value1','value2','value3')
        if type_str.startswith('enum('):
            enum_match = re.match(r"enum\((.*?)\)", type_str)
            if enum_match:
                # Extract enum values, handling quotes
                enum_str = enum_match.group(1)
                enum_values = [val.strip("'\"") for val in re.findall(r"'([^']*)'|\"([^\"]*)\"", enum_str)]
                return MySQLDataType.ENUM, None, None, None, enum_values
        
        # Handle SET types: set('value1','value2')
        if type_str.startswith('set('):
            set_match = re.match(r"set\((.*?)\)", type_str)
            if set_match:
                set_str = set_match.group(1)
                set_values = [val.strip("'\"") for val in re.findall(r"'([^']*)'|\"([^\"]*)\"", set_str)]
                return MySQLDataType.SET, None, None, None, set_values
        
        # Handle types with length: varchar(50), char(10), int(11)
        length_match = re.match(r"(\w+)\((\d+)\)", type_str)
        if length_match:
            base_type_str, length_str = length_match.groups()
            max_length = int(length_str)
            
            # Map to enum
            try:
                base_type = MySQLDataType(base_type_str)
                return base_type, max_length, None, None, None
            except ValueError:
                pass
        
        # Handle DECIMAL/NUMERIC with precision and scale: decimal(10,2)
        decimal_match = re.match(r"(decimal|numeric)\((\d+),(\d+)\)", type_str)
        if decimal_match:
            base_type_str, precision_str, scale_str = decimal_match.groups()
            precision = int(precision_str)
            scale = int(scale_str)
            return MySQLDataType.DECIMAL, None, precision, scale, None
        
        # Handle simple types without parameters
        simple_type = type_str.split('(')[0]  # Remove any parentheses
        try:
            base_type = MySQLDataType(simple_type)
            return base_type, None, None, None, None
        except ValueError:
            # Default to TEXT for unknown types
            logger.warning(f"Unknown type: {type_str}, defaulting to TEXT")
            return MySQLDataType.TEXT, None, None, None, None
    
    def _get_numeric_range(self, base_type: MySQLDataType, length: Optional[int], precision: Optional[int], scale: Optional[int]) -> Tuple[Optional[float], Optional[float]]:
        """Calculate min/max values for numeric types."""
        if base_type == MySQLDataType.TINYINT:
            return -128.0, 127.0
        elif base_type == MySQLDataType.SMALLINT:
            return -32768.0, 32767.0
        elif base_type == MySQLDataType.MEDIUMINT:
            return -8388608.0, 8388607.0
        elif base_type == MySQLDataType.INT:
            return -2147483648.0, 2147483647.0
        elif base_type == MySQLDataType.BIGINT:
            return -9223372036854775808.0, 9223372036854775807.0
        elif base_type == MySQLDataType.DECIMAL and precision is not None:
            # For DECIMAL(p,s), max value is 10^(p-s) - 10^(-s)
            if scale is not None:
                max_val = (10 ** (precision - scale)) - (10 ** (-scale))
                return -max_val, max_val
        elif base_type == MySQLDataType.FLOAT:
            return -3.402823466e+38, 3.402823466e+38
        elif base_type == MySQLDataType.DOUBLE:
            return -1.7976931348623157e+308, 1.7976931348623157e+308
        
        return None, None
    
    def _get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        query = f"""
        SELECT 
            COLUMN_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME,
            CONSTRAINT_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = '{self.database_name}'
        AND TABLE_NAME = '{table_name}'
        AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        
        try:
            result = self.db_connection.execute_query(query)
            
            foreign_keys = []
            for row in result:
                column_name, ref_table, ref_column, constraint_name = row
                foreign_keys.append({
                    'column': column_name,
                    'referenced_table': ref_table,
                    'referenced_column': ref_column,
                    'constraint_name': constraint_name
                })
            
            return foreign_keys
        except Exception as e:
            logger.debug(f"Could not get foreign keys for {table_name}: {e}")
            return []
    
    def _get_check_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """Get check constraints for a table."""
        # MySQL 8.0+ has check constraints - use correct column name
        query = f"""
        SELECT 
            CONSTRAINT_NAME,
            CHECK_CLAUSE
        FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS 
        WHERE CONSTRAINT_SCHEMA = '{self.database_name}'
        """
        
        try:
            result = self.db_connection.execute_query(query)
            
            constraints = []
            for row in result:
                constraint_name, check_clause = row
                # Simple filtering by table name in constraint name (common pattern)
                if table_name.lower() in constraint_name.lower():
                    constraints.append({
                        'name': constraint_name,
                        'condition': check_clause
                    })
            
            return constraints
        except Exception as e:
            logger.debug(f"Could not get check constraints for {table_name}: {e}")
            return []
    
    def _get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get index information for a table."""
        quoted_table = self.db_connection.quote_identifier(table_name)
        result = self.db_connection.execute_query(f"SHOW INDEX FROM {quoted_table}")
        
        indexes = []
        for row in result:
            table, non_unique, key_name, seq_in_index, column_name, collation, cardinality, sub_part, packed, null, index_type, comment, index_comment, visible, expression = row
            
            indexes.append({
                'name': key_name,
                'column': column_name,
                'is_unique': non_unique == 0,
                'type': index_type,
                'sequence': seq_in_index
            })
        
        return indexes
    
    def _get_unique_constraints(self, table_name: str) -> List[List[str]]:
        """Get unique constraints (including composite unique keys)."""
        indexes = self._get_indexes(table_name)
        
        # Group by constraint name for composite keys
        unique_groups = {}
        for idx in indexes:
            if idx['is_unique'] and idx['name'] != 'PRIMARY':
                if idx['name'] not in unique_groups:
                    unique_groups[idx['name']] = []
                unique_groups[idx['name']].append(idx['column'])
        
        # Sort by sequence and return as list of column lists
        unique_constraints = []
        for constraint_name, columns in unique_groups.items():
            unique_constraints.append(columns)
        
        return unique_constraints
    
    def _get_row_count(self, table_name: str) -> int:
        """Get current row count for a table."""
        try:
            quoted_table = self.db_connection.quote_identifier(table_name)
            result = self.db_connection.execute_query(f"SELECT COUNT(*) FROM {quoted_table}")
            return result[0][0] if result else 0
        except Exception as e:
            logger.debug(f"Could not get row count for {table_name}: {e}")
            return 0


def print_table_specs(table_specs: Dict[str, TableSpec], max_tables: int = 5):
    """Pretty print table specifications for debugging."""
    print("🔍 DATABASE SPECIFICATION ANALYSIS")
    print("=" * 80)
    print()
    
    tables_shown = 0
    for table_name, spec in table_specs.items():
        if tables_shown >= max_tables:
            print(f"... and {len(table_specs) - max_tables} more tables")
            break
            
        print(f"📋 TABLE: {table_name.upper()}")
        print(f"   Rows: {spec.row_count:,}")
        print(f"   Primary Keys: {', '.join(spec.primary_keys)}")
        print()
        
        print("   Column Specifications:")
        print("   " + "-" * 70)
        print("   Name                | Type           | Size | Null | Default")
        print("   " + "-" * 70)
        
        for col in spec.columns[:10]:  # Show first 10 columns
            type_info = col.data_type
            if col.max_length:
                size_info = str(col.max_length)
            elif col.precision:
                size_info = f"{col.precision},{col.scale}" if col.scale else str(col.precision)
            else:
                size_info = "N/A"
            
            null_info = "YES" if col.is_nullable else "NO"
            default_info = str(col.default_value)[:10] if col.default_value else "NULL"
            
            print(f"   {col.name:<19} | {type_info:<14} | {size_info:<4} | {null_info:<4} | {default_info}")
        
        if len(spec.columns) > 10:
            print(f"   ... and {len(spec.columns) - 10} more columns")
        
        if spec.foreign_keys:
            print(f"\n   Foreign Keys: {len(spec.foreign_keys)}")
            for fk in spec.foreign_keys[:3]:
                print(f"      {fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}")
        
        if spec.check_constraints:
            print(f"\n   Check Constraints: {len(spec.check_constraints)}")
            for check in spec.check_constraints[:3]:
                print(f"      {check['name']}: {check['condition']}")
        
        print()
        tables_shown += 1
