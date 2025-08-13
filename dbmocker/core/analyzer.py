"""Database schema analysis and introspection utilities."""

import logging
import re
from typing import List, Dict, Any, Optional, Set
from collections import Counter
from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Inspector
from sqlalchemy.exc import SQLAlchemyError

from .database import DatabaseConnection
from .models import (
    DatabaseSchema, TableInfo, ColumnInfo, ConstraintInfo, IndexInfo,
    ColumnType, ConstraintType
)


logger = logging.getLogger(__name__)


class SchemaAnalyzer:
    """Analyzes database schema and existing data patterns."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize with database connection."""
        self.db_connection = db_connection
        self.inspector: Optional[Inspector] = None
        
    def analyze_schema(self, include_tables: Optional[List[str]] = None,
                      exclude_tables: Optional[List[str]] = None,
                      analyze_data_patterns: bool = True) -> DatabaseSchema:
        """Analyze complete database schema."""
        logger.info("Starting database schema analysis")
        
        if not self.db_connection.test_connection():
            raise ConnectionError("Database connection is not available")
        
        self.inspector = inspect(self.db_connection.engine)
        
        # Get database name
        database_name = self._get_database_name()
        
        # Get all table names
        table_names = self.inspector.get_table_names()
        
        # Filter tables based on include/exclude lists
        if include_tables:
            table_names = [t for t in table_names if t in include_tables]
        if exclude_tables:
            table_names = [t for t in table_names if t not in exclude_tables]
        
        logger.info(f"Found {len(table_names)} tables to analyze: {table_names}")
        
        # Analyze each table
        tables = []
        for table_name in table_names:
            try:
                logger.info(f"Analyzing table: {table_name}")
                table_info = self._analyze_table(table_name, analyze_data_patterns)
                tables.append(table_info)
            except Exception as e:
                logger.error(f"Failed to analyze table {table_name}: {e}")
                continue
        
        # Build dependency relationships
        self._build_table_relationships(tables)
        
        schema = DatabaseSchema(
            database_name=database_name,
            tables=tables,
            views=self.inspector.get_view_names(),
        )
        
        logger.info(f"Schema analysis complete. Analyzed {len(tables)} tables.")
        return schema
    
    def _get_database_name(self) -> str:
        """Get the current database name."""
        try:
            # Try different methods based on database type
            driver = self.db_connection.config.driver
            
            if driver == "postgresql":
                result = self.db_connection.execute_query("SELECT current_database()")
                return result[0][0]
            elif driver == "mysql":
                result = self.db_connection.execute_query("SELECT DATABASE()")
                return result[0][0]
            else:
                return self.db_connection.config.database
        except Exception as e:
            logger.warning(f"Could not determine database name: {e}")
            return self.db_connection.config.database
    
    def _analyze_table(self, table_name: str, analyze_patterns: bool = True) -> TableInfo:
        """Analyze a single table."""
        # Get basic table info
        columns = self._get_columns(table_name)
        constraints = self._get_constraints(table_name)
        indexes = self._get_indexes(table_name)
        row_count = self._get_row_count(table_name)
        
        # Analyze data patterns if requested
        if analyze_patterns and row_count > 0:
            self._analyze_data_patterns(table_name, columns)
        
        table_info = TableInfo(
            name=table_name,
            columns=columns,
            constraints=constraints,
            indexes=indexes,
            row_count=row_count
        )
        
        # Separate foreign keys for easier access
        table_info.foreign_keys = [
            c for c in constraints if c.type == ConstraintType.FOREIGN_KEY
        ]
        
        return table_info
    
    def _get_columns(self, table_name: str) -> List[ColumnInfo]:
        """Get column information for a table."""
        columns = []
        
        for column in self.inspector.get_columns(table_name):
            col_type = self._map_column_type(column["type"])
            
            col_info = ColumnInfo(
                name=column["name"],
                data_type=col_type,
                is_nullable=column["nullable"],
                default_value=column["default"],
                is_auto_increment=column.get("autoincrement", False),
                comment=column.get("comment")
            )
            
            # Set type-specific attributes
            self._set_column_type_attributes(col_info, column["type"])
            
            columns.append(col_info)
        
        return columns
    
    def _map_column_type(self, sqlalchemy_type) -> ColumnType:
        """Map SQLAlchemy type to our ColumnType enum."""
        type_name = str(sqlalchemy_type).lower()
        
        # Integer types
        if any(t in type_name for t in ["integer", "int", "serial"]):
            return ColumnType.INTEGER
        elif any(t in type_name for t in ["bigint", "bigserial"]):
            return ColumnType.BIGINT
        elif any(t in type_name for t in ["smallint", "smallserial"]):
            return ColumnType.SMALLINT
        
        # Decimal/Float types
        elif any(t in type_name for t in ["decimal", "numeric"]):
            return ColumnType.DECIMAL
        elif "float" in type_name:
            return ColumnType.FLOAT
        elif any(t in type_name for t in ["double", "real"]):
            return ColumnType.DOUBLE
        
        # String types
        elif any(t in type_name for t in ["varchar", "varying"]):
            return ColumnType.VARCHAR
        elif any(t in type_name for t in ["text", "longtext", "mediumtext"]):
            return ColumnType.TEXT
        elif "char" in type_name:
            return ColumnType.CHAR
        
        # Date/Time types
        elif "timestamp" in type_name:
            return ColumnType.TIMESTAMP
        elif "datetime" in type_name:
            return ColumnType.DATETIME
        elif "date" in type_name:
            return ColumnType.DATE
        elif "time" in type_name:
            return ColumnType.TIME
        
        # Other types
        elif any(t in type_name for t in ["boolean", "bool"]):
            return ColumnType.BOOLEAN
        elif "json" in type_name:
            return ColumnType.JSON
        elif any(t in type_name for t in ["uuid", "guid"]):
            return ColumnType.UUID
        elif "enum" in type_name:
            return ColumnType.ENUM
        elif any(t in type_name for t in ["blob", "binary", "bytea"]):
            return ColumnType.BLOB
        
        # Default to VARCHAR for unknown types
        return ColumnType.VARCHAR
    
    def _set_column_type_attributes(self, col_info: ColumnInfo, sqlalchemy_type) -> None:
        """Set type-specific attributes on column info."""
        try:
            # Length for string types
            if hasattr(sqlalchemy_type, "length") and sqlalchemy_type.length:
                col_info.max_length = sqlalchemy_type.length
            
            # Precision and scale for decimal types
            if hasattr(sqlalchemy_type, "precision") and sqlalchemy_type.precision:
                col_info.precision = sqlalchemy_type.precision
            if hasattr(sqlalchemy_type, "scale") and sqlalchemy_type.scale:
                col_info.scale = sqlalchemy_type.scale
            
            # Enum values
            if hasattr(sqlalchemy_type, "enums") and sqlalchemy_type.enums:
                col_info.enum_values = list(sqlalchemy_type.enums)
        except Exception as e:
            logger.debug(f"Could not extract type attributes: {e}")
    
    def _get_constraints(self, table_name: str) -> List[ConstraintInfo]:
        """Get constraint information for a table."""
        constraints = []
        
        # Primary key
        try:
            pk = self.inspector.get_pk_constraint(table_name)
            if pk and pk["constrained_columns"]:
                constraints.append(ConstraintInfo(
                    name=pk["name"] or f"{table_name}_pkey",
                    type=ConstraintType.PRIMARY_KEY,
                    columns=pk["constrained_columns"]
                ))
        except Exception as e:
            logger.debug(f"Could not get primary key for {table_name}: {e}")
        
        # Foreign keys
        try:
            for fk in self.inspector.get_foreign_keys(table_name):
                constraints.append(ConstraintInfo(
                    name=fk["name"],
                    type=ConstraintType.FOREIGN_KEY,
                    columns=fk["constrained_columns"],
                    referenced_table=fk["referred_table"],
                    referenced_columns=fk["referred_columns"],
                    on_delete=fk.get("ondelete"),
                    on_update=fk.get("onupdate")
                ))
        except Exception as e:
            logger.debug(f"Could not get foreign keys for {table_name}: {e}")
        
        # Unique constraints
        try:
            for uc in self.inspector.get_unique_constraints(table_name):
                constraints.append(ConstraintInfo(
                    name=uc["name"],
                    type=ConstraintType.UNIQUE,
                    columns=uc["column_names"]
                ))
        except Exception as e:
            logger.debug(f"Could not get unique constraints for {table_name}: {e}")
        
        # Check constraints
        try:
            for cc in self.inspector.get_check_constraints(table_name):
                constraints.append(ConstraintInfo(
                    name=cc["name"],
                    type=ConstraintType.CHECK,
                    columns=[],  # Check constraints don't have specific columns
                    check_condition=cc.get("sqltext")
                ))
        except Exception as e:
            logger.debug(f"Could not get check constraints for {table_name}: {e}")
        
        return constraints
    
    def _get_indexes(self, table_name: str) -> List[IndexInfo]:
        """Get index information for a table."""
        indexes = []
        
        try:
            for index in self.inspector.get_indexes(table_name):
                indexes.append(IndexInfo(
                    name=index["name"],
                    columns=index["column_names"],
                    is_unique=index["unique"],
                    index_type=index.get("type")
                ))
        except Exception as e:
            logger.debug(f"Could not get indexes for {table_name}: {e}")
        
        return indexes
    
    def _get_row_count(self, table_name: str) -> int:
        """Get approximate row count for a table."""
        try:
            result = self.db_connection.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            return result[0][0] if result else 0
        except Exception as e:
            logger.debug(f"Could not get row count for {table_name}: {e}")
            return 0
    
    def _analyze_data_patterns(self, table_name: str, columns: List[ColumnInfo]) -> None:
        """Analyze existing data to detect patterns."""
        logger.debug(f"Analyzing data patterns for table: {table_name}")
        
        # Sample some data for pattern analysis
        sample_size = min(1000, self._get_row_count(table_name))
        if sample_size == 0:
            return
        
        try:
            # Get sample data
            query = f"SELECT * FROM {table_name} LIMIT {sample_size}"
            sample_data = self.db_connection.execute_query(query)
            
            if not sample_data:
                return
            
            # Analyze each column
            for i, column in enumerate(columns):
                column_values = [row[i] for row in sample_data if row[i] is not None]
                
                if not column_values:
                    continue
                
                # Store sample values
                column.sample_values = column_values[:10]  # Keep first 10 for reference
                
                # Detect patterns based on data type
                if column.data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
                    self._analyze_numeric_patterns(column, column_values)
                elif column.data_type in [ColumnType.DECIMAL, ColumnType.FLOAT, ColumnType.DOUBLE]:
                    self._analyze_numeric_patterns(column, column_values)
                elif column.data_type in [ColumnType.VARCHAR, ColumnType.TEXT, ColumnType.CHAR]:
                    self._analyze_string_patterns(column, column_values)
                elif column.data_type in [ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP]:
                    self._analyze_date_patterns(column, column_values)
        
        except Exception as e:
            logger.debug(f"Pattern analysis failed for {table_name}: {e}")
    
    def _analyze_numeric_patterns(self, column: ColumnInfo, values: List[Any]) -> None:
        """Analyze patterns in numeric data."""
        try:
            numeric_values = [float(v) for v in values if v is not None]
            if numeric_values:
                column.min_value = min(numeric_values)
                column.max_value = max(numeric_values)
        except Exception as e:
            logger.debug(f"Numeric pattern analysis failed for {column.name}: {e}")
    
    def _analyze_string_patterns(self, column: ColumnInfo, values: List[Any]) -> None:
        """Analyze patterns in string data."""
        try:
            string_values = [str(v) for v in values if v is not None]
            if not string_values:
                return
            
            # Calculate average length
            lengths = [len(s) for s in string_values]
            column.avg_length = sum(lengths) / len(lengths)
            
            # Detect common patterns
            patterns = []
            
            # Email pattern
            email_count = sum(1 for s in string_values if re.match(r'^[^@]+@[^@]+\.[^@]+$', s))
            if email_count > len(string_values) * 0.5:
                patterns.append("email")
            
            # Phone pattern
            phone_count = sum(1 for s in string_values if re.match(r'^\+?[\d\s\-\(\)]+$', s))
            if phone_count > len(string_values) * 0.5:
                patterns.append("phone")
            
            # URL pattern
            url_count = sum(1 for s in string_values if re.match(r'^https?://', s))
            if url_count > len(string_values) * 0.5:
                patterns.append("url")
            
            # UUID pattern
            uuid_count = sum(1 for s in string_values if re.match(
                r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', s, re.I))
            if uuid_count > len(string_values) * 0.5:
                patterns.append("uuid")
            
            if patterns:
                column.detected_pattern = ",".join(patterns)
        
        except Exception as e:
            logger.debug(f"String pattern analysis failed for {column.name}: {e}")
    
    def _analyze_date_patterns(self, column: ColumnInfo, values: List[Any]) -> None:
        """Analyze patterns in date/time data."""
        try:
            # This is a simplified analysis - could be expanded
            # to detect date ranges, common formats, etc.
            pass
        except Exception as e:
            logger.debug(f"Date pattern analysis failed for {column.name}: {e}")
    
    def _build_table_relationships(self, tables: List[TableInfo]) -> None:
        """Build referenced_by relationships between tables."""
        # Create a map of table names to table objects
        table_map = {table.name: table for table in tables}
        
        # Build reverse foreign key relationships
        for table in tables:
            for fk in table.foreign_keys:
                if fk.referenced_table and fk.referenced_table in table_map:
                    referenced_table = table_map[fk.referenced_table]
                    
                    # Create a reverse FK constraint
                    reverse_fk = ConstraintInfo(
                        name=f"{fk.name}_reverse",
                        type=ConstraintType.FOREIGN_KEY,
                        columns=fk.referenced_columns or [],
                        referenced_table=table.name,
                        referenced_columns=fk.columns
                    )
                    referenced_table.referenced_by.append(reverse_fk)
