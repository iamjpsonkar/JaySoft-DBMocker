"""Specification-driven data generator that uses exact database column specifications."""

import logging
import random
import string
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional
import json

from faker import Faker

from .db_spec_analyzer import TableSpec, ColumnSpec, MySQLDataType, DatabaseSpecAnalyzer
from .database import DatabaseConnection
from .dependency_resolver import DependencyResolver, InsertionPlan
from .smart_generator import SmartFKValueManager

logger = logging.getLogger(__name__)


class SpecificationDrivenGenerator:
    """Data generator that strictly follows database column specifications."""
    
    def __init__(self, db_connection: DatabaseConnection, table_specs: Dict[str, TableSpec]):
        self.db_connection = db_connection
        self.table_specs = table_specs
        self.faker = Faker()
        
        # Create dependency resolver using table specs
        self.dependency_resolver = self._create_dependency_resolver()
        self.insertion_plan = self.dependency_resolver.create_insertion_plan()
        
        # FK value manager for smart foreign key generation
        self.fk_manager = SmartFKValueManager(db_connection, None)  # Schema not needed
        
        # Track generated primary keys for FK reference
        self._generated_pks: Dict[str, List[Any]] = {}
        
        # Unique value tracking
        self._unique_values: Dict[str, Dict[str, set]] = {}
        
    def _create_dependency_resolver(self) -> DependencyResolver:
        """Create dependency resolver from table specifications."""
        # Convert table specs to the format expected by DependencyResolver
        # For now, we'll create a simple dependency graph based on foreign keys
        dependencies = {}
        
        for table_name, spec in self.table_specs.items():
            dependencies[table_name] = []
            for fk in spec.foreign_keys:
                if fk['referenced_table'] in self.table_specs:
                    dependencies[table_name].append(fk['referenced_table'])
        
        # Create a simplified resolver (we'll enhance this if needed)
        from .dependency_resolver import InsertionPlan
        from collections import defaultdict, deque
        
        # Topological sort
        in_degree = defaultdict(int)
        graph = defaultdict(list)
        all_tables = set(self.table_specs.keys())
        
        for table in all_tables:
            if table not in in_degree:
                in_degree[table] = 0
            
            for dep in dependencies.get(table, []):
                if dep in all_tables:
                    graph[dep].append(table)
                    in_degree[table] += 1
        
        queue = deque([table for table in all_tables if in_degree[table] == 0])
        result = []
        
        while queue:
            table = queue.popleft()
            result.append(table)
            
            for dependent in graph[table]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # Create insertion plan
        insertion_plan = InsertionPlan(
            insertion_order=result,
            dependency_graph=dependencies,
            circular_dependencies=[],
            independent_tables=[t for t in all_tables if not dependencies.get(t)]
        )
        
        # Create a mock resolver
        class MockResolver:
            def create_insertion_plan(self):
                return insertion_plan
        
        return MockResolver()
    
    def generate_data_for_all_tables(self, rows_per_table: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """Generate data for all tables following dependency order and exact specifications."""
        logger.info(f"Starting specification-driven generation for {len(self.table_specs)} tables")
        
        batches = self.insertion_plan.get_insertion_batches()
        all_data = {}
        
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)}: {', '.join(batch)}")
            
            for table_name in batch:
                if table_name not in self.table_specs:
                    continue
                
                spec = self.table_specs[table_name]
                logger.info(f"Generating {rows_per_table} rows for {table_name} based on exact specifications")
                
                table_data = self._generate_table_data(spec, rows_per_table)
                all_data[table_name] = table_data
                
                # Cache generated primary keys
                self._cache_primary_keys(spec, table_data)
                
                logger.info(f"Generated {len(table_data)} rows for {table_name}")
        
        return all_data
    
    def _generate_table_data(self, spec: TableSpec, rows_count: int) -> List[Dict[str, Any]]:
        """Generate data for a specific table using its exact specifications."""
        rows = []
        
        # Initialize unique value tracking for this table
        self._unique_values[spec.name] = {}
        for col in spec.columns:
            if col.is_unique or col.is_primary_key:
                self._unique_values[spec.name][col.name] = set()
        
        for i in range(rows_count):
            row = self._generate_row(spec, i + 1)
            rows.append(row)
        
        return rows
    
    def _generate_row(self, spec: TableSpec, row_number: int) -> Dict[str, Any]:
        """Generate a single row based on exact column specifications."""
        row = {}
        
        # First pass: Generate non-FK columns
        for col in spec.columns:
            if not self._is_foreign_key_column(spec, col.name):
                row[col.name] = self._generate_column_value(col, spec, row_number)
        
        # Second pass: Generate FK columns
        for col in spec.columns:
            if self._is_foreign_key_column(spec, col.name):
                row[col.name] = self._generate_foreign_key_value(col, spec)
        
        return row
    
    def _is_foreign_key_column(self, spec: TableSpec, column_name: str) -> bool:
        """Check if a column is a foreign key."""
        return any(fk['column'] == column_name for fk in spec.foreign_keys)
    
    def _generate_column_value(self, col: ColumnSpec, spec: TableSpec, row_number: int) -> Any:
        """Generate value for a column based on its exact specification."""
        
        # Handle NULL values
        if col.is_nullable and random.random() < 0.1:  # 10% chance of NULL
            return None
        
        # Handle default values
        if col.default_value and col.default_value.upper() != 'NULL' and random.random() < 0.3:
            return self._parse_default_value(col.default_value, col.base_type)
        
        # Handle auto-increment primary keys
        if col.is_auto_increment:
            # Get the highest existing value and continue from there
            existing_max = self._get_max_existing_value(spec.name, col.name)
            return existing_max + row_number
        
        # Generate based on data type and constraints
        return self._generate_by_type_and_constraints(col, spec, row_number)
    
    def _generate_by_type_and_constraints(self, col: ColumnSpec, spec: TableSpec, row_number: int) -> Any:
        """Generate value based on exact MySQL type and constraints."""
        
        base_type = col.base_type
        
        if base_type == MySQLDataType.TINYINT:
            return self._generate_integer_value(col, -128, 127)
        
        elif base_type == MySQLDataType.SMALLINT:
            return self._generate_integer_value(col, -32768, 32767)
        
        elif base_type == MySQLDataType.MEDIUMINT:
            return self._generate_integer_value(col, -8388608, 8388607)
        
        elif base_type == MySQLDataType.INT:
            return self._generate_integer_value(col, -2147483648, 2147483647)
        
        elif base_type == MySQLDataType.BIGINT:
            return self._generate_integer_value(col, -9223372036854775808, 9223372036854775807)
        
        elif base_type == MySQLDataType.DECIMAL:
            return self._generate_decimal_value(col)
        
        elif base_type == MySQLDataType.FLOAT:
            return round(random.uniform(-999999.99, 999999.99), 2)
        
        elif base_type == MySQLDataType.DOUBLE:
            return round(random.uniform(-999999999.99, 999999999.99), 2)
        
        elif base_type in [MySQLDataType.CHAR, MySQLDataType.VARCHAR]:
            return self._generate_string_value(col, spec)
        
        elif base_type in [MySQLDataType.TEXT, MySQLDataType.TINYTEXT, MySQLDataType.MEDIUMTEXT, MySQLDataType.LONGTEXT]:
            return self._generate_text_value(col, spec)
        
        elif base_type == MySQLDataType.JSON:
            return self._generate_json_value(col)
        
        elif base_type == MySQLDataType.ENUM:
            return self._generate_enum_value(col)
        
        elif base_type == MySQLDataType.SET:
            return self._generate_set_value(col)
        
        elif base_type in [MySQLDataType.DATE, MySQLDataType.DATETIME, MySQLDataType.TIMESTAMP]:
            return self._generate_datetime_value(col)
        
        elif base_type == MySQLDataType.TIME:
            return self._generate_time_value()
        
        elif base_type == MySQLDataType.YEAR:
            return random.randint(1901, 2155)
        
        elif base_type in [MySQLDataType.BINARY, MySQLDataType.VARBINARY, MySQLDataType.BLOB, MySQLDataType.TINYBLOB, MySQLDataType.MEDIUMBLOB, MySQLDataType.LONGBLOB]:
            return self._generate_binary_value(col)
        
        else:
            # Fallback for unknown types
            logger.warning(f"Unknown type {base_type} for column {col.name}, using string fallback")
            return self.faker.word()
    
    def _generate_integer_value(self, col: ColumnSpec, type_min: int, type_max: int) -> int:
        """Generate integer value within type and column constraints."""
        min_val = max(type_min, col.min_value or type_min)
        max_val = min(type_max, col.max_value or type_max)
        
        # Special handling for boolean-like tinyint(1)
        if col.base_type == MySQLDataType.TINYINT and col.max_length == 1:
            return random.randint(0, 1)
        
        value = random.randint(int(min_val), int(max_val))
        
        # Ensure uniqueness if required
        if col.is_unique or col.is_primary_key:
            value = self._ensure_unique_value(col, value)
        
        return value
    
    def _generate_decimal_value(self, col: ColumnSpec) -> Decimal:
        """Generate decimal value with exact precision and scale."""
        if col.precision is None:
            return Decimal(str(round(random.uniform(0, 999999.99), 2)))
        
        scale = col.scale or 0
        precision = col.precision
        
        # Generate random number with exact precision and scale
        max_integer_digits = precision - scale
        max_value = (10 ** max_integer_digits) - 1
        
        integer_part = random.randint(0, max_value)
        
        if scale > 0:
            decimal_part = random.randint(0, (10 ** scale) - 1)
            value_str = f"{integer_part}.{decimal_part:0{scale}d}"
        else:
            value_str = str(integer_part)
        
        return Decimal(value_str)
    
    def _generate_string_value(self, col: ColumnSpec, spec: TableSpec) -> str:
        """Generate string value based on column name patterns and exact length constraints."""
        max_length = col.max_length or 255
        
        # Generate based on column name patterns
        name_lower = col.name.lower()
        
        if 'email' in name_lower:
            email = self.faker.email()
            return email[:max_length] if len(email) > max_length else email
        
        elif any(pattern in name_lower for pattern in ['phone', 'mobile', 'tel']):
            phone = self.faker.phone_number()
            return self._smart_truncate_phone(phone, max_length)
        
        elif any(pattern in name_lower for pattern in ['name', 'title']):
            if max_length <= 10:
                name = self.faker.first_name()
            elif max_length <= 25:
                name = self.faker.name()
            else:
                name = self.faker.name() + " " + self.faker.last_name()
            return name[:max_length]
        
        elif any(pattern in name_lower for pattern in ['address', 'location']):
            if max_length <= 50:
                addr = self.faker.street_address()
            else:
                addr = self.faker.address().replace('\n', ', ')
            return addr[:max_length]
        
        elif any(pattern in name_lower for pattern in ['url', 'website', 'link']):
            url = self.faker.url()
            return url[:max_length] if len(url) > max_length else url
        
        elif any(pattern in name_lower for pattern in ['code', 'id']) and col.name != 'id':
            # Generate code-like strings
            if max_length <= 10:
                return ''.join(random.choices(string.ascii_uppercase + string.digits, k=min(max_length, 8)))
            else:
                return self.faker.uuid4()[:max_length]
        
        elif 'comment' in name_lower or 'description' in name_lower:
            if max_length <= 50:
                text = self.faker.sentence()
            else:
                text = self.faker.text(max_nb_chars=max_length)
            return text[:max_length]
        
        else:
            # Generic string generation based on length
            if max_length <= 5:
                return ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, max_length)))
            elif max_length <= 20:
                return self.faker.word()[:max_length]
            elif max_length <= 50:
                return self.faker.sentence()[:max_length]
            else:
                return self.faker.text(max_nb_chars=max_length)[:max_length]
    
    def _generate_text_value(self, col: ColumnSpec, spec: TableSpec) -> str:
        """Generate text value for TEXT columns."""
        name_lower = col.name.lower()
        
        if 'comment' in name_lower or 'description' in name_lower or 'note' in name_lower:
            return self.faker.text(max_nb_chars=1000)
        else:
            return self.faker.paragraph()
    
    def _generate_json_value(self, col: ColumnSpec) -> str:
        """Generate JSON value based on column name context."""
        name_lower = col.name.lower()
        
        if 'config' in name_lower or 'setting' in name_lower:
            data = {
                "enabled": self.faker.boolean(),
                "timeout": random.randint(30, 3600),
                "retries": random.randint(1, 5),
                "debug": self.faker.boolean()
            }
        elif 'meta' in name_lower or 'metadata' in name_lower:
            data = {
                "id": random.randint(1000, 9999),
                "name": self.faker.name(),
                "tags": [self.faker.word() for _ in range(random.randint(1, 4))],
                "value": round(random.uniform(1, 1000), 2),
                "active": self.faker.boolean(),
                "priority": random.choice(["low", "medium", "high", "critical"]),
                "created_at": self.faker.iso8601()
            }
        else:
            # Generic JSON
            data = {
                "status": random.choice(["active", "inactive", "pending"]),
                "count": random.randint(0, 100),
                "data": self.faker.sentence()
            }
        
        return json.dumps(data)
    
    def _generate_enum_value(self, col: ColumnSpec) -> str:
        """Generate ENUM value from available options."""
        if col.enum_values:
            return random.choice(col.enum_values)
        else:
            return "default"
    
    def _generate_set_value(self, col: ColumnSpec) -> str:
        """Generate SET value (comma-separated values)."""
        if col.enum_values:
            selected_count = random.randint(1, min(3, len(col.enum_values)))
            selected_values = random.sample(col.enum_values, selected_count)
            return ",".join(selected_values)
        else:
            return "default"
    
    def _generate_datetime_value(self, col: ColumnSpec) -> datetime:
        """Generate datetime value."""
        if col.base_type == MySQLDataType.DATE:
            return self.faker.date_between(start_date='-5y', end_date='today')
        else:
            return self.faker.date_time_between(start_date='-5y', end_date='now')
    
    def _generate_time_value(self) -> str:
        """Generate time value."""
        return f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
    
    def _generate_binary_value(self, col: ColumnSpec) -> bytes:
        """Generate binary value."""
        length = min(col.max_length or 16, 32)  # Limit binary size
        return bytes([random.randint(0, 255) for _ in range(length)])
    
    def _generate_foreign_key_value(self, col: ColumnSpec, spec: TableSpec) -> Any:
        """Generate foreign key value using existing or generated data."""
        # Find the FK constraint for this column
        fk_info = None
        for fk in spec.foreign_keys:
            if fk['column'] == col.name:
                fk_info = fk
                break
        
        if not fk_info:
            # Fallback to regular value generation
            return self._generate_by_type_and_constraints(col, spec, 1)
        
        referenced_table = fk_info['referenced_table']
        referenced_column = fk_info['referenced_column']
        
        # Try to get existing values first
        existing_values = self.fk_manager.get_existing_values(referenced_table, referenced_column)
        
        # Add any generated values
        if referenced_table in self._generated_pks:
            existing_values.extend(self._generated_pks[referenced_table])
        
        if existing_values:
            return random.choice(existing_values)
        else:
            # Generate a fallback value that matches the column type
            logger.warning(f"No FK values available for {col.name} -> {referenced_table}.{referenced_column}")
            return self._generate_by_type_and_constraints(col, spec, 1)
    
    def _cache_primary_keys(self, spec: TableSpec, table_data: List[Dict[str, Any]]):
        """Cache generated primary key values for FK reference."""
        if not spec.primary_keys:
            return
        
        if spec.name not in self._generated_pks:
            self._generated_pks[spec.name] = []
        
        for row in table_data:
            for pk_col in spec.primary_keys:
                if pk_col in row and row[pk_col] is not None:
                    self._generated_pks[spec.name].append(row[pk_col])
    
    def _get_max_existing_value(self, table_name: str, column_name: str) -> int:
        """Get the maximum existing value for auto-increment columns."""
        try:
            quoted_table = self.db_connection.quote_identifier(table_name)
            quoted_column = self.db_connection.quote_identifier(column_name)
            query = f"SELECT COALESCE(MAX({quoted_column}), 0) FROM {quoted_table}"
            result = self.db_connection.execute_query(query)
            return result[0][0] if result else 0
        except Exception as e:
            logger.debug(f"Could not get max value for {table_name}.{column_name}: {e}")
            return 0
    
    def _ensure_unique_value(self, col: ColumnSpec, value: Any) -> Any:
        """Ensure value is unique for unique columns."""
        table_name = None  # We need to pass this in context
        
        # For now, simple increment approach for integers
        if isinstance(value, int):
            original_value = value
            attempt = 0
            while value in self._unique_values.get(table_name, {}).get(col.name, set()):
                value = original_value + attempt + 1
                attempt += 1
                if attempt > 1000:  # Prevent infinite loops
                    break
            
            # Track this value
            if table_name in self._unique_values and col.name in self._unique_values[table_name]:
                self._unique_values[table_name][col.name].add(value)
        
        return value
    
    def _smart_truncate_phone(self, phone: str, max_length: int) -> str:
        """Smart phone number truncation."""
        if len(phone) <= max_length:
            return phone
        
        # Remove extensions and extra formatting
        import re
        phone = re.sub(r'(x|ext)\d+$', '', phone)
        if len(phone) <= max_length:
            return phone
        
        # Keep only digits and basic formatting
        digits = re.sub(r'[^\d\-\(\)\+\.]', '', phone)
        return digits[:max_length]
    
    def _parse_default_value(self, default_str: str, data_type: MySQLDataType) -> Any:
        """Parse default value string into appropriate Python type."""
        if default_str.upper() in ['NULL', 'CURRENT_TIMESTAMP']:
            return None
        
        if data_type in [MySQLDataType.INT, MySQLDataType.TINYINT, MySQLDataType.SMALLINT, MySQLDataType.MEDIUMINT, MySQLDataType.BIGINT]:
            try:
                return int(default_str)
            except ValueError:
                return 0
        
        elif data_type in [MySQLDataType.DECIMAL, MySQLDataType.FLOAT, MySQLDataType.DOUBLE]:
            try:
                return float(default_str)
            except ValueError:
                return 0.0
        
        else:
            # String types - remove quotes
            return default_str.strip("'\"")
