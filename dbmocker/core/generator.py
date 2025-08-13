"""Data generation engine with constraint handling and pattern detection."""

import logging
import random
import string
import uuid
from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Any, List, Dict, Optional, Union, Callable, Set
from faker import Faker
import json

from .models import (
    TableInfo, ColumnInfo, ConstraintInfo, DatabaseSchema,
    ColumnType, ConstraintType, GenerationConfig, TableGenerationConfig,
    ColumnGenerationConfig
)


logger = logging.getLogger(__name__)


class DataGenerator:
    """Generates realistic mock data respecting database constraints."""
    
    def __init__(self, schema: DatabaseSchema, config: GenerationConfig):
        """Initialize data generator with schema and configuration."""
        self.schema = schema
        self.config = config
        self.faker = Faker()
        
        # Set random seed for reproducibility
        if config.seed is not None:
            random.seed(config.seed)
            Faker.seed(config.seed)
        
        # Cache for generated values to maintain referential integrity
        self._generated_values: Dict[str, Dict[str, List[Any]]] = {}
        self._existing_values: Dict[str, Dict[str, Set[Any]]] = {}
        
        # Custom generators
        self._custom_generators: Dict[str, Callable] = self._build_custom_generators()
    
    def generate_data_for_table(self, table_name: str, num_rows: int) -> List[Dict[str, Any]]:
        """Generate data for a specific table."""
        table = self.schema.get_table(table_name)
        if not table:
            raise ValueError(f"Table {table_name} not found in schema")
        
        logger.info(f"Generating {num_rows} rows for table: {table_name}")
        
        generated_rows = []
        table_config = self.config.table_configs.get(table_name, TableGenerationConfig())
        
        # Initialize value cache for this table
        if table_name not in self._generated_values:
            self._generated_values[table_name] = {}
        
        for i in range(num_rows):
            try:
                row = self._generate_row(table, table_config)
                generated_rows.append(row)
                
                # Cache generated values for FK references
                self._cache_generated_values(table_name, row)
                
                if (i + 1) % 1000 == 0:
                    logger.debug(f"Generated {i + 1}/{num_rows} rows for {table_name}")
            
            except Exception as e:
                logger.error(f"Failed to generate row {i + 1} for {table_name}: {e}")
                continue
        
        logger.info(f"Successfully generated {len(generated_rows)} rows for {table_name}")
        return generated_rows
    
    def _generate_row(self, table: TableInfo, table_config: TableGenerationConfig) -> Dict[str, Any]:
        """Generate a single row of data for a table."""
        row = {}
        
        # First pass: generate non-FK columns
        for column in table.columns:
            if not self._is_foreign_key_column(table, column.name):
                row[column.name] = self._generate_column_value(column, table_config)
        
        # Second pass: generate FK columns with proper references
        for column in table.columns:
            if self._is_foreign_key_column(table, column.name):
                row[column.name] = self._generate_foreign_key_value(table, column)
        
        return row
    
    def _generate_column_value(self, column: ColumnInfo, 
                             table_config: TableGenerationConfig) -> Any:
        """Generate a value for a single column."""
        # Check for custom column configuration
        column_config = table_config.column_configs.get(column.name)
        
        # Handle null values
        null_prob = column_config.null_probability if column_config else 0.0
        if column.is_nullable and random.random() < null_prob:
            return None
        
        # Use custom generator if specified
        if column_config and column_config.generator_function:
            return self._apply_custom_generator(column_config.generator_function, column)
        
        # Use pattern-based generation if available
        if column.detected_pattern:
            return self._generate_from_pattern(column, column_config)
        
        # Generate based on data type
        return self._generate_by_type(column, column_config)
    
    def _generate_by_type(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig]) -> Any:
        """Generate value based on column data type."""
        if column.data_type == ColumnType.INTEGER:
            return self._generate_integer(column, config)
        elif column.data_type == ColumnType.BIGINT:
            return self._generate_bigint(column, config)
        elif column.data_type == ColumnType.SMALLINT:
            return self._generate_smallint(column, config)
        elif column.data_type == ColumnType.DECIMAL:
            return self._generate_decimal(column, config)
        elif column.data_type == ColumnType.FLOAT:
            return self._generate_float(column, config)
        elif column.data_type == ColumnType.DOUBLE:
            return self._generate_double(column, config)
        elif column.data_type == ColumnType.VARCHAR:
            return self._generate_varchar(column, config)
        elif column.data_type == ColumnType.TEXT:
            return self._generate_text(column, config)
        elif column.data_type == ColumnType.CHAR:
            return self._generate_char(column, config)
        elif column.data_type == ColumnType.BOOLEAN:
            return self._generate_boolean(column, config)
        elif column.data_type == ColumnType.DATE:
            return self._generate_date(column, config)
        elif column.data_type == ColumnType.TIME:
            return self._generate_time(column, config)
        elif column.data_type == ColumnType.DATETIME:
            return self._generate_datetime(column, config)
        elif column.data_type == ColumnType.TIMESTAMP:
            return self._generate_timestamp(column, config)
        elif column.data_type == ColumnType.JSON:
            return self._generate_json(column, config)
        elif column.data_type == ColumnType.UUID:
            return self._generate_uuid(column, config)
        elif column.data_type == ColumnType.ENUM:
            return self._generate_enum(column, config)
        elif column.data_type == ColumnType.BLOB:
            return self._generate_blob(column, config)
        else:
            # Default to string
            return self._generate_varchar(column, config)
    
    def _generate_integer(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig]) -> int:
        """Generate integer value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        min_val = int(config.min_value) if config and config.min_value else column.min_value or 1
        max_val = int(config.max_value) if config and config.max_value else column.max_value or 2147483647
        
        return random.randint(int(min_val), int(max_val))
    
    def _generate_bigint(self, column: ColumnInfo, 
                        config: Optional[ColumnGenerationConfig]) -> int:
        """Generate bigint value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        min_val = int(config.min_value) if config and config.min_value else column.min_value or 1
        max_val = int(config.max_value) if config and config.max_value else column.max_value or 9223372036854775807
        
        return random.randint(int(min_val), int(max_val))
    
    def _generate_smallint(self, column: ColumnInfo, 
                          config: Optional[ColumnGenerationConfig]) -> int:
        """Generate smallint value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        min_val = int(config.min_value) if config and config.min_value else column.min_value or 1
        max_val = int(config.max_value) if config and config.max_value else column.max_value or 32767
        
        return random.randint(int(min_val), int(max_val))
    
    def _generate_decimal(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig]) -> Decimal:
        """Generate decimal value."""
        if config and config.possible_values:
            return Decimal(str(random.choice(config.possible_values)))
        
        precision = column.precision or 10
        scale = column.scale or 2
        
        # Generate random decimal within precision/scale constraints
        max_digits = precision - scale
        integer_part = random.randint(0, 10**max_digits - 1)
        decimal_part = random.randint(0, 10**scale - 1)
        
        return Decimal(f"{integer_part}.{decimal_part:0{scale}d}")
    
    def _generate_float(self, column: ColumnInfo, 
                       config: Optional[ColumnGenerationConfig]) -> float:
        """Generate float value."""
        if config and config.possible_values:
            return float(random.choice(config.possible_values))
        
        min_val = float(config.min_value) if config and config.min_value else column.min_value or 0.0
        max_val = float(config.max_value) if config and config.max_value else column.max_value or 1000000.0
        
        return random.uniform(min_val, max_val)
    
    def _generate_double(self, column: ColumnInfo, 
                        config: Optional[ColumnGenerationConfig]) -> float:
        """Generate double value."""
        return self._generate_float(column, config)
    
    def _generate_varchar(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig]) -> str:
        """Generate varchar value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        if config and config.pattern:
            return self._generate_from_regex_pattern(config.pattern)
        
        max_length = column.max_length or 255
        if config:
            max_length = config.max_length or max_length
            min_length = config.min_length or 1
        else:
            min_length = 1
        
        # Use average length from existing data if available
        if column.avg_length:
            target_length = min(int(column.avg_length), max_length)
        else:
            target_length = random.randint(min_length, max_length)
        
        # Generate realistic text
        return self.faker.text(max_nb_chars=target_length)[:target_length]
    
    def _generate_text(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate text value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        max_length = config.max_length if config else 1000
        return self.faker.text(max_nb_chars=max_length)
    
    def _generate_char(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate char value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        length = column.max_length or 1
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def _generate_boolean(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig]) -> bool:
        """Generate boolean value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        return random.choice([True, False])
    
    def _generate_date(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> date:
        """Generate date value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        return self.faker.date_between(start_date='-30y', end_date='today')
    
    def _generate_time(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate time value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        return self.faker.time()
    
    def _generate_datetime(self, column: ColumnInfo, 
                          config: Optional[ColumnGenerationConfig]) -> datetime:
        """Generate datetime value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        return self.faker.date_time_between(start_date='-30y', end_date='now')
    
    def _generate_timestamp(self, column: ColumnInfo, 
                           config: Optional[ColumnGenerationConfig]) -> datetime:
        """Generate timestamp value."""
        return self._generate_datetime(column, config)
    
    def _generate_json(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate JSON value."""
        if config and config.possible_values:
            return json.dumps(random.choice(config.possible_values))
        
        # Generate simple JSON object
        data = {
            "id": random.randint(1, 1000),
            "name": self.faker.name(),
            "value": random.uniform(0, 100),
            "active": random.choice([True, False])
        }
        return json.dumps(data)
    
    def _generate_uuid(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate UUID value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        return str(uuid.uuid4())
    
    def _generate_enum(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate enum value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        if column.enum_values:
            return random.choice(column.enum_values)
        
        # Fallback to generic enum values
        return random.choice(['option1', 'option2', 'option3'])
    
    def _generate_blob(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> bytes:
        """Generate blob value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate random bytes
        size = random.randint(10, 1000)
        return bytes(random.getrandbits(8) for _ in range(size))
    
    def _generate_from_pattern(self, column: ColumnInfo, 
                             config: Optional[ColumnGenerationConfig]) -> Any:
        """Generate value based on detected pattern."""
        patterns = column.detected_pattern.split(',')
        
        if 'email' in patterns:
            return self.faker.email()
        elif 'phone' in patterns:
            return self.faker.phone_number()
        elif 'url' in patterns:
            return self.faker.url()
        elif 'uuid' in patterns:
            return str(uuid.uuid4())
        else:
            return self._generate_by_type(column, config)
    
    def _generate_from_regex_pattern(self, pattern: str) -> str:
        """Generate string from regex pattern (simplified)."""
        # This is a simplified implementation
        # In production, you might want to use a library like rstr
        return self.faker.text(max_nb_chars=50)
    
    def _is_foreign_key_column(self, table: TableInfo, column_name: str) -> bool:
        """Check if column is a foreign key."""
        for fk in table.foreign_keys:
            if column_name in fk.columns:
                return True
        return False
    
    def _generate_foreign_key_value(self, table: TableInfo, column: ColumnInfo) -> Any:
        """Generate a valid foreign key value."""
        # Find the foreign key constraint for this column
        fk_constraint = None
        for fk in table.foreign_keys:
            if column.name in fk.columns:
                fk_constraint = fk
                break
        
        if not fk_constraint or not fk_constraint.referenced_table:
            return None
        
        referenced_table = fk_constraint.referenced_table
        referenced_column = fk_constraint.referenced_columns[0] if fk_constraint.referenced_columns else 'id'
        
        # Try to get existing values from cache or generate new ones
        if self.config.preserve_existing_data and random.random() < self.config.reuse_existing_values:
            # Try to reuse existing values
            existing_values = self._get_existing_values(referenced_table, referenced_column)
            if existing_values:
                return random.choice(list(existing_values))
        
        # Try to get from generated values cache
        if referenced_table in self._generated_values:
            table_cache = self._generated_values[referenced_table]
            if referenced_column in table_cache and table_cache[referenced_column]:
                return random.choice(table_cache[referenced_column])
        
        # Generate a new value based on the referenced column type
        ref_table = self.schema.get_table(referenced_table)
        if ref_table:
            ref_column = ref_table.get_column(referenced_column)
            if ref_column:
                return self._generate_by_type(ref_column, None)
        
        # Fallback to integer
        return random.randint(1, 1000)
    
    def _cache_generated_values(self, table_name: str, row: Dict[str, Any]) -> None:
        """Cache generated values for foreign key references."""
        for column_name, value in row.items():
            if value is not None:
                if column_name not in self._generated_values[table_name]:
                    self._generated_values[table_name][column_name] = []
                self._generated_values[table_name][column_name].append(value)
    
    def _get_existing_values(self, table_name: str, column_name: str) -> Set[Any]:
        """Get existing values from the database (cached)."""
        cache_key = f"{table_name}.{column_name}"
        if cache_key in self._existing_values:
            return self._existing_values[cache_key]
        
        # This would be implemented to query the database for existing values
        # For now, return empty set
        self._existing_values[cache_key] = set()
        return self._existing_values[cache_key]
    
    def _apply_custom_generator(self, generator_name: str, column: ColumnInfo) -> Any:
        """Apply custom generator function."""
        if generator_name in self._custom_generators:
            return self._custom_generators[generator_name](column)
        else:
            logger.warning(f"Custom generator '{generator_name}' not found")
            return self._generate_by_type(column, None)
    
    def _build_custom_generators(self) -> Dict[str, Callable]:
        """Build dictionary of custom generator functions."""
        return {
            'name': lambda col: self.faker.name(),
            'email': lambda col: self.faker.email(),
            'phone': lambda col: self.faker.phone_number(),
            'address': lambda col: self.faker.address(),
            'company': lambda col: self.faker.company(),
            'username': lambda col: self.faker.user_name(),
            'password': lambda col: self.faker.password(),
            'credit_card': lambda col: self.faker.credit_card_number(),
            'ip_address': lambda col: self.faker.ipv4(),
            'url': lambda col: self.faker.url(),
            'lorem': lambda col: self.faker.text(),
            'country': lambda col: self.faker.country(),
            'city': lambda col: self.faker.city(),
            'state': lambda col: self.faker.state(),
            'zipcode': lambda col: self.faker.zipcode(),
        }
