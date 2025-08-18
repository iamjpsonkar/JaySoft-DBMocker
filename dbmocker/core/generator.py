"""Data generation engine with constraint handling and pattern detection."""

import logging
import random
import re
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
    
    def __init__(self, schema: DatabaseSchema, config: GenerationConfig, db_connection=None):
        """Initialize data generator with schema and configuration."""
        self.schema = schema
        self.config = config
        self.db_connection = db_connection
        self.faker = Faker()
        
        # Set random seed for reproducibility
        if config.seed is not None:
            random.seed(config.seed)
            Faker.seed(config.seed)
        
        # Cache for generated values to maintain referential integrity
        self._generated_values: Dict[str, Dict[str, List[Any]]] = {}
        self._existing_values: Dict[str, Dict[str, Set[Any]]] = {}
        self._primary_key_counters: Dict[str, int] = {}
        
        # Stop flag for halting generation mid-process
        self.stop_flag = None
    
    def set_stop_flag(self, stop_flag):
        """Set the stop flag for halting generation."""
        self.stop_flag = stop_flag
        
        # Constraint handling caches
        self._unique_value_sets: Dict[str, Set[Any]] = {}  # For UNIQUE constraints
        self._composite_unique_sets: Dict[str, Set[tuple]] = {}  # For composite UNIQUE constraints
        self._check_constraint_cache: Dict[str, Any] = {}  # For CHECK constraints
        
        # Custom generators
        self._custom_generators: Dict[str, Callable] = self._build_custom_generators()
        
        # Pattern-based generators
        self._pattern_generator = None
        if hasattr(schema, 'table_patterns') and schema.table_patterns:
            from .pattern_analyzer import PatternBasedGenerator
            self._pattern_generator = PatternBasedGenerator(schema.table_patterns)
    
    def generate_data_for_table(self, table_name: str, num_rows: int) -> List[Dict[str, Any]]:
        """Generate data for a specific table."""
        table = self.schema.get_table(table_name)
        if not table:
            logger.warning(f"Table {table_name} not found in schema, skipping generation")
            return []
        
        logger.info(f"Generating {num_rows} rows for table: {table_name}")
        
        generated_rows = []
        table_config = self.config.table_configs.get(table_name, TableGenerationConfig())
        
        # Initialize value cache for this table
        if table_name not in self._generated_values:
            self._generated_values[table_name] = {}
        
        for i in range(num_rows):
            # Check stop flag every 100 rows for responsiveness
            if self.stop_flag and i % 100 == 0 and self.stop_flag.is_set():
                logger.info(f"🛑 Generation stopped at row {i + 1}/{num_rows} for table {table_name}")
                break
                
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
        
        # First pass: generate all columns (including FK columns with configuration)
        for column in table.columns:
            row[column.name] = self._generate_column_value(column, table_config, table)
        
        # Second pass: generate FK columns with proper references (but respect column configuration)
        for column in table.columns:
            if self._is_foreign_key_column(table, column.name):
                # Check if column has specific configuration - if so, respect it
                column_config = table_config.column_configs.get(column.name)
                if column_config and (column_config.possible_values or column_config.min_value is not None or column_config.max_value is not None):
                    logger.debug(f"FK column {column.name} has configuration, keeping configured value")
                    # Keep the configured value from first pass
                    continue
                else:
                    logger.debug(f"FK column {column.name} has no configuration, using FK generation")
                    row[column.name] = self._generate_foreign_key_value(table, column)
        
        # Third pass: validate composite unique constraints
        row = self._validate_composite_unique_constraints(table, row)
        
        return row
    
    def _generate_column_value(self, column: ColumnInfo, 
                             table_config: TableGenerationConfig,
                             table: Optional[TableInfo] = None) -> Any:
        """Generate a value for a single column."""
        # Check for custom column configuration
        column_config = table_config.column_configs.get(column.name)
        
        # DEBUG: Log configuration application
        if column.name in ['user_id', 'merchant_profile_id', 'payment_to_collect']:
            logger.debug(f"Processing column {column.name}, config exists: {column_config is not None}")
            if column_config:
                logger.debug(f"Config for {column.name}: {column_config}")
        
        # Handle global duplicate setting FIRST (highest priority)
        if self.config.duplicate_allowed and self._can_allow_duplicates(table, column.name):
            # Apply global duplicate settings based on mode
            if self.config.global_duplicate_mode == "allow_duplicates":
                # Simple duplicate mode - use same value for all rows
                cache_key = f"global_duplicate_{table.name}_{column.name}"
                if not hasattr(self, '_global_duplicate_cache'):
                    self._global_duplicate_cache = {}
                
                if cache_key not in self._global_duplicate_cache:
                    # Generate the duplicate value once using basic type generation
                    self._global_duplicate_cache[cache_key] = self._generate_by_type(column, column_config, table)
                    logger.debug(f"Generated and cached global duplicate value for {column.name}: {self._global_duplicate_cache[cache_key]}")
                
                return self._global_duplicate_cache[cache_key]
            
            elif self.config.global_duplicate_mode == "smart_duplicates":
                # Smart duplicates with global settings
                return self._generate_smart_duplicate_value_global(column, table)
        
        elif self.config.duplicate_allowed:
            logger.debug(f"Column {column.name} has constraints that prevent duplicates, generating unique value")
        
        # Handle column-specific duplicate mode (if no global setting applied)
        if column_config and hasattr(column_config, 'duplicate_mode'):
            duplicate_mode = column_config.duplicate_mode
            
            if duplicate_mode == "allow_duplicates":
                if hasattr(column_config, 'duplicate_value') and column_config.duplicate_value is not None:
                    # Use the specified duplicate value
                    logger.debug(f"Using specified duplicate value for {column.name}: {column_config.duplicate_value}")
                    return column_config.duplicate_value
                elif self._can_allow_duplicates(table, column.name):
                    # Generate one value and cache it for this column if constraints allow
                    cache_key = f"duplicate_{table.name}_{column.name}"
                    if not hasattr(self, '_duplicate_cache'):
                        self._duplicate_cache = {}
                    
                    if cache_key not in self._duplicate_cache:
                        # Generate the duplicate value once using basic type generation
                        self._duplicate_cache[cache_key] = self._generate_by_type(column, column_config, table)
                        logger.debug(f"Generated and cached duplicate value for {column.name}: {self._duplicate_cache[cache_key]}")
                    
                    return self._duplicate_cache[cache_key]
                else:
                    logger.warning(f"Column {column.name} has constraints that prevent duplicates, using generate_new mode")
            
            elif duplicate_mode == "smart_duplicates":
                # Smart duplicates: generate limited set of values with controlled probability
                return self._generate_smart_duplicate_value(column, column_config, table)
        
        # For generate_new mode or no duplicate config, continue with normal generation
        
        # Handle possible_values (second priority constraint)
        if column_config and column_config.possible_values:
            logger.debug(f"Using possible_values for {column.name}: {column_config.possible_values}")
            return random.choice(column_config.possible_values)
        
        # Handle default values first
        if not column.is_nullable and self._has_default_value(column) and random.random() < 0.3:
            return self._get_default_value(column)
        
        # Handle null values (but respect NOT NULL constraint)
        null_prob = column_config.null_probability if column_config else 0.0
        if column.is_nullable and random.random() < null_prob:
            return None
        
        # Use custom generator if specified
        if column_config and column_config.generator_function:
            return self._apply_custom_generator(column_config.generator_function, column)
        
        # Handle primary key first (most important constraint)
        if table and self._is_primary_key_column(table, column.name):
            logger.debug(f"Column {column.name} is primary key, generating unique PK")
            if column.data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
                return self._generate_unique_primary_key(table.name, column.name)
        
        # Generate unique value if column has unique constraint
        if table and self._is_unique_column(table, column.name):
            logger.debug(f"Column {column.name} has unique constraint, generating unique value")
            return self._generate_unique_value(table, column, column_config)
        
        # Use existing data pattern-based generation if available (NEW FEATURE)
        if self._pattern_generator and table:
            def base_generator():
                return self._generate_constrained_value(column, column_config, table)
            
            pattern_value = self._pattern_generator.generate_realistic_value(
                table.name, column.name, base_generator
            )
            if pattern_value is not None:
                logger.debug(f"Column {column.name} using pattern-based generation from existing data")
                return pattern_value
        
        # Use pattern-based generation if available
        if column.detected_pattern:
            logger.debug(f"Column {column.name} has detected pattern: {column.detected_pattern}")
            return self._generate_from_pattern(column, column_config)
        
        # Generate based on column name patterns (higher priority than generic generation)
        if self._should_use_column_name_generation(column):
            logger.debug(f"Column {column.name} using name-based generation")
            return self._generate_by_column_name(column, column_config)
        
        # Generate based on data type with constraint validation
        logger.debug(f"Column {column.name} using constrained value generation")
        return self._generate_constrained_value(column, column_config, table)
    
    def _generate_by_type(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig],
                         table: Optional[TableInfo] = None) -> Any:
        """Generate value based on column data type."""
        if column.data_type == ColumnType.INTEGER:
            return self._generate_integer(column, config, table)
        elif column.data_type == ColumnType.BIGINT:
            return self._generate_bigint(column, config, table)
        elif column.data_type == ColumnType.SMALLINT:
            return self._generate_smallint(column, config, table)
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
        elif column.data_type == ColumnType.JSONB:
            return self._generate_json(column, config)  # Same as JSON for generation
        elif column.data_type == ColumnType.UUID:
            return self._generate_uuid(column, config)
        elif column.data_type == ColumnType.ENUM:
            return self._generate_enum(column, config)
        elif column.data_type == ColumnType.BLOB:
            return self._generate_blob(column, config)
        elif column.data_type == ColumnType.XML:
            return self._generate_xml(column, config)
        elif column.data_type == ColumnType.INET:
            return self._generate_inet(column, config)
        elif column.data_type == ColumnType.CIDR:
            return self._generate_cidr(column, config)
        elif column.data_type == ColumnType.MACADDR:
            return self._generate_macaddr(column, config)
        elif column.data_type == ColumnType.GEOMETRY:
            return self._generate_geometry(column, config)
        elif column.data_type == ColumnType.POINT:
            return self._generate_point(column, config)
        elif column.data_type == ColumnType.POLYGON:
            return self._generate_polygon(column, config)
        elif column.data_type == ColumnType.ARRAY:
            return self._generate_array(column, config)
        elif column.data_type == ColumnType.MONEY:
            return self._generate_money(column, config)
        elif column.data_type == ColumnType.BYTEA:
            return self._generate_bytea(column, config)
        elif column.data_type == ColumnType.VARBINARY:
            return self._generate_varbinary(column, config)
        else:
            # Default to string
            return self._generate_varchar(column, config)
    
    def _generate_integer(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig], 
                         table: Optional[TableInfo] = None) -> int:
        """Generate integer value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        min_val = int(config.min_value) if config and config.min_value else column.min_value or 1
        max_val = int(config.max_value) if config and config.max_value else column.max_value or 2147483647
        
        # Ensure min_val < max_val to avoid "low >= high" error
        if min_val >= max_val:
            max_val = min_val + 1000000  # Give a reasonable range
        
        return random.randint(int(min_val), int(max_val))
    
    def _generate_unique_primary_key(self, table_name: str, column_name: str) -> int:
        """Generate a unique primary key value."""
        counter_key = f"{table_name}.{column_name}"
        
        if counter_key not in self._primary_key_counters:
            # Initialize counter based on existing maximum value
            max_existing = self._get_max_primary_key_value(table_name, column_name)
            self._primary_key_counters[counter_key] = max_existing
        
        # Increment and return next unique value
        self._primary_key_counters[counter_key] += 1
        return self._primary_key_counters[counter_key]
    
    def _generate_bigint(self, column: ColumnInfo, 
                        config: Optional[ColumnGenerationConfig],
                        table: Optional[TableInfo] = None) -> int:
        """Generate bigint value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        min_val = int(config.min_value) if config and config.min_value else column.min_value or 1
        max_val = int(config.max_value) if config and config.max_value else column.max_value or 9223372036854775807
        
        # Ensure min_val < max_val to avoid "low >= high" error
        if min_val >= max_val:
            max_val = min_val + 1000000  # Give a reasonable range
        
        return random.randint(int(min_val), int(max_val))
    
    def _generate_smallint(self, column: ColumnInfo, 
                          config: Optional[ColumnGenerationConfig],
                          table: Optional[TableInfo] = None) -> int:
        """Generate smallint value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        min_val = int(config.min_value) if config and config.min_value else column.min_value or 1
        max_val = int(config.max_value) if config and config.max_value else column.max_value or 32767
        
        # Ensure min_val < max_val to avoid "low >= high" error
        if min_val >= max_val:
            max_val = min_val + 1000  # Give a reasonable range for smallint
        
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
        max_integer = max(0, 10**max_digits - 1)
        max_decimal = max(0, 10**scale - 1)
        
        integer_part = random.randint(0, max_integer) if max_integer > 0 else 0
        decimal_part = random.randint(0, max_decimal) if max_decimal > 0 else 0
        
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
            # Ensure min_length < max_length to avoid "low >= high" error
            if min_length >= max_length:
                target_length = min_length
            else:
                target_length = random.randint(min_length, max_length)
        
        # Generate realistic text, handling short fields
        if target_length < 5:
            # For very short fields, use letters/words instead of text()
            if target_length <= 3:
                return ''.join(random.choices(string.ascii_lowercase, k=target_length))
            else:
                # Use short words for 4-character fields
                words = ['test', 'data', 'demo', 'temp', 'prod', 'dev', 'user', 'app']
                return random.choice([w for w in words if len(w) <= target_length])
        else:
            return self.faker.text(max_nb_chars=target_length)[:target_length]
    
    def _generate_text(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate text value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        max_length = config.max_length if config else 1000
        
        # Handle short text fields
        if max_length < 5:
            if max_length <= 3:
                return ''.join(random.choices(string.ascii_lowercase, k=max_length))
            else:
                words = ['test', 'data', 'demo', 'temp', 'prod', 'dev', 'user', 'app']
                return random.choice([w for w in words if len(w) <= max_length])
        else:
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
        """Generate JSON value with advanced schema support."""
        if config and config.possible_values:
            return json.dumps(random.choice(config.possible_values))
        
        # Analyze column name to generate appropriate JSON structure
        column_name_lower = column.name.lower()
        
        # Generate context-aware JSON based on column name patterns
        if any(pattern in column_name_lower for pattern in ['config', 'setting', 'preference']):
            data = self._generate_config_json()
        elif any(pattern in column_name_lower for pattern in ['meta', 'metadata', 'info']):
            data = self._generate_metadata_json()
        elif any(pattern in column_name_lower for pattern in ['address', 'location']):
            data = self._generate_address_json()
        elif any(pattern in column_name_lower for pattern in ['profile', 'user', 'person']):
            data = self._generate_profile_json()
        elif any(pattern in column_name_lower for pattern in ['payment', 'transaction', 'billing']):
            data = self._generate_payment_json()
        elif any(pattern in column_name_lower for pattern in ['product', 'item', 'catalog']):
            data = self._generate_product_json()
        elif any(pattern in column_name_lower for pattern in ['session', 'token', 'auth']):
            data = self._generate_session_json()
        else:
            # Default generic JSON object
            data = self._generate_generic_json()
        
        return json.dumps(data)
    
    def _generate_config_json(self) -> Dict[str, Any]:
        """Generate configuration-style JSON."""
        return {
            "theme": random.choice(["light", "dark", "auto"]),
            "language": random.choice(["en", "es", "fr", "de", "zh"]),
            "notifications": {
                "email": random.choice([True, False]),
                "push": random.choice([True, False]),
                "sms": random.choice([True, False])
            },
            "privacy_level": random.choice(["public", "friends", "private"]),
            "auto_save": random.choice([True, False]),
            "timeout": random.randint(300, 3600)
        }
    
    def _generate_metadata_json(self) -> Dict[str, Any]:
        """Generate metadata-style JSON."""
        return {
            "id": random.randint(1, 10000),
            "name": self.faker.name(),
            "value": round(random.uniform(0, 1000), 2),
            "active": random.choice([True, False]),
            "created_at": self.faker.date_time_between(start_date='-2y').isoformat(),
            "tags": [self.faker.word() for _ in range(random.randint(1, 4))],
            "priority": random.choice(["low", "medium", "high", "critical"])
        }
    
    def _generate_address_json(self) -> Dict[str, Any]:
        """Generate address-style JSON."""
        return {
            "street": self.faker.street_address(),
            "city": self.faker.city(),
            "state": self.faker.state(),
            "zip_code": self.faker.zipcode(),
            "country": self.faker.country(),
            "coordinates": {
                "latitude": float(self.faker.latitude()),
                "longitude": float(self.faker.longitude())
            },
            "type": random.choice(["home", "work", "billing", "shipping"])
        }
    
    def _generate_profile_json(self) -> Dict[str, Any]:
        """Generate user profile-style JSON."""
        return {
            "name": self.faker.name(),
            "age": random.randint(18, 80),
            "email": self.faker.email(),
            "phone": self.faker.phone_number(),
            "avatar": self.faker.image_url(),
            "bio": self.faker.text(max_nb_chars=200),
            "skills": [self.faker.job() for _ in range(random.randint(2, 5))],
            "social": {
                "linkedin": self.faker.url(),
                "twitter": f"@{self.faker.user_name()}",
                "website": self.faker.url()
            },
            "verified": random.choice([True, False])
        }
    
    def _generate_payment_json(self) -> Dict[str, Any]:
        """Generate payment/transaction-style JSON."""
        return {
            "amount": round(random.uniform(10, 5000), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "INR", "JPY"]),
            "method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]),
            "status": random.choice(["pending", "completed", "failed", "refunded"]),
            "reference": f"TXN{random.randint(100000, 999999)}",
            "fees": round(random.uniform(0, 50), 2),
            "description": self.faker.sentence(),
            "merchant": {
                "name": self.faker.company(),
                "id": random.randint(1000, 9999)
            }
        }
    
    def _generate_product_json(self) -> Dict[str, Any]:
        """Generate product/catalog-style JSON."""
        return {
            "name": self.faker.catch_phrase(),
            "sku": f"SKU{random.randint(100000, 999999)}",
            "price": round(random.uniform(10, 1000), 2),
            "category": random.choice(["electronics", "clothing", "books", "home", "sports"]),
            "brand": self.faker.company(),
            "in_stock": random.choice([True, False]),
            "quantity": random.randint(0, 1000),
            "dimensions": {
                "width": round(random.uniform(1, 100), 1),
                "height": round(random.uniform(1, 100), 1),
                "depth": round(random.uniform(1, 100), 1),
                "weight": round(random.uniform(0.1, 50), 2)
            },
            "tags": [self.faker.word() for _ in range(random.randint(2, 6))]
        }
    
    def _generate_session_json(self) -> Dict[str, Any]:
        """Generate session/auth-style JSON."""
        return {
            "token": self.faker.sha256(),
            "expires_at": (datetime.now() + timedelta(hours=random.randint(1, 24))).isoformat(),
            "user_agent": self.faker.user_agent(),
            "ip_address": self.faker.ipv4(),
            "device": {
                "type": random.choice(["desktop", "mobile", "tablet"]),
                "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"])
            },
            "permissions": random.sample(["read", "write", "delete", "admin", "user"], random.randint(1, 3)),
            "last_activity": self.faker.date_time_between(start_date='-1d').isoformat()
        }
    
    def _generate_generic_json(self) -> Dict[str, Any]:
        """Generate generic JSON object."""
        return {
            "id": random.randint(1, 1000),
            "name": self.faker.name(),
            "value": round(random.uniform(0, 100), 2),
            "active": random.choice([True, False]),
            "timestamp": datetime.now().isoformat(),
            "data": {
                "type": random.choice(["A", "B", "C"]),
                "priority": random.randint(1, 10),
                "notes": self.faker.sentence()
            }
        }
    
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
    
    def _generate_xml(self, column: ColumnInfo, 
                     config: Optional[ColumnGenerationConfig]) -> str:
        """Generate XML value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate simple XML structure
        root_tag = random.choice(["data", "record", "item", "document"])
        content = f'''<?xml version="1.0" encoding="UTF-8"?>
<{root_tag}>
    <id>{random.randint(1, 10000)}</id>
    <name>{self.faker.name()}</name>
    <description>{self.faker.sentence()}</description>
    <created>{datetime.now().isoformat()}</created>
    <active>{str(random.choice([True, False])).lower()}</active>
</{root_tag}>'''
        return content
    
    def _generate_inet(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate IP address (IPv4 or IPv6)."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # 80% IPv4, 20% IPv6
        if random.random() < 0.8:
            return self.faker.ipv4()
        else:
            return self.faker.ipv6()
    
    def _generate_cidr(self, column: ColumnInfo, 
                      config: Optional[ColumnGenerationConfig]) -> str:
        """Generate CIDR network address."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate CIDR notation
        if random.random() < 0.8:  # IPv4 CIDR
            base_ip = self.faker.ipv4()
            prefix = random.choice([8, 16, 24, 28, 30])
            return f"{base_ip}/{prefix}"
        else:  # IPv6 CIDR
            base_ip = self.faker.ipv6()
            prefix = random.choice([32, 48, 56, 64, 96, 128])
            return f"{base_ip}/{prefix}"
    
    def _generate_macaddr(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig]) -> str:
        """Generate MAC address."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate MAC address in standard format
        mac = [random.randint(0, 255) for _ in range(6)]
        return ':'.join([f'{x:02x}' for x in mac])
    
    def _generate_geometry(self, column: ColumnInfo, 
                          config: Optional[ColumnGenerationConfig]) -> str:
        """Generate geometry data (WKT format)."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate random geometry type
        geom_type = random.choice(["POINT", "LINESTRING", "POLYGON"])
        
        if geom_type == "POINT":
            return self._generate_point(column, config)
        elif geom_type == "LINESTRING":
            # Generate a simple line with 2-5 points
            points = []
            for _ in range(random.randint(2, 5)):
                x = round(random.uniform(-180, 180), 6)
                y = round(random.uniform(-90, 90), 6)
                points.append(f"{x} {y}")
            return f"LINESTRING({', '.join(points)})"
        else:  # POLYGON
            return self._generate_polygon(column, config)
    
    def _generate_point(self, column: ColumnInfo, 
                       config: Optional[ColumnGenerationConfig]) -> str:
        """Generate point geometry."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate random longitude/latitude
        longitude = round(random.uniform(-180, 180), 6)
        latitude = round(random.uniform(-90, 90), 6)
        return f"POINT({longitude} {latitude})"
    
    def _generate_polygon(self, column: ColumnInfo, 
                         config: Optional[ColumnGenerationConfig]) -> str:
        """Generate polygon geometry."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate a simple square polygon
        center_x = round(random.uniform(-179, 179), 6)
        center_y = round(random.uniform(-89, 89), 6)
        size = round(random.uniform(0.001, 1.0), 6)
        
        points = [
            f"{center_x - size} {center_y - size}",
            f"{center_x + size} {center_y - size}",
            f"{center_x + size} {center_y + size}",
            f"{center_x - size} {center_y + size}",
            f"{center_x - size} {center_y - size}"  # Close the polygon
        ]
        return f"POLYGON(({', '.join(points)}))"
    
    def _generate_array(self, column: ColumnInfo, 
                       config: Optional[ColumnGenerationConfig]) -> str:
        """Generate array value (PostgreSQL format)."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate array of strings by default
        size = random.randint(1, 5)
        elements = [f'"{self.faker.word()}"' for _ in range(size)]
        return '{' + ', '.join(elements) + '}'
    
    def _generate_money(self, column: ColumnInfo, 
                       config: Optional[ColumnGenerationConfig]) -> str:
        """Generate money value."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate money amount with currency symbol
        amount = round(random.uniform(0.01, 999999.99), 2)
        currency = random.choice(['$', '€', '£', '¥', '₹'])
        return f"{currency}{amount:,.2f}"
    
    def _generate_bytea(self, column: ColumnInfo, 
                       config: Optional[ColumnGenerationConfig]) -> str:
        """Generate bytea value (PostgreSQL binary data)."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate hex-encoded binary data
        size = random.randint(10, 100)
        hex_data = ''.join([f'{random.randint(0, 255):02x}' for _ in range(size)])
        return f'\\x{hex_data}'
    
    def _generate_varbinary(self, column: ColumnInfo, 
                           config: Optional[ColumnGenerationConfig]) -> bytes:
        """Generate varbinary value (MySQL binary data)."""
        if config and config.possible_values:
            return random.choice(config.possible_values)
        
        # Generate random binary data
        max_length = column.max_length or 255
        size = random.randint(1, min(max_length, 100))
        return bytes([random.randint(0, 255) for _ in range(size)])
    
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
        # Generate pattern-based text (simplified implementation)
        return self.faker.word() + str(random.randint(100, 999))
    
    def _safe_text_generation(self, max_length: int) -> str:
        """Generate text safely handling short length requirements."""
        if max_length < 5:
            if max_length <= 3:
                return ''.join(random.choices(string.ascii_lowercase, k=max_length))
            else:
                words = ['test', 'data', 'demo', 'temp', 'prod', 'dev', 'user', 'app']
                return random.choice([w for w in words if len(w) <= max_length])
        else:
            return self.faker.text(max_nb_chars=max_length)
    
    def _is_foreign_key_column(self, table: TableInfo, column_name: str) -> bool:
        """Check if column is a foreign key."""
        for fk in table.foreign_keys:
            if column_name in fk.columns:
                return True
        return False
    
    def _is_primary_key_column(self, table: TableInfo, column_name: str) -> bool:
        """Check if column is a primary key."""
        pk_columns = table.get_primary_key_columns()
        return column_name in pk_columns
    
    def _is_unique_column(self, table: TableInfo, column_name: str) -> bool:
        """Check if column has a unique constraint."""
        for constraint in table.constraints:
            if constraint.type == ConstraintType.UNIQUE and column_name in constraint.columns:
                return True
        return False
    
    def _get_unique_constraints(self, table: TableInfo) -> List[ConstraintInfo]:
        """Get all unique constraints for a table."""
        return [c for c in table.constraints if c.type == ConstraintType.UNIQUE]
    
    def _get_check_constraints(self, table: TableInfo) -> List[ConstraintInfo]:
        """Get all check constraints for a table."""
        return [c for c in table.constraints if c.type == ConstraintType.CHECK]
    
    def _has_default_value(self, column: ColumnInfo) -> bool:
        """Check if column has a default value."""
        return column.default_value is not None
    
    def _get_default_value(self, column: ColumnInfo) -> Any:
        """Get the default value for a column."""
        if column.default_value is None:
            return None
        
        # Handle special default values
        default_str = str(column.default_value).lower()
        
        if default_str in ('current_timestamp', 'now()', 'getdate()'):
            return datetime.now()
        elif default_str in ('current_date', 'curdate()'):
            return date.today()
        elif default_str in ('null',):
            return None
        else:
            # Try to parse the default value based on column type
            return self._parse_default_value(column.default_value, column.data_type)
    
    def _parse_default_value(self, default_value: Any, data_type: ColumnType) -> Any:
        """Parse default value based on data type."""
        try:
            if data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
                return int(default_value)
            elif data_type in [ColumnType.DECIMAL, ColumnType.FLOAT, ColumnType.DOUBLE]:
                return float(default_value)
            elif data_type == ColumnType.BOOLEAN:
                return bool(default_value) if not isinstance(default_value, str) else default_value.lower() in ('true', '1', 'yes')
            else:
                return str(default_value)
        except (ValueError, TypeError):
            return default_value
    
    def _generate_unique_value(self, table: TableInfo, column: ColumnInfo, 
                              config: Optional[ColumnGenerationConfig]) -> Any:
        """Generate a unique value for a column with unique constraint."""
        cache_key = f"{table.name}.{column.name}"
        
        if cache_key not in self._unique_value_sets:
            self._unique_value_sets[cache_key] = set()
            # Load existing unique values if preserving data
            if self.config.preserve_existing_data:
                existing_values = self._get_existing_values(table.name, column.name)
                self._unique_value_sets[cache_key].update(existing_values)
        
        max_attempts = 1000  # Prevent infinite loops
        attempt = 0
        
        while attempt < max_attempts:
            # Generate a regular value
            value = self._generate_by_type(column, config, table)
            
            # Check if it's unique
            if value not in self._unique_value_sets[cache_key]:
                self._unique_value_sets[cache_key].add(value)
                return value
            
            attempt += 1
        
        # If we can't generate unique value, create a suffixed version
        base_value = self._generate_by_type(column, config, table)
        suffix = 1
        while f"{base_value}_{suffix}" in self._unique_value_sets[cache_key]:
            suffix += 1
        
        unique_value = f"{base_value}_{suffix}"
        self._unique_value_sets[cache_key].add(unique_value)
        return unique_value
    
    def _generate_constrained_value(self, column: ColumnInfo, 
                                   config: Optional[ColumnGenerationConfig],
                                   table: Optional[TableInfo] = None) -> Any:
        """Generate value with constraint validation."""
        # Handle ENUM values
        if column.data_type == ColumnType.ENUM and column.enum_values:
            return random.choice(column.enum_values)
        
        # Generate base value
        value = self._generate_by_type(column, config, table)
        
        # Apply length constraints (enhanced for better handling)
        if column.max_length and isinstance(value, str):
            if len(value) > column.max_length:
                # Smart truncation for different data types
                if any(pattern in column.name.lower() for pattern in ['phone', 'mobile', 'tel']):
                    # For phone numbers, keep format but truncate intelligently
                    value = self._truncate_phone_number(value, column.max_length)
                elif any(pattern in column.name.lower() for pattern in ['email', 'mail']):
                    # For emails, truncate before @ and reconstruct
                    value = self._truncate_email(value, column.max_length)
                elif any(pattern in column.name.lower() for pattern in ['url', 'link', 'website']):
                    # For URLs, truncate but keep valid format
                    value = self._truncate_url(value, column.max_length)
                else:
                    # Generic string truncation
                    value = value[:column.max_length]
        elif column.max_length and isinstance(value, (int, float)):
            # Truncate numeric values to fit
            value_str = str(value)
            if len(value_str) > column.max_length:
                truncated_str = value_str[:column.max_length]
                try:
                    if column.data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
                        value = int(truncated_str)
                    else:
                        value = float(truncated_str)
                except ValueError:
                    # Fallback to a simple number that fits
                    value = int('1' * min(column.max_length, 9)) if column.data_type in [
                        ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT
                    ] else 1.0
        
        # Apply range constraints (config takes priority over column introspection)
        effective_min_value = config.min_value if config and config.min_value is not None else column.min_value
        effective_max_value = config.max_value if config and config.max_value is not None else column.max_value
        
        if effective_min_value is not None and isinstance(value, (int, float)):
            value = max(value, effective_min_value)
            # Ensure integer types remain integers
            if column.data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
                value = int(value)
        if effective_max_value is not None and isinstance(value, (int, float)):
            value = min(value, effective_max_value)
            # Ensure integer types remain integers
            if column.data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
                value = int(value)
        
        # Validate CHECK constraints if table is provided
        if table:
            value = self._validate_check_constraints(table, column, value)
        
        return value
    
    def _validate_check_constraints(self, table: TableInfo, column: ColumnInfo, value: Any) -> Any:
        """Validate value against CHECK constraints."""
        check_constraints = self._get_check_constraints(table)
        
        for constraint in check_constraints:
            if column.name in constraint.columns and constraint.check_condition:
                # Simple validation for common CHECK constraints
                condition = constraint.check_condition.lower()
                
                # Handle range checks like "value > 0", "value BETWEEN 1 AND 100"
                if isinstance(value, (int, float)):
                    if '> 0' in condition or '>= 1' in condition:
                        value = max(value, 1) if value <= 0 else value
                    elif 'between' in condition:
                        # Extract range from "value BETWEEN 1 AND 100"
                        import re
                        match = re.search(r'between\s+(\d+)\s+and\s+(\d+)', condition)
                        if match:
                            min_val, max_val = int(match.group(1)), int(match.group(2))
                            value = max(min_val, min(max_val, value))
                
                # Handle string length checks
                elif isinstance(value, str):
                    if 'length(' in condition:
                        match = re.search(r'length\([^)]+\)\s*>=?\s*(\d+)', condition)
                        if match:
                            min_length = int(match.group(1))
                            while len(value) < min_length:
                                value += value  # Repeat value to meet length
                            
        return value
    
    def _truncate_phone_number(self, phone: str, max_length: int) -> str:
        """Intelligently truncate phone number while maintaining format."""
        if len(phone) <= max_length:
            return phone
        
        # Remove extensions first (x12345 or ext123)
        import re
        base_phone = re.sub(r'(x|ext)\d+$', '', phone)
        if len(base_phone) <= max_length:
            return base_phone
        
        # Remove country codes and formatting
        digits_only = re.sub(r'[^\d]', '', phone)
        if len(digits_only) <= max_length:
            return digits_only[:max_length]
        
        # Keep the last N digits (most important part)
        return digits_only[-max_length:]
    
    def _truncate_email(self, email: str, max_length: int) -> str:
        """Intelligently truncate email while maintaining valid format."""
        if len(email) <= max_length:
            return email
        
        if '@' not in email:
            return email[:max_length]
        
        local, domain = email.split('@', 1)
        
        # Reserve space for @ and domain
        domain_space = len(domain) + 1  # +1 for @
        
        if domain_space >= max_length:
            # Domain too long, create a minimal email
            return f"user@{domain[:max_length-6]}.com"
        
        # Truncate local part
        available_local = max_length - domain_space
        if available_local < 1:
            return f"u@{domain[:max_length-3]}.co"
        
        return f"{local[:available_local]}@{domain}"
    
    def _truncate_url(self, url: str, max_length: int) -> str:
        """Intelligently truncate URL while maintaining valid format."""
        if len(url) <= max_length:
            return url
        
        # Ensure it starts with http/https
        if url.startswith(('http://', 'https://')):
            protocol = 'https://' if url.startswith('https://') else 'http://'
            remaining = url[len(protocol):]
        else:
            protocol = 'http://'
            remaining = url
        
        # Reserve space for protocol
        available = max_length - len(protocol)
        if available < 4:  # Need at least "a.co"
            return f"{protocol}a.co"
        
        # Try to keep domain structure
        if '/' in remaining:
            domain = remaining.split('/')[0]
            if len(domain) <= available:
                return f"{protocol}{domain}"
        
        return f"{protocol}{remaining[:available]}"
    
    def _should_use_column_name_generation(self, column: ColumnInfo) -> bool:
        """Check if column should use name-based generation."""
        column_name = column.name.lower()
        
        # Boolean-like columns
        if any(pattern in column_name for pattern in [
            'is_', 'has_', 'can_', 'should_', 'active', 'enabled', 'visible', 
            'deleted', 'archived', 'published', 'verified', 'confirmed'
        ]):
            return True
        
        # Email columns
        if any(pattern in column_name for pattern in ['email', 'mail']):
            return True
        
        # Phone columns
        if any(pattern in column_name for pattern in ['phone', 'mobile', 'tel']):
            return True
        
        # URL columns
        if any(pattern in column_name for pattern in ['url', 'website', 'link']):
            return True
        
        # Name columns
        if any(pattern in column_name for pattern in ['name', 'title', 'label']) and 'file' not in column_name:
            return True
        
        return False
    
    def _generate_by_column_name(self, column: ColumnInfo, config: Optional[ColumnGenerationConfig]) -> Any:
        """Generate value based on column name patterns."""
        column_name = column.name.lower()
        
        # Boolean-like columns
        if any(pattern in column_name for pattern in [
            'is_', 'has_', 'can_', 'should_', 'active', 'enabled', 'visible', 
            'deleted', 'archived', 'published', 'verified', 'confirmed'
        ]):
            return random.choice([0, 1])
        
        # Email columns
        if any(pattern in column_name for pattern in ['email', 'mail']):
            email = self.faker.email()
            # Apply length constraints if needed
            if column.max_length and len(email) > column.max_length:
                return self._truncate_email(email, column.max_length)
            return email
        
        # Phone columns
        if any(pattern in column_name for pattern in ['phone', 'mobile', 'tel']):
            # Generate appropriate phone number based on column length
            if column.max_length:
                if column.max_length <= 10:
                    # Short format: 1234567890
                    phone = ''.join([str(random.randint(0, 9)) for _ in range(min(10, column.max_length))])
                elif column.max_length <= 15:
                    # Medium format: (123)456-7890
                    phone = f"({random.randint(100, 999)}){random.randint(100, 999)}-{random.randint(1000, 9999)}"
                else:
                    # Full format with possible extension
                    phone = self.faker.phone_number()
                
                # Ensure it fits
                if len(phone) > column.max_length:
                    return self._truncate_phone_number(phone, column.max_length)
                return phone
            else:
                return self.faker.phone_number()
        
        # URL columns
        if any(pattern in column_name for pattern in ['url', 'website', 'link']):
            url = self.faker.url()
            if column.max_length and len(url) > column.max_length:
                return self._truncate_url(url, column.max_length)
            return url
        
        # Name columns
        if any(pattern in column_name for pattern in ['name', 'title', 'label']) and 'file' not in column_name:
            if 'first' in column_name:
                return self.faker.first_name()
            elif 'last' in column_name:
                return self.faker.last_name()
            elif 'company' in column_name or 'business' in column_name:
                return self.faker.company()
            else:
                name = self.faker.name()
                if column.max_length and len(name) > column.max_length:
                    return name[:column.max_length]
                return name
        
        # Fallback to type-based generation
        return self._generate_by_type(column, config)
    
    def _validate_composite_unique_constraints(self, table: TableInfo, row: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and fix composite unique constraints."""
        unique_constraints = self._get_unique_constraints(table)
        
        for constraint in unique_constraints:
            if len(constraint.columns) > 1:  # Composite unique constraint
                cache_key = f"{table.name}.{'.'.join(constraint.columns)}"
                
                if cache_key not in self._composite_unique_sets:
                    self._composite_unique_sets[cache_key] = set()
                    # Load existing composite values if preserving data
                    if self.config.preserve_existing_data:
                        existing_combinations = self._get_existing_composite_values(table.name, constraint.columns)
                        self._composite_unique_sets[cache_key].update(existing_combinations)
                
                # Create tuple of current values for this constraint
                current_combination = tuple(row.get(col) for col in constraint.columns)
                
                # If combination already exists, modify one of the non-primary-key columns
                if current_combination in self._composite_unique_sets[cache_key]:
                    # Find a column to modify (prefer non-PK, non-FK columns)
                    pk_columns = table.get_primary_key_columns()
                    modifiable_columns = [
                        col for col in constraint.columns 
                        if col not in pk_columns and not self._is_foreign_key_column(table, col)
                    ]
                    
                    if modifiable_columns:
                        # Modify the first modifiable column
                        col_to_modify = modifiable_columns[0]
                        column_info = table.get_column(col_to_modify)
                        
                        if column_info:
                            # Generate a new value with a unique suffix
                            original_value = row[col_to_modify]
                            suffix = 1
                            
                            while True:
                                if isinstance(original_value, str):
                                    new_value = f"{original_value}_{suffix}"
                                elif isinstance(original_value, (int, float)):
                                    new_value = original_value + suffix
                                else:
                                    new_value = f"{original_value}_{suffix}"
                                
                                # Create new combination
                                new_combination = tuple(
                                    new_value if col == col_to_modify else row.get(col) 
                                    for col in constraint.columns
                                )
                                
                                if new_combination not in self._composite_unique_sets[cache_key]:
                                    row[col_to_modify] = new_value
                                    current_combination = new_combination
                                    break
                                
                                suffix += 1
                                if suffix > 1000:  # Prevent infinite loops
                                    break
                
                # Add the final combination to cache
                self._composite_unique_sets[cache_key].add(current_combination)
        
        return row
    
    def _get_existing_composite_values(self, table_name: str, columns: List[str]) -> Set[tuple]:
        """Get existing composite values for unique constraint validation."""
        if not self.db_connection:
            return set()
        
        try:
            quoted_table = self.db_connection.quote_identifier(table_name)
            quoted_columns = [self.db_connection.quote_identifier(col) for col in columns]
            columns_str = ', '.join(quoted_columns)
            
            query = f"SELECT DISTINCT {columns_str} FROM {quoted_table}"
            result = self.db_connection.execute_query(query)
            
            if result:
                return {tuple(row) for row in result}
        except Exception as e:
            logger.debug(f"Could not fetch existing composite values for {table_name}.{columns}: {e}")
        
        return set()
    
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
        if cache_key not in self._existing_values:
            self._existing_values[cache_key] = set()
            
            if self.db_connection:
                try:
                    quoted_table = self.db_connection.quote_identifier(table_name)
                    quoted_column = self.db_connection.quote_identifier(column_name)
                    query = f"SELECT DISTINCT {quoted_column} FROM {quoted_table} WHERE {quoted_column} IS NOT NULL"
                    result = self.db_connection.execute_query(query)
                    if result:
                        self._existing_values[cache_key] = {row[0] for row in result}
                except Exception as e:
                    logger.debug(f"Could not fetch existing values for {table_name}.{column_name}: {e}")
        
        return self._existing_values[cache_key]
    
    def _get_max_primary_key_value(self, table_name: str, column_name: str) -> int:
        """Get the maximum existing primary key value."""
        if self.db_connection:
            try:
                quoted_table = self.db_connection.quote_identifier(table_name)
                quoted_column = self.db_connection.quote_identifier(column_name)
                query = f"SELECT COALESCE(MAX({quoted_column}), 0) FROM {quoted_table}"
                result = self.db_connection.execute_query(query)
                if result and result[0] and result[0][0] is not None:
                    return int(result[0][0])
            except Exception as e:
                logger.debug(f"Could not get max primary key for {table_name}.{column_name}: {e}")
        return 0
    
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
            'lorem': lambda col: self._safe_text_generation(col.max_length or 100),
            'country': lambda col: self.faker.country(),
            'city': lambda col: self.faker.city(),
            'state': lambda col: self.faker.state(),
            'zipcode': lambda col: self.faker.zipcode(),
        }
    
    def _is_primary_key_column(self, table: Optional[TableInfo], column_name: str) -> bool:
        """Check if a column is a primary key."""
        if table is None:
            return False
        pk_columns = table.get_primary_key_columns()
        return column_name in pk_columns
    
    def _generate_smart_duplicate_value(self, column: ColumnInfo, 
                                      config: ColumnGenerationConfig,
                                      table: Optional[TableInfo] = None) -> Any:
        """Generate values with controlled duplication for realistic data distribution."""
        # Skip for primary keys and unique columns
        if self._is_primary_key_column(table, column.name) or self._is_unique_column(table, column.name):
            return self._generate_by_type(column, config, table)
        
        cache_key = f"smart_duplicate_{table.name}_{column.name}"
        if not hasattr(self, '_smart_duplicate_cache'):
            self._smart_duplicate_cache = {}
        
        if cache_key not in self._smart_duplicate_cache:
            self._smart_duplicate_cache[cache_key] = {
                'values': [],
                'usage_count': {}
            }
        
        cache = self._smart_duplicate_cache[cache_key]
        
        # Generate initial set of values if empty
        max_values = getattr(config, 'max_duplicate_values', 10)
        if len(cache['values']) < max_values:
            new_value = self._generate_by_type(column, config, table)
            cache['values'].append(new_value)
            cache['usage_count'][new_value] = 0
            logger.debug(f"Added new smart duplicate value for {column.name}: {new_value}")
            return new_value
        
        # Use probability to decide between reusing and generating new
        duplicate_prob = getattr(config, 'duplicate_probability', 0.5)
        
        if random.random() < duplicate_prob:
            # Reuse existing value (prefer less used ones)
            values_by_usage = sorted(cache['values'], key=lambda v: cache['usage_count'].get(v, 0))
            selected_value = values_by_usage[0]  # Pick least used
            cache['usage_count'][selected_value] += 1
            logger.debug(f"Reusing smart duplicate value for {column.name}: {selected_value}")
            return selected_value
        else:
            # Generate new value but limit total unique values
            if len(cache['values']) < max_values:
                new_value = self._generate_by_type(column, config, table)
                cache['values'].append(new_value)
                cache['usage_count'][new_value] = 1
                logger.debug(f"Generated new smart duplicate value for {column.name}: {new_value}")
                return new_value
            else:
                # Reuse random existing value
                selected_value = random.choice(cache['values'])
                cache['usage_count'][selected_value] += 1
                logger.debug(f"Reusing random smart duplicate value for {column.name}: {selected_value}")
                return selected_value
    
    def _generate_smart_duplicate_value_global(self, column: ColumnInfo, 
                                             table: Optional[TableInfo] = None) -> Any:
        """Generate values with controlled duplication using global configuration."""
        # Skip for primary keys and unique columns
        if self._is_primary_key_column(table, column.name) or self._is_unique_column(table, column.name):
            column_config = None
            return self._generate_by_type(column, column_config, table)
        
        cache_key = f"global_smart_duplicate_{table.name}_{column.name}"
        if not hasattr(self, '_global_smart_duplicate_cache'):
            self._global_smart_duplicate_cache = {}
        
        if cache_key not in self._global_smart_duplicate_cache:
            self._global_smart_duplicate_cache[cache_key] = {
                'values': [],
                'usage_count': {}
            }
        
        cache = self._global_smart_duplicate_cache[cache_key]
        
        # Generate initial set of values if empty
        max_values = self.config.global_max_duplicate_values
        if len(cache['values']) < max_values:
            column_config = None
            new_value = self._generate_by_type(column, column_config, table)
            cache['values'].append(new_value)
            cache['usage_count'][new_value] = 0
            logger.debug(f"Added new global smart duplicate value for {column.name}: {new_value}")
            return new_value
        
        # Use global probability to decide between reusing and generating new
        duplicate_prob = self.config.global_duplicate_probability
        
        if random.random() < duplicate_prob:
            # Reuse existing value (prefer less used ones)
            values_by_usage = sorted(cache['values'], key=lambda v: cache['usage_count'].get(v, 0))
            selected_value = values_by_usage[0]  # Pick least used
            cache['usage_count'][selected_value] += 1
            logger.debug(f"Reusing global smart duplicate value for {column.name}: {selected_value}")
            return selected_value
        else:
            # Generate new value but limit total unique values
            if len(cache['values']) < max_values:
                column_config = None
                new_value = self._generate_by_type(column, column_config, table)
                cache['values'].append(new_value)
                cache['usage_count'][new_value] = 1
                logger.debug(f"Generated new global smart duplicate value for {column.name}: {new_value}")
                return new_value
            else:
                # Reuse random existing value
                selected_value = random.choice(cache['values'])
                cache['usage_count'][selected_value] += 1
                logger.debug(f"Reusing random global smart duplicate value for {column.name}: {selected_value}")
                return selected_value
    
    def _is_unique_column(self, table: Optional[TableInfo], column_name: str) -> bool:
        """Check if a column has a unique constraint."""
        if table is None:
            return False
        
        # Check for unique constraints on this column
        for constraint in table.constraints:
            if constraint.type == ConstraintType.UNIQUE and column_name in constraint.columns:
                return True
        return False
    
    def _can_allow_duplicates(self, table: Optional[TableInfo], column_name: str) -> bool:
        """Check if a column can allow duplicate values based on its constraints."""
        if table is None:
            return True
        
        # Check if column is primary key
        if self._is_primary_key_column(table, column_name):
            logger.debug(f"Column {column_name} is primary key - duplicates not allowed")
            return False
        
        # Check if column has unique constraint
        if self._is_unique_column(table, column_name):
            logger.debug(f"Column {column_name} has unique constraint - duplicates not allowed")
            return False
        
        # Check for unique constraints on this column
        for constraint in table.constraints:
            if constraint.type == ConstraintType.UNIQUE and column_name in constraint.columns:
                logger.debug(f"Column {column_name} is part of unique constraint - duplicates not allowed")
                return False
        
        # Check if column has auto increment
        column_info = table.get_column(column_name)
        if column_info and column_info.is_auto_increment:
            logger.debug(f"Column {column_name} is auto increment - duplicates not practical")
            return False
        
        # Column can allow duplicates
        logger.debug(f"Column {column_name} can allow duplicates")
        return True
