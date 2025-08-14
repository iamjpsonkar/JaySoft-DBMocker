"""Smart data generator with dependency-aware FK generation."""

import logging
import random
from typing import Dict, List, Set, Any, Optional, Tuple
from collections import defaultdict

from .models import (
    DatabaseSchema, TableInfo, ColumnInfo, GenerationConfig, 
    TableGenerationConfig, ColumnGenerationConfig
)
from .dependency_resolver import DependencyResolver, InsertionPlan
from .generator import DataGenerator
from .database import DatabaseConnection

logger = logging.getLogger(__name__)


class SmartFKValueManager:
    """Manages FK values using existing data and dependency awareness."""
    
    def __init__(self, db_connection: DatabaseConnection, schema: DatabaseSchema, config: GenerationConfig = None):
        self.db_connection = db_connection
        self.schema = schema
        self.config = config
        self._existing_values_cache: Dict[str, Dict[str, List[Any]]] = {}
        self._generated_values_cache: Dict[str, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
        
    def get_existing_values(self, table_name: str, column_name: str = 'id') -> List[Any]:
        """Get existing values from a table column."""
        cache_key = f"{table_name}.{column_name}"
        
        if cache_key not in self._existing_values_cache:
            try:
                quoted_table = self.db_connection.quote_identifier(table_name)
                quoted_column = self.db_connection.quote_identifier(column_name)
                query = f"SELECT DISTINCT {quoted_column} FROM {quoted_table} WHERE {quoted_column} IS NOT NULL"
                result = self.db_connection.execute_query(query)
                
                values = [row[0] for row in result] if result else []
                self._existing_values_cache[cache_key] = values
                logger.debug(f"Cached {len(values)} existing values for {cache_key}")
                
            except Exception as e:
                logger.debug(f"Could not get existing values for {cache_key}: {e}")
                self._existing_values_cache[cache_key] = []
        
        return self._existing_values_cache[cache_key]
    
    def add_generated_value(self, table_name: str, column_name: str, value: Any):
        """Track a newly generated value."""
        self._generated_values_cache[table_name][column_name].append(value)
    
    def get_all_available_values(self, table_name: str, column_name: str = 'id') -> List[Any]:
        """Get both existing and recently generated values."""
        existing = self.get_existing_values(table_name, column_name)
        generated = self._generated_values_cache[table_name][column_name]
        return existing + generated
    
    def get_random_fk_value(self, referenced_table: str, referenced_column: str = 'id') -> Optional[Any]:
        """Get a random FK value from available values."""
        available_values = self.get_all_available_values(referenced_table, referenced_column)
        
        if available_values:
            return random.choice(available_values)
        else:
            logger.warning(f"No available values for FK: {referenced_table}.{referenced_column}")
            return None
    
    def get_mixed_mode_fk_value(self, referenced_table: str, referenced_column: str = 'id') -> Optional[Any]:
        """Get FK value in mixed mode, prioritizing existing data for tables marked as use_existing."""
        if self.config and referenced_table in self.config.use_existing_tables:
            # For tables marked as "use existing", only use existing data
            existing_values = self.get_existing_values(referenced_table, referenced_column)
            if existing_values:
                return random.choice(existing_values)
            else:
                logger.warning(f"No existing values found for {referenced_table}.{referenced_column} (marked as use_existing)")
                return None
        else:
            # Check if the referenced table is unselected (not in table_configs for generation)
            # If so, it means we should use existing data from that unselected table
            if self.config and hasattr(self.config, 'table_configs'):
                is_table_selected_for_generation = False
                
                # Check if table is configured for generation (rows > 0 and not use_existing_data)
                if referenced_table in self.config.table_configs:
                    table_config = self.config.table_configs[referenced_table]
                    if (table_config.rows_to_generate > 0 and 
                        not getattr(table_config, 'use_existing_data', False)):
                        is_table_selected_for_generation = True
                
                # If referenced table is not selected for generation, use existing data
                if not is_table_selected_for_generation:
                    existing_values = self.get_existing_values(referenced_table, referenced_column)
                    if existing_values:
                        logger.debug(f"Using existing data from unselected table {referenced_table} for FK")
                        return random.choice(existing_values)
                    else:
                        logger.warning(f"No existing values found in unselected table {referenced_table}.{referenced_column}")
                        return None
            
            # For tables selected for generation, prefer existing if configured, otherwise use all available
            if self.config and self.config.prefer_existing_fk_values:
                existing_values = self.get_existing_values(referenced_table, referenced_column)
                if existing_values:
                    return random.choice(existing_values)
            
            # Fall back to all available values (existing + generated)
            return self.get_random_fk_value(referenced_table, referenced_column)


class DependencyAwareGenerator(DataGenerator):
    """Enhanced generator that respects table dependencies and uses smart FK generation."""
    
    def __init__(self, schema: DatabaseSchema, config: GenerationConfig, db_connection: DatabaseConnection):
        super().__init__(schema, config, db_connection)
        self.dependency_resolver = DependencyResolver(schema)
        self.insertion_plan = self.dependency_resolver.create_insertion_plan()
        self.fk_manager = SmartFKValueManager(db_connection, schema, config)
        
    def generate_data_for_all_tables(self, rows_per_table: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """Generate data for all tables in dependency order."""
        logger.info("Starting dependency-aware data generation")
        
        # Get insertion batches
        batches = self.insertion_plan.get_insertion_batches()
        all_data = {}
        
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)}: {', '.join(batch)}")
            
            for table_name in batch:
                table = self._get_table_by_name(table_name)
                if not table:
                    continue
                
                # Get table-specific configuration
                table_config = self.config.table_configs.get(
                    table_name, 
                    TableGenerationConfig(rows_to_generate=rows_per_table)
                )
                
                # Check if this table should use existing data (mixed mode)
                if (table_name in self.config.use_existing_tables or 
                    table_config.use_existing_data):
                    logger.info(f"Skipping data generation for {table_name} - using existing data")
                    all_data[table_name] = []  # Empty list indicates existing data should be used
                    
                    # Cache existing primary key values for FK reference
                    self._cache_existing_pk_values(table)
                    
                    logger.info(f"Using existing data for {table_name}")
                else:
                    # Generate data for this table
                    table_data = self._generate_table_data_smart(table, table_config)
                    all_data[table_name] = table_data
                    
                    # Cache generated primary key values for FK reference
                    self._cache_generated_pk_values(table, table_data)
                    
                    logger.info(f"Generated {len(table_data)} rows for {table_name}")
        
        return all_data
    
    def _get_table_by_name(self, table_name: str) -> Optional[TableInfo]:
        """Get table info by name."""
        for table in self.schema.tables:
            if table.name == table_name:
                return table
        return None
    
    def _generate_table_data_smart(self, table: TableInfo, table_config: TableGenerationConfig) -> List[Dict[str, Any]]:
        """Generate data for a table with smart FK handling."""
        rows = []
        
        for i in range(table_config.rows_to_generate):
            row = self._generate_row_smart(table, table_config)
            rows.append(row)
        
        return rows
    
    def _generate_row_smart(self, table: TableInfo, table_config: TableGenerationConfig) -> Dict[str, Any]:
        """Generate a single row with smart FK generation."""
        row = {}
        
        # First pass: generate all non-FK columns
        for column in table.columns:
            if not self._is_foreign_key_column(table, column.name):
                row[column.name] = self._generate_column_value(column, table_config, table)
        
        # Second pass: generate FK columns using smart FK manager
        for column in table.columns:
            if self._is_foreign_key_column(table, column.name):
                # Check if column has specific configuration - if so, respect it
                column_config = table_config.column_configs.get(column.name)
                if column_config and (column_config.possible_values or column_config.min_value is not None or column_config.max_value is not None):
                    logger.debug(f"FK column {column.name} has configuration, keeping configured value")
                    # Generate the configured value
                    row[column.name] = self._generate_column_value(column, table_config, table)
                else:
                    # Use smart FK generation with mixed mode support
                    fk_value = self._generate_smart_fk_value(table, column)
                    if fk_value is not None:
                        row[column.name] = fk_value
                        logger.debug(f"Generated smart FK value for {column.name}: {fk_value}")
                    else:
                        # Fallback to original FK generation
                        row[column.name] = self._generate_foreign_key_value(table, column)
        
        # Third pass: validate composite unique constraints
        row = self._validate_composite_unique_constraints(table, row)
        
        # Fourth pass: Apply smart constraint validation to prevent 9h9h errors
        row = self._apply_smart_constraint_validation(table, row)
        
        return row
    
    def _apply_smart_constraint_validation(self, table: TableInfo, row: Dict[str, Any]) -> Dict[str, Any]:
        """Apply smart constraint validation to prevent data too long errors."""
        for column in table.columns:
            if column.name in row and row[column.name] is not None:
                value = row[column.name]
                
                # Handle string length constraints
                if isinstance(value, str) and column.max_length:
                    if len(value) > column.max_length:
                        # Apply smart truncation based on column name patterns
                        if any(pattern in column.name.lower() for pattern in ['phone', 'mobile', 'tel']):
                            row[column.name] = self._smart_truncate_phone(value, column.max_length)
                        elif any(pattern in column.name.lower() for pattern in ['email', 'mail']):
                            row[column.name] = self._smart_truncate_email(value, column.max_length)
                        elif any(pattern in column.name.lower() for pattern in ['url', 'link', 'website']):
                            row[column.name] = self._smart_truncate_url(value, column.max_length)
                        elif any(pattern in column.name.lower() for pattern in ['address', 'location']):
                            row[column.name] = self._smart_truncate_address(value, column.max_length)
                        elif any(pattern in column.name.lower() for pattern in ['name', 'title']):
                            row[column.name] = self._smart_truncate_name(value, column.max_length)
                        else:
                            # Generic truncation
                            row[column.name] = value[:column.max_length]
                        
                        logger.debug(f"Truncated {column.name} from {len(value)} to {len(row[column.name])} chars")
        
        return row
    
    def _smart_truncate_phone(self, phone: str, max_length: int) -> str:
        """Intelligently truncate phone number while maintaining format."""
        if len(phone) <= max_length:
            return phone
        
        # Remove extensions first (x12345 or ext123)
        import re
        base_phone = re.sub(r'(x|ext)\d+$', '', phone)
        if len(base_phone) <= max_length:
            return base_phone
        
        # Remove country codes and formatting, keep core digits
        digits_only = re.sub(r'[^\d]', '', phone)
        if len(digits_only) <= max_length:
            return digits_only[:max_length]
        
        # Keep the most important part (last N digits)
        if max_length >= 10:
            return digits_only[-10:]  # Standard phone number length
        else:
            return digits_only[-max_length:]
    
    def _smart_truncate_email(self, email: str, max_length: int) -> str:
        """Intelligently truncate email while maintaining format."""
        if len(email) <= max_length:
            return email
        
        if '@' in email:
            username, domain = email.split('@', 1)
            # Try to keep domain intact
            if len(domain) + 1 < max_length:  # +1 for @
                username_limit = max_length - len(domain) - 1
                return f"{username[:username_limit]}@{domain}"
        
        # Fallback to simple truncation
        return email[:max_length]
    
    def _smart_truncate_url(self, url: str, max_length: int) -> str:
        """Intelligently truncate URL while maintaining validity."""
        if len(url) <= max_length:
            return url
        
        # Try to keep protocol and domain
        import re
        if match := re.match(r'^(https?://)([^/]+)', url):
            protocol_domain = match.group(0)
            if len(protocol_domain) <= max_length:
                return protocol_domain
        
        # Fallback
        return url[:max_length]
    
    def _smart_truncate_address(self, address: str, max_length: int) -> str:
        """Intelligently truncate address."""
        if len(address) <= max_length:
            return address
        
        # Try to keep meaningful parts (remove adjectives first)
        words = address.split()
        if len(words) > 1:
            # Keep important words (numbers, street names)
            important_words = [w for w in words if w[0].isupper() or w.isdigit()]
            truncated = ' '.join(important_words)
            if len(truncated) <= max_length:
                return truncated
        
        # Fallback to word-based truncation
        result = ""
        for word in words:
            if len(result) + len(word) + 1 <= max_length:
                result += (" " if result else "") + word
            else:
                break
        
        return result if result else address[:max_length]
    
    def _smart_truncate_name(self, name: str, max_length: int) -> str:
        """Intelligently truncate name."""
        if len(name) <= max_length:
            return name
        
        # Try to keep first and last name
        words = name.split()
        if len(words) >= 2:
            first_last = f"{words[0]} {words[-1]}"
            if len(first_last) <= max_length:
                return first_last
            
            # Try just first name
            if len(words[0]) <= max_length:
                return words[0]
        
        # Fallback
        return name[:max_length]
    
    def _generate_smart_fk_value(self, table: TableInfo, column: ColumnInfo) -> Optional[Any]:
        """Generate FK value using dependency information."""
        # Find the FK constraint for this column
        fk_constraint = None
        for fk in table.foreign_keys:
            if column.name in fk.columns:
                fk_constraint = fk
                break
        
        if not fk_constraint or not fk_constraint.referenced_table:
            return None
        
        referenced_table = fk_constraint.referenced_table
        column_index = fk_constraint.columns.index(column.name)
        referenced_column = (
            fk_constraint.referenced_columns[column_index] 
            if fk_constraint.referenced_columns and column_index < len(fk_constraint.referenced_columns)
            else 'id'
        )
        
        # Get available FK values using mixed mode logic
        fk_value = self.fk_manager.get_mixed_mode_fk_value(referenced_table, referenced_column)
        
        return fk_value
    
    def _cache_generated_pk_values(self, table: TableInfo, table_data: List[Dict[str, Any]]):
        """Cache generated primary key values for FK reference."""
        pk_columns = table.get_primary_key_columns()
        
        for pk_column in pk_columns:
            if pk_column in table_data[0] if table_data else {}:
                for row in table_data:
                    if pk_column in row and row[pk_column] is not None:
                        self.fk_manager.add_generated_value(table.name, pk_column, row[pk_column])
    
    def _cache_existing_pk_values(self, table: TableInfo):
        """Cache existing primary key values for FK reference in mixed mode."""
        pk_columns = table.get_primary_key_columns()
        
        for pk_column in pk_columns:
            # Get existing values and cache them in the FK manager
            existing_values = self.fk_manager.get_existing_values(table.name, pk_column)
            logger.debug(f"Cached {len(existing_values)} existing PK values for {table.name}.{pk_column}")
    
    def get_insertion_plan(self) -> InsertionPlan:
        """Get the dependency-aware insertion plan."""
        return self.insertion_plan
    
    def analyze_fk_dependencies_for_selection(self) -> Dict[str, List[str]]:
        """Analyze FK dependencies between selected and unselected tables."""
        selected_tables = set()
        unselected_tables = set()
        
        # Determine which tables are selected for generation
        for table_name, table_config in self.config.table_configs.items():
            if (table_config.rows_to_generate > 0 and 
                not getattr(table_config, 'use_existing_data', False)):
                selected_tables.add(table_name)
            else:
                unselected_tables.add(table_name)
        
        # Find FK dependencies from selected tables to unselected tables
        fk_dependencies = {}
        
        for table in self.schema.tables:
            if table.name in selected_tables:
                unselected_dependencies = []
                
                for fk in table.foreign_keys:
                    if fk.referenced_table in unselected_tables:
                        unselected_dependencies.append(fk.referenced_table)
                
                if unselected_dependencies:
                    fk_dependencies[table.name] = unselected_dependencies
        
        return fk_dependencies
    
    def validate_fk_integrity_for_selection(self) -> Dict[str, Dict[str, bool]]:
        """Validate that unselected referenced tables have existing data for FK integrity."""
        validation_results = {}
        fk_dependencies = self.analyze_fk_dependencies_for_selection()
        
        for selected_table, referenced_tables in fk_dependencies.items():
            validation_results[selected_table] = {}
            
            table_info = next((t for t in self.schema.tables if t.name == selected_table), None)
            if not table_info:
                continue
            
            for fk in table_info.foreign_keys:
                if fk.referenced_table in referenced_tables:
                    # Check if referenced table has existing data
                    referenced_column = fk.referenced_columns[0] if fk.referenced_columns else 'id'
                    existing_values = self.fk_manager.get_existing_values(fk.referenced_table, referenced_column)
                    
                    validation_results[selected_table][fk.referenced_table] = len(existing_values) > 0
        
        return validation_results
    
    def suggest_table_configuration(self, table_name: str) -> Optional[TableGenerationConfig]:
        """Suggest optimal configuration for a table based on dependencies."""
        table = self._get_table_by_name(table_name)
        if not table:
            return None
        
        column_configs = {}
        
        # Get FK suggestions
        fk_suggestions = self.dependency_resolver.suggest_fk_value_sources(table_name)
        
        for column_name, suggestion in fk_suggestions.items():
            # Get available values from the referenced table
            available_values = self.fk_manager.get_existing_values(
                suggestion['source_table'], 
                suggestion['source_column']
            )
            
            if available_values:
                # Use existing values as possible_values
                if len(available_values) <= 50:  # Don't create huge configs
                    column_configs[column_name] = ColumnGenerationConfig(
                        possible_values=available_values[:50]  # Limit to 50 values
                    )
                else:
                    # Use range for large datasets
                    if all(isinstance(v, int) for v in available_values[:10]):
                        min_val = min(available_values)
                        max_val = max(available_values)
                        column_configs[column_name] = ColumnGenerationConfig(
                            min_value=min_val,
                            max_value=max_val
                        )
        
        return TableGenerationConfig(
            rows_to_generate=10,
            column_configs=column_configs
        )


def create_optimal_generation_config(
    schema: DatabaseSchema, 
    db_connection: DatabaseConnection,
    base_rows_per_table: int = 10
) -> GenerationConfig:
    """Create optimal generation configuration based on database analysis."""
    
    resolver = DependencyResolver(schema)
    fk_manager = SmartFKValueManager(db_connection, schema)
    
    table_configs = {}
    
    for table in schema.tables:
        column_configs = {}
        
        # Get FK suggestions for this table
        fk_suggestions = resolver.suggest_fk_value_sources(table.name)
        
        for column_name, suggestion in fk_suggestions.items():
            # Get available values from referenced table
            available_values = fk_manager.get_existing_values(
                suggestion['source_table'],
                suggestion['source_column']
            )
            
            if available_values:
                if len(available_values) <= 20:
                    # Use specific values for small datasets
                    column_configs[column_name] = ColumnGenerationConfig(
                        possible_values=available_values
                    )
                else:
                    # Use range for larger datasets
                    if all(isinstance(v, int) for v in available_values[:10]):
                        min_val = min(available_values)
                        max_val = max(available_values)
                        column_configs[column_name] = ColumnGenerationConfig(
                            min_value=min_val,
                            max_value=max_val
                        )
        
        if column_configs:
            table_configs[table.name] = TableGenerationConfig(
                rows_to_generate=base_rows_per_table,
                column_configs=column_configs
            )
    
    return GenerationConfig(
        batch_size=1000,
        table_configs=table_configs,
        preserve_existing_data=True,
        reuse_existing_values=0.8  # High reuse probability for FK values
    )
