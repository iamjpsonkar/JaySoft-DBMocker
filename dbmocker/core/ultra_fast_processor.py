"""
Ultra-Fast Bulk Data Processor
Designed specifically for generating and inserting millions of records with maximum efficiency.
"""

import logging
import time
import gc
import psutil
import threading
import multiprocessing as mp
import queue
import numpy as np
import random
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple, Iterator, Union
from dataclasses import dataclass, field
import pickle
import sqlite3
import io
import csv
import json
from pathlib import Path

from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Boolean, Float
from sqlalchemy.dialects import postgresql, mysql, sqlite
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool, StaticPool

from .models import DatabaseSchema, TableInfo, GenerationConfig, ColumnType
from .database import DatabaseConnection
from .enhanced_models import (
    EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy, 
    InsertionStrategy, PerformanceReport
)
from .fast_data_reuse import FastDataReuser, create_fast_data_reuser, DataReuse


logger = logging.getLogger(__name__)


@dataclass
class UltraFastTask:
    """Task for ultra-fast processing."""
    task_id: str
    table_name: str
    row_start: int
    row_end: int
    batch_size: int
    worker_type: str  # 'thread' or 'process'
    config_snapshot: Dict[str, Any]
    seed_offset: int = 0


class MemoryMappedValueGenerator:
    """Memory-mapped value generator for ultra-fast access."""
    
    def __init__(self, cache_dir: str = "/tmp/dbmocker_cache"):
        """Initialize memory-mapped generator."""
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Pre-generated value pools
        self.value_pools = {}
        self.pool_files = {}
        
        # Statistics
        self.cache_hits = 0
        self.cache_misses = 0
        
        logger.info(f"🗃️  Initialized memory-mapped generator at {cache_dir}")
    
    def create_value_pool(self, pool_name: str, data_type: ColumnType, 
                         pool_size: int = 100000) -> str:
        """Create a pre-generated pool of values."""
        pool_file = self.cache_dir / f"{pool_name}_{data_type.value}_{pool_size}.npy"
        
        if pool_file.exists():
            logger.info(f"📂 Loading existing pool: {pool_name}")
            return str(pool_file)
        
        logger.info(f"🏗️  Creating value pool: {pool_name} ({pool_size:,} values)")
        
        # Generate values based on type
        if data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
            values = np.random.randint(1, 1000000, size=pool_size, dtype=np.int64)
        elif data_type in [ColumnType.FLOAT, ColumnType.DOUBLE]:
            values = np.random.uniform(0.0, 1000000.0, size=pool_size).astype(np.float64)
        elif data_type == ColumnType.BOOLEAN:
            values = np.random.choice([0, 1], size=pool_size, dtype=np.int8)
        elif data_type == ColumnType.VARCHAR:
            # Generate string indices for lookup
            values = np.random.randint(0, 10000, size=pool_size, dtype=np.int32)
        else:
            # Default to integers
            values = np.random.randint(1, 100000, size=pool_size, dtype=np.int64)
        
        # Save to disk
        np.save(pool_file, values)
        self.value_pools[pool_name] = pool_file
        
        logger.info(f"✅ Created pool {pool_name}: {pool_file.stat().st_size / 1024 / 1024:.1f}MB")
        return str(pool_file)
    
    def get_values(self, pool_name: str, count: int, offset: int = 0) -> np.ndarray:
        """Get values from pool."""
        if pool_name not in self.value_pools:
            self.cache_misses += count
            return None
        
        pool_file = self.value_pools[pool_name]
        
        try:
            # Memory-map the file for ultra-fast access
            values = np.load(pool_file, mmap_mode='r')
            
            # Get requested slice with wraparound
            pool_size = len(values)
            start_idx = offset % pool_size
            end_idx = (offset + count) % pool_size
            
            if end_idx > start_idx:
                result = values[start_idx:end_idx]
            else:
                # Wraparound case
                result = np.concatenate([values[start_idx:], values[:end_idx]])
            
            self.cache_hits += count
            return result
            
        except Exception as e:
            logger.error(f"Failed to read pool {pool_name}: {e}")
            self.cache_misses += count
            return None
    
    def get_hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0


class UltraFastStringGenerator:
    """Ultra-fast string generation using pre-computed templates."""
    
    def __init__(self):
        """Initialize string generator."""
        # Pre-computed string components
        self.prefixes = [
            "user", "admin", "test", "prod", "dev", "demo", "temp", "sys",
            "app", "web", "api", "db", "cache", "queue", "worker", "service"
        ]
        
        self.suffixes = [
            "001", "002", "003", "data", "info", "main", "backup", "temp",
            "new", "old", "active", "inactive", "primary", "secondary"
        ]
        
        self.words = [
            "apple", "banana", "cherry", "dragon", "eagle", "forest", "garden", "house",
            "island", "jungle", "kingdom", "laptop", "mountain", "network", "ocean", "planet"
        ]
        
        # Pre-compute common combinations
        self._precompute_combinations()
        
        logger.info(f"🔤 Initialized ultra-fast string generator")
    
    def _precompute_combinations(self):
        """Pre-compute common string combinations."""
        self.email_domains = ["example.com", "test.org", "demo.net", "fake.io", "mock.dev"]
        self.phone_formats = ["###-###-####", "(###) ###-####", "###.###.####"]
        
        # Pre-generate common patterns
        self.common_strings = []
        for prefix in self.prefixes[:8]:
            for suffix in self.suffixes[:8]:
                self.common_strings.append(f"{prefix}_{suffix}")
        
        for word in self.words[:10]:
            for i in range(100, 1000, 100):
                self.common_strings.append(f"{word}{i}")
    
    def generate_batch(self, count: int, pattern: str = "default", 
                      max_length: int = 50) -> List[str]:
        """Generate batch of strings ultra-fast."""
        if pattern == "email":
            return [f"user{i}@{np.random.choice(self.email_domains)}" 
                   for i in range(count)]
        elif pattern == "name":
            return [f"{np.random.choice(self.words).title()}{i}" 
                   for i in range(count)]
        elif pattern == "phone":
            format_template = np.random.choice(self.phone_formats)
            return [format_template.replace("#", str(np.random.randint(0, 9)))
                   for _ in range(count)]
        else:
            # Use pre-computed combinations for speed
            return [np.random.choice(self.common_strings) for _ in range(count)]


class UltraFastDataGenerator:
    """Ultra-fast data generator optimized for millions of records."""
    
    def __init__(self, schema: DatabaseSchema, config: EnhancedGenerationConfig, 
                 db_connection: DatabaseConnection = None):
        """Initialize ultra-fast generator."""
        self.schema = schema
        self.config = config
        self.db_connection = db_connection
        
        # Initialize optimized components
        self.value_generator = MemoryMappedValueGenerator()
        self.string_generator = UltraFastStringGenerator()
        
        # Pre-allocate arrays for batch generation
        self.batch_arrays = {}
        
        # Performance tracking
        self.rows_generated = 0
        self.start_time = None
        
        # Pre-analyze tables and create optimal strategies
        self._analyze_and_optimize()
        
        logger.info(f"⚡ Ultra-fast generator initialized for {len(schema.tables)} tables")
    
    def _analyze_and_optimize(self):
        """Analyze schema and create optimal generation strategies."""
        self.table_strategies = {}
        
        for table in self.schema.tables:
            strategy = self._create_table_strategy(table)
            self.table_strategies[table.name] = strategy
            
            # Create value pools for high-frequency columns
            self._create_value_pools_for_table(table)
    
    def _create_table_strategy(self, table: TableInfo) -> Dict[str, Any]:
        """Create optimal generation strategy for table."""
        # Store current table for FK resolution
        self.current_table = table
        
        strategy = {
            'table_name': table.name,
            'column_strategies': {},
            'batch_size': self.config.performance.batch_size,
            'use_numpy': True,
            'pre_allocate': True
        }
        
        for column in table.columns:
            # Skip auto-increment columns - let the database handle them
            if hasattr(column, 'is_auto_increment') and column.is_auto_increment:
                continue
            col_strategy = self._create_column_strategy(column)
            strategy['column_strategies'][column.name] = col_strategy
        
        return strategy
    
    def _create_column_strategy(self, column) -> Dict[str, Any]:
        """Create optimal generation strategy for column with proper constraint handling."""
        strategy = {
            'name': column.name,
            'type': column.data_type,
            'method': 'default',
            'nullable': column.is_nullable if hasattr(column, 'is_nullable') else True
        }
        
        # Analyze column name patterns for better type detection
        column_name_lower = column.name.lower()
        
        # Handle boolean columns (including common boolean-like names)
        if (column.data_type == ColumnType.BOOLEAN or 
            column_name_lower.startswith('is_') or  # Any column starting with 'is_'
            column_name_lower.startswith('has_') or  # Any column starting with 'has_'
            column_name_lower.startswith('can_') or  # Any column starting with 'can_'
            column_name_lower in ['active', 'enabled', 'deleted', 'valid', 'visible', 'published', 'verified', 'confirmed', 'archived']):
            strategy['method'] = 'boolean'
            return strategy
        
        # Special case: MySQL tinyint(1) columns are typically boolean regardless of name
        # Common patterns: created_on_oms, flags, status indicators, collect values
        if (column.data_type == ColumnType.INTEGER and 
            (column_name_lower.endswith('_oms') or column_name_lower.endswith('_flag') or 
             column_name_lower.endswith('_status') or column_name_lower.endswith('_indicator') or
             column_name_lower.endswith('_collect') or column_name_lower.startswith('payment_to_') or
             # Add other suspicious boolean-like integer column patterns
             'flag' in column_name_lower or 'status' in column_name_lower)):
            strategy['method'] = 'boolean'
            return strategy
        
        # Handle datetime/timestamp columns - ONLY if data type is actually datetime
        if column.data_type in [ColumnType.DATETIME, ColumnType.TIMESTAMP, ColumnType.DATE]:
            strategy['method'] = 'datetime'
            strategy['format'] = 'datetime' if 'date' in column_name_lower else 'timestamp'
            return strategy
        # Handle VARCHAR/TEXT columns with datetime-like names
        elif (column.data_type in [ColumnType.VARCHAR, ColumnType.TEXT] and
              any(keyword in column_name_lower for keyword in ['created', 'modified', 'updated', 'date', 'time', '_on', '_at'])):
            strategy['method'] = 'datetime'
            strategy['format'] = 'string'  # Generate datetime as string for text columns
            return strategy
        
        # Handle numeric columns
        if column.data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
            strategy['method'] = 'numpy_int'
            
            # Set reasonable ranges based on column type and name
            if column.data_type == ColumnType.SMALLINT:
                strategy['min_val'] = column.min_value or 1
                strategy['max_val'] = column.max_value or 32767
            elif 'id' in column_name_lower and column_name_lower.endswith('_id'):
                # Check if this is a foreign key column
                if self._is_foreign_key_column(column):
                    strategy['method'] = 'foreign_key'
                    strategy['fk_info'] = self._get_foreign_key_info(column)
                else:
                    # Regular ID columns should be smaller ranges  
                    strategy['min_val'] = 1
                    strategy['max_val'] = 1000  
            else:
                strategy['min_val'] = column.min_value or 1
                strategy['max_val'] = column.max_value or 100000  # More reasonable than 1M
        
        elif column.data_type in [ColumnType.FLOAT, ColumnType.DOUBLE]:
            strategy['method'] = 'numpy_float'
            strategy['min_val'] = column.min_value or 0.0
            strategy['max_val'] = column.max_value or 10000.0  # More reasonable range
        
        # Handle DECIMAL columns
        elif column.data_type == ColumnType.DECIMAL:
            strategy['method'] = 'decimal'
            strategy['min_val'] = column.min_value or 0.0
            strategy['max_val'] = column.max_value or 10000.0
            strategy['precision'] = getattr(column, 'precision', 10)
            strategy['scale'] = getattr(column, 'scale', 2)
        
        # Handle string columns with smart type detection
        elif column.data_type in [ColumnType.VARCHAR, ColumnType.TEXT, ColumnType.CHAR]:
            # Check if this might be an ENUM column based on name patterns
            if (column_name_lower in ['current_status', 'status', 'collect_type'] or 
                column_name_lower.endswith('_status') or 
                column_name_lower.endswith('_type')):
                # Treat as ENUM for common status/type columns
                strategy['method'] = 'enum'
                strategy['enum_values'] = self._get_enum_values(column.name)
            elif 'email' in column_name_lower:
                strategy['method'] = 'string_email'
            elif any(name_part in column_name_lower for name_part in ['name', 'title', 'label']):
                strategy['method'] = 'string_name'
            elif any(phone_part in column_name_lower for phone_part in ['phone', 'mobile', 'number']):
                strategy['method'] = 'string_phone'
            elif column_name_lower in ['description', 'comment', 'notes', 'content']:
                strategy['method'] = 'string_text'
            elif column_name_lower.endswith('_id') and column.data_type in [ColumnType.VARCHAR, ColumnType.CHAR]:
                strategy['method'] = 'string_id'  # For string-based IDs
            else:
                strategy['method'] = 'string_default'
            
            strategy['max_length'] = column.max_length or 50
        
        # Handle ENUM columns
        elif column.data_type == ColumnType.ENUM:
            strategy['method'] = 'enum'
            # Get ENUM values from database for this specific column
            strategy['enum_values'] = self._get_enum_values(column.name)
            
        # Handle JSON columns
        elif column.data_type in [ColumnType.JSON, ColumnType.JSONB]:
            if 'aggregator' in column_name_lower:
                strategy['method'] = 'json_aggregator'
            else:
                strategy['method'] = 'json_default'
        
        else:
            strategy['method'] = 'fallback'
        
        # Add null avoidance flag - never generate nulls for NOT NULL columns
        strategy['avoid_null'] = not strategy.get('nullable', True)
        
        return strategy
    
    def _get_enum_values(self, column_name: str) -> List[str]:
        """Get ENUM values for a specific column by querying INFORMATION_SCHEMA."""
        try:
            # Try to get actual ENUM values from database if connection is available
            if hasattr(self, 'db_connection') and self.db_connection and hasattr(self, 'current_table'):
                table_name = self.current_table.name
                query = """
                    SELECT COLUMN_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = %s 
                    AND COLUMN_NAME = %s
                """
                try:
                    result = self.db_connection.execute_query(query, (table_name, column_name))
                    if result and result[0] and result[0][0]:
                        column_type = result[0][0]
                        # Parse ENUM values from COLUMN_TYPE like "enum('created','resent','expired')"
                        if column_type.startswith('enum('):
                            enum_part = column_type[5:-1]  # Remove 'enum(' and ')'
                            # Split by comma and clean up quotes
                            enum_values = [val.strip().strip("'\"") for val in enum_part.split(',')]
                            if enum_values:
                                logger.debug(f"Found ENUM values for {column_name}: {enum_values}")
                                return enum_values
                except Exception as e:
                    logger.debug(f"Could not query ENUM values for {column_name}: {e}")
            
            # Fallback to smart defaults based on column name
            if column_name == 'current_status':
                return ['created', 'resent', 'expired', 'cancelled', 'paid', 'failed', 'pending']
            elif column_name == 'status':
                return ['started', 'completed', 'partial_paid', 'failed', 'pending', 'refund_done', 'refund_initiated', 'partial_refund', 'refund_failed', 'refund_pending', 'refund_acknowledge']
            elif column_name == 'collect_type':
                return ['SINGLE', 'MULTI', 'SPLIT', 'ADVANCE']
            elif column_name == 'integration_type':
                return ['extension', 'aggregator']
            elif column_name == 'extension_type':
                return ['payment', 'payout', 'loyality', 'offline']
            elif column_name == 'payment_required_type':
                return ['REQUIRED', 'BLOCKED']
            elif 'status' in column_name.lower():
                return ['active', 'inactive', 'pending', 'completed']
            elif 'type' in column_name.lower():
                return ['type1', 'type2', 'type3']
            else:
                return ['option1', 'option2', 'option3']
        except Exception as e:
            logger.warning(f"Failed to get ENUM values for {column_name}: {e}")
            return ['default']
    
    def _is_foreign_key_column(self, column) -> bool:
        """Check if a column is a foreign key."""
        # This requires access to table schema with FK information
        if hasattr(self, 'current_table') and self.current_table:
            for fk in self.current_table.foreign_keys:
                if column.name in fk.columns:
                    return True
        return False
    
    def _get_foreign_key_info(self, column) -> Dict[str, Any]:
        """Get foreign key information for a column."""
        if hasattr(self, 'current_table') and self.current_table:
            for fk in self.current_table.foreign_keys:
                if column.name in fk.columns:
                    return {
                        'referenced_table': fk.referenced_table,
                        'referenced_column': fk.referenced_columns[0] if fk.referenced_columns else 'id',
                        'constraint_name': fk.name
                    }
        return {}
    
    def _generate_foreign_key_batch(self, strategy: Dict[str, Any], batch_size: int) -> List[Any]:
        """Generate a batch of valid foreign key values."""
        fk_info = strategy.get('fk_info', {})
        referenced_table = fk_info.get('referenced_table')
        referenced_column = fk_info.get('referenced_column', 'id')
        
        if not referenced_table:
            logger.warning("No referenced table found for FK, using fallback values")
            return [random.randint(1, 10) for _ in range(batch_size)]
        
        # Get valid FK values from the database
        try:
            if hasattr(self, 'db_connection') and self.db_connection:
                # Check if this FK has unique constraint
                column_name = strategy.get('name', '')
                is_unique_fk = self._is_unique_fk_column(column_name)
                
                if is_unique_fk:
                    # For unique FKs, get unused values or create new referenced records
                    return self._generate_unique_fk_batch(referenced_table, referenced_column, column_name, batch_size)
                else:
                    # For non-unique FKs, get any available values
                    query = f"SELECT DISTINCT {referenced_column} FROM {referenced_table} WHERE {referenced_column} IS NOT NULL LIMIT 1000"
                    result = self.db_connection.execute_query(query)
                    if result:
                        available_values = [row[0] for row in result]
                        if available_values:
                            return [random.choice(available_values) for _ in range(batch_size)]
        except Exception as e:
            logger.warning(f"Failed to fetch FK values for {referenced_table}.{referenced_column}: {e}")
        
        # Fallback to a reasonable range for FK values
        return [random.randint(1, 10) for _ in range(batch_size)]
    
    def _is_unique_fk_column(self, column_name: str) -> bool:
        """Check if FK column has unique constraint."""
        if hasattr(self, 'current_table') and self.current_table:
            for constraint in self.current_table.constraints:
                if hasattr(constraint, 'type') and constraint.type.value == 'unique' and column_name in constraint.columns:
                    return True
        return False
    
    def _generate_unique_fk_batch(self, referenced_table: str, referenced_column: str, 
                                column_name: str, batch_size: int) -> List[Any]:
        """Generate batch of unique FK values, creating referenced records if needed."""
        try:
            # Get available FK values
            available_query = f"SELECT DISTINCT {referenced_column} FROM {referenced_table} WHERE {referenced_column} IS NOT NULL LIMIT 1000"
            available_result = self.db_connection.execute_query(available_query)
            available_values = [row[0] for row in available_result] if available_result else []
            
            # Get already used FK values
            current_table_name = self.current_table.name if hasattr(self, 'current_table') else 'unknown'
            used_query = f"SELECT DISTINCT {column_name} FROM {current_table_name} WHERE {column_name} IS NOT NULL"
            used_result = self.db_connection.execute_query(used_query)
            used_values = set([row[0] for row in used_result]) if used_result else set()
            
            # Find unused values
            unused_values = [val for val in available_values if val not in used_values]
            
            result_values = []
            for i in range(batch_size):
                if unused_values:
                    # Use an unused value
                    selected_value = unused_values.pop(0)
                    result_values.append(selected_value)
                    used_values.add(selected_value)  # Mark as used
                else:
                    # Need to create a new referenced record
                    new_fk_value = self._create_referenced_record_simple(referenced_table, referenced_column)
                    if new_fk_value is not None:
                        result_values.append(new_fk_value)
                        used_values.add(new_fk_value)
                    else:
                        # Last resort fallback
                        fallback_value = max(available_values) + 1000 + i if available_values else 1000 + i
                        result_values.append(fallback_value)
            
            return result_values
            
        except Exception as e:
            logger.error(f"Failed to generate unique FK batch for {referenced_table}: {e}")
            # Return fallback values
            return [1000 + i for i in range(batch_size)]
    
    def _create_referenced_record_simple(self, referenced_table: str, referenced_column: str) -> Any:
        """Create a simple referenced record and return its ID."""
        try:
            # This is a simplified version - just insert minimal required data
            # In practice, this would need to analyze the referenced table schema
            
            if referenced_table == 'payment_link_details':
                # Special case for payment_link_details - create minimal record
                record_data = {
                    'external_order_id': f'order_{int(time.time())}_{random.randint(1000, 9999)}',
                    'amount': round(random.uniform(10.0, 1000.0), 2),
                    'meta': '{}',
                    'external_customer_id': f'cust_{random.randint(1000, 9999)}',
                    'unique_link_id': f'link_{int(time.time())}_{random.randint(1000, 9999)}',
                    'application_id': f'app_{random.randint(1000, 9999)}'
                }
                
                # Insert the record
                with self.db_connection.get_session() as session:
                    columns = ', '.join([self.db_connection.quote_identifier(col) for col in record_data.keys()])
                    insert_query = f"INSERT INTO {self.db_connection.quote_identifier(referenced_table)} ({columns}) VALUES ({', '.join([f':{col}' for col in record_data.keys()])})"
                    result = session.execute(text(insert_query), record_data)
                    session.commit()
                    
                    if hasattr(result, 'lastrowid') and result.lastrowid:
                        logger.info(f"Created new {referenced_table} record with ID {result.lastrowid}")
                        return result.lastrowid
            
            # Generic fallback for other tables - return a high ID hoping it works
            return random.randint(10000, 99999)
            
        except Exception as e:
            logger.error(f"Failed to create referenced record in {referenced_table}: {e}")
            return None
    
    def _create_value_pools_for_table(self, table: TableInfo):
        """Create value pools for table columns."""
        for column in table.columns:
            if column.data_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.SMALLINT]:
                pool_name = f"{table.name}_{column.name}_int"
                self.value_generator.create_value_pool(pool_name, column.data_type, 100000)
            
            elif column.data_type in [ColumnType.FLOAT, ColumnType.DOUBLE]:
                pool_name = f"{table.name}_{column.name}_float"
                self.value_generator.create_value_pool(pool_name, column.data_type, 100000)
    
    def generate_ultra_fast_batch(self, table_name: str, batch_size: int, 
                                 offset: int = 0) -> List[Dict[str, Any]]:
        """Generate batch using ultra-fast methods."""
        if table_name not in self.table_strategies:
            logger.error(f"No strategy found for table {table_name}")
            return []
        
        strategy = self.table_strategies[table_name]
        table = self.schema.get_table(table_name)
        
        # Pre-allocate result array
        batch_data = []
        
        # Generate using vectorized operations where possible
        column_data = {}
        
        for column in table.columns:
            # Skip auto-increment columns - let the database handle them
            if hasattr(column, 'is_auto_increment') and column.is_auto_increment:
                continue
            col_strategy = strategy['column_strategies'][column.name]
            column_data[column.name] = self._generate_column_batch(
                col_strategy, batch_size, offset
            )
        
        # Combine into rows
        for i in range(batch_size):
            row = {}
            for col_name, values in column_data.items():
                if isinstance(values, np.ndarray):
                    row[col_name] = values[i].item()
                elif isinstance(values, list):
                    row[col_name] = values[i]
                else:
                    row[col_name] = values
            batch_data.append(row)
        
        return batch_data
    
    def _generate_column_batch(self, strategy: Dict[str, Any], 
                              batch_size: int, offset: int) -> Union[np.ndarray, List[Any]]:
        """Generate batch of values for a column with proper constraint handling."""
        method = strategy['method']
        
        # Handle boolean columns properly (return 0/1 instead of large integers)
        if method == 'boolean':
            return np.random.randint(0, 2, size=batch_size)  # More compatible way to generate 0/1
        
        # Handle datetime/timestamp columns
        elif method == 'datetime':
            from datetime import datetime, timedelta
            import random
            base_date = datetime.now() - timedelta(days=365)  # Start from 1 year ago
            return [(base_date + timedelta(days=random.randint(0, 365), 
                                         hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59))).strftime('%Y-%m-%d %H:%M:%S')
                   for i in range(batch_size)]
        
        # Handle foreign key columns
        elif method == 'foreign_key':
            return self._generate_foreign_key_batch(strategy, batch_size)
        
        # Handle integer columns
        elif method == 'numpy_int':
            min_val = strategy.get('min_val', 1)
            max_val = strategy.get('max_val', 1000000)
            
            # Ensure min_val < max_val to avoid "low >= high" error
            if min_val >= max_val:
                max_val = min_val + 1000
            
            return np.random.randint(min_val, max_val, size=batch_size, dtype=np.int64)
        
        # Handle float columns
        elif method == 'numpy_float':
            min_val = strategy.get('min_val', 0.0)
            max_val = strategy.get('max_val', 1000000.0)
            
            # Ensure min_val < max_val
            if min_val >= max_val:
                max_val = min_val + 1000.0
                
            return np.random.uniform(min_val, max_val, size=batch_size)
        
        elif method == 'numpy_bool':
            return np.random.randint(0, 2, size=batch_size)  # Use 0/1 instead of True/False
        
        # Handle string columns with improved generation
        elif method.startswith('string_'):
            pattern = method.replace('string_', '')
            max_length = strategy.get('max_length', 50)
            
            # Generate specific types instead of using generic string generator
            if pattern == 'email':
                domains = ['test.org', 'example.com', 'demo.net', 'fake.io', 'mock.dev']
                return [f"user{offset + i}@{np.random.choice(domains)}" 
                       for i in range(batch_size)]
            elif pattern == 'id':
                # Add randomness to avoid collisions with existing unique values
                import random
                base_id = random.randint(100000, 999999)
                return [f"ID{base_id + i:06d}" for i in range(batch_size)]
            elif pattern == 'text':
                texts = ['Sample description', 'Lorem ipsum text', 'Generated content']
                return [f"{np.random.choice(texts)} {offset + i}" for i in range(batch_size)]
            elif pattern in ['name', 'default']:
                # Add randomness to avoid collisions for potentially unique string columns
                import random
                prefixes = ['Alpha', 'Beta', 'Gamma', 'Delta', 'Phoenix', 'Dragon', 'Eagle', 'Tiger']
                max_length = strategy.get('max_length', 50)
                results = []
                for i in range(batch_size):
                    prefix = np.random.choice(prefixes)
                    suffix = random.randint(1000, 9999)
                    generated = f"{prefix}{suffix}"
                    # Truncate to max_length if needed
                    if len(generated) > max_length:
                        generated = generated[:max_length]
                    results.append(generated)
                return results
            else:
                # Use the existing string generator for other patterns
                return self.string_generator.generate_batch(batch_size, pattern, max_length)
        
        # Handle JSON columns
        elif method == 'json_aggregator':
            # Generate simple aggregator JSON objects
            import json
            providers = ["Stripe", "Fynd", "Jio", "Razorpay", "Openapi", "Jiopay"]
            result = []
            for i in range(batch_size):
                provider = np.random.choice(providers)
                data = {provider: f"cust_{np.random.randint(100, 999)}"}
                result.append(json.dumps(data))
            return result
        
        elif method == 'json_default':
            # Generate simple JSON objects
            import json
            result = []
            for i in range(batch_size):
                data = {
                    "id": offset + i,
                    "type": "default", 
                    "active": bool(np.random.randint(0, 2))
                }
                result.append(json.dumps(data))
            return result
        
        # Handle ENUM columns
        elif method == 'enum':
            enum_values = strategy.get('enum_values', ['default'])
            return [np.random.choice(enum_values) for _ in range(batch_size)]
        
        # Handle DECIMAL columns
        elif method == 'decimal':
            from decimal import Decimal
            min_val = strategy.get('min_val', 0.0)
            max_val = strategy.get('max_val', 10000.0)
            scale = strategy.get('scale', 2)
            
            # Ensure min_val < max_val
            if min_val >= max_val:
                max_val = min_val + 1000.0
            
            result = []
            for i in range(batch_size):
                # Generate random float and convert to Decimal with proper scale
                random_float = np.random.uniform(min_val, max_val)
                # Round to the specified scale
                decimal_value = Decimal(str(round(random_float, scale)))
                result.append(decimal_value)
            return result
        
        else:
            # Improved fallback generation
            return [f"data_{offset + i}" for i in range(batch_size)]


class UltraFastInserter:
    """Ultra-fast database inserter for millions of records."""
    
    def __init__(self, db_config, performance_config):
        """Initialize ultra-fast inserter."""
        self.db_config = db_config
        self.performance_config = performance_config
        
        # Create optimized engine
        self.engine = self._create_optimized_engine()
        
        # Prepare for bulk operations
        self.prepared_statements = {}
        
        logger.info(f"🚀 Ultra-fast inserter initialized")
    
    def _create_optimized_engine(self) -> Engine:
        """Create optimized database engine."""
        connection_url = self._build_connection_url()
        
        # Optimized settings for maximum throughput
        engine_kwargs = {
            'echo': False,
            'future': True,
            'pool_pre_ping': False,  # Disable for speed
            'pool_recycle': -1,      # No recycling for speed
        }
        
        if self.db_config.driver == 'sqlite':
            engine_kwargs.update({
                'poolclass': StaticPool,
                'connect_args': {
                    'check_same_thread': False,
                    'timeout': 20,
                    'isolation_level': None  # Autocommit for speed
                }
            })
        else:
            engine_kwargs.update({
                'poolclass': QueuePool,
                'pool_size': 20,
                'max_overflow': 0,  # No overflow for predictable performance
                'pool_timeout': 1
            })
        
        return create_engine(connection_url, **engine_kwargs)
    
    def _build_connection_url(self) -> str:
        """Build optimized connection URL."""
        if self.db_config.driver == "postgresql":
            return f"postgresql+psycopg2://{self.db_config.username}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}/{self.db_config.database}"
        elif self.db_config.driver == "mysql":
            return f"mysql+pymysql://{self.db_config.username}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}/{self.db_config.database}"
        elif self.db_config.driver == "sqlite":
            return f"sqlite:///{self.db_config.database}"
        else:
            raise ValueError(f"Unsupported driver: {self.db_config.driver}")
    
    def ultra_fast_insert(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Perform ultra-fast bulk insert."""
        if not data:
            return 0
        
        # Prepare optimized insert statement
        if table_name not in self.prepared_statements:
            self._prepare_insert_statement(table_name, data[0])
        
        # Execute bulk insert with optimal method
        return self._execute_bulk_insert(table_name, data)
    
    def _prepare_insert_statement(self, table_name: str, sample_row: Dict[str, Any]):
        """Prepare optimized insert statement."""
        columns = list(sample_row.keys())
        placeholders = ', '.join([f':{col}' for col in columns])
        quoted_columns = ', '.join([self._quote_identifier(col) for col in columns])
        quoted_table = self._quote_identifier(table_name)
        
        stmt = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
        self.prepared_statements[table_name] = stmt
    
    def _execute_bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Execute bulk insert with database-specific optimizations."""
        stmt = self.prepared_statements[table_name]
        
        with self.engine.connect() as conn:
            # Configure database-specific optimizations
            if self.db_config.driver == 'sqlite':
                # SQLite-specific optimizations
                conn.execute(text("PRAGMA synchronous = OFF"))
                conn.execute(text("PRAGMA journal_mode = MEMORY"))
                conn.execute(text("PRAGMA temp_store = MEMORY"))
                conn.execute(text("PRAGMA cache_size = 100000"))
            
            elif self.db_config.driver == 'postgresql':
                # PostgreSQL-specific optimizations
                conn.execute(text("SET synchronous_commit = OFF"))
                conn.execute(text("SET wal_buffers = '16MB'"))
                conn.execute(text("SET checkpoint_segments = 32"))
            
            elif self.db_config.driver == 'mysql':
                # MySQL-specific optimizations
                # Don't set autocommit=0 since DatabaseConnection already handles isolation_level
                conn.execute(text("SET unique_checks = 0"))
                conn.execute(text("SET foreign_key_checks = 0"))
            
            # Execute bulk insert with proper transaction management
            try:
                # For AUTOCOMMIT mode, execute directly without manual transaction management
                if self.db_config.driver == "mysql":
                    # Enable autocommit for this specific operation to ensure immediate commit
                    conn.execute(text("SET autocommit = 1"))
                
                conn.execute(text(stmt), data)
                
                # Ensure the operation is flushed immediately
                if hasattr(conn, 'commit'):
                    conn.commit()
                
                return len(data)
                
            except Exception as e:
                logger.error(f"Ultra-fast bulk insert failed: {e}")
                raise e
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote identifier based on database type."""
        if self.db_config.driver == "mysql":
            return f"`{identifier}`"
        elif self.db_config.driver == "postgresql":
            return f'"{identifier}"'
        elif self.db_config.driver == "sqlite":
            return f'"{identifier}"'
        else:
            return identifier


class UltraFastProcessor:
    """Main ultra-fast processor for millions of records."""
    
    def __init__(self, schema: DatabaseSchema, config: EnhancedGenerationConfig, 
                 db_connection: DatabaseConnection):
        """Initialize ultra-fast processor."""
        self.schema = schema
        self.config = config
        self.db_connection = db_connection
        
        # Initialize components
        self.generator = UltraFastDataGenerator(schema, config, db_connection)
        self.inserter = UltraFastInserter(db_connection.config, config.performance)
        
        # Initialize fast data reuser if enabled
        self.fast_data_reuser = None
        if (config.duplicates.enable_fast_data_reuse or 
            config.duplicates.global_duplicate_strategy == DuplicateStrategy.FAST_DATA_REUSE):
            
            data_reuse_config = DataReuse(
                enable_data_reuse=True,
                sample_size=config.duplicates.data_reuse_sample_size,
                reuse_probability=config.duplicates.data_reuse_probability,
                constraint_respect=config.duplicates.respect_constraints,
                fast_mode=config.duplicates.fast_insertion_mode,
                progress_interval=config.duplicates.progress_update_interval
            )
            
            self.fast_data_reuser = FastDataReuser(db_connection, schema, data_reuse_config)
            logger.info("🔄 Fast data reuser enabled for ultra-fast insertion")
        
        # Performance tracking
        self.start_time = None
        self.report = PerformanceReport()
        
        logger.info(f"⚡ Ultra-fast processor ready for maximum performance")
    
    def process_millions_of_records(self, table_name: str, total_rows: int,
                                   progress_callback: Optional[callable] = None) -> PerformanceReport:
        """Process millions of records with maximum efficiency."""
        self.start_time = time.time()
        
        logger.info(f"🎯 Processing {total_rows:,} records for table '{table_name}'")
        logger.info(f"⚙️  Performance mode: {self.config.performance.performance_mode}")
        
        # Check if fast data reuse is enabled and applicable
        if (self.fast_data_reuser and 
            (self.config.duplicates.enable_fast_data_reuse or 
             self.config.duplicates.global_duplicate_strategy == DuplicateStrategy.FAST_DATA_REUSE)):
            logger.info("🔄 Using fast data reuse for ultra-fast insertion")
            return self._process_with_fast_data_reuse(table_name, total_rows, progress_callback)
        
        # Choose optimal strategy based on scale
        if total_rows >= 1000000:  # 1M+ records
            return self._process_ultra_scale(table_name, total_rows, progress_callback)
        elif total_rows >= 100000:  # 100K+ records
            return self._process_large_scale(table_name, total_rows, progress_callback)
        else:
            return self._process_standard_scale(table_name, total_rows, progress_callback)
    
    def _process_ultra_scale(self, table_name: str, total_rows: int,
                            progress_callback: Optional[callable] = None) -> PerformanceReport:
        """Process ultra-scale (1M+) records."""
        logger.info(f"🚀 Ultra-scale processing: {total_rows:,} records")
        
        batch_size = min(100000, self.config.performance.batch_size)
        total_inserted = 0
        batch_number = 1
        
        # Memory monitoring
        initial_memory = psutil.virtual_memory().percent
        
        with tqdm(total=total_rows, desc=f"Ultra-processing {table_name}", 
                 unit="rows", unit_scale=True) as pbar:
            
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                current_batch_size = batch_end - batch_start
                
                # Generate batch
                batch_gen_start = time.time()
                batch_data = self.generator.generate_ultra_fast_batch(
                    table_name, current_batch_size, batch_start
                )
                batch_gen_time = time.time() - batch_gen_start
                
                # Insert batch
                batch_insert_start = time.time()
                inserted = self.inserter.ultra_fast_insert(table_name, batch_data)
                batch_insert_time = time.time() - batch_insert_start
                
                total_inserted += inserted
                
                # Update progress
                pbar.update(inserted)
                if progress_callback:
                    progress_callback(table_name, total_inserted, total_rows)
                
                # Performance metrics
                gen_rate = len(batch_data) / batch_gen_time if batch_gen_time > 0 else 0
                insert_rate = inserted / batch_insert_time if batch_insert_time > 0 else 0
                
                logger.info(f"📊 Batch {batch_number}: Gen={gen_rate:,.0f} rows/s, Insert={insert_rate:,.0f} rows/s")
                
                batch_number += 1
                
                # Memory management
                if batch_number % 10 == 0:
                    current_memory = psutil.virtual_memory().percent
                    if current_memory > initial_memory + 20:
                        logger.warning(f"Memory usage increased: {current_memory:.1f}%")
                        gc.collect()
        
        # Final report
        total_time = time.time() - self.start_time
        overall_rate = total_inserted / total_time if total_time > 0 else 0
        
        self.report.total_rows_generated = total_inserted
        self.report.total_time_seconds = total_time
        self.report.average_rows_per_second = overall_rate
        
        logger.info(f"🎉 Ultra-scale completed: {total_inserted:,} rows in {total_time:.2f}s ({overall_rate:,.0f} rows/s)")
        
        return self.report
    
    def _process_large_scale(self, table_name: str, total_rows: int,
                           progress_callback: Optional[callable] = None) -> PerformanceReport:
        """Process large-scale (100K+) records."""
        logger.info(f"📈 Large-scale processing: {total_rows:,} records")
        
        # Use parallel processing for large scale
        num_workers = min(8, self.config.performance.max_workers)
        rows_per_worker = total_rows // num_workers
        
        tasks = []
        for i in range(num_workers):
            start_row = i * rows_per_worker
            end_row = total_rows if i == num_workers - 1 else (i + 1) * rows_per_worker
            
            task = UltraFastTask(
                task_id=f"worker_{i+1}",
                table_name=table_name,
                row_start=start_row,
                row_end=end_row,
                batch_size=self.config.performance.batch_size // 2,
                worker_type='thread',
                config_snapshot=self.config.dict(),
                seed_offset=i * 1000
            )
            tasks.append(task)
        
        total_inserted = 0
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_task = {
                executor.submit(self._process_worker_task, task): task
                for task in tasks
            }
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    total_inserted += result['rows_inserted']
                    
                    if progress_callback:
                        progress_callback(table_name, total_inserted, total_rows)
                    
                    logger.info(f"✅ {task.task_id}: {result['rows_inserted']:,} rows completed")
                except Exception as e:
                    logger.error(f"❌ {task.task_id} failed: {e}")
        
        total_time = time.time() - self.start_time
        overall_rate = total_inserted / total_time if total_time > 0 else 0
        
        self.report.total_rows_generated = total_inserted
        self.report.total_time_seconds = total_time
        self.report.average_rows_per_second = overall_rate
        self.report.threads_used = num_workers
        
        logger.info(f"🎉 Large-scale completed: {total_inserted:,} rows in {total_time:.2f}s ({overall_rate:,.0f} rows/s)")
        
        return self.report
    
    def _process_standard_scale(self, table_name: str, total_rows: int,
                              progress_callback: Optional[callable] = None) -> PerformanceReport:
        """Process standard-scale (<100K) records."""
        logger.info(f"📊 Standard-scale processing: {total_rows:,} records")
        
        batch_size = self.config.performance.batch_size
        total_inserted = 0
        
        for batch_start in range(0, total_rows, batch_size):
            current_batch_size = min(batch_size, total_rows - batch_start)
            
            batch_data = self.generator.generate_ultra_fast_batch(
                table_name, current_batch_size, batch_start
            )
            
            inserted = self.inserter.ultra_fast_insert(table_name, batch_data)
            total_inserted += inserted
            
            if progress_callback:
                progress_callback(table_name, total_inserted, total_rows)
        
        total_time = time.time() - self.start_time
        overall_rate = total_inserted / total_time if total_time > 0 else 0
        
        self.report.total_rows_generated = total_inserted
        self.report.total_time_seconds = total_time
        self.report.average_rows_per_second = overall_rate
        
        logger.info(f"🎉 Standard-scale completed: {total_inserted:,} rows in {total_time:.2f}s ({overall_rate:,.0f} rows/s)")
        
        return self.report
    
    def _process_with_fast_data_reuse(self, table_name: str, total_rows: int,
                                    progress_callback: Optional[callable] = None) -> PerformanceReport:
        """Process using fast data reuse for ultra-fast insertion."""
        logger.info(f"🔄 Fast data reuse processing: {total_rows:,} records")
        
        try:
            # Enhanced progress callback that includes performance tracking
            def enhanced_progress_callback(table, current, total):
                if progress_callback:
                    progress_callback(table, current, total)
                
                # Update performance metrics
                elapsed = time.time() - self.start_time
                rate = current / elapsed if elapsed > 0 else 0
                
                # Log progress every 10K rows for very frequent updates
                if current % 10000 == 0:
                    logger.info(f"🚀 Fast reuse progress: {current:,}/{total:,} ({rate:,.0f} rows/s)")
            
            # Use fast data reuser for ultra-fast insertion
            result = self.fast_data_reuser.fast_insert_millions(
                table_name, total_rows, enhanced_progress_callback
            )
            
            # Convert to PerformanceReport
            total_time = result['time_seconds']
            self.report.total_rows_generated = result['rows_inserted']
            self.report.total_time_seconds = total_time
            self.report.average_rows_per_second = result['average_rate']
            
            # Get reuse statistics
            reuse_stats = self.fast_data_reuser.get_reuse_statistics(table_name)
            
            logger.info(f"🎉 Fast data reuse completed!")
            logger.info(f"📊 Final stats: {result['rows_inserted']:,} rows in {total_time:.2f}s ({result['average_rate']:,.0f} rows/s)")
            logger.info(f"🔄 Reuse efficiency: {reuse_stats.get('reuse_ratio', 0):.1%}")
            logger.info(f"📝 Method: {result['method']}")
            
            return self.report
            
        except Exception as e:
            logger.error(f"❌ Fast data reuse failed: {e}")
            # Fallback to standard processing
            logger.info("🔄 Falling back to standard ultra-scale processing")
            return self._process_ultra_scale(table_name, total_rows, progress_callback)
    
    def _process_worker_task(self, task: UltraFastTask) -> Dict[str, Any]:
        """Process a worker task."""
        rows_to_process = task.row_end - task.row_start
        total_inserted = 0
        
        for batch_start in range(task.row_start, task.row_end, task.batch_size):
            batch_end = min(batch_start + task.batch_size, task.row_end)
            batch_size = batch_end - batch_start
            
            batch_data = self.generator.generate_ultra_fast_batch(
                task.table_name, batch_size, batch_start
            )
            
            inserted = self.inserter.ultra_fast_insert(task.table_name, batch_data)
            total_inserted += inserted
        
        return {
            'rows_inserted': total_inserted,
            'task_id': task.task_id
        }


def create_ultra_fast_processor(schema: DatabaseSchema, config: EnhancedGenerationConfig,
                               db_connection: DatabaseConnection) -> UltraFastProcessor:
    """Factory function to create ultra-fast processor."""
    return UltraFastProcessor(schema, config, db_connection)
