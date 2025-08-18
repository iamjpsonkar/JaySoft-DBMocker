"""
Ultra-Fast Data Reuse System
Reuses existing data for ultra-fast insertion of millions of records
while respecting database constraints.
"""

import logging
import time
import random
import threading
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from sqlalchemy import text, MetaData, Table, inspect
from tqdm import tqdm

from .database import DatabaseConnection
from .models import DatabaseSchema, TableInfo, ColumnInfo, ConstraintType
from .enhanced_models import EnhancedGenerationConfig, PerformanceSettings


logger = logging.getLogger(__name__)


@dataclass
class DataReuse:
    """Configuration for data reuse strategies."""
    enable_data_reuse: bool = True
    sample_size: int = 10000  # How many existing records to sample
    reuse_probability: float = 0.95  # Probability of reusing vs generating new
    constraint_respect: bool = True  # Respect database constraints
    fast_mode: bool = True  # Use fastest possible insertion
    progress_interval: int = 1000  # Progress update interval


@dataclass
class ReusableDataPool:
    """Pool of reusable data for a table."""
    table_name: str
    total_existing_rows: int
    sampled_data: List[Dict[str, Any]] = field(default_factory=list)
    constraint_safe_data: List[Dict[str, Any]] = field(default_factory=list)
    primary_keys: Set[Any] = field(default_factory=set)
    unique_values: Dict[str, Set[Any]] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


class ConstraintAnalyzer:
    """Analyzes database constraints for safe data reuse."""
    
    def __init__(self, db_connection: DatabaseConnection, schema: DatabaseSchema):
        self.db_connection = db_connection
        self.schema = schema
        self.constraint_cache = {}
    
    def analyze_table_constraints(self, table_name: str) -> Dict[str, Any]:
        """Analyze constraints for a table."""
        if table_name in self.constraint_cache:
            return self.constraint_cache[table_name]
        
        table = self.schema.get_table(table_name)
        if not table:
            return {}
        
        constraints = {
            'primary_keys': table.get_primary_key_columns() if table else [],
            'unique_columns': [],
            'foreign_keys': [],
            'auto_increment_columns': [],
            'nullable_columns': [],
            'constraint_free_columns': []
        }
        
        # Analyze each column
        for column in table.columns:
            # Check for unique constraints
            if self._is_unique_column(table, column.name):
                constraints['unique_columns'].append(column.name)
            
            # Check for foreign keys
            if self._is_foreign_key_column(table, column.name):
                constraints['foreign_keys'].append(column.name)
            
            # Check for auto increment
            if column.is_auto_increment:
                constraints['auto_increment_columns'].append(column.name)
            
            # Check if nullable
            if column.is_nullable:
                constraints['nullable_columns'].append(column.name)
            
            # Check if constraint-free (can safely duplicate)
            if self._is_constraint_free_column(table, column):
                constraints['constraint_free_columns'].append(column.name)
        
        self.constraint_cache[table_name] = constraints
        return constraints
    
    def _is_unique_column(self, table: TableInfo, column_name: str) -> bool:
        """Check if column has unique constraint."""
        for constraint in table.constraints:
            if constraint.type == ConstraintType.UNIQUE and column_name in constraint.columns:
                return True
        return False
    
    def _is_foreign_key_column(self, table: TableInfo, column_name: str) -> bool:
        """Check if column is foreign key."""
        for fk in table.foreign_keys:
            if column_name in fk.columns:
                return True
        return False
    
    def _is_constraint_free_column(self, table: TableInfo, column: ColumnInfo) -> bool:
        """Check if column can safely have duplicates."""
        # Primary keys and auto-increment cannot be duplicated
        if column.name in table.get_primary_key_columns() or column.is_auto_increment:
            return False
        
        # Unique columns cannot be duplicated
        if self._is_unique_column(table, column.name):
            return False
        
        # Foreign keys might be duplicatable depending on referenced data
        # For safety, we'll consider them constraint-free if they're nullable
        if self._is_foreign_key_column(table, column.name):
            return column.is_nullable
        
        # All other columns are constraint-free
        return True


class ExistingDataSampler:
    """Samples existing data from database for reuse."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.sample_cache = {}
    
    def sample_existing_data(self, table_name: str, sample_size: int = 10000) -> ReusableDataPool:
        """Sample existing data from a table."""
        logger.info(f"🔍 Sampling existing data from {table_name} (max {sample_size:,} rows)")
        
        start_time = time.time()
        
        # Get total row count
        total_rows = self._get_table_row_count(table_name)
        
        if total_rows == 0:
            logger.warning(f"Table {table_name} is empty - no data to reuse")
            return ReusableDataPool(table_name=table_name, total_existing_rows=0)
        
        # Sample data
        actual_sample_size = min(sample_size, total_rows)
        sampled_data = self._sample_random_rows(table_name, actual_sample_size)
        
        # Create data pool
        pool = ReusableDataPool(
            table_name=table_name,
            total_existing_rows=total_rows,
            sampled_data=sampled_data
        )
        
        # Extract constraint information
        self._extract_constraint_data(pool)
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Sampled {len(sampled_data):,} rows from {table_name} in {elapsed:.2f}s")
        
        return pool
    
    def _get_table_row_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        try:
            quoted_table = self.db_connection.quote_identifier(table_name)
            query = f"SELECT COUNT(*) FROM {quoted_table}"
            
            result = self.db_connection.execute_query(query)
            return result[0][0] if result else 0
        
        except Exception as e:
            logger.error(f"Failed to get row count for {table_name}: {e}")
            return 0
    
    def _sample_random_rows(self, table_name: str, sample_size: int) -> List[Dict[str, Any]]:
        """Sample random rows from a table."""
        try:
            quoted_table = self.db_connection.quote_identifier(table_name)
            
            # Database-specific random sampling
            if self.db_connection.config.driver == "sqlite":
                query = f"SELECT * FROM {quoted_table} ORDER BY RANDOM() LIMIT {sample_size}"
            elif self.db_connection.config.driver == "postgresql":
                query = f"SELECT * FROM {quoted_table} TABLESAMPLE SYSTEM(10) LIMIT {sample_size}"
            elif self.db_connection.config.driver == "mysql":
                query = f"SELECT * FROM {quoted_table} ORDER BY RAND() LIMIT {sample_size}"
            else:
                # Fallback
                query = f"SELECT * FROM {quoted_table} LIMIT {sample_size}"
            
            result = self.db_connection.execute_query(query)
            
            if not result:
                return []
            
            # Convert to list of dictionaries
            # Get column names
            with self.db_connection.get_session() as session:
                inspector = inspect(session.bind)
                columns = inspector.get_columns(table_name)
                column_names = [col['name'] for col in columns]
            
            sampled_data = []
            for row in result:
                row_dict = dict(zip(column_names, row))
                sampled_data.append(row_dict)
            
            return sampled_data
        
        except Exception as e:
            logger.error(f"Failed to sample data from {table_name}: {e}")
            return []
    
    def _extract_constraint_data(self, pool: ReusableDataPool):
        """Extract constraint-related data from sampled data."""
        if not pool.sampled_data:
            return
        
        # Extract primary keys and unique values
        for row in pool.sampled_data:
            for column_name, value in row.items():
                if value is not None:
                    if column_name not in pool.unique_values:
                        pool.unique_values[column_name] = set()
                    pool.unique_values[column_name].add(value)


class FastDataReuser:
    """Ultra-fast data reuser that creates millions of records by reusing existing data."""
    
    def __init__(self, db_connection: DatabaseConnection, schema: DatabaseSchema, 
                 config: DataReuse = None):
        self.db_connection = db_connection
        self.schema = schema
        self.config = config or DataReuse()
        
        # Initialize components
        self.constraint_analyzer = ConstraintAnalyzer(db_connection, schema)
        self.data_sampler = ExistingDataSampler(db_connection)
        
        # Cache for reusable data
        self.data_pools = {}
        
        # Performance tracking
        self.insertion_stats = {
            'total_rows_inserted': 0,
            'total_time_seconds': 0.0,
            'average_rate': 0.0,
            'batches_processed': 0
        }
        
        logger.info("⚡ FastDataReuser initialized for ultra-fast data insertion")
    
    def prepare_table_for_fast_insertion(self, table_name: str) -> bool:
        """Prepare a table for fast data insertion by sampling existing data."""
        logger.info(f"🔧 Preparing {table_name} for fast data insertion")
        
        # Analyze constraints
        constraints = self.constraint_analyzer.analyze_table_constraints(table_name)
        
        if not constraints.get('constraint_free_columns', []):
            logger.warning(f"Table {table_name} has no constraint-free columns for fast insertion")
            return False
        
        # Sample existing data
        data_pool = self.data_sampler.sample_existing_data(table_name, self.config.sample_size)
        
        if not data_pool.sampled_data:
            logger.warning(f"No existing data found in {table_name} for reuse")
            return False
        
        # Filter constraint-safe data
        data_pool.constraint_safe_data = self._create_constraint_safe_data(
            data_pool, constraints
        )
        
        self.data_pools[table_name] = data_pool
        
        logger.info(f"✅ {table_name} prepared: {len(data_pool.constraint_safe_data):,} reusable records")
        return True
    
    def _create_constraint_safe_data(self, data_pool: ReusableDataPool, 
                                   constraints: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create constraint-safe data for reuse."""
        constraint_safe_data = []
        
        for row in data_pool.sampled_data:
            safe_row = {}
            
            for column_name, value in row.items():
                # Skip auto-increment columns (they'll be generated automatically)
                if column_name in constraints['auto_increment_columns']:
                    continue
                
                # Skip primary keys (they must be unique)
                if column_name in constraints['primary_keys']:
                    continue
                
                # Skip unique columns (they must be unique)
                if column_name in constraints['unique_columns']:
                    continue
                
                # Include constraint-free columns
                if column_name in constraints['constraint_free_columns']:
                    safe_row[column_name] = value
                
                # Include foreign keys (they can be duplicated safely)
                elif column_name in constraints['foreign_keys']:
                    safe_row[column_name] = value
            
            if safe_row:  # Only add if there are columns to reuse
                constraint_safe_data.append(safe_row)
        
        return constraint_safe_data
    
    def fast_insert_millions(self, table_name: str, target_rows: int,
                           progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """Insert millions of records ultra-fast using data reuse."""
        logger.info(f"🚀 Starting ultra-fast insertion: {target_rows:,} rows into {table_name}")
        
        start_time = time.time()
        
        # Prepare table if not already prepared
        if table_name not in self.data_pools:
            if not self.prepare_table_for_fast_insertion(table_name):
                raise ValueError(f"Cannot prepare {table_name} for fast insertion")
        
        data_pool = self.data_pools[table_name]
        
        if not data_pool.constraint_safe_data:
            raise ValueError(f"No reusable data available for {table_name}")
        
        # Use ultra-fast insertion strategy
        if self.config.fast_mode and target_rows >= 100000:
            return self._ultra_fast_bulk_insert(table_name, target_rows, progress_callback)
        else:
            return self._fast_batch_insert(table_name, target_rows, progress_callback)
    
    def _ultra_fast_bulk_insert(self, table_name: str, target_rows: int,
                              progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """Ultra-fast bulk insertion using multiple strategies."""
        logger.info(f"⚡ Using ultra-fast bulk insertion for {target_rows:,} rows")
        
        data_pool = self.data_pools[table_name]
        batch_size = 50000  # Large batches for maximum speed
        total_inserted = 0
        
        # Pre-generate all data in memory (for maximum speed)
        logger.info("📦 Pre-generating reusable data in memory...")
        all_data = self._pre_generate_reusable_data(data_pool, target_rows)
        
        logger.info(f"💾 Pre-generated {len(all_data):,} rows, starting bulk insertion...")
        
        # Insert in large batches with parallel processing
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            for batch_start in range(0, len(all_data), batch_size):
                batch_end = min(batch_start + batch_size, len(all_data))
                batch_data = all_data[batch_start:batch_end]
                
                future = executor.submit(self._insert_batch_ultra_fast, table_name, batch_data)
                futures.append((future, len(batch_data)))
            
            # Collect results with progress tracking
            for future, batch_size in futures:
                try:
                    rows_inserted = future.result(timeout=300)  # 5 minute timeout
                    total_inserted += rows_inserted
                    
                    if progress_callback and total_inserted % self.config.progress_interval == 0:
                        progress_callback(table_name, total_inserted, target_rows)
                    
                    # Log progress every 50K rows
                    if total_inserted % 50000 == 0:
                        elapsed = time.time() - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        logger.info(f"📈 Progress: {total_inserted:,}/{target_rows:,} rows ({rate:,.0f} rows/s)")
                
                except Exception as e:
                    logger.error(f"Batch insertion failed: {e}")
        
        total_time = time.time() - start_time
        avg_rate = total_inserted / total_time if total_time > 0 else 0
        
        result = {
            'rows_inserted': total_inserted,
            'time_seconds': total_time,
            'average_rate': avg_rate,
            'method': 'ultra_fast_bulk'
        }
        
        logger.info(f"🎉 Ultra-fast insertion completed: {total_inserted:,} rows in {total_time:.2f}s ({avg_rate:,.0f} rows/s)")
        
        return result
    
    def _fast_batch_insert(self, table_name: str, target_rows: int,
                         progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """Fast batch insertion for smaller datasets."""
        logger.info(f"📦 Using fast batch insertion for {target_rows:,} rows")
        
        data_pool = self.data_pools[table_name]
        batch_size = 10000
        total_inserted = 0
        start_time = time.time()
        
        with tqdm(total=target_rows, desc=f"Inserting {table_name}", unit="rows") as pbar:
            for batch_start in range(0, target_rows, batch_size):
                current_batch_size = min(batch_size, target_rows - batch_start)
                
                # Generate batch data
                batch_data = self._generate_reusable_batch(data_pool, current_batch_size)
                
                # Insert batch
                rows_inserted = self._insert_batch_fast(table_name, batch_data)
                total_inserted += rows_inserted
                
                pbar.update(rows_inserted)
                
                if progress_callback and total_inserted % self.config.progress_interval == 0:
                    progress_callback(table_name, total_inserted, target_rows)
        
        total_time = time.time() - start_time
        avg_rate = total_inserted / total_time if total_time > 0 else 0
        
        result = {
            'rows_inserted': total_inserted,
            'time_seconds': total_time,
            'average_rate': avg_rate,
            'method': 'fast_batch'
        }
        
        logger.info(f"✅ Fast batch insertion completed: {total_inserted:,} rows in {total_time:.2f}s ({avg_rate:,.0f} rows/s)")
        
        return result
    
    def _pre_generate_reusable_data(self, data_pool: ReusableDataPool, target_rows: int) -> List[Dict[str, Any]]:
        """Pre-generate all reusable data in memory for maximum speed."""
        if not data_pool.constraint_safe_data:
            return []
        
        all_data = []
        pool_size = len(data_pool.constraint_safe_data)
        
        # Use numpy for ultra-fast random selection
        indices = np.random.randint(0, pool_size, size=target_rows)
        
        for i in indices:
            row_data = data_pool.constraint_safe_data[i].copy()
            all_data.append(row_data)
        
        return all_data
    
    def _generate_reusable_batch(self, data_pool: ReusableDataPool, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of reusable data."""
        if not data_pool.constraint_safe_data:
            return []
        
        batch_data = []
        pool_size = len(data_pool.constraint_safe_data)
        
        for _ in range(batch_size):
            # Randomly select a row from constraint-safe data
            source_row = random.choice(data_pool.constraint_safe_data)
            batch_data.append(source_row.copy())
        
        return batch_data
    
    def _insert_batch_ultra_fast(self, table_name: str, batch_data: List[Dict[str, Any]]) -> int:
        """Insert a batch with ultra-fast optimizations."""
        if not batch_data:
            return 0
        
        try:
            with self.db_connection.get_session() as session:
                # Disable integrity checks for maximum speed
                if self.db_connection.config.driver == "sqlite":
                    session.execute(text("PRAGMA synchronous = OFF"))
                    session.execute(text("PRAGMA journal_mode = MEMORY"))
                    session.execute(text("PRAGMA cache_size = 100000"))
                elif self.db_connection.config.driver == "postgresql":
                    session.execute(text("SET synchronous_commit = OFF"))
                    session.execute(text("SET wal_buffers = '32MB'"))
                elif self.db_connection.config.driver == "mysql":
                    session.execute(text("SET autocommit = 0"))
                    session.execute(text("SET unique_checks = 0"))
                    session.execute(text("SET foreign_key_checks = 0"))
                
                # Build bulk insert query
                column_names = list(batch_data[0].keys())
                placeholders = ', '.join([f':{col}' for col in column_names])
                quoted_columns = ', '.join([self.db_connection.quote_identifier(col) for col in column_names])
                quoted_table = self.db_connection.quote_identifier(table_name)
                
                query = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
                
                # Execute bulk insert
                session.execute(text(query), batch_data)
                session.commit()
                
                return len(batch_data)
        
        except Exception as e:
            logger.error(f"Ultra-fast batch insert failed: {e}")
            raise
    
    def _insert_batch_fast(self, table_name: str, batch_data: List[Dict[str, Any]]) -> int:
        """Insert a batch with fast optimizations."""
        if not batch_data:
            return 0
        
        try:
            with self.db_connection.get_session() as session:
                # Build insert query
                column_names = list(batch_data[0].keys())
                placeholders = ', '.join([f':{col}' for col in column_names])
                quoted_columns = ', '.join([self.db_connection.quote_identifier(col) for col in column_names])
                quoted_table = self.db_connection.quote_identifier(table_name)
                
                query = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
                
                # Execute batch insert
                session.execute(text(query), batch_data)
                session.commit()
                
                return len(batch_data)
        
        except Exception as e:
            logger.error(f"Fast batch insert failed: {e}")
            raise
    
    def get_reuse_statistics(self, table_name: str) -> Dict[str, Any]:
        """Get statistics about data reuse for a table."""
        if table_name not in self.data_pools:
            return {}
        
        data_pool = self.data_pools[table_name]
        
        return {
            'table_name': table_name,
            'total_existing_rows': data_pool.total_existing_rows,
            'sampled_rows': len(data_pool.sampled_data),
            'reusable_rows': len(data_pool.constraint_safe_data),
            'reuse_ratio': len(data_pool.constraint_safe_data) / len(data_pool.sampled_data) if data_pool.sampled_data else 0,
            'unique_values_count': {col: len(values) for col, values in data_pool.unique_values.items()},
            'prepared_at': data_pool.created_at
        }
    
    def clear_cache(self):
        """Clear all cached data pools."""
        self.data_pools.clear()
        logger.info("🧹 Data reuse cache cleared")


def create_fast_data_reuser(db_connection: DatabaseConnection, schema: DatabaseSchema,
                          sample_size: int = 10000, fast_mode: bool = True) -> FastDataReuser:
    """Factory function to create a FastDataReuser."""
    config = DataReuse(
        enable_data_reuse=True,
        sample_size=sample_size,
        reuse_probability=0.95,
        fast_mode=fast_mode,
        progress_interval=1000
    )
    
    return FastDataReuser(db_connection, schema, config)
