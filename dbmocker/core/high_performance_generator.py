"""
Ultra-High Performance Data Generator
Optimized for generating millions of records with multi-threading, connection pooling,
bulk operations, streaming, and intelligent caching.
"""

import logging
import multiprocessing as mp
import time
import gc
import psutil
import threading
import queue
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple, Callable, Iterator, Union
from dataclasses import dataclass, field
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
import random
import pickle
import json
from tqdm import tqdm
import os

from .models import (
    DatabaseSchema, TableInfo, GenerationConfig, GenerationStats, 
    ColumnGenerationConfig, TableGenerationConfig, ColumnType
)
from .database import DatabaseConnection, DatabaseConfig
from .generator import DataGenerator


logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance tracking metrics."""
    total_rows_generated: int = 0
    total_time_seconds: float = 0.0
    rows_per_second: float = 0.0
    peak_memory_mb: float = 0.0
    threads_used: int = 0
    processes_used: int = 0
    batches_processed: int = 0
    cache_hit_rate: float = 0.0
    generation_errors: List[str] = field(default_factory=list)


@dataclass
class BulkGenerationTask:
    """Represents a bulk data generation task."""
    table_name: str
    start_row: int
    end_row: int
    batch_size: int
    worker_id: str
    seed: Optional[int] = None
    config_override: Optional[Dict[str, Any]] = None


class ConnectionPool:
    """High-performance database connection pool."""
    
    def __init__(self, db_config: DatabaseConfig, pool_size: int = 10, max_overflow: int = 5):
        """Initialize connection pool."""
        self.db_config = db_config
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self._engine = None
        self._create_engine()
    
    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling."""
        connection_url = self._build_connection_url()
        
        self._engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
            future=True,
            connect_args=self._get_connect_args()
        )
        
        logger.info(f"Created connection pool: size={self.pool_size}, max_overflow={self.max_overflow}")
    
    def _build_connection_url(self) -> str:
        """Build connection URL."""
        if self.db_config.driver == "postgresql":
            driver_name = "postgresql+psycopg2"
        elif self.db_config.driver == "mysql":
            driver_name = "mysql+pymysql"
        elif self.db_config.driver == "sqlite":
            return f"sqlite:///{self.db_config.database}"
        else:
            raise ValueError(f"Unsupported driver: {self.db_config.driver}")
        
        base_url = f"{driver_name}://{self.db_config.username}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}"
        
        if self.db_config.database:
            return f"{base_url}/{self.db_config.database}"
        else:
            return base_url
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """Get driver-specific connection arguments."""
        args = {}
        
        if self.db_config.driver == "mysql":
            args["charset"] = self.db_config.charset
            if self.db_config.ssl_mode:
                args["ssl_mode"] = self.db_config.ssl_mode
        elif self.db_config.driver == "postgresql":
            if self.db_config.ssl_mode:
                args["sslmode"] = self.db_config.ssl_mode
        
        return args
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        conn = self._engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_bulk_insert(self, query: str, data: List[Dict[str, Any]]) -> int:
        """Execute bulk insert with connection from pool."""
        with self.get_connection() as conn:
            trans = None
            try:
                # Only begin a new transaction if one isn't already active
                if not conn.in_transaction():
                    trans = conn.begin()
                
                result = conn.execute(text(query), data)
                
                # Only commit if we started the transaction
                if trans is not None:
                    trans.commit()
                
                return len(data)
            except Exception as e:
                # Only rollback if we started the transaction
                if trans is not None:
                    trans.rollback()
                raise e
    
    def close(self):
        """Close all connections in pool."""
        if self._engine:
            self._engine.dispose()


class IntelligentCache:
    """Intelligent caching system for generated data patterns."""
    
    def __init__(self, max_cache_size_mb: int = 500):
        """Initialize cache with memory limit."""
        self.max_cache_size_bytes = max_cache_size_mb * 1024 * 1024
        self.current_cache_size = 0
        self.cache_data = {}
        self.access_counts = {}
        self.last_access = {}
        self.cache_hits = 0
        self.cache_misses = 0
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            if key in self.cache_data:
                self.cache_hits += 1
                self.access_counts[key] = self.access_counts.get(key, 0) + 1
                self.last_access[key] = time.time()
                return self.cache_data[key]
            else:
                self.cache_misses += 1
                return None
    
    def put(self, key: str, value: Any):
        """Put value in cache with automatic eviction."""
        with self._lock:
            # Estimate memory usage
            try:
                value_size = len(pickle.dumps(value))
            except:
                value_size = 1024  # Default estimate
            
            # Check if we need to evict
            while (self.current_cache_size + value_size > self.max_cache_size_bytes and 
                   len(self.cache_data) > 0):
                self._evict_least_recently_used()
            
            # Add to cache
            if key not in self.cache_data:
                self.current_cache_size += value_size
            
            self.cache_data[key] = value
            self.access_counts[key] = self.access_counts.get(key, 0) + 1
            self.last_access[key] = time.time()
    
    def _evict_least_recently_used(self):
        """Evict least recently used item."""
        if not self.last_access:
            return
        
        # Find least recently used key
        lru_key = min(self.last_access.keys(), key=lambda k: self.last_access[k])
        
        # Remove from cache
        if lru_key in self.cache_data:
            try:
                value_size = len(pickle.dumps(self.cache_data[lru_key]))
                self.current_cache_size -= value_size
            except:
                pass
            
            del self.cache_data[lru_key]
            del self.access_counts[lru_key]
            del self.last_access[lru_key]
    
    def get_hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0
    
    def clear(self):
        """Clear all cache data."""
        with self._lock:
            self.cache_data.clear()
            self.access_counts.clear()
            self.last_access.clear()
            self.current_cache_size = 0


class StreamingDataGenerator:
    """Memory-efficient streaming data generator for very large datasets."""
    
    def __init__(self, schema: DatabaseSchema, config: GenerationConfig, 
                 connection_pool: ConnectionPool, cache: IntelligentCache):
        """Initialize streaming generator."""
        self.schema = schema
        self.config = config
        self.connection_pool = connection_pool
        self.cache = cache
        self.base_generator = DataGenerator(schema, config, None)
    
    def generate_streaming_data(self, table_name: str, total_rows: int, 
                              chunk_size: int = 50000) -> Iterator[List[Dict[str, Any]]]:
        """Generate data in chunks for memory efficiency."""
        table = self.schema.get_table(table_name)
        if not table:
            logger.error(f"Table {table_name} not found")
            return
        
        logger.info(f"🌊 Starting streaming generation: {total_rows:,} rows in chunks of {chunk_size:,}")
        
        rows_generated = 0
        chunk_number = 1
        
        while rows_generated < total_rows:
            current_chunk_size = min(chunk_size, total_rows - rows_generated)
            
            # Check memory usage and adjust chunk size if needed
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:
                current_chunk_size = max(1000, current_chunk_size // 2)
                logger.warning(f"High memory usage ({memory_percent:.1f}%), reducing chunk size to {current_chunk_size:,}")
            
            # Generate chunk
            chunk_start_time = time.time()
            chunk_data = self._generate_chunk_optimized(table, current_chunk_size, rows_generated)
            chunk_time = time.time() - chunk_start_time
            
            rows_per_sec = len(chunk_data) / chunk_time if chunk_time > 0 else 0
            
            logger.info(f"📦 Chunk {chunk_number}: Generated {len(chunk_data):,} rows in {chunk_time:.2f}s ({rows_per_sec:,.0f} rows/sec)")
            
            yield chunk_data
            
            rows_generated += len(chunk_data)
            chunk_number += 1
            
            # Force garbage collection after each chunk
            gc.collect()
    
    def _generate_chunk_optimized(self, table: TableInfo, chunk_size: int, offset: int) -> List[Dict[str, Any]]:
        """Generate optimized chunk using caching and bulk operations."""
        cache_key = f"table_template_{table.name}"
        template = self.cache.get(cache_key)
        
        if template is None:
            # Create template for efficient generation
            template = self._create_generation_template(table)
            self.cache.put(cache_key, template)
        
        # Generate chunk using template
        chunk_data = []
        for i in range(chunk_size):
            row = self._generate_row_from_template(table, template, offset + i)
            chunk_data.append(row)
        
        return chunk_data
    
    def _create_generation_template(self, table: TableInfo) -> Dict[str, Any]:
        """Create optimized generation template for table."""
        template = {
            'table_name': table.name,
            'columns': {},
            'primary_keys': table.get_primary_key_columns(),
            'foreign_keys': [fk.columns for fk in table.foreign_keys],
            'constraints': {}
        }
        
        table_config = self.config.table_configs.get(table.name, TableGenerationConfig())
        
        for column in table.columns:
            col_config = table_config.column_configs.get(column.name, ColumnGenerationConfig())
            
            template['columns'][column.name] = {
                'data_type': column.data_type,
                'config': col_config,
                'constraints': {
                    'nullable': column.is_nullable,
                    'max_length': column.max_length,
                    'precision': column.precision,
                    'scale': column.scale,
                    'enum_values': column.enum_values
                }
            }
        
        return template
    
    def _generate_row_from_template(self, table: TableInfo, template: Dict[str, Any], row_index: int) -> Dict[str, Any]:
        """Generate single row using pre-computed template."""
        row = {}
        table_config = self.config.table_configs.get(table.name, TableGenerationConfig())
        
        for column_name, column_template in template['columns'].items():
            column = table.get_column(column_name)
            if column:
                # Use cached generation logic where possible
                cache_key = f"column_gen_{table.name}_{column_name}_{row_index % 1000}"
                cached_value = self.cache.get(cache_key)
                
                if cached_value is not None and self._can_use_cached_value(column):
                    row[column_name] = cached_value
                else:
                    # Generate new value
                    value = self.base_generator._generate_column_value(column, table_config, table)
                    row[column_name] = value
                    
                    # Cache simple values
                    if self._should_cache_value(column, value):
                        self.cache.put(cache_key, value)
        
        return row
    
    def _can_use_cached_value(self, column) -> bool:
        """Check if cached value can be reused for column."""
        # Don't cache primary keys or unique columns
        return not (column.name in ['id'] or 'unique' in str(column.name).lower())
    
    def _should_cache_value(self, column, value) -> bool:
        """Check if value should be cached."""
        # Cache small, reusable values
        if value is None:
            return False
        
        if isinstance(value, str) and len(value) > 100:
            return False
        
        return True


class HighPerformanceGenerator:
    """Ultra-high performance data generator optimized for millions of records."""
    
    def __init__(self, schema: DatabaseSchema, config: GenerationConfig, db_connection: DatabaseConnection):
        """Initialize high-performance generator."""
        self.schema = schema
        self.config = config
        self.db_connection = db_connection
        
        # Performance components
        self.connection_pool = ConnectionPool(db_connection.config, pool_size=20, max_overflow=10)
        self.cache = IntelligentCache(max_cache_size_mb=1000)  # 1GB cache
        self.streaming_generator = StreamingDataGenerator(schema, config, self.connection_pool, self.cache)
        
        # Performance tracking
        self.metrics = PerformanceMetrics()
        self.start_time = None
        
        # Adaptive configuration
        self._calculate_optimal_settings()
        
        logger.info(f"🚀 High-performance generator initialized")
        logger.info(f"💾 System: {psutil.cpu_count()} CPUs, {psutil.virtual_memory().total / (1024**3):.1f}GB RAM")
        logger.info(f"⚙️  Optimal batch size: {self.optimal_batch_size:,}")
        logger.info(f"🧵 Optimal threads: {self.optimal_threads}")
    
    def _calculate_optimal_settings(self):
        """Calculate optimal settings based on system resources."""
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Adaptive batch sizing
        if memory_gb >= 32:
            self.optimal_batch_size = 100000
            self.optimal_threads = min(cpu_count, 16)
            self.chunk_size_for_streaming = 100000
        elif memory_gb >= 16:
            self.optimal_batch_size = 50000
            self.optimal_threads = min(cpu_count, 12)
            self.chunk_size_for_streaming = 50000
        elif memory_gb >= 8:
            self.optimal_batch_size = 25000
            self.optimal_threads = min(cpu_count, 8)
            self.chunk_size_for_streaming = 25000
        else:
            self.optimal_batch_size = 10000
            self.optimal_threads = min(cpu_count, 4)
            self.chunk_size_for_streaming = 10000
        
        # Override with user settings if provided
        if self.config.batch_size:
            self.optimal_batch_size = self.config.batch_size
        if self.config.max_workers:
            self.optimal_threads = min(self.config.max_workers, self.optimal_threads)
    
    def generate_millions_of_records(self, table_name: str, total_rows: int,
                                   progress_callback: Optional[Callable] = None,
                                   use_streaming: bool = True) -> GenerationStats:
        """Generate millions of records with optimal performance."""
        self.start_time = time.time()
        self.metrics = PerformanceMetrics()
        
        logger.info(f"🎯 Target: Generate {total_rows:,} records for table '{table_name}'")
        
        # Choose strategy based on size and memory
        estimated_memory_mb = self._estimate_memory_usage(table_name, total_rows)
        available_memory_mb = psutil.virtual_memory().available / (1024**2)
        
        if use_streaming or estimated_memory_mb > available_memory_mb * 0.6:
            logger.info(f"📊 Using streaming strategy (estimated: {estimated_memory_mb:.0f}MB, available: {available_memory_mb:.0f}MB)")
            return self._generate_with_streaming_bulk_insert(table_name, total_rows, progress_callback)
        else:
            logger.info(f"🔄 Using parallel strategy")
            return self._generate_with_parallel_bulk_insert(table_name, total_rows, progress_callback)
    
    def _estimate_memory_usage(self, table_name: str, rows: int) -> float:
        """Estimate memory usage in MB."""
        table = self.schema.get_table(table_name)
        if not table:
            return 1000  # Default estimate
        
        # Estimate 150 bytes per column per row (includes Python overhead)
        bytes_per_row = len(table.columns) * 150
        total_bytes = rows * bytes_per_row
        return total_bytes / (1024**2)
    
    def _generate_with_streaming_bulk_insert(self, table_name: str, total_rows: int,
                                           progress_callback: Optional[Callable] = None) -> GenerationStats:
        """Generate using streaming with bulk inserts."""
        stats = GenerationStats()
        stats.table_stats[table_name] = {
            'rows_requested': total_rows,
            'rows_inserted': 0,
            'time_seconds': 0,
            'errors': []
        }
        
        total_inserted = 0
        chunk_number = 1
        
        # Create progress bar
        with tqdm(total=total_rows, desc=f"Generating {table_name}", unit="rows", unit_scale=True) as pbar:
            
            # Generate and insert in streaming chunks
            for chunk_data in self.streaming_generator.generate_streaming_data(
                table_name, total_rows, self.chunk_size_for_streaming
            ):
                if not chunk_data:
                    continue
                
                # Bulk insert chunk
                try:
                    chunk_start = time.time()
                    inserted = self._bulk_insert_chunk(table_name, chunk_data)
                    chunk_time = time.time() - chunk_start
                    
                    total_inserted += inserted
                    self.metrics.batches_processed += 1
                    
                    # Update progress
                    pbar.update(inserted)
                    if progress_callback:
                        progress_callback(table_name, total_inserted, total_rows)
                    
                    # Calculate performance metrics
                    rows_per_sec = inserted / chunk_time if chunk_time > 0 else 0
                    logger.info(f"📈 Chunk {chunk_number}: {inserted:,} rows inserted in {chunk_time:.2f}s ({rows_per_sec:,.0f} rows/sec)")
                    
                    chunk_number += 1
                    
                except Exception as e:
                    error_msg = f"Chunk {chunk_number} insertion failed: {e}"
                    logger.error(error_msg)
                    stats.errors.append(error_msg)
                    self.metrics.generation_errors.append(error_msg)
        
        # Update final statistics
        total_time = time.time() - self.start_time
        stats.total_time_seconds = total_time
        stats.total_rows_generated = total_inserted
        stats.table_stats[table_name]['rows_inserted'] = total_inserted
        stats.table_stats[table_name]['time_seconds'] = total_time
        
        self.metrics.total_rows_generated = total_inserted
        self.metrics.total_time_seconds = total_time
        self.metrics.rows_per_second = total_inserted / total_time if total_time > 0 else 0
        self.metrics.cache_hit_rate = self.cache.get_hit_rate()
        
        logger.info(f"🎉 Streaming generation completed!")
        logger.info(f"📊 Final stats: {total_inserted:,} rows in {total_time:.2f}s ({self.metrics.rows_per_second:,.0f} rows/sec)")
        logger.info(f"🎯 Cache hit rate: {self.metrics.cache_hit_rate:.1%}")
        
        return stats
    
    def _generate_with_parallel_bulk_insert(self, table_name: str, total_rows: int,
                                          progress_callback: Optional[Callable] = None) -> GenerationStats:
        """Generate using parallel threads with bulk inserts."""
        stats = GenerationStats()
        stats.table_stats[table_name] = {
            'rows_requested': total_rows,
            'rows_inserted': 0,
            'time_seconds': 0,
            'errors': []
        }
        
        # Create generation tasks
        tasks = self._create_parallel_tasks(table_name, total_rows)
        total_inserted = 0
        
        logger.info(f"🧵 Using {self.optimal_threads} threads for parallel generation")
        
        with tqdm(total=total_rows, desc=f"Generating {table_name}", unit="rows", unit_scale=True) as pbar:
            with ThreadPoolExecutor(max_workers=self.optimal_threads) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(self._process_bulk_generation_task, task): task
                    for task in tasks
                }
                
                # Process completed tasks
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        task_result = future.result()
                        inserted = task_result['rows_inserted']
                        total_inserted += inserted
                        
                        pbar.update(inserted)
                        if progress_callback:
                            progress_callback(table_name, total_inserted, total_rows)
                        
                        logger.info(f"✅ Task {task.worker_id}: {inserted:,} rows completed")
                        
                    except Exception as e:
                        error_msg = f"Task {task.worker_id} failed: {e}"
                        logger.error(error_msg)
                        stats.errors.append(error_msg)
                        self.metrics.generation_errors.append(error_msg)
        
        # Update final statistics
        total_time = time.time() - self.start_time
        stats.total_time_seconds = total_time
        stats.total_rows_generated = total_inserted
        stats.table_stats[table_name]['rows_inserted'] = total_inserted
        stats.table_stats[table_name]['time_seconds'] = total_time
        
        self.metrics.total_rows_generated = total_inserted
        self.metrics.total_time_seconds = total_time
        self.metrics.rows_per_second = total_inserted / total_time if total_time > 0 else 0
        self.metrics.threads_used = self.optimal_threads
        self.metrics.cache_hit_rate = self.cache.get_hit_rate()
        
        logger.info(f"🎉 Parallel generation completed!")
        logger.info(f"📊 Final stats: {total_inserted:,} rows in {total_time:.2f}s ({self.metrics.rows_per_second:,.0f} rows/sec)")
        
        return stats
    
    def _create_parallel_tasks(self, table_name: str, total_rows: int) -> List[BulkGenerationTask]:
        """Create parallel generation tasks."""
        tasks = []
        rows_per_task = max(1000, total_rows // self.optimal_threads)
        
        for i in range(self.optimal_threads):
            start_row = i * rows_per_task
            if i == self.optimal_threads - 1:
                end_row = total_rows  # Last task gets remaining rows
            else:
                end_row = start_row + rows_per_task
            
            if start_row >= total_rows:
                break
            
            task = BulkGenerationTask(
                table_name=table_name,
                start_row=start_row,
                end_row=end_row,
                batch_size=self.optimal_batch_size,
                worker_id=f"worker_{i+1}",
                seed=self.config.seed + i if self.config.seed else None
            )
            tasks.append(task)
        
        return tasks
    
    def _process_bulk_generation_task(self, task: BulkGenerationTask) -> Dict[str, Any]:
        """Process a bulk generation task."""
        thread_id = threading.current_thread().ident
        start_time = time.time()
        rows_to_generate = task.end_row - task.start_row
        
        logger.info(f"🧵 {task.worker_id} (TID: {thread_id}): Starting {rows_to_generate:,} rows")
        
        # Create thread-local generator
        thread_config = GenerationConfig(**self.config.dict())
        if task.seed:
            thread_config.seed = task.seed
        
        generator = DataGenerator(self.schema, thread_config, None)
        
        # Generate data in batches
        total_inserted = 0
        rows_generated = 0
        
        while rows_generated < rows_to_generate:
            batch_size = min(task.batch_size, rows_to_generate - rows_generated)
            
            # Generate batch
            batch_data = generator.generate_data_for_table(task.table_name, batch_size)
            
            # Bulk insert batch
            if batch_data:
                inserted = self._bulk_insert_chunk(task.table_name, batch_data)
                total_inserted += inserted
                rows_generated += len(batch_data)
                
                if rows_generated % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = rows_generated / elapsed if elapsed > 0 else 0
                    logger.debug(f"🧵 {task.worker_id}: Progress {rows_generated:,}/{rows_to_generate:,} ({rate:,.0f} rows/sec)")
        
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"✅ {task.worker_id}: Completed {total_inserted:,} rows in {elapsed:.2f}s ({rate:,.0f} rows/sec)")
        
        return {
            'rows_inserted': total_inserted,
            'time_seconds': elapsed,
            'worker_id': task.worker_id
        }
    
    def _bulk_insert_chunk(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Perform bulk insert for a chunk of data."""
        if not data:
            return 0
        
        table = self.schema.get_table(table_name)
        if not table:
            raise ValueError(f"Table {table_name} not found")
        
        # Build bulk insert query
        column_names = list(data[0].keys())
        placeholders = ', '.join([f':{col}' for col in column_names])
        quoted_columns = ', '.join([self._quote_identifier(col) for col in column_names])
        quoted_table = self._quote_identifier(table_name)
        
        query = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
        
        # Execute bulk insert using connection pool
        return self.connection_pool.execute_bulk_insert(query, data)
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote identifier based on database type."""
        if self.db_connection.config.driver == "mysql":
            return f"`{identifier}`"
        elif self.db_connection.config.driver == "postgresql":
            return f'"{identifier}"'
        elif self.db_connection.config.driver == "sqlite":
            return f'"{identifier}"'
        else:
            return identifier
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get detailed performance report."""
        memory_info = psutil.virtual_memory()
        
        return {
            'generation_metrics': {
                'total_rows_generated': self.metrics.total_rows_generated,
                'total_time_seconds': self.metrics.total_time_seconds,
                'rows_per_second': self.metrics.rows_per_second,
                'batches_processed': self.metrics.batches_processed,
                'threads_used': self.metrics.threads_used,
                'processes_used': self.metrics.processes_used,
                'generation_errors': len(self.metrics.generation_errors)
            },
            'cache_metrics': {
                'hit_rate': self.cache.get_hit_rate(),
                'cache_size_mb': self.cache.current_cache_size / (1024**2),
                'cached_items': len(self.cache.cache_data)
            },
            'system_metrics': {
                'peak_memory_usage_percent': memory_info.percent,
                'available_memory_mb': memory_info.available / (1024**2),
                'cpu_count': psutil.cpu_count(),
                'optimal_batch_size': self.optimal_batch_size,
                'optimal_threads': self.optimal_threads
            },
            'connection_pool': {
                'pool_size': self.connection_pool.pool_size,
                'max_overflow': self.connection_pool.max_overflow
            }
        }
    
    def cleanup(self):
        """Cleanup resources."""
        logger.info("🧹 Cleaning up high-performance generator resources")
        
        if hasattr(self, 'connection_pool'):
            self.connection_pool.close()
        
        if hasattr(self, 'cache'):
            self.cache.clear()
        
        # Force garbage collection
        gc.collect()
        
        logger.info("✅ Cleanup completed")


def create_high_performance_generator(schema: DatabaseSchema, config: GenerationConfig, 
                                    db_connection: DatabaseConnection) -> HighPerformanceGenerator:
    """Factory function to create high-performance generator."""
    return HighPerformanceGenerator(schema, config, db_connection)
