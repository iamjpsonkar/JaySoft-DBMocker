"""High-performance parallel data generator with multi-threading and multi-processing support."""

import logging
import multiprocessing as mp
import time
import sys
import gc
import psutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple, Callable
import random
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from .models import DatabaseSchema, TableInfo, GenerationConfig, GenerationStats, ColumnGenerationConfig, TableGenerationConfig
from .database import DatabaseConnection
from .generator import DataGenerator
from .inserter import DataInserter

logger = logging.getLogger(__name__)


@dataclass
class GenerationTask:
    """Represents a data generation task."""
    table_name: str
    start_row: int
    end_row: int
    seed: Optional[int] = None
    task_id: str = ""


class ParallelDataGenerator:
    """High-performance data generator with multi-threading and multi-processing support."""
    
    def __init__(self, schema: DatabaseSchema, config: GenerationConfig, db_connection: DatabaseConnection):
        """Initialize parallel data generator."""
        self.schema = schema
        self.config = config
        self.db_connection = db_connection
        
        # Performance tracking
        self.generation_stats = GenerationStats()
        
        # Adaptive configuration based on system resources
        self._adaptive_config = self._calculate_adaptive_config()
        
        # Memory monitoring for large datasets
        self._memory_threshold = 0.8  # 80% memory usage threshold
        
        logger.info(f"Parallel generator initialized: {self._adaptive_config}")
    
    def _calculate_adaptive_config(self) -> Dict[str, Any]:
        """Calculate optimal configuration based on system resources."""
        cpu_count = mp.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Adaptive batch sizing based on memory
        if memory_gb >= 16:
            optimal_batch_size = 50000
            optimal_processes = min(cpu_count - 1, 8)
        elif memory_gb >= 8:
            optimal_batch_size = 25000
            optimal_processes = min(cpu_count - 1, 4)
        else:
            optimal_batch_size = 10000
            optimal_processes = min(cpu_count - 1, 2)
        
        # Override with user config if specified
        batch_size = self.config.batch_size if self.config.batch_size else optimal_batch_size
        max_processes = min(self.config.max_processes, optimal_processes) if self.config.max_processes else optimal_processes
        
        return {
            'batch_size': batch_size,
            'max_processes': max_processes,
            'memory_gb': memory_gb,
            'cpu_count': cpu_count,
            'rows_per_process_threshold': self.config.rows_per_process or 100000
        }
        
    def generate_data_for_table_parallel(self, table_name: str, num_rows: int) -> List[Dict[str, Any]]:
        """Generate data for a table using parallel processing when beneficial."""
        table = self.schema.get_table(table_name)
        if not table:
            logger.warning(f"Table {table_name} not found in schema")
            return []
        
        start_time = time.time()
        logger.info(f"Starting parallel generation for {table_name}: {num_rows:,} rows")
        
        # Determine optimal processing strategy
        use_multiprocessing = (
            self.config.enable_multiprocessing and 
            num_rows >= self._adaptive_config['rows_per_process_threshold'] and
            self._adaptive_config['max_processes'] > 1
        )
        
        # Memory check for very large datasets
        estimated_memory_mb = self._estimate_memory_usage(table, num_rows)
        available_memory_mb = psutil.virtual_memory().available / (1024**2)
        
        logger.info(f"Memory estimate: {estimated_memory_mb:.1f}MB, Available: {available_memory_mb:.1f}MB")
        
        if estimated_memory_mb > available_memory_mb * 0.8:
            logger.warning("Large dataset detected, using streaming generation")
            return self._generate_with_streaming(table, num_rows)
        elif use_multiprocessing:
            return self._generate_with_multiprocessing(table, num_rows)
        elif num_rows >= 10000 and self.config.max_workers > 1:
            return self._generate_with_multithreading(table, num_rows)
        else:
            # Use single-threaded generation for smaller datasets
            return self._generate_single_threaded(table, num_rows)
    
    def _estimate_memory_usage(self, table: TableInfo, num_rows: int) -> float:
        """Estimate memory usage for generating data."""
        # Rough estimation: average 100 bytes per column per row
        avg_bytes_per_row = len(table.columns) * 100
        
        # Add overhead for Python objects and processing
        total_bytes = num_rows * avg_bytes_per_row * 2  # 2x for overhead
        
        return total_bytes / (1024**2)  # Convert to MB
    
    def _generate_with_multiprocessing(self, table: TableInfo, num_rows: int) -> List[Dict[str, Any]]:
        """Generate data using multiprocessing for very large datasets."""
        logger.info(f"Using multiprocessing with {self.config.max_processes} processes for {num_rows:,} rows")
        
        # Calculate rows per process
        rows_per_process = max(1, num_rows // self.config.max_processes)
        tasks = []
        
        # Create generation tasks
        for i in range(self.config.max_processes):
            start_row = i * rows_per_process
            if i == self.config.max_processes - 1:
                # Last process gets remaining rows
                end_row = num_rows
            else:
                end_row = start_row + rows_per_process
            
            if start_row >= num_rows:
                break
                
            # Create unique seed for each process
            process_seed = None
            if self.config.seed is not None:
                process_seed = self.config.seed + i
            
            task = GenerationTask(
                table_name=table.name,
                start_row=start_row,
                end_row=end_row,
                seed=process_seed,
                task_id=f"process_{i}"
            )
            tasks.append(task)
        
        # Process tasks in parallel
        all_data = []
        
        try:
            # Use spawn method for better isolation
            ctx = mp.get_context('spawn')
            with ProcessPoolExecutor(
                max_workers=self.config.max_processes,
                mp_context=ctx
            ) as executor:
                # Submit tasks
                future_to_task = {
                    executor.submit(
                        _generate_data_worker_process,
                        self.schema, 
                        self.config, 
                        self.db_connection.config, 
                        task
                    ): task for task in tasks
                }
                
                # Collect results
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result(timeout=300)  # 5 minute timeout per process
                        all_data.extend(result)
                        logger.info(f"Process {task.task_id} completed: {len(result)} rows")
                    except Exception as e:
                        logger.error(f"Process {task.task_id} failed: {e}")
                        # Continue with other processes
        
        except Exception as e:
            logger.error(f"Multiprocessing failed: {e}")
            # Fallback to single-threaded generation
            logger.info("Falling back to single-threaded generation")
            return self._generate_single_threaded(table, num_rows)
        
        logger.info(f"Multiprocessing completed: {len(all_data):,} rows generated")
        return all_data
    
    def _generate_with_multithreading(self, table: TableInfo, num_rows: int) -> List[Dict[str, Any]]:
        """Generate data using multithreading for medium datasets."""
        logger.info(f"Using multithreading with {self.config.max_workers} threads for {num_rows:,} rows")
        
        # Calculate rows per thread
        rows_per_thread = max(1, num_rows // self.config.max_workers)
        tasks = []
        
        # Create generation tasks
        for i in range(self.config.max_workers):
            start_row = i * rows_per_thread
            if i == self.config.max_workers - 1:
                # Last thread gets remaining rows
                end_row = num_rows
            else:
                end_row = start_row + rows_per_thread
            
            if start_row >= num_rows:
                break
                
            # Create unique seed for each thread
            thread_seed = None
            if self.config.seed is not None:
                thread_seed = self.config.seed + i * 1000
            
            task = GenerationTask(
                table_name=table.name,
                start_row=start_row,
                end_row=end_row,
                seed=thread_seed,
                task_id=f"thread_{i}"
            )
            tasks.append(task)
        
        # Process tasks in parallel
        all_data = []
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit tasks
            future_to_task = {
                executor.submit(
                    _generate_data_worker_thread,
                    self.schema, 
                    self.config, 
                    self.db_connection, 
                    task
                ): task for task in tasks
            }
            
            # Collect results
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result(timeout=120)  # 2 minute timeout per thread
                    all_data.extend(result)
                    logger.info(f"Thread {task.task_id} completed: {len(result)} rows")
                except Exception as e:
                    logger.error(f"Thread {task.task_id} failed: {e}")
                    # Continue with other threads
        
        logger.info(f"Multithreading completed: {len(all_data):,} rows generated")
        return all_data
    
    def _generate_single_threaded(self, table: TableInfo, num_rows: int) -> List[Dict[str, Any]]:
        """Generate data using single thread."""
        logger.info(f"Using single-threaded generation for {num_rows:,} rows")
        
        # Create a standard generator
        generator = DataGenerator(self.schema, self.config, self.db_connection)
        return generator.generate_data_for_table(table.name, num_rows)
    
    def generate_data_for_all_tables_parallel(self, rows_per_table: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """Generate data for all tables using parallel processing."""
        logger.info("Starting parallel generation for all tables")
        
        # Get tables in dependency order
        from .dependency_resolver import DependencyResolver
        resolver = DependencyResolver(self.schema)
        insertion_plan = resolver.create_insertion_plan()
        batches = insertion_plan.get_insertion_batches()
        
        all_data = {}
        total_start_time = time.time()
        
        # Process each dependency batch
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing dependency batch {batch_num}/{len(batches)}: {batch}")
            
            # Process tables in each batch in parallel (since they have no dependencies on each other)
            if len(batch) > 1 and self.config.max_workers > 1:
                batch_data = self._generate_batch_parallel(batch, rows_per_table)
            else:
                batch_data = self._generate_batch_sequential(batch, rows_per_table)
            
            all_data.update(batch_data)
        
        total_time = time.time() - total_start_time
        total_rows = sum(len(data) for data in all_data.values())
        
        logger.info(f"Parallel generation completed: {total_rows:,} rows in {total_time:.2f}s")
        
        return all_data
    
    def _generate_batch_parallel(self, batch: List[str], rows_per_table: int) -> Dict[str, List[Dict[str, Any]]]:
        """Generate data for a batch of tables in parallel."""
        batch_data = {}
        
        with ThreadPoolExecutor(max_workers=min(len(batch), self.config.max_workers)) as executor:
            # Submit generation tasks for each table in the batch
            future_to_table = {
                executor.submit(self.generate_data_for_table_parallel, table_name, rows_per_table): table_name
                for table_name in batch
            }
            
            # Collect results
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result(timeout=600)  # 10 minute timeout per table
                    batch_data[table_name] = result
                    logger.info(f"Table {table_name} completed: {len(result)} rows")
                except Exception as e:
                    logger.error(f"Table {table_name} failed: {e}")
                    batch_data[table_name] = []
        
        return batch_data
    
    def _generate_batch_sequential(self, batch: List[str], rows_per_table: int) -> Dict[str, List[Dict[str, Any]]]:
        """Generate data for a batch of tables sequentially."""
        batch_data = {}
        
        for table_name in batch:
            try:
                result = self.generate_data_for_table_parallel(table_name, rows_per_table)
                batch_data[table_name] = result
                logger.info(f"Table {table_name} completed: {len(result)} rows")
            except Exception as e:
                logger.error(f"Table {table_name} failed: {e}")
                batch_data[table_name] = []
        
        return batch_data
    
    def _generate_with_streaming(self, table: TableInfo, num_rows: int) -> List[Dict[str, Any]]:
        """Generate data using streaming approach for very large datasets to manage memory."""
        logger.info(f"Using streaming generation for {num_rows:,} rows to manage memory usage")
        
        # Calculate optimal chunk size based on available memory
        available_memory_mb = psutil.virtual_memory().available / (1024**2)
        chunk_size = min(
            100000,  # Max chunk size
            max(1000, int(available_memory_mb * 1024 * 1024 / (len(table.columns) * 200)))  # Dynamic sizing
        )
        
        all_data = []
        total_generated = 0
        
        # Process in chunks to manage memory
        for chunk_start in range(0, num_rows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, num_rows)
            chunk_rows = chunk_end - chunk_start
            
            logger.info(f"Generating chunk {chunk_start//chunk_size + 1}: rows {chunk_start+1}-{chunk_end}")
            
            # Generate chunk using appropriate method
            if chunk_rows >= 10000 and self.config.max_workers > 1:
                chunk_data = self._generate_with_multithreading(table, chunk_rows)
            else:
                chunk_data = self._generate_single_threaded(table, chunk_rows)
            
            all_data.extend(chunk_data)
            total_generated += len(chunk_data)
            
            # Force garbage collection after each chunk
            gc.collect()
            
            # Monitor memory usage
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > self._memory_threshold * 100:
                logger.warning(f"High memory usage detected: {memory_percent:.1f}%")
                # Reduce chunk size for next iteration
                chunk_size = max(1000, chunk_size // 2)
            
            logger.debug(f"Chunk completed: {len(chunk_data)} rows, Total: {total_generated}/{num_rows}")
        
        logger.info(f"Streaming generation completed: {total_generated:,} rows")
        return all_data


def _generate_data_worker_process(schema: DatabaseSchema, config: GenerationConfig, 
                                 db_config, task: GenerationTask) -> List[Dict[str, Any]]:
    """Worker function for multiprocessing data generation."""
    # Create new database connection for this process
    db_conn = DatabaseConnection(db_config)
    
    try:
        # Create generator with task-specific seed
        task_config = GenerationConfig(**config.dict())
        task_config.seed = task.seed
        
        generator = EnhancedDataGenerator(schema, task_config, db_conn)
        
        # Generate data for the specified range
        num_rows = task.end_row - task.start_row
        return generator.generate_data_for_table(task.table_name, num_rows)
    
    finally:
        if db_conn:
            db_conn.close()


def _generate_data_worker_thread(schema: DatabaseSchema, config: GenerationConfig,
                                db_connection: DatabaseConnection, task: GenerationTask) -> List[Dict[str, Any]]:
    """Worker function for multithreading data generation."""
    # Create generator with task-specific seed
    task_config = GenerationConfig(**config.dict())
    task_config.seed = task.seed
    
    generator = EnhancedDataGenerator(schema, task_config, db_connection)
    
    # Generate data for the specified range
    num_rows = task.end_row - task.start_row
    return generator.generate_data_for_table(task.table_name, num_rows)


class EnhancedDataGenerator(DataGenerator):
    """Enhanced data generator with duplicate support and other improvements."""
    
    def _generate_column_value(self, column, table_config, table=None):
        """Enhanced column value generation with duplicate support."""
        # Check for custom column configuration
        column_config = table_config.column_configs.get(column.name)
        
        # Handle global duplicate setting FIRST (highest priority)
        if self.config.allow_duplicates and random.random() < self.config.duplicate_probability:
            if self._can_allow_duplicates(table, column.name):
                # Use global duplicate mode - generate one value and cache it
                cache_key = f"global_duplicate_{table.name}_{column.name}"
                if not hasattr(self, '_global_duplicate_cache'):
                    self._global_duplicate_cache = {}
                
                if cache_key not in self._global_duplicate_cache:
                    # Generate the duplicate value once using basic type generation
                    self._global_duplicate_cache[cache_key] = self._generate_base_value(column, table_config, table)
                    logger.debug(f"Generated and cached global duplicate value for {column.name}: {self._global_duplicate_cache[cache_key]}")
                
                return self._global_duplicate_cache[cache_key]
            else:
                logger.debug(f"Column {column.name} has constraints that prevent duplicates, generating unique value")
        
        # Handle column-specific duplicate mode (if no global setting applied)
        if column_config and hasattr(column_config, 'duplicate_mode') and column_config.duplicate_mode == "allow_duplicates":
            if hasattr(column_config, 'duplicate_value') and column_config.duplicate_value is not None:
                # Use the specified duplicate value
                return column_config.duplicate_value
            elif self._can_allow_duplicates(table, column.name):
                # Generate one value and cache it for this column if constraints allow
                cache_key = f"duplicate_{table.name}_{column.name}"
                if not hasattr(self, '_duplicate_cache'):
                    self._duplicate_cache = {}
                
                if cache_key not in self._duplicate_cache:
                    # Generate the duplicate value once using the parent's base generation
                    self._duplicate_cache[cache_key] = self._generate_base_value(column, table_config, table)
                
                return self._duplicate_cache[cache_key]
            else:
                logger.warning(f"Column {column.name} has constraints that prevent duplicates, using generate_new mode")
        
        # Use standard generation for all other cases
        return super()._generate_column_value(column, table_config, table)
    
    def _generate_base_value(self, column, table_config, table=None):
        """Generate a base value without duplicate handling."""
        # Use the parent's type-based generation directly
        return self._generate_by_type(column, table_config.column_configs.get(column.name), table)


class ParallelDataInserter(DataInserter):
    """Enhanced data inserter with parallel processing support."""
    
    def insert_data_parallel(self, table_name: str, data: List[Dict[str, Any]], 
                           batch_size: int = 1000, max_workers: int = 4,
                           progress_callback: Optional[Callable] = None) -> GenerationStats:
        """Insert data using parallel batch processing."""
        if not data:
            logger.warning(f"No data to insert for table: {table_name}")
            return GenerationStats()
        
        table = self.schema.get_table(table_name)
        if not table:
            logger.warning(f"Table {table_name} not found in schema")
            return GenerationStats()
        
        logger.info(f"Starting parallel insertion: {len(data):,} rows into {table_name}")
        start_time = time.time()
        
        # Split data into batches
        batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
        logger.info(f"Split data into {len(batches)} batches of size {batch_size}")
        
        stats = GenerationStats()
        stats.table_stats[table_name] = {
            'rows_requested': len(data),
            'rows_inserted': 0,
            'errors': []
        }
        
        total_inserted = 0
        
        # Use threading for database operations (not multiprocessing due to connection sharing)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(self._insert_batch_safe, table, batch, batch_idx): batch_idx
                for batch_idx, batch in enumerate(batches)
            }
            
            # Process completed batches
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    rows_inserted = future.result(timeout=60)  # 1 minute timeout per batch
                    total_inserted += rows_inserted
                    
                    if progress_callback:
                        progress_callback(table_name, total_inserted, len(data))
                    
                    logger.debug(f"Batch {batch_idx + 1}/{len(batches)} completed: {rows_inserted} rows")
                
                except Exception as e:
                    error_msg = f"Batch {batch_idx + 1} failed: {e}"
                    logger.error(error_msg)
                    stats.errors.append(error_msg)
                    stats.table_stats[table_name]['errors'].append(error_msg)
        
        # Update statistics
        end_time = time.time()
        stats.tables_processed = 1
        stats.total_rows_generated = total_inserted
        stats.total_time_seconds = end_time - start_time
        stats.table_stats[table_name]['rows_inserted'] = total_inserted
        stats.table_stats[table_name]['time_seconds'] = end_time - start_time
        
        logger.info(f"Parallel insertion completed: {total_inserted:,}/{len(data)} rows in {end_time - start_time:.2f}s")
        
        return stats
    
    def _insert_batch_safe(self, table: TableInfo, batch: List[Dict[str, Any]], batch_idx: int) -> int:
        """Thread-safe batch insertion with individual connection."""
        if not batch:
            return 0
        
        try:
            # Create a new connection for this thread to avoid conflicts
            thread_db_conn = DatabaseConnection(self.db_connection.config)
            thread_db_conn.connect()
            
            try:
                with thread_db_conn.get_session() as session:
                    # Build insert query with properly quoted column names
                    column_names = list(batch[0].keys())
                    placeholders = ', '.join([f':{col}' for col in column_names])
                    quoted_columns = ', '.join([thread_db_conn.quote_identifier(col) for col in column_names])
                    
                    quoted_table = thread_db_conn.quote_identifier(table.name)
                    query = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
                    
                    # Execute batch insert
                    session.execute(text(query), batch)
                    session.commit()
                    
                    return len(batch)
            finally:
                thread_db_conn.close()
        
        except Exception as e:
            logger.error(f"Thread batch insert failed for batch {batch_idx}: {e}")
            raise
