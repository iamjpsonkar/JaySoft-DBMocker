"""Efficient bulk data insertion system with transaction management."""

import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text, insert
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from .database import DatabaseConnection
from .models import TableInfo, DatabaseSchema, GenerationStats


logger = logging.getLogger(__name__)


class DataInserter:
    """Handles efficient bulk insertion of generated data."""
    
    def __init__(self, db_connection: DatabaseConnection, schema: DatabaseSchema):
        """Initialize data inserter."""
        self.db_connection = db_connection
        self.schema = schema
        
    def insert_data(self, table_name: str, data: List[Dict[str, Any]], 
                   batch_size: int = 1000, max_workers: int = 4,
                   progress_callback: Optional[callable] = None) -> GenerationStats:
        """Insert data into a table with batching and progress tracking."""
        if not data:
            logger.warning(f"No data to insert for table: {table_name}")
            return GenerationStats()
        
        table = self.schema.get_table(table_name)
        if not table:
            raise ValueError(f"Table {table_name} not found in schema")
        
        logger.info(f"Inserting {len(data)} rows into table: {table_name}")
        start_time = time.time()
        
        stats = GenerationStats()
        stats.table_stats[table_name] = {
            'rows_requested': len(data),
            'rows_inserted': 0,
            'errors': []
        }
        
        try:
            # Split data into batches
            batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
            logger.info(f"Split data into {len(batches)} batches of size {batch_size}")
            
            # Use single-threaded approach for database writes to avoid conflicts
            total_inserted = 0
            
            with tqdm(total=len(data), desc=f"Inserting {table_name}") as pbar:
                for i, batch in enumerate(batches):
                    try:
                        rows_inserted = self._insert_batch(table, batch)
                        total_inserted += rows_inserted
                        
                        pbar.update(rows_inserted)
                        
                        if progress_callback:
                            progress_callback(table_name, total_inserted, len(data))
                        
                        logger.debug(f"Batch {i + 1}/{len(batches)} completed: {rows_inserted} rows")
                    
                    except Exception as e:
                        error_msg = f"Batch {i + 1} failed: {e}"
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
            
            logger.info(f"Successfully inserted {total_inserted}/{len(data)} rows into {table_name} "
                       f"in {end_time - start_time:.2f} seconds")
        
        except Exception as e:
            error_msg = f"Failed to insert data into {table_name}: {e}"
            logger.error(error_msg)
            stats.errors.append(error_msg)
            raise
        
        return stats
    
    def _insert_batch(self, table: TableInfo, batch: List[Dict[str, Any]]) -> int:
        """Insert a single batch of data."""
        if not batch:
            return 0
        
        try:
            with self.db_connection.get_session() as session:
                # Build insert query
                column_names = list(batch[0].keys())
                placeholders = ', '.join([f':{col}' for col in column_names])
                columns_str = ', '.join(column_names)
                
                query = f"INSERT INTO {table.name} ({columns_str}) VALUES ({placeholders})"
                
                # Execute batch insert
                session.execute(text(query), batch)
                session.commit()
                
                return len(batch)
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during batch insert: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during batch insert: {e}")
            raise
    
    def truncate_table(self, table_name: str) -> None:
        """Truncate a table."""
        logger.info(f"Truncating table: {table_name}")
        
        try:
            with self.db_connection.get_session() as session:
                # Disable foreign key checks temporarily if supported
                if self.db_connection.config.driver == "mysql":
                    session.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                elif self.db_connection.config.driver == "postgresql":
                    session.execute(text("SET session_replication_role = replica"))
                elif self.db_connection.config.driver == "sqlite":
                    session.execute(text("PRAGMA foreign_keys = OFF"))
                
                # Truncate table (use DELETE for SQLite)
                if self.db_connection.config.driver == "sqlite":
                    session.execute(text(f"DELETE FROM {table_name}"))
                    session.execute(text(f"DELETE FROM sqlite_sequence WHERE name='{table_name}'"))  # Reset autoincrement
                else:
                    session.execute(text(f"TRUNCATE TABLE {table_name}"))
                
                # Re-enable foreign key checks
                if self.db_connection.config.driver == "mysql":
                    session.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                elif self.db_connection.config.driver == "postgresql":
                    session.execute(text("SET session_replication_role = DEFAULT"))
                elif self.db_connection.config.driver == "sqlite":
                    session.execute(text("PRAGMA foreign_keys = ON"))
                
                session.commit()
                logger.info(f"Successfully truncated table: {table_name}")
        
        except SQLAlchemyError as e:
            logger.error(f"Failed to truncate table {table_name}: {e}")
            raise
    
    def insert_data_parallel(self, table_data: Dict[str, List[Dict[str, Any]]],
                           batch_size: int = 1000, max_workers: int = 4,
                           progress_callback: Optional[callable] = None) -> GenerationStats:
        """Insert data for multiple tables in parallel (experimental)."""
        logger.info(f"Starting parallel data insertion for {len(table_data)} tables")
        
        start_time = time.time()
        stats = GenerationStats()
        
        # Sort tables by dependency order
        sorted_tables = self._sort_tables_by_dependencies(list(table_data.keys()))
        
        total_rows = sum(len(data) for data in table_data.values())
        
        with tqdm(total=total_rows, desc="Inserting data") as pbar:
            for table_name in sorted_tables:
                if table_name not in table_data:
                    continue
                
                data = table_data[table_name]
                table_stats = self.insert_data(
                    table_name, data, batch_size, max_workers=1,  # Use single thread per table
                    progress_callback=lambda tn, inserted, total: pbar.update(1)
                )
                
                # Merge statistics
                stats.tables_processed += table_stats.tables_processed
                stats.total_rows_generated += table_stats.total_rows_generated
                stats.errors.extend(table_stats.errors)
                stats.table_stats.update(table_stats.table_stats)
        
        stats.total_time_seconds = time.time() - start_time
        logger.info(f"Parallel insertion completed in {stats.total_time_seconds:.2f} seconds")
        
        return stats
    
    def _sort_tables_by_dependencies(self, table_names: List[str]) -> List[str]:
        """Sort tables by foreign key dependencies."""
        dependencies = self.schema.get_table_dependencies()
        sorted_tables = []
        remaining_tables = set(table_names)
        
        while remaining_tables:
            # Find tables with no unresolved dependencies
            ready_tables = []
            for table in remaining_tables:
                table_deps = dependencies.get(table, [])
                unresolved_deps = [dep for dep in table_deps if dep in remaining_tables]
                if not unresolved_deps:
                    ready_tables.append(table)
            
            if not ready_tables:
                # Circular dependency or other issue - just take remaining tables
                logger.warning("Circular dependency detected, proceeding with remaining tables")
                ready_tables = list(remaining_tables)
            
            # Add ready tables to sorted list
            for table in ready_tables:
                sorted_tables.append(table)
                remaining_tables.remove(table)
        
        return sorted_tables
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get current row count for a table."""
        try:
            result = self.db_connection.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            return result[0][0] if result else 0
        except Exception as e:
            logger.error(f"Failed to get row count for {table_name}: {e}")
            return 0
    
    def verify_data_integrity(self, table_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """Verify data integrity after insertion."""
        logger.info("Verifying data integrity")
        
        if table_names is None:
            table_names = [table.name for table in self.schema.tables]
        
        integrity_report = {
            'total_tables_checked': 0,
            'tables_with_issues': [],
            'foreign_key_violations': [],
            'constraint_violations': [],
            'summary': {}
        }
        
        for table_name in table_names:
            table = self.schema.get_table(table_name)
            if not table:
                continue
            
            integrity_report['total_tables_checked'] += 1
            table_issues = []
            
            # Check foreign key constraints
            for fk in table.foreign_keys:
                try:
                    violations = self._check_foreign_key_integrity(table_name, fk)
                    if violations:
                        table_issues.extend(violations)
                        integrity_report['foreign_key_violations'].extend(violations)
                except Exception as e:
                    logger.error(f"Failed to check FK integrity for {table_name}: {e}")
            
            # Check unique constraints
            for constraint in table.constraints:
                if constraint.type.value == 'unique':
                    try:
                        violations = self._check_unique_constraint_integrity(table_name, constraint)
                        if violations:
                            table_issues.extend(violations)
                            integrity_report['constraint_violations'].extend(violations)
                    except Exception as e:
                        logger.error(f"Failed to check unique constraint for {table_name}: {e}")
            
            if table_issues:
                integrity_report['tables_with_issues'].append(table_name)
            
            integrity_report['summary'][table_name] = {
                'row_count': self.get_table_row_count(table_name),
                'issues_found': len(table_issues)
            }
        
        return integrity_report
    
    def _check_foreign_key_integrity(self, table_name: str, fk_constraint) -> List[str]:
        """Check foreign key constraint integrity."""
        violations = []
        
        try:
            # Build query to find FK violations
            local_columns = ', '.join(fk_constraint.columns)
            referenced_columns = ', '.join(fk_constraint.referenced_columns or ['id'])
            
            query = f"""
                SELECT COUNT(*) FROM {table_name} t1
                LEFT JOIN {fk_constraint.referenced_table} t2
                ON {' AND '.join([
                    f't1.{lc} = t2.{rc}' 
                    for lc, rc in zip(fk_constraint.columns, fk_constraint.referenced_columns or ['id'])
                ])}
                WHERE t2.{fk_constraint.referenced_columns[0] if fk_constraint.referenced_columns else 'id'} IS NULL
                AND t1.{fk_constraint.columns[0]} IS NOT NULL
            """
            
            result = self.db_connection.execute_query(query)
            violation_count = result[0][0] if result else 0
            
            if violation_count > 0:
                violations.append(
                    f"Foreign key violation in {table_name}.{local_columns} -> "
                    f"{fk_constraint.referenced_table}.{referenced_columns}: "
                    f"{violation_count} orphaned records"
                )
        
        except Exception as e:
            logger.debug(f"Could not check FK integrity: {e}")
        
        return violations
    
    def _check_unique_constraint_integrity(self, table_name: str, constraint) -> List[str]:
        """Check unique constraint integrity."""
        violations = []
        
        try:
            columns = ', '.join(constraint.columns)
            query = f"""
                SELECT {columns}, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY {columns}
                HAVING COUNT(*) > 1
            """
            
            result = self.db_connection.execute_query(query)
            if result:
                violations.append(
                    f"Unique constraint violation in {table_name}.{columns}: "
                    f"{len(result)} duplicate groups found"
                )
        
        except Exception as e:
            logger.debug(f"Could not check unique constraint integrity: {e}")
        
        return violations
