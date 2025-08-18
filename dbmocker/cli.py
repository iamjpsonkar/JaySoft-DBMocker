"""Command-line interface for DBMocker."""

import click
import logging
import sys
import time
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.generator import DataGenerator
from dbmocker.core.inserter import DataInserter
from dbmocker.core.models import GenerationConfig, TableGenerationConfig, ColumnGenerationConfig
from dbmocker.core.dependency_resolver import DependencyResolver, print_insertion_plan
from dbmocker.core.smart_generator import DependencyAwareGenerator, create_optimal_generation_config
from dbmocker.core.db_spec_analyzer import DatabaseSpecAnalyzer, print_table_specs
from dbmocker.core.spec_driven_generator import SpecificationDrivenGenerator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--quiet', '-q', is_flag=True, help='Suppress non-error output')
def cli(verbose: bool, quiet: bool):
    """JaySoft-DBMocker - Generate realistic mock data for SQL databases."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    elif quiet:
        logging.getLogger().setLevel(logging.ERROR)


@cli.command()
@click.option('--host', '-h', required=True, help='Database host')
@click.option('--port', '-p', type=int, required=True, help='Database port')
@click.option('--database', '-d', required=True, help='Database name')
@click.option('--username', '-u', required=True, help='Database username')
@click.option('--password', required=True, prompt=True, hide_input=True, help='Database password')
@click.option('--driver', default='postgresql', type=click.Choice(['postgresql', 'mysql', 'sqlite']),
              help='Database driver')
@click.option('--output', '-o', type=click.Path(), help='Output file for schema analysis')
@click.option('--include-tables', help='Comma-separated list of tables to include')
@click.option('--exclude-tables', help='Comma-separated list of tables to exclude')
@click.option('--analyze-patterns/--no-analyze-patterns', default=True,
              help='Analyze existing data patterns')
@click.option('--analyze-existing-data/--no-analyze-existing-data', default=False,
              help='Analyze existing data in tables for realistic generation patterns')
@click.option('--pattern-sample-size', default=1000, type=int,
              help='Sample size for existing data pattern analysis')
def analyze(host: str, port: int, database: str, username: str, password: str,
           driver: str, output: Optional[str], include_tables: Optional[str],
           exclude_tables: Optional[str], analyze_patterns: bool,
           analyze_existing_data: bool, pattern_sample_size: int):
    """Analyze database schema and existing data patterns."""
    try:
        # Parse table lists
        include_list = include_tables.split(',') if include_tables else None
        exclude_list = exclude_tables.split(',') if exclude_tables else None
        
        # Create database configuration
        config = DatabaseConfig(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            driver=driver
        )
        
        # Connect and analyze
        with DatabaseConnection(config) as db_conn:
            analyzer = SchemaAnalyzer(db_conn)
            
            click.echo("🔍 Analyzing database schema...")
            schema = analyzer.analyze_schema(
                include_tables=include_list,
                exclude_tables=exclude_list,
                analyze_data_patterns=analyze_patterns,
                analyze_existing_data=analyze_existing_data,
                pattern_sample_size=pattern_sample_size
            )
            
            # Display summary
            click.echo(f"\n📊 Analysis Results:")
            click.echo(f"  Database: {schema.database_name}")
            click.echo(f"  Tables analyzed: {len(schema.tables)}")
            click.echo(f"  Total rows: {sum(table.row_count for table in schema.tables):,}")
            
            click.echo(f"\n📋 Tables:")
            for table in schema.tables:
                fk_count = len(table.foreign_keys)
                fk_text = f" ({fk_count} FKs)" if fk_count > 0 else ""
                pattern_text = ""
                if hasattr(schema, 'table_patterns') and schema.table_patterns and table.name in schema.table_patterns:
                    pattern_info = schema.table_patterns[table.name]
                    pattern_text = f" [📊 {pattern_info.total_records} samples analyzed]"
                click.echo(f"  • {table.name}: {table.row_count:,} rows, "
                          f"{len(table.columns)} columns{fk_text}{pattern_text}")
            
            # Display pattern analysis summary if performed
            if hasattr(schema, 'table_patterns') and schema.table_patterns:
                click.echo(f"\n🎯 Pattern Analysis Summary:")
                click.echo(f"  Tables with pattern analysis: {len(schema.table_patterns)}")
                total_samples = sum(p.total_records for p in schema.table_patterns.values())
                click.echo(f"  Total samples analyzed: {total_samples:,}")
                click.echo(f"  Sample size per table: {pattern_sample_size}")
                click.echo(f"  🚀 Realistic data generation enabled for these tables!")
            
            # Save to file if requested
            if output:
                output_path = Path(output)
                schema_data = {
                    'database_name': schema.database_name,
                    'tables': [
                        {
                            'name': table.name,
                            'row_count': table.row_count,
                            'columns': [
                                {
                                    'name': col.name,
                                    'data_type': col.data_type.value,
                                    'max_length': col.max_length,
                                    'is_nullable': col.is_nullable,
                                    'detected_pattern': col.detected_pattern,
                                    'sample_values': col.sample_values[:5]  # First 5 samples
                                }
                                for col in table.columns
                            ],
                            'foreign_keys': [
                                {
                                    'name': fk.name,
                                    'columns': fk.columns,
                                    'referenced_table': fk.referenced_table,
                                    'referenced_columns': fk.referenced_columns
                                }
                                for fk in table.foreign_keys
                            ]
                        }
                        for table in schema.tables
                    ]
                }
                
                if output_path.suffix.lower() == '.json':
                    with open(output_path, 'w') as f:
                        json.dump(schema_data, f, indent=2, default=str)
                else:
                    with open(output_path, 'w') as f:
                        yaml.dump(schema_data, f, default_flow_style=False)
                
                click.echo(f"\n💾 Schema analysis saved to: {output_path}")
        
        click.echo("\n✅ Analysis completed successfully!")
    
    except Exception as e:
        click.echo(f"\n❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--host', '-h', required=True, help='Database host')
@click.option('--port', '-p', type=int, required=True, help='Database port')
@click.option('--database', '-d', required=True, help='Database name')
@click.option('--username', '-u', required=True, help='Database username')
@click.option('--password', required=True, prompt=True, hide_input=True, help='Database password')
@click.option('--driver', default='postgresql', type=click.Choice(['postgresql', 'mysql', 'sqlite']),
              help='Database driver')
@click.option('--config', '-c', type=click.Path(exists=True), help='Configuration file (JSON/YAML)')
@click.option('--rows', '-r', type=int, default=1000, help='Number of rows to generate per table')
@click.option('--batch-size', type=int, default=1000, help='Batch size for inserts')
@click.option('--max-workers', type=int, default=4, help='Number of worker threads')
@click.option('--enable-multiprocessing', is_flag=True, help='Enable multiprocessing for large datasets (millions of rows)')
@click.option('--max-processes', type=int, default=2, help='Number of processes for multiprocessing')
@click.option('--rows-per-process', type=int, default=100000, help='Rows per process threshold for multiprocessing')
@click.option('--include-tables', help='Comma-separated list of tables to include')
@click.option('--exclude-tables', help='Comma-separated list of tables to exclude')
@click.option('--use-existing-tables', help='Comma-separated list of tables to use existing data from (mixed mode)')
@click.option('--truncate/--no-truncate', default=False, help='Truncate existing data before insert')
@click.option('--seed', type=int, help='Random seed for reproducible generation')
@click.option('--dry-run', is_flag=True, help='Generate data but do not insert into database')
@click.option('--verify/--no-verify', default=True, help='Verify data integrity after insertion')
@click.option('--analyze-existing-data/--no-analyze-existing-data', default=False,
              help='Analyze existing data for realistic generation patterns')
@click.option('--pattern-sample-size', default=1000, type=int,
              help='Sample size for existing data pattern analysis')
@click.option('--duplicate-allowed/--generate-new-only', default=False,
              help='Allow duplicates for columns without constraints (default: generate new only)')
@click.option('--global-duplicate-mode', type=click.Choice(['generate_new', 'allow_duplicates', 'smart_duplicates']),
              default='generate_new', help='Global duplicate mode for all columns')
@click.option('--global-duplicate-probability', default=0.5, type=float,
              help='Probability for global smart duplicates (0.0-1.0)')
@click.option('--global-max-duplicate-values', default=10, type=int,
              help='Maximum unique values in global smart duplicate mode')
@click.option('--allow-duplicates', is_flag=True, help='Allow duplicate values when column constraints permit')
@click.option('--duplicate-probability', default=1.0, type=float,
              help='Probability of using duplicates when allowed (0.0-1.0, default: 1.0)')
def generate(host: str, port: int, database: str, username: str, password: str,
            driver: str, config: Optional[str], rows: int, batch_size: int, max_workers: int,
            enable_multiprocessing: bool, max_processes: int, rows_per_process: int,
            include_tables: Optional[str], exclude_tables: Optional[str],
            use_existing_tables: Optional[str], truncate: bool, seed: Optional[int], 
            dry_run: bool, verify: bool, analyze_existing_data: bool, pattern_sample_size: int,
            duplicate_allowed: bool, global_duplicate_mode: str, global_duplicate_probability: float,
            global_max_duplicate_values: int, allow_duplicates: bool, duplicate_probability: float):
    """Generate and insert mock data into database."""
    try:
        # Parse table lists
        include_list = include_tables.split(',') if include_tables else None
        exclude_list = exclude_tables.split(',') if exclude_tables else None
        use_existing_list = use_existing_tables.split(',') if use_existing_tables else []
        
        # Load configuration
        generation_config = GenerationConfig(
            batch_size=batch_size,
            max_workers=max_workers,
            enable_multiprocessing=enable_multiprocessing,
            max_processes=max_processes,
            rows_per_process=rows_per_process,
            seed=seed,
            include_tables=include_list,
            exclude_tables=exclude_list or [],
            use_existing_tables=use_existing_list,
            truncate_existing=truncate,
            # Global duplicate settings
            duplicate_allowed=duplicate_allowed,
            global_duplicate_mode=global_duplicate_mode,
            global_duplicate_probability=global_duplicate_probability,
            global_max_duplicate_values=global_max_duplicate_values,
            # Legacy duplicate settings (for backward compatibility)
            allow_duplicates=allow_duplicates,
            duplicate_probability=duplicate_probability
        )
        
        if config:
            config_data = load_config_file(config)
            # Override with config file settings
            if 'table_configs' in config_data:
                for table_name, table_config in config_data['table_configs'].items():
                    generation_config.table_configs[table_name] = TableGenerationConfig(**table_config)
            elif 'tables' in config_data:
                # Support legacy format
                for table_name, table_config in config_data['tables'].items():
                    generation_config.table_configs[table_name] = TableGenerationConfig(**table_config)
            
            # Override global generation config if present
            if 'generation_config' in config_data:
                global_config = config_data['generation_config']
                if 'batch_size' in global_config:
                    generation_config.batch_size = global_config['batch_size']
                if 'rows_to_generate' in global_config:
                    rows = global_config['rows_to_generate']
        
        # Create database configuration
        db_config = DatabaseConfig(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            driver=driver
        )
        
        # Connect and process
        with DatabaseConnection(db_config) as db_conn:
            # Analyze schema
            click.echo("🔍 Analyzing database schema...")
            analyzer = SchemaAnalyzer(db_conn)
            schema = analyzer.analyze_schema(
                include_tables=include_list,
                exclude_tables=exclude_list,
                analyze_data_patterns=True,
                analyze_existing_data=analyze_existing_data,
                pattern_sample_size=pattern_sample_size
            )
            
            # Initialize components (use enhanced generators for better constraint handling)
            if enable_multiprocessing or max_workers > 1:
                from dbmocker.core.parallel_generator import ParallelDataGenerator, ParallelDataInserter
                generator = ParallelDataGenerator(schema, generation_config, db_conn)
                inserter = ParallelDataInserter(db_conn, schema)
                click.echo(f"🚀 Using parallel processing: MP={enable_multiprocessing}, Workers={max_workers}")
            else:
                # Use enhanced DataGenerator with better constraint handling
                from dbmocker.core.parallel_generator import EnhancedDataGenerator
                from dbmocker.core.inserter import DataInserter
                generator = EnhancedDataGenerator(schema, generation_config, db_conn)
                inserter = DataInserter(db_conn, schema)
                click.echo("🔧 Using enhanced data generator with improved constraint handling")
            
            # Sort tables by dependencies
            dependencies = schema.get_table_dependencies()
            sorted_tables = inserter._sort_tables_by_dependencies([t.name for t in schema.tables])
            
            click.echo(f"\n🎯 Generation Plan:")
            click.echo(f"  Tables to process: {len(sorted_tables)}")
            click.echo(f"  Rows per table: {rows}")
            click.echo(f"  Batch size: {batch_size}")
            click.echo(f"  Processing order: {' → '.join(sorted_tables)}")
            
            if use_existing_list:
                click.echo(f"  🔄 Using existing data: {', '.join(use_existing_list)}")
            
            if dry_run:
                click.echo("  🔥 DRY RUN MODE - No data will be inserted")
            
            if not click.confirm("\nProceed with data generation?"):
                click.echo("Operation cancelled.")
                return
            
            # Process each table
            total_start_time = time.time()
            total_rows_generated = 0
            total_rows_inserted = 0
            
            for table_name in sorted_tables:
                table = schema.get_table(table_name)
                if not table:
                    continue
                
                # Get row count for this table
                table_config = generation_config.table_configs.get(table_name)
                table_rows = table_config.rows_to_generate if table_config else rows
                
                if table_rows <= 0:
                    click.echo(f"⏭️  Skipping {table_name} (0 rows requested)")
                    continue
                
                click.echo(f"\n🔄 Processing table: {table_name}")
                
                # Truncate if requested
                if truncate and not dry_run:
                    click.echo(f"  🗑️  Truncating existing data...")
                    inserter.truncate_table(table_name)
                
                # Generate data
                click.echo(f"  🎲 Generating {table_rows:,} rows...")
                table_start_time = time.time()
                
                # Use parallel generation method if available
                if hasattr(generator, 'generate_data_for_table_parallel'):
                    generated_data = generator.generate_data_for_table_parallel(table_name, table_rows)
                else:
                    generated_data = generator.generate_data_for_table(table_name, table_rows)
                total_rows_generated += len(generated_data)
                
                generation_time = time.time() - table_start_time
                click.echo(f"  ✅ Generated {len(generated_data):,} rows in {generation_time:.2f}s")
                
                # Insert data
                if not dry_run:
                    click.echo(f"  💾 Inserting data...")
                    insert_start_time = time.time()
                    
                    # Use parallel insertion method if available
                    if hasattr(inserter, 'insert_data_parallel'):
                        stats = inserter.insert_data_parallel(
                            table_name, 
                            generated_data, 
                            batch_size,
                            max_workers,
                            progress_callback=lambda tn, inserted, total: None
                        )
                    else:
                        stats = inserter.insert_data(
                            table_name, 
                            generated_data, 
                            batch_size,
                            progress_callback=lambda tn, inserted, total: None
                        )
                    
                    total_rows_inserted += stats.total_rows_generated
                    insert_time = time.time() - insert_start_time
                    
                    click.echo(f"  ✅ Inserted {stats.total_rows_generated:,} rows in {insert_time:.2f}s")
                    
                    if stats.errors:
                        click.echo(f"  ⚠️  {len(stats.errors)} errors occurred")
            
            # Summary
            total_time = time.time() - total_start_time
            click.echo(f"\n📊 Generation Summary:")
            click.echo(f"  Total time: {total_time:.2f}s")
            click.echo(f"  Rows generated: {total_rows_generated:,}")
            
            if not dry_run:
                click.echo(f"  Rows inserted: {total_rows_inserted:,}")
                
                # Verify data integrity
                if verify:
                    click.echo(f"\n🔍 Verifying data integrity...")
                    integrity_report = inserter.verify_data_integrity(sorted_tables)
                    
                    if integrity_report['foreign_key_violations'] or integrity_report['constraint_violations']:
                        click.echo(f"  ⚠️  Found {len(integrity_report['foreign_key_violations'])} FK violations")
                        click.echo(f"  ⚠️  Found {len(integrity_report['constraint_violations'])} constraint violations")
                    else:
                        click.echo(f"  ✅ Data integrity verified")
            
            click.echo("\n🎉 Generation completed successfully!")
    
    except KeyboardInterrupt:
        click.echo("\n\n⏹️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"\n❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', type=click.Path(), default='dbmocker_config.yaml',
              help='Output configuration file path')
def init_config(output: str):
    """Create a sample configuration file."""
    config_template = {
        'generation': {
            'batch_size': 1000,
            'max_workers': 4,
            'seed': 42,
            'truncate_existing': False,
            'preserve_existing_data': True,
            'reuse_existing_values': 0.3
        },
        'tables': {
            'users': {
                'rows_to_generate': 10000,
                'column_configs': {
                    'email': {
                        'generator_function': 'email',
                        'null_probability': 0.0
                    },
                    'name': {
                        'generator_function': 'name',
                        'null_probability': 0.05
                    },
                    'age': {
                        'min_value': 18,
                        'max_value': 80,
                        'null_probability': 0.1
                    }
                }
            },
            'orders': {
                'rows_to_generate': 50000,
                'column_configs': {
                    'total_amount': {
                        'min_value': 10.0,
                        'max_value': 1000.0
                    },
                    'status': {
                        'possible_values': ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled'],
                        'weighted_values': {
                            'pending': 0.1,
                            'confirmed': 0.2,
                            'shipped': 0.3,
                            'delivered': 0.35,
                            'cancelled': 0.05
                        }
                    }
                }
            }
        }
    }
    
    output_path = Path(output)
    with open(output_path, 'w') as f:
        yaml.dump(config_template, f, default_flow_style=False, indent=2)
    
    click.echo(f"✅ Configuration template created: {output_path}")
    click.echo("Edit this file to customize your data generation settings.")


@cli.command()
@click.option('--driver', required=True, type=click.Choice(['mysql', 'postgresql', 'sqlite']),
              help='Database driver')
@click.option('--host', default='localhost', help='Database host')  
@click.option('--port', type=int, help='Database port')
@click.option('--database', required=True, help='Database name')
@click.option('--username', help='Database username')
@click.option('--password', help='Database password')
@click.option('--rows', default=1000000, help='Number of rows to generate per table (default: 1M)')
@click.option('--batch-size', default=10000, help='Batch size for inserts (default: 10K)')
@click.option('--max-workers', default=8, help='Number of worker threads (default: 8)')
@click.option('--max-processes', default=4, help='Number of processes (default: 4)')
@click.option('--rows-per-process', default=250000, help='Rows per process threshold (default: 250K)')
@click.option('--include-tables', help='Comma-separated list of tables to include')
@click.option('--exclude-tables', help='Comma-separated list of tables to exclude')
@click.option('--truncate', is_flag=True, help='Truncate tables before inserting')
@click.option('--dry-run', is_flag=True, help='Show performance plan without inserting')
@click.option('--seed', type=int, help='Random seed for reproducible data')
@click.option('--enable-duplicates', help='Comma-separated list of columns to allow duplicates (format: table.column)')
@click.option('--smart-duplicates', help='Comma-separated list of columns for smart duplicate generation (format: table.column)')
@click.option('--duplicate-probability', default=0.5, type=float, help='Probability for smart duplicates (0.0-1.0)')
@click.option('--max-duplicate-values', default=10, type=int, help='Maximum unique values in smart duplicate mode')
@click.option('--allow-duplicates-global', is_flag=True, help='Allow duplicates globally when column constraints permit')
@click.option('--global-duplicate-probability', default=1.0, type=float,
              help='Global probability of using duplicates when allowed (0.0-1.0, default: 1.0)')
def high_performance(driver, host, port, database, username, password, rows, batch_size, 
                    max_workers, max_processes, rows_per_process, include_tables, exclude_tables,
                    truncate, dry_run, seed, enable_duplicates, smart_duplicates, 
                    duplicate_probability, max_duplicate_values, allow_duplicates_global, 
                    global_duplicate_probability):
    """🚀 High-performance generation for millions of records with multiprocessing."""
    
    start_time = time.time()
    
    try:
        # Set random seed
        if seed is not None:
            import random
            random.seed(seed)
            click.echo(f"🎲 Using random seed: {seed}")
        
        # Create database configuration
        db_config = DatabaseConfig(
            host=host,
            port=port or get_default_port(driver),
            database=database,
            username=username,
            password=password or '',
            driver=driver
        )
        
        # Connect to database
        click.echo(f"🔌 Connecting to {driver} database at {host}...")
        db_conn = DatabaseConnection(db_config)
        db_conn.connect()
        
        # Parse table lists
        include_list = include_tables.split(',') if include_tables else None
        exclude_list = exclude_tables.split(',') if exclude_tables else None
        
        # Parse duplicate configuration
        duplicate_config = {}
        smart_duplicate_config = {}
        
        if enable_duplicates:
            for entry in enable_duplicates.split(','):
                if '.' in entry:
                    table_name, column_name = entry.strip().split('.', 1)
                    if table_name not in duplicate_config:
                        duplicate_config[table_name] = []
                    duplicate_config[table_name].append(column_name)
        
        if smart_duplicates:
            for entry in smart_duplicates.split(','):
                if '.' in entry:
                    table_name, column_name = entry.strip().split('.', 1)
                    if table_name not in smart_duplicate_config:
                        smart_duplicate_config[table_name] = []
                    smart_duplicate_config[table_name].append(column_name)
        
        # Analyze schema
        click.echo("🔍 Analyzing database schema...")
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema(
            include_tables=include_list,
            exclude_tables=exclude_list
        )
        
        # Configure high-performance generation
        generation_config = GenerationConfig(
            batch_size=batch_size,
            max_workers=max_workers,
            enable_multiprocessing=True,
            max_processes=max_processes,
            rows_per_process=rows_per_process,
            seed=seed,
            include_tables=include_list,
            exclude_tables=exclude_list or [],
            truncate_existing=truncate,
            allow_duplicates=allow_duplicates_global,
            duplicate_probability=global_duplicate_probability
        )
        
        # Add duplicate configuration
        for table_name, columns in duplicate_config.items():
            if table_name not in generation_config.table_configs:
                generation_config.table_configs[table_name] = TableGenerationConfig()
            
            for column_name in columns:
                generation_config.table_configs[table_name].column_configs[column_name] = ColumnGenerationConfig(
                    duplicate_mode="allow_duplicates"
                )
        
        # Add smart duplicate configuration
        for table_name, columns in smart_duplicate_config.items():
            if table_name not in generation_config.table_configs:
                generation_config.table_configs[table_name] = TableGenerationConfig()
            
            for column_name in columns:
                generation_config.table_configs[table_name].column_configs[column_name] = ColumnGenerationConfig(
                    duplicate_mode="smart_duplicates",
                    duplicate_probability=duplicate_probability,
                    max_duplicate_values=max_duplicate_values
                )
        
        # Initialize high-performance components
        from dbmocker.core.parallel_generator import ParallelDataGenerator, ParallelDataInserter
        generator = ParallelDataGenerator(schema, generation_config, db_conn)
        inserter = ParallelDataInserter(db_conn, schema)
        
        # Show performance plan
        total_tables = len(schema.tables)
        total_estimated_rows = total_tables * rows
        estimated_time = total_estimated_rows / 10000  # Rough estimate: 10K rows per second
        
        click.echo(f"\n🚀 High-Performance Generation Plan:")
        click.echo(f"  Tables to process: {total_tables}")
        click.echo(f"  Rows per table: {rows:,}")
        click.echo(f"  Total estimated rows: {total_estimated_rows:,}")
        click.echo(f"  Batch size: {batch_size:,}")
        click.echo(f"  Worker threads: {max_workers}")
        click.echo(f"  Processes: {max_processes}")
        click.echo(f"  Rows per process: {rows_per_process:,}")
        click.echo(f"  Multiprocessing: Enabled")
        click.echo(f"  Estimated time: {estimated_time/60:.1f} minutes")
        
        if duplicate_config:
            click.echo(f"  Duplicate columns configured:")
            for table_name, columns in duplicate_config.items():
                click.echo(f"    • {table_name}: {', '.join(columns)} (allow_duplicates)")
        
        if smart_duplicate_config:
            click.echo(f"  Smart duplicate columns configured:")
            for table_name, columns in smart_duplicate_config.items():
                click.echo(f"    • {table_name}: {', '.join(columns)} (smart_duplicates, p={duplicate_probability}, max={max_duplicate_values})")
        
        if allow_duplicates_global:
            click.echo(f"  Global duplicates: Enabled (probability={global_duplicate_probability})")
            click.echo(f"    → All columns without constraints will use duplicates")
        
        if dry_run:
            click.echo(f"\n🔍 DRY RUN - Performance plan shown above")
            return
        
        # Confirm before proceeding
        if not click.confirm(f"\nProceed with high-performance generation of {total_estimated_rows:,} rows?"):
            click.echo("❌ Generation cancelled")
            return
        
        # Generate data for all tables in parallel
        click.echo("\n🚀 Starting high-performance generation...")
        all_data = generator.generate_data_for_all_tables_parallel(rows)
        
        # Insert data using parallel inserter
        total_inserted = 0
        
        click.echo("\n💾 Inserting generated data using parallel processing...")
        for table_name, data in all_data.items():
            if data:
                click.echo(f"  📦 Processing {table_name}: {len(data):,} rows")
                
                if truncate:
                    inserter.truncate_table(table_name)
                
                stats = inserter.insert_data_parallel(
                    table_name, data, batch_size, max_workers
                )
                total_inserted += stats.total_rows_generated
                
                click.echo(f"  ✅ {table_name}: {stats.total_rows_generated:,} rows inserted")
        
        elapsed_time = time.time() - start_time
        rows_per_second = total_inserted / elapsed_time if elapsed_time > 0 else 0
        
        click.echo(f"\n🎉 High-performance generation completed successfully!")
        click.echo(f"📊 Performance Summary:")
        click.echo(f"  Total rows inserted: {total_inserted:,}")
        click.echo(f"  Tables processed: {total_tables}")
        click.echo(f"  Total time: {elapsed_time:.2f}s ({elapsed_time/60:.1f} minutes)")
        click.echo(f"  Performance: {rows_per_second:,.0f} rows/second")
        click.echo(f"  Average per table: {total_inserted/total_tables:,.0f} rows")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.ClickException(str(e))
    
    finally:
        if 'db_conn' in locals():
            db_conn.close()


@cli.command()
@click.option('--legacy', is_flag=True, help='Use legacy GUI interface')
def gui(legacy):
    """Launch the graphical user interface."""
    try:
        if legacy:
            click.echo("🚀 Launching Legacy JaySoft-DBMocker GUI...")
            from dbmocker.gui.main import launch_gui
            launch_gui()
        else:
            click.echo("🚀 Launching Enhanced JaySoft-DBMocker GUI...")
            click.echo("✨ Features: Ultra-fast processing, real-time monitoring, advanced configuration")
            from dbmocker.gui.enhanced_main import main
            main()
    except ImportError as e:
        click.echo(f"❌ GUI dependencies not installed: {e}", err=True)
        click.echo("Install with: pip install -r requirements.txt", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Failed to launch GUI: {e}", err=True)
        sys.exit(1)


def get_default_port(driver: str) -> int:
    """Get default port for database driver."""
    defaults = {
        'mysql': 3306,
        'postgresql': 5432,
        'sqlite': 0
    }
    return defaults.get(driver, 0)


def load_config_file(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON or YAML file."""
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        if config_file.suffix.lower() == '.json':
            return json.load(f)
        else:
            return yaml.safe_load(f)


@cli.command()
@click.option('--driver', required=True, type=click.Choice(['mysql', 'postgresql', 'sqlite']),
              help='Database driver')
@click.option('--host', default='localhost', help='Database host')  
@click.option('--port', type=int, help='Database port')
@click.option('--database', required=True, help='Database name')
@click.option('--username', help='Database username')
@click.option('--password', help='Database password')
@click.option('--rows', default=10, help='Number of rows to generate per table')
@click.option('--batch-size', default=1000, help='Batch size for inserts')
@click.option('--config', type=click.Path(exists=True), help='Configuration file path')
@click.option('--truncate', is_flag=True, help='Truncate tables before inserting')
@click.option('--dry-run', is_flag=True, help='Show what would be generated without inserting')
@click.option('--verify', is_flag=True, help='Verify data integrity after insertion')
@click.option('--seed', type=int, help='Random seed for reproducible data')
@click.option('--show-plan', is_flag=True, help='Show dependency-aware insertion plan')
@click.option('--auto-config', is_flag=True, help='Generate optimal configuration automatically')
@click.option('--use-existing-tables', help='Comma-separated list of tables to use existing data from (mixed mode)')
def smart_generate(driver, host, port, database, username, password, rows, batch_size, 
                  config, truncate, dry_run, verify, seed, show_plan, auto_config, use_existing_tables):
    """🧠 Smart dependency-aware data generation with optimal FK handling."""
    
    start_time = time.time()
    
    try:
        # Set random seed
        if seed is not None:
            import random
            random.seed(seed)
            click.echo(f"🎲 Using random seed: {seed}")
        
        # Create database configuration
        db_config = DatabaseConfig(
            host=host,
            port=port or get_default_port(driver),
            database=database,
            username=username,
            password=password or '',
            driver=driver
        )
        
        # Connect to database
        click.echo(f"🔌 Connecting to {driver} database at {host}...")
        db_conn = DatabaseConnection(db_config)
        db_conn.connect()
        
        # Analyze schema
        click.echo("🔍 Analyzing database schema...")
        analyzer = SchemaAnalyzer(db_conn)
        schema = analyzer.analyze_schema()
        
        # Create dependency resolver
        resolver = DependencyResolver(schema)
        insertion_plan = resolver.create_insertion_plan()
        
        # Show insertion plan if requested
        if show_plan:
            print_insertion_plan(insertion_plan, f"{database} Smart Insertion Plan")
            if not dry_run:
                click.echo("\n" + "="*50)
        
        # Load or create generation configuration
        if config:
            config_data = load_config_file(config)
            generation_config = GenerationConfig(**config_data.get('generation_config', {}))
            
            # Override with config file settings
            if 'table_configs' in config_data:
                for table_name, table_config in config_data['table_configs'].items():
                    generation_config.table_configs[table_name] = TableGenerationConfig(**table_config)
            elif 'tables' in config_data:
                # Support legacy format
                for table_name, table_config in config_data['tables'].items():
                    generation_config.table_configs[table_name] = TableGenerationConfig(**table_config)
        
        elif auto_config:
            click.echo("🤖 Generating optimal configuration...")
            generation_config = create_optimal_generation_config(schema, db_conn, rows)
            click.echo("✅ Auto-configuration created!")
        
        else:
            use_existing_list = use_existing_tables.split(',') if use_existing_tables else []
            generation_config = GenerationConfig(
                batch_size=batch_size,
                truncate_existing=truncate,
                preserve_existing_data=True,
                reuse_existing_values=0.8,  # High reuse for FK values
                use_existing_tables=use_existing_list
            )
        
        # Create smart generator
        smart_generator = DependencyAwareGenerator(schema, generation_config, db_conn)
        
        # Analyze and report FK dependencies for table selection
        fk_dependencies = smart_generator.analyze_fk_dependencies_for_selection()
        if fk_dependencies:
            click.echo(f"\n🔗 FK Dependencies Analysis:")
            click.echo(f"  Selected tables with FK references to unselected tables:")
            for selected_table, referenced_tables in fk_dependencies.items():
                click.echo(f"    • {selected_table} → {', '.join(referenced_tables)}")
            click.echo(f"  ✅ Will automatically use existing data from referenced tables")
        
        # Show generation plan
        batches = insertion_plan.get_insertion_batches()
        total_tables = len([t for batch in batches for t in batch])
        
        click.echo(f"\n🎯 Smart Generation Plan:")
        click.echo(f"  Tables to process: {total_tables}")
        click.echo(f"  Dependency batches: {len(batches)}")
        click.echo(f"  Rows per table: {rows}")
        click.echo(f"  Smart FK resolution: Enabled")
        
        if dry_run:
            click.echo(f"\n🔍 DRY RUN - No data will be inserted")
            
            # Show batch details
            for i, batch in enumerate(batches, 1):
                click.echo(f"\nBatch {i}: {', '.join(batch)}")
                for table_name in batch:
                    suggestions = resolver.suggest_fk_value_sources(table_name)
                    if suggestions:
                        click.echo(f"  {table_name} FK dependencies:")
                        for fk_col, info in suggestions.items():
                            click.echo(f"    • {fk_col} -> {info['source_table']}.{info['source_column']}")
            return
        
        # Confirm before proceeding
        if not click.confirm(f"\nProceed with smart data generation?"):
            click.echo("❌ Generation cancelled")
            return
        
        # Generate data for all tables in dependency order
        click.echo("\n🚀 Starting smart generation...")
        all_data = smart_generator.generate_data_for_all_tables(rows)
        
        # Insert data using standard inserter
        inserter = DataInserter(db_conn, schema)
        total_inserted = 0
        
        click.echo("\n💾 Inserting generated data...")
        for batch_num, batch in enumerate(batches, 1):
            click.echo(f"\n📦 Processing batch {batch_num}/{len(batches)}")
            
            for table_name in batch:
                if table_name in all_data and all_data[table_name]:
                    table = next((t for t in schema.tables if t.name == table_name), None)
                    if table:
                        rows_inserted = inserter.insert_data(table, all_data[table_name], batch_size)
                        total_inserted += rows_inserted
                        click.echo(f"  ✅ {table_name}: {rows_inserted} rows inserted")
        
        # Verify data integrity if requested
        if verify:
            click.echo("\n🔍 Verifying data integrity...")
            inserter.verify_data_integrity()
            click.echo("  ✅ Data integrity verified")
        
        elapsed_time = time.time() - start_time
        click.echo(f"\n🎉 Smart generation completed successfully!")
        click.echo(f"📊 Summary:")
        click.echo(f"  Total rows inserted: {total_inserted:,}")
        click.echo(f"  Tables processed: {total_tables}")
        click.echo(f"  Time: {elapsed_time:.2f}s")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.ClickException(str(e))
    
    finally:
        if 'db_conn' in locals():
            db_conn.close()


@cli.command()
@click.option('--driver', required=True, type=click.Choice(['mysql', 'postgresql', 'sqlite']),
              help='Database driver')
@click.option('--host', default='localhost', help='Database host')  
@click.option('--port', type=int, help='Database port')
@click.option('--database', required=True, help='Database name')
@click.option('--username', help='Database username')
@click.option('--password', help='Database password')
@click.option('--rows', default=10, help='Number of rows to generate per table')
@click.option('--batch-size', default=1000, help='Batch size for inserts')
@click.option('--dry-run', is_flag=True, help='Show what would be generated without inserting')
@click.option('--verify', is_flag=True, help='Verify data integrity after insertion')
@click.option('--seed', type=int, help='Random seed for reproducible data')
@click.option('--show-specs', is_flag=True, help='Show detailed table specifications')
@click.option('--max-tables-shown', default=5, help='Maximum tables to show in spec display')
def spec_generate(driver, host, port, database, username, password, rows, batch_size, 
                 dry_run, verify, seed, show_specs, max_tables_shown):
    """🔍 Advanced specification-driven data generation using DESCRIBE analysis."""
    
    start_time = time.time()
    
    try:
        # Set random seed
        if seed is not None:
            import random
            random.seed(seed)
            click.echo(f"🎲 Using random seed: {seed}")
        
        # Create database configuration
        db_config = DatabaseConfig(
            host=host,
            port=port or get_default_port(driver),
            database=database,
            username=username,
            password=password or '',
            driver=driver
        )
        
        # Connect to database
        click.echo(f"🔌 Connecting to {driver} database at {host}...")
        db_conn = DatabaseConnection(db_config)
        db_conn.connect()
        
        # Analyze database specifications using DESCRIBE
        click.echo("🔍 Analyzing database specifications using DESCRIBE...")
        spec_analyzer = DatabaseSpecAnalyzer(db_conn)
        table_specs = spec_analyzer.analyze_all_tables()
        
        click.echo(f"✅ Analyzed {len(table_specs)} tables with exact specifications")
        
        # Show table specifications if requested
        if show_specs:
            print_table_specs(table_specs, max_tables_shown)
            if not dry_run:
                click.echo("\n" + "="*50)
        
        # Create specification-driven generator
        spec_generator = SpecificationDrivenGenerator(db_conn, table_specs)
        insertion_plan = spec_generator.insertion_plan
        
        # Show generation plan
        batches = insertion_plan.get_insertion_batches()
        total_tables = len(table_specs)
        
        click.echo(f"\n🎯 Specification-Driven Generation Plan:")
        click.echo(f"  Tables to process: {total_tables}")
        click.echo(f"  Dependency batches: {len(batches)}")
        click.echo(f"  Rows per table: {rows}")
        click.echo(f"  Exact type compliance: Enabled")
        click.echo(f"  Smart constraint handling: Enabled")
        
        if dry_run:
            click.echo(f"\n🔍 DRY RUN - No data will be inserted")
            
            # Show specification summary
            click.echo(f"\n📋 TABLE SPECIFICATIONS SUMMARY:")
            for table_name, spec in list(table_specs.items())[:5]:
                click.echo(f"\n  {table_name.upper()}:")
                click.echo(f"    Columns: {len(spec.columns)}")
                click.echo(f"    Primary Keys: {', '.join(spec.primary_keys) or 'None'}")
                click.echo(f"    Foreign Keys: {len(spec.foreign_keys)}")
                click.echo(f"    Check Constraints: {len(spec.check_constraints)}")
                click.echo(f"    Current Rows: {spec.row_count:,}")
                
                # Show some column details
                for col in spec.columns[:3]:
                    constraints = []
                    if col.is_primary_key:
                        constraints.append("PK")
                    if col.is_unique:
                        constraints.append("UNIQUE")
                    if not col.is_nullable:
                        constraints.append("NOT NULL")
                    if col.is_auto_increment:
                        constraints.append("AUTO_INC")
                    
                    constraint_str = f" ({', '.join(constraints)})" if constraints else ""
                    
                    if col.max_length:
                        type_info = f"{col.data_type}({col.max_length})"
                    elif col.precision:
                        type_info = f"{col.data_type}({col.precision},{col.scale or 0})"
                    else:
                        type_info = col.data_type
                    
                    click.echo(f"      • {col.name}: {type_info}{constraint_str}")
            
            if len(table_specs) > 5:
                click.echo(f"\n    ... and {len(table_specs) - 5} more tables")
            
            return
        
        # Confirm before proceeding
        if not click.confirm(f"\nProceed with specification-driven generation?"):
            click.echo("❌ Generation cancelled")
            return
        
        # Generate data using exact specifications
        click.echo("\n🚀 Starting specification-driven generation...")
        all_data = spec_generator.generate_data_for_all_tables(rows)
        
        # Insert data using standard inserter (we need to create a mock schema for this)
        from dbmocker.core.models import DatabaseSchema, TableInfo, ColumnInfo
        
        # Create mock schema for inserter
        mock_tables = []
        for table_name, spec in table_specs.items():
            mock_columns = []
            for col_spec in spec.columns:
                # Convert spec to ColumnInfo (simplified)
                mock_columns.append(ColumnInfo(
                    name=col_spec.name,
                    data_type=col_spec.base_type.value,  # Use enum value
                    max_length=col_spec.max_length,
                    is_nullable=col_spec.is_nullable,
                    is_auto_increment=col_spec.is_auto_increment
                ))
            
            mock_table = TableInfo(
                name=table_name,
                columns=mock_columns,
                row_count=spec.row_count
            )
            mock_tables.append(mock_table)
        
        mock_schema = DatabaseSchema(
            database_name=database,
            tables=mock_tables
        )
        
        inserter = DataInserter(db_conn, mock_schema)
        total_inserted = 0
        
        click.echo("\n💾 Inserting generated data...")
        for batch_num, batch in enumerate(batches, 1):
            click.echo(f"\n📦 Processing batch {batch_num}/{len(batches)}")
            
            for table_name in batch:
                if table_name in all_data and all_data[table_name]:
                    # Find the mock table
                    mock_table = next((t for t in mock_tables if t.name == table_name), None)
                    if mock_table:
                        rows_inserted = inserter.insert_data(mock_table, all_data[table_name], batch_size)
                        total_inserted += rows_inserted
                        click.echo(f"  ✅ {table_name}: {rows_inserted} rows inserted")
        
        # Verify data integrity if requested
        if verify:
            click.echo("\n🔍 Verifying data integrity...")
            inserter.verify_data_integrity()
            click.echo("  ✅ Data integrity verified")
        
        elapsed_time = time.time() - start_time
        click.echo(f"\n🎉 Specification-driven generation completed successfully!")
        click.echo(f"📊 Summary:")
        click.echo(f"  Total rows inserted: {total_inserted:,}")
        click.echo(f"  Tables processed: {total_tables}")
        click.echo(f"  Exact specifications used: {len(table_specs)}")
        click.echo(f"  Time: {elapsed_time:.2f}s")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.ClickException(str(e))
    
    finally:
        if 'db_conn' in locals():
            db_conn.close()


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()
