"""
Enhanced CLI for DBMocker with ultra-fast processing capabilities.
Supports millions of records generation with optimized performance options.
"""

import click
import logging
import time
import sys
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

from .core.database import DatabaseConnection, DatabaseConfig
from .core.analyzer import SchemaAnalyzer
from .core.enhanced_models import (
    EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy,
    create_high_performance_config, create_bulk_generation_request
)
from .core.ultra_fast_processor import UltraFastProcessor
from .core.high_performance_generator import HighPerformanceGenerator


# Configure logging
def setup_logging(verbose: bool = False, quiet: bool = False):
    """Setup logging configuration."""
    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Reduce sqlalchemy logging
    logging.getLogger('sqlalchemy').setLevel(logging.WARNING)


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--quiet', '-q', is_flag=True, help='Quiet mode (warnings only)')
@click.pass_context
def cli(ctx, verbose, quiet):
    """
    DBMocker Enhanced CLI - Ultra-fast database mock data generation.
    
    Optimized for generating millions of records with maximum performance.
    """
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['quiet'] = quiet
    setup_logging(verbose, quiet)


@cli.command()
@click.option('--host', '-h', default='', help='Database host (not required for SQLite)')
@click.option('--port', '-p', type=int, default=0, help='Database port (not required for SQLite)')
@click.option('--database', '-d', required=True, help='Database name')
@click.option('--username', '-u', default='', help='Database username (not required for SQLite)')
@click.option('--password', '-w', default='', help='Database password (not required for SQLite)')
@click.option('--driver', type=click.Choice(['postgresql', 'mysql', 'sqlite']), 
              default='postgresql', help='Database driver')
@click.option('--table', '-t', help='Specific table name (default: all tables)')
@click.option('--rows', '-r', type=int, default=1000, help='Number of rows per table')
@click.option('--performance-mode', 
              type=click.Choice(['standard', 'high_speed', 'memory_efficient', 'balanced', 'ultra_high']),
              default='balanced', help='Performance optimization mode')
@click.option('--enable-duplicates', is_flag=True, help='Enable duplicate value generation')
@click.option('--duplicate-strategy',
              type=click.Choice(['generate_new', 'allow_simple', 'smart_duplicates', 'cached_pool', 'fast_data_reuse']),
              default='smart_duplicates', help='Duplicate handling strategy')
@click.option('--batch-size', type=int, help='Batch size for operations (auto-calculated if not set)')
@click.option('--max-workers', type=int, help='Maximum number of worker threads (auto-calculated if not set)')
@click.option('--streaming', is_flag=True, help='Use streaming mode for very large datasets')
@click.option('--seed', type=int, help='Random seed for reproducible data')
@click.option('--truncate', is_flag=True, help='Truncate tables before inserting data')
@click.option('--output-format', type=click.Choice(['database', 'json', 'csv']),
              default='database', help='Output format')
@click.option('--output-path', help='Output file path (for non-database formats)')
@click.option('--config-file', help='Configuration file path (JSON/YAML)')
@click.option('--dry-run', is_flag=True, help='Dry run - analyze only, do not generate data')
@click.option('--sample-size', type=int, default=10000, help='Sample size for fast data reuse (default: 10000)')
@click.option('--reuse-probability', type=float, default=0.95, help='Probability of reusing data (default: 0.95)')
@click.option('--progress-interval', type=int, default=1000, help='Progress update interval in rows (default: 1000)')
@click.pass_context
def generate(ctx, host, port, database, username, password, driver, table, rows,
             performance_mode, enable_duplicates, duplicate_strategy, batch_size, max_workers,
             streaming, seed, truncate, output_format, output_path, config_file, dry_run,
             sample_size, reuse_probability, progress_interval):
    """
    Generate mock data with ultra-fast performance.
    
    Examples:
    
    \b
    # Generate 1M records with ultra-high performance
    dbmocker generate -h localhost -p 5432 -d mydb -u user -w pass \\
                     --performance-mode ultra_high --rows 1000000 \\
                     --enable-duplicates --streaming
    
    \b
    # Generate for specific table with smart duplicates
    dbmocker generate -h localhost -p 5432 -d mydb -u user -w pass \\
                     --table users --rows 500000 \\
                     --duplicate-strategy smart_duplicates
    
    \b
    # Use fast data reuse for ultra-fast insertion of millions
    dbmocker generate -h localhost -p 5432 -d mydb -u user -w pass \\
                     --table users --rows 10000000 \\
                     --duplicate-strategy fast_data_reuse \\
                     --sample-size 50000 --reuse-probability 0.95
    
    \b
    # Use configuration file
    dbmocker generate --config-file config.yaml
    """
    start_time = time.time()
    
    try:
        # Load configuration from file if provided
        if config_file:
            config_data = load_config_file(config_file)
            # Override with CLI parameters
            config_data = merge_config_with_cli_params(config_data, locals())
        else:
            config_data = create_config_from_cli_params(locals())
        
        # Create database connection
        db_config = DatabaseConfig(
            host=config_data['host'],
            port=config_data['port'],
            database=config_data['database'],
            username=config_data['username'],
            password=config_data['password'],
            driver=config_data['driver']
        )
        
        click.echo(f"🔗 Connecting to {driver} database at {host}:{port}/{database}")
        
        with DatabaseConnection(db_config) as db_conn:
            db_conn.connect()
            
            # Analyze schema
            click.echo("🔍 Analyzing database schema...")
            analyzer = SchemaAnalyzer(db_conn)
            include_tables = [table] if table else None
            schema = analyzer.analyze_schema(include_tables=include_tables)
            
            if dry_run:
                display_schema_analysis(schema, table)
                return
            
            # Prepare generation configuration
            perf_mode = PerformanceMode(performance_mode)
            dup_strategy = DuplicateStrategy(duplicate_strategy) if enable_duplicates else DuplicateStrategy.GENERATE_NEW
            
            # Determine target tables and rows
            target_tables = {}
            if table:
                if schema.get_table(table):
                    target_tables[table] = rows
                else:
                    click.echo(f"❌ Table '{table}' not found in schema", err=True)
                    sys.exit(1)
            else:
                # All tables
                target_tables = {t.name: rows for t in schema.tables}
            
            # Create enhanced configuration
            config = create_high_performance_config(
                target_tables=target_tables,
                performance_mode=perf_mode,
                enable_duplicates=enable_duplicates,
                duplicate_strategy=dup_strategy,
                batch_size=batch_size,
                max_workers=max_workers,
                seed=seed,
                truncate_existing=truncate
            )
            
            # Apply fast data reuse settings if selected
            if duplicate_strategy == 'fast_data_reuse':
                config.duplicates.enable_fast_data_reuse = True
                config.duplicates.data_reuse_sample_size = sample_size
                config.duplicates.data_reuse_probability = reuse_probability
                config.duplicates.progress_update_interval = progress_interval
                config.duplicates.fast_insertion_mode = True
                config.duplicates.respect_constraints = True
                click.echo(f"🔄 Fast Data Reuse configured:")
                click.echo(f"   Sample size: {sample_size:,}")
                click.echo(f"   Reuse probability: {reuse_probability:.1%}")
                click.echo(f"   Progress interval: {progress_interval:,} rows")
            
            # Override streaming setting
            if streaming:
                config.generation_mode = "streaming"
            
            # Display generation plan
            display_generation_plan(target_tables, config)
            
            if not click.confirm("Proceed with data generation?"):
                click.echo("❌ Generation cancelled")
                return
            
            # Choose processor based on scale and performance mode
            total_rows = sum(target_tables.values())
            
            # Force ultra-fast processor for fast data reuse or large datasets
            use_ultra_fast = (perf_mode == PerformanceMode.ULTRA_HIGH or 
                            total_rows >= 1000000 or 
                            config.duplicates.enable_fast_data_reuse or
                            duplicate_strategy == 'fast_data_reuse')
            
            if use_ultra_fast:
                click.echo("⚡ Using Ultra-Fast Processor for maximum performance")
                processor = UltraFastProcessor(schema, config, db_conn)
                
                # Process each table
                for table_name, row_count in target_tables.items():
                    click.echo(f"\n🚀 Processing table '{table_name}': {row_count:,} rows")
                    
                    def progress_callback(table, current, total):
                        # Update every 1000 records as requested
                        if current % progress_interval == 0:
                            progress = (current / total) * 100
                            elapsed = time.time() - start_time
                            rate = current / elapsed if elapsed > 0 else 0
                            click.echo(f"  📊 {table}: {current:,}/{total:,} ({progress:.1f}%) | Rate: {rate:,.0f} rows/s")
                    
                    report = processor.process_millions_of_records(
                        table_name, row_count, progress_callback
                    )
                    
                    display_performance_report(table_name, report)
            
            else:
                click.echo("🔧 Using High-Performance Generator")
                generator = HighPerformanceGenerator(schema, config, db_conn)
                
                # Process each table
                for table_name, row_count in target_tables.items():
                    click.echo(f"\n📈 Generating data for table '{table_name}': {row_count:,} rows")
                    
                    stats = generator.generate_millions_of_records(
                        table_name, row_count, use_streaming=streaming
                    )
                    
                    display_generation_stats(table_name, stats)
            
            # Final summary
            total_time = time.time() - start_time
            total_rows_generated = sum(target_tables.values())
            overall_rate = total_rows_generated / total_time if total_time > 0 else 0
            
            click.echo(f"\n🎉 Generation completed successfully!")
            click.echo(f"📊 Total: {total_rows_generated:,} rows in {total_time:.2f}s ({overall_rate:,.0f} rows/sec)")
            click.echo(f"💾 Performance mode: {performance_mode}")
            click.echo(f"🔄 Duplicate strategy: {duplicate_strategy if enable_duplicates else 'disabled'}")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        if ctx.obj and ctx.obj.get('verbose'):
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option('--host', '-h', default='', help='Database host (not required for SQLite)')
@click.option('--port', '-p', type=int, default=0, help='Database port (not required for SQLite)')
@click.option('--database', '-d', required=True, help='Database name')
@click.option('--username', '-u', default='', help='Database username (not required for SQLite)')
@click.option('--password', '-w', default='', help='Database password (not required for SQLite)')
@click.option('--driver', type=click.Choice(['postgresql', 'mysql', 'sqlite']), 
              default='postgresql', help='Database driver')
@click.option('--table', '-t', help='Specific table name (default: all tables)')
@click.option('--output-format', type=click.Choice(['table', 'json', 'yaml']),
              default='table', help='Output format')
def analyze(host, port, database, username, password, driver, table, output_format):
    """
    Analyze database schema and provide optimization recommendations.
    
    Examples:
    
    \b
    # Analyze entire database
    dbmocker analyze -h localhost -p 5432 -d mydb -u user -w pass
    
    \b
    # Analyze specific table with JSON output
    dbmocker analyze -h localhost -p 5432 -d mydb -u user -w pass \\
                    --table users --output-format json
    """
    try:
        # Create database connection
        db_config = DatabaseConfig(
            host=host, port=port, database=database,
            username=username, password=password, driver=driver
        )
        
        click.echo(f"🔗 Connecting to {driver} database at {host}:{port}/{database}")
        
        with DatabaseConnection(db_config) as db_conn:
            db_conn.connect()
            
            # Analyze schema
            click.echo("🔍 Analyzing database schema...")
            analyzer = SchemaAnalyzer(db_conn)
            include_tables = [table] if table else None
            schema = analyzer.analyze_schema(include_tables=include_tables)
            
            # Display analysis
            if table:
                display_table_analysis(schema, table, output_format)
            else:
                display_schema_analysis(schema, None, output_format)
    
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', default='dbmocker_config.yaml', help='Output configuration file')
@click.option('--format', type=click.Choice(['yaml', 'json']), default='yaml', help='Configuration format')
@click.option('--performance-mode', 
              type=click.Choice(['standard', 'high_speed', 'memory_efficient', 'balanced', 'ultra_high']),
              default='balanced', help='Performance optimization mode')
@click.option('--include-examples', is_flag=True, help='Include example configurations')
def init_config(output, format, performance_mode, include_examples):
    """
    Initialize configuration file with optimized settings.
    
    Examples:
    
    \b
    # Create basic configuration
    dbmocker init-config
    
    \b
    # Create ultra-high performance configuration with examples
    dbmocker init-config --performance-mode ultra_high --include-examples
    """
    try:
        config = create_sample_config(performance_mode, include_examples)
        
        output_path = Path(output)
        
        if format == 'yaml':
            with open(output_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, indent=2)
        else:
            with open(output_path, 'w') as f:
                json.dump(config, f, indent=2)
        
        click.echo(f"✅ Configuration file created: {output_path}")
        click.echo(f"📝 Format: {format.upper()}")
        click.echo(f"⚙️  Performance mode: {performance_mode}")
        
        if include_examples:
            click.echo("📚 Examples included - review and customize as needed")
    
    except Exception as e:
        click.echo(f"❌ Error creating configuration: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--performance-mode', 
              type=click.Choice(['standard', 'high_speed', 'memory_efficient', 'balanced', 'ultra_high']),
              default='balanced', help='Performance mode to benchmark')
@click.option('--rows', type=int, default=100000, help='Number of rows for benchmark')
@click.option('--tables', type=int, default=5, help='Number of test tables')
def benchmark(performance_mode, rows, tables):
    """
    Run performance benchmarks to test system capabilities.
    
    Examples:
    
    \b
    # Basic benchmark
    dbmocker benchmark
    
    \b
    # Ultra-high performance benchmark with 1M rows
    dbmocker benchmark --performance-mode ultra_high --rows 1000000
    """
    try:
        click.echo(f"🏃 Running benchmark: {performance_mode} mode")
        click.echo(f"📊 Test parameters: {rows:,} rows × {tables} tables = {rows * tables:,} total rows")
        
        # TODO: Implement benchmark functionality
        # This would create temporary in-memory databases and test generation speed
        
        click.echo("🚧 Benchmark functionality coming soon!")
        click.echo("💡 Use 'dbmocker generate' with --dry-run to analyze performance potential")
    
    except Exception as e:
        click.echo(f"❌ Benchmark failed: {e}", err=True)
        sys.exit(1)


# Helper functions

def load_config_file(config_path: str) -> Dict[str, Any]:
    """Load configuration from file."""
    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(path, 'r') as f:
        if path.suffix.lower() in ['.yaml', '.yml']:
            return yaml.safe_load(f)
        elif path.suffix.lower() == '.json':
            return json.load(f)
        else:
            raise ValueError(f"Unsupported configuration format: {path.suffix}")


def merge_config_with_cli_params(config_data: Dict[str, Any], cli_params: Dict[str, Any]) -> Dict[str, Any]:
    """Merge configuration file data with CLI parameters."""
    # CLI parameters override config file
    for key, value in cli_params.items():
        if value is not None:
            config_data[key] = value
    
    return config_data


def create_config_from_cli_params(cli_params: Dict[str, Any]) -> Dict[str, Any]:
    """Create configuration from CLI parameters."""
    return {key: value for key, value in cli_params.items() if value is not None}


def display_schema_analysis(schema, specific_table: Optional[str] = None, format: str = 'table'):
    """Display schema analysis results."""
    if specific_table:
        table = schema.get_table(specific_table)
        if not table:
            click.echo(f"❌ Table '{specific_table}' not found")
            return
        
        click.echo(f"\n📋 Table Analysis: {specific_table}")
        click.echo(f"  Columns: {len(table.columns)}")
        click.echo(f"  Constraints: {len(table.constraints)}")
        click.echo(f"  Foreign Keys: {len(table.foreign_keys)}")
        
        if format == 'table':
            click.echo("\n  Columns:")
            for col in table.columns:
                nullable = "NULL" if col.is_nullable else "NOT NULL"
                click.echo(f"    {col.name}: {col.data_type.value} {nullable}")
    else:
        click.echo(f"\n📊 Schema Analysis: {schema.database_name}")
        click.echo(f"  Total Tables: {len(schema.tables)}")
        
        if format == 'table':
            click.echo("\n  Tables:")
            for table in schema.tables:
                click.echo(f"    {table.name}: {len(table.columns)} columns")


def display_generation_plan(target_tables: Dict[str, int], config):
    """Display generation plan."""
    total_rows = sum(target_tables.values())
    
    click.echo(f"\n📋 Generation Plan")
    click.echo(f"  Performance Mode: {config.performance.performance_mode}")
    click.echo(f"  Total Tables: {len(target_tables)}")
    click.echo(f"  Total Rows: {total_rows:,}")
    click.echo(f"  Batch Size: {config.performance.batch_size:,}")
    click.echo(f"  Max Workers: {config.performance.max_workers}")
    
    if config.duplicates.global_duplicate_enabled:
        click.echo(f"  Duplicate Strategy: {config.duplicates.global_duplicate_strategy}")
    
    click.echo(f"\n  Tables:")
    for table_name, rows in target_tables.items():
        click.echo(f"    {table_name}: {rows:,} rows")


def display_performance_report(table_name: str, report):
    """Display performance report."""
    click.echo(f"\n📊 Performance Report - {table_name}")
    click.echo(f"  Rows Generated: {report.total_rows_generated:,}")
    click.echo(f"  Time Taken: {report.total_time_seconds:.2f}s")
    click.echo(f"  Average Rate: {report.average_rows_per_second:,.0f} rows/sec")
    
    if hasattr(report, 'cache_hit_rate'):
        click.echo(f"  Cache Hit Rate: {report.cache_hit_rate:.1%}")


def display_generation_stats(table_name: str, stats):
    """Display generation statistics."""
    click.echo(f"\n📈 Generation Stats - {table_name}")
    click.echo(f"  Rows Generated: {stats.total_rows_generated:,}")
    click.echo(f"  Time Taken: {stats.total_time_seconds:.2f}s")
    
    if stats.total_time_seconds > 0:
        rate = stats.total_rows_generated / stats.total_time_seconds
        click.echo(f"  Average Rate: {rate:,.0f} rows/sec")


def create_sample_config(performance_mode: str, include_examples: bool) -> Dict[str, Any]:
    """Create sample configuration."""
    config = {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'database': 'your_database',
            'username': 'your_username',
            'password': 'your_password',
            'driver': 'postgresql'
        },
        'generation': {
            'performance_mode': performance_mode,
            'enable_duplicates': True,
            'duplicate_strategy': 'smart_duplicates',
            'batch_size': 25000 if performance_mode == 'ultra_high' else 10000,
            'max_workers': 8 if performance_mode == 'ultra_high' else 4,
            'seed': 42
        },
        'tables': {
            'users': {'rows': 100000},
            'orders': {'rows': 500000},
            'products': {'rows': 10000}
        }
    }
    
    if include_examples:
        config['examples'] = {
            'high_performance_example': {
                'performance_mode': 'ultra_high',
                'tables': {
                    'large_table': {'rows': 1000000}
                }
            },
            'memory_efficient_example': {
                'performance_mode': 'memory_efficient',
                'streaming': True,
                'chunk_size': 10000
            }
        }
    
    return config


if __name__ == '__main__':
    cli()
