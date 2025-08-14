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
from dbmocker.core.models import GenerationConfig, TableGenerationConfig
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
@click.option('--include-tables', help='Comma-separated list of tables to include')
@click.option('--exclude-tables', help='Comma-separated list of tables to exclude')
@click.option('--truncate/--no-truncate', default=False, help='Truncate existing data before insert')
@click.option('--seed', type=int, help='Random seed for reproducible generation')
@click.option('--dry-run', is_flag=True, help='Generate data but do not insert into database')
@click.option('--verify/--no-verify', default=True, help='Verify data integrity after insertion')
@click.option('--analyze-existing-data/--no-analyze-existing-data', default=False,
              help='Analyze existing data for realistic generation patterns')
@click.option('--pattern-sample-size', default=1000, type=int,
              help='Sample size for existing data pattern analysis')
def generate(host: str, port: int, database: str, username: str, password: str,
            driver: str, config: Optional[str], rows: int, batch_size: int,
            include_tables: Optional[str], exclude_tables: Optional[str],
            truncate: bool, seed: Optional[int], dry_run: bool, verify: bool,
            analyze_existing_data: bool, pattern_sample_size: int):
    """Generate and insert mock data into database."""
    try:
        # Parse table lists
        include_list = include_tables.split(',') if include_tables else None
        exclude_list = exclude_tables.split(',') if exclude_tables else None
        
        # Load configuration
        generation_config = GenerationConfig(
            batch_size=batch_size,
            seed=seed,
            include_tables=include_list,
            exclude_tables=exclude_list or [],
            truncate_existing=truncate
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
            
            # Initialize components
            generator = DataGenerator(schema, generation_config, db_conn)
            inserter = DataInserter(db_conn, schema)
            
            # Sort tables by dependencies
            dependencies = schema.get_table_dependencies()
            sorted_tables = inserter._sort_tables_by_dependencies([t.name for t in schema.tables])
            
            click.echo(f"\n🎯 Generation Plan:")
            click.echo(f"  Tables to process: {len(sorted_tables)}")
            click.echo(f"  Rows per table: {rows}")
            click.echo(f"  Batch size: {batch_size}")
            click.echo(f"  Processing order: {' → '.join(sorted_tables)}")
            
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
                
                generated_data = generator.generate_data_for_table(table_name, table_rows)
                total_rows_generated += len(generated_data)
                
                generation_time = time.time() - table_start_time
                click.echo(f"  ✅ Generated {len(generated_data):,} rows in {generation_time:.2f}s")
                
                # Insert data
                if not dry_run:
                    click.echo(f"  💾 Inserting data...")
                    insert_start_time = time.time()
                    
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
def gui():
    """Launch the graphical user interface."""
    try:
        from dbmocker.gui.main import launch_gui
        click.echo("🚀 Launching JaySoft-DBMocker GUI...")
        launch_gui()
    except ImportError:
        click.echo("❌ GUI dependencies not installed. Install with: pip install jaysoft-dbmocker[gui]", err=True)
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
def smart_generate(driver, host, port, database, username, password, rows, batch_size, 
                  config, truncate, dry_run, verify, seed, show_plan, auto_config):
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
            generation_config = GenerationConfig(
                batch_size=batch_size,
                truncate_existing=truncate,
                preserve_existing_data=True,
                reuse_existing_values=0.8  # High reuse for FK values
            )
        
        # Create smart generator
        smart_generator = DependencyAwareGenerator(schema, generation_config, db_conn)
        
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
