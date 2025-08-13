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
    """JaySoft:DBMocker - Generate realistic mock data for SQL databases."""
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
def analyze(host: str, port: int, database: str, username: str, password: str,
           driver: str, output: Optional[str], include_tables: Optional[str],
           exclude_tables: Optional[str], analyze_patterns: bool):
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
                analyze_data_patterns=analyze_patterns
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
                click.echo(f"  • {table.name}: {table.row_count:,} rows, "
                          f"{len(table.columns)} columns{fk_text}")
            
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
def generate(host: str, port: int, database: str, username: str, password: str,
            driver: str, config: Optional[str], rows: int, batch_size: int,
            include_tables: Optional[str], exclude_tables: Optional[str],
            truncate: bool, seed: Optional[int], dry_run: bool, verify: bool):
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
            for table_name, table_config in config_data.get('tables', {}).items():
                generation_config.table_configs[table_name] = TableGenerationConfig(**table_config)
        
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
                analyze_data_patterns=True
            )
            
            # Initialize components
            generator = DataGenerator(schema, generation_config)
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
        click.echo("🚀 Launching JaySoft:DBMocker GUI...")
        launch_gui()
    except ImportError:
        click.echo("❌ GUI dependencies not installed. Install with: pip install jaysoft-dbmocker[gui]", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Failed to launch GUI: {e}", err=True)
        sys.exit(1)


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


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()
