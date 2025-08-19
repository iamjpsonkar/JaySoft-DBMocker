#!/usr/bin/env python3
"""
Complete Database Test Suite for DBMocker
Tests ALL tables with ALL operation combinations and verifies actual data insertion
"""

import sys
import time
import traceback
from itertools import product
from typing import Dict, Any, List, Tuple

# Add current directory to path
sys.path.insert(0, '.')

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.enhanced_cli import generate
from dbmocker.core.enhanced_models import PerformanceMode, DuplicateStrategy
from click.testing import CliRunner
import click

class CompleteDatabaseTestSuite:
    """Complete test suite for all tables and all operation combinations."""
    
    def __init__(self):
        self.db_config = DatabaseConfig(
            driver='mysql',
            host='127.0.0.1',
            port=3306,
            database='gringotts_test',
            username='root',
            password=''
        )
        
        # Test combinations
        self.performance_modes = ['standard', 'high_speed', 'memory_efficient', 'balanced', 'ultra_high']
        self.duplicate_strategies = ['generate_new', 'allow_simple', 'smart_duplicates', 'cached_pool', 'fast_data_reuse']
        
        # Results tracking
        self.results = []
        self.insertion_results = {}
        
        # Click runner setup
        self.runner = CliRunner()
        self.original_confirm = click.confirm
        click.confirm = lambda *args, **kwargs: True

    def get_all_tables(self) -> List[str]:
        """Get all tables in the database."""
        with DatabaseConnection(self.db_config) as db_conn:
            analyzer = SchemaAnalyzer(db_conn)
            schema = analyzer.analyze_schema(analyze_data_patterns=False)
            return [table.name for table in schema.tables]

    def get_table_row_count(self, table_name: str) -> int:
        """Get current row count for a table."""
        try:
            with DatabaseConnection(self.db_config) as db_conn:
                result = db_conn.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                return result[0][0] if result else 0
        except Exception as e:
            print(f"  ⚠️  Error counting rows in {table_name}: {e}")
            return 0

    def test_single_combination(self, table_name: str, performance_mode: str, duplicate_strategy: str) -> Dict[str, Any]:
        """Test a single table/performance/strategy combination."""
        start_time = time.time()
        
        # Get row count before
        rows_before = self.get_table_row_count(table_name)
        
        # Build CLI arguments
        args = [
            '--driver', 'mysql',
            '--host', '127.0.0.1',
            '--port', '3306',
            '--database', 'gringotts_test',
            '--username', 'root',
            '--password', '',
            '--table', table_name,
            '--rows', '1',  # Just 1 row for testing
            '--performance-mode', performance_mode,
            '--duplicate-strategy', duplicate_strategy
        ]
        
        # Add strategy-specific args
        if duplicate_strategy == 'fast_data_reuse':
            args.extend(['--sample-size', '100', '--reuse-probability', '0.8', '--progress-interval', '1000'])
        
        # Execute test
        try:
            result = self.runner.invoke(generate, args, catch_exceptions=True)
            success = result.exit_code == 0
            error_msg = str(result.exception) if result.exception else None
            
        except Exception as e:
            success = False
            error_msg = str(e)
        
        # Get row count after
        rows_after = self.get_table_row_count(table_name)
        rows_inserted = rows_after - rows_before
        
        elapsed_time = time.time() - start_time
        
        return {
            'table': table_name,
            'performance_mode': performance_mode,
            'duplicate_strategy': duplicate_strategy,
            'success': success,
            'rows_before': rows_before,
            'rows_after': rows_after,
            'rows_inserted': rows_inserted,
            'time_taken': elapsed_time,
            'error': error_msg
        }

    def run_complete_test(self):
        """Run complete test for all tables and all combinations."""
        print("🚀 Starting Complete Database Test Suite")
        print(f"📊 Database: mysql://root:@127.0.0.1:3306/gringotts_test")
        
        # Get all tables
        tables = self.get_all_tables()
        print(f"📋 Found {len(tables)} tables to test")
        print(f"🔧 Testing {len(self.performance_modes)} performance modes × {len(self.duplicate_strategies)} strategies")
        
        total_combinations = len(tables) * len(self.performance_modes) * len(self.duplicate_strategies)
        print(f"🎯 Total test combinations: {total_combinations}")
        
        # Show tables
        print(f"\n📋 Tables to test:")
        for i, table in enumerate(sorted(tables), 1):
            print(f"   {i:2d}. {table}")
        
        print(f"\n" + "="*80)
        
        test_count = 0
        successful_insertions = 0
        failed_insertions = 0
        
        # Test each table with each combination
        for table_name in sorted(tables):
            print(f"\n🏗️  TESTING TABLE: {table_name}")
            print("-" * 60)
            
            table_results = []
            
            for performance_mode, duplicate_strategy in product(self.performance_modes, self.duplicate_strategies):
                test_count += 1
                
                print(f"🧪 Test {test_count}/{total_combinations}: {performance_mode} + {duplicate_strategy}")
                
                result = self.test_single_combination(table_name, performance_mode, duplicate_strategy)
                table_results.append(result)
                
                if result['success'] and result['rows_inserted'] > 0:
                    successful_insertions += 1
                    status = f"✅ SUCCESS (+{result['rows_inserted']} rows)"
                elif result['success'] and result['rows_inserted'] == 0:
                    status = f"⚠️  NO INSERTION (exit 0 but 0 rows)"
                else:
                    failed_insertions += 1
                    status = f"❌ FAILED: {result['error'][:50] if result['error'] else 'Unknown error'}..."
                
                print(f"   {status} ({result['time_taken']:.2f}s)")
            
            # Summary for this table
            table_successes = sum(1 for r in table_results if r['success'] and r['rows_inserted'] > 0)
            table_failures = sum(1 for r in table_results if not r['success'] or r['rows_inserted'] == 0)
            
            print(f"\n📊 {table_name} Summary: {table_successes}/{len(table_results)} successful insertions")
            
            self.results.extend(table_results)
        
        # Final summary
        self.generate_final_report(successful_insertions, failed_insertions, total_combinations)

    def generate_final_report(self, successful_insertions: int, failed_insertions: int, total_combinations: int):
        """Generate comprehensive final report."""
        print(f"\n" + "="*80)
        print("📊 COMPLETE DATABASE TEST SUMMARY REPORT")
        print("="*80)
        
        successful_tests = sum(1 for r in self.results if r['success'])
        total_actual_insertions = sum(1 for r in self.results if r['rows_inserted'] > 0)
        
        print(f"🎯 Total Test Combinations: {total_combinations}")
        print(f"✅ Successful CLI Executions: {successful_tests} ({successful_tests/total_combinations*100:.1f}%)")
        print(f"📊 Actual Data Insertions: {total_actual_insertions} ({total_actual_insertions/total_combinations*100:.1f}%)")
        print(f"❌ Failed/No Insertion: {total_combinations - total_actual_insertions} ({(total_combinations - total_actual_insertions)/total_combinations*100:.1f}%)")
        
        # Performance mode analysis
        print(f"\n🚀 PERFORMANCE MODE ANALYSIS:")
        for mode in self.performance_modes:
            mode_results = [r for r in self.results if r['performance_mode'] == mode]
            mode_insertions = sum(1 for r in mode_results if r['rows_inserted'] > 0)
            print(f"   {mode:<18}: {mode_insertions}/{len(mode_results)} ({mode_insertions/len(mode_results)*100:.1f}%)")
        
        # Duplicate strategy analysis
        print(f"\n🔄 DUPLICATE STRATEGY ANALYSIS:")
        for strategy in self.duplicate_strategies:
            strategy_results = [r for r in self.results if r['duplicate_strategy'] == strategy]
            strategy_insertions = sum(1 for r in strategy_results if r['rows_inserted'] > 0)
            print(f"   {strategy:<18}: {strategy_insertions}/{len(strategy_results)} ({strategy_insertions/len(strategy_results)*100:.1f}%)")
        
        # Table analysis
        print(f"\n🏗️  TABLE ANALYSIS:")
        tables = sorted(set(r['table'] for r in self.results))
        for table in tables:
            table_results = [r for r in self.results if r['table'] == table]
            table_insertions = sum(1 for r in table_results if r['rows_inserted'] > 0)
            print(f"   {table:<30}: {table_insertions}/{len(table_results)} ({table_insertions/len(table_results)*100:.1f}%)")
        
        # Top performing combinations
        successful_results = [r for r in self.results if r['rows_inserted'] > 0]
        if successful_results:
            print(f"\n🏆 TOP PERFORMING COMBINATIONS (with actual insertions):")
            sorted_results = sorted(successful_results, key=lambda x: x['time_taken'])
            for i, result in enumerate(sorted_results[:10], 1):
                print(f"   {i:2d}. {result['table']:<25} | {result['performance_mode']:<15} | {result['duplicate_strategy']:<18} | {result['time_taken']:.2f}s")
        
        # Problematic combinations
        failed_results = [r for r in self.results if r['rows_inserted'] == 0]
        if failed_results:
            print(f"\n⚠️  PROBLEMATIC COMBINATIONS (no insertions):")
            # Group by error type
            error_groups = {}
            for result in failed_results:
                error_key = result['error'][:100] if result['error'] else 'No error but no insertion'
                if error_key not in error_groups:
                    error_groups[error_key] = []
                error_groups[error_key].append(result)
            
            for error, results in error_groups.items():
                print(f"\n   Error: {error}")
                print(f"   Affected: {len(results)} combinations")
                if len(results) <= 5:
                    for r in results:
                        print(f"     - {r['table']} | {r['performance_mode']} | {r['duplicate_strategy']}")
                else:
                    print(f"     - (showing first 3 of {len(results)})")
                    for r in results[:3]:
                        print(f"     - {r['table']} | {r['performance_mode']} | {r['duplicate_strategy']}")
        
        print(f"\n" + "="*80)

    def cleanup(self):
        """Cleanup resources."""
        click.confirm = self.original_confirm

def main():
    """Main test execution."""
    suite = CompleteDatabaseTestSuite()
    
    try:
        suite.run_complete_test()
    except KeyboardInterrupt:
        print(f"\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        traceback.print_exc()
    finally:
        suite.cleanup()

if __name__ == "__main__":
    main()
