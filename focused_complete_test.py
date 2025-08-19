#!/usr/bin/env python3
"""
Focused Complete Test Suite for DBMocker
Tests a representative sample of tables with all operation combinations
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

class FocusedCompleteTestSuite:
    """Focused test suite for representative tables with all operation combinations."""
    
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
        
        # Representative sample of tables (different complexity levels)
        self.test_tables = [
            'aggregator',           # Simple table
            'user',                 # Medium complexity with FK
            'merchant',             # Core business table  
            'transaction',          # Complex table with multiple FKs
            'order',                # High complexity table
            'payment_link',         # Another complex table
            'currency',             # Simple lookup table
            'refund'                # Complex financial table
        ]
        
        # Results tracking
        self.results = []
        
        # Click runner setup
        self.runner = CliRunner()
        self.original_confirm = click.confirm
        click.confirm = lambda *args, **kwargs: True

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information including row count and column count."""
        try:
            with DatabaseConnection(self.db_config) as db_conn:
                # Get row count with properly quoted table name
                quoted_table = db_conn.quote_identifier(table_name)
                result = db_conn.execute_query(f"SELECT COUNT(*) FROM {quoted_table}")
                row_count = result[0][0] if result else 0
                
                # Get column count
                result = db_conn.execute_query(f"DESCRIBE {quoted_table}")
                column_count = len(result) if result else 0
                
                return {
                    'row_count': row_count,
                    'column_count': column_count
                }
        except Exception as e:
            return {
                'row_count': 0,
                'column_count': 0,
                'error': str(e)
            }

    def test_single_combination(self, table_name: str, performance_mode: str, duplicate_strategy: str) -> Dict[str, Any]:
        """Test a single table/performance/strategy combination."""
        start_time = time.time()
        
        # Get row count before
        rows_before = self.get_table_info(table_name)['row_count']
        
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
        
        # Enable duplicates for all strategies except generate_new
        if duplicate_strategy != 'generate_new':
            args.append('--enable-duplicates')
        
        # Add strategy-specific args
        if duplicate_strategy == 'fast_data_reuse':
            args.extend(['--sample-size', '50', '--reuse-probability', '0.8'])
        
        # Execute test
        try:
            result = self.runner.invoke(generate, args, catch_exceptions=True)
            success = result.exit_code == 0
            error_msg = str(result.exception) if result.exception else None
            
        except Exception as e:
            success = False
            error_msg = str(e)
        
        # Get row count after
        rows_after = self.get_table_info(table_name)['row_count']
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

    def run_focused_test(self):
        """Run focused test for representative tables and all combinations."""
        print("🚀 Starting Focused Complete Database Test Suite")
        print(f"📊 Database: mysql://root:@127.0.0.1:3306/gringotts_test")
        print(f"🎯 Testing {len(self.test_tables)} representative tables")
        
        total_combinations = len(self.test_tables) * len(self.performance_modes) * len(self.duplicate_strategies)
        print(f"🔧 Total test combinations: {total_combinations}")
        
        # Show table info
        print(f"\n📋 Representative Tables to Test:")
        for i, table in enumerate(self.test_tables, 1):
            info = self.get_table_info(table)
            print(f"   {i}. {table:<25} ({info['row_count']:,} rows, {info['column_count']} columns)")
        
        print(f"\n" + "="*80)
        
        test_count = 0
        successful_insertions = 0
        
        # Test each table with each combination
        for table_name in self.test_tables:
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
                    status = f"❌ FAILED: {result['error'][:50] if result['error'] else 'Unknown error'}..."
                
                print(f"   {status} ({result['time_taken']:.2f}s)")
            
            # Summary for this table
            table_successes = sum(1 for r in table_results if r['success'] and r['rows_inserted'] > 0)
            
            print(f"\n📊 {table_name} Summary: {table_successes}/{len(table_results)} successful insertions")
            
            self.results.extend(table_results)
        
        # Final summary
        self.generate_final_report(total_combinations)

    def generate_final_report(self, total_combinations: int):
        """Generate comprehensive final report."""
        print(f"\n" + "="*80)
        print("📊 FOCUSED COMPLETE DATABASE TEST SUMMARY")
        print("="*80)
        
        successful_tests = sum(1 for r in self.results if r['success'])
        total_actual_insertions = sum(1 for r in self.results if r['rows_inserted'] > 0)
        
        print(f"🎯 Total Test Combinations: {total_combinations}")
        print(f"✅ Successful CLI Executions: {successful_tests}/{total_combinations} ({successful_tests/total_combinations*100:.1f}%)")
        print(f"📊 Actual Data Insertions: {total_actual_insertions}/{total_combinations} ({total_actual_insertions/total_combinations*100:.1f}%)")
        
        # Performance mode analysis
        print(f"\n🚀 PERFORMANCE MODE ANALYSIS:")
        for mode in self.performance_modes:
            mode_results = [r for r in self.results if r['performance_mode'] == mode]
            mode_insertions = sum(1 for r in mode_results if r['rows_inserted'] > 0)
            avg_time = sum(r['time_taken'] for r in mode_results) / len(mode_results)
            print(f"   {mode:<18}: {mode_insertions}/{len(mode_results)} ({mode_insertions/len(mode_results)*100:.1f}%) | Avg: {avg_time:.2f}s")
        
        # Duplicate strategy analysis
        print(f"\n🔄 DUPLICATE STRATEGY ANALYSIS:")
        for strategy in self.duplicate_strategies:
            strategy_results = [r for r in self.results if r['duplicate_strategy'] == strategy]
            strategy_insertions = sum(1 for r in strategy_results if r['rows_inserted'] > 0)
            avg_time = sum(r['time_taken'] for r in strategy_results) / len(strategy_results)
            print(f"   {strategy:<18}: {strategy_insertions}/{len(strategy_results)} ({strategy_insertions/len(strategy_results)*100:.1f}%) | Avg: {avg_time:.2f}s")
        
        # Table analysis
        print(f"\n🏗️  TABLE ANALYSIS:")
        for table in self.test_tables:
            table_results = [r for r in self.results if r['table'] == table]
            table_insertions = sum(1 for r in table_results if r['rows_inserted'] > 0)
            avg_time = sum(r['time_taken'] for r in table_results) / len(table_results)
            print(f"   {table:<25}: {table_insertions}/{len(table_results)} ({table_insertions/len(table_results)*100:.1f}%) | Avg: {avg_time:.2f}s")
        
        # Fastest successful combinations
        successful_results = [r for r in self.results if r['rows_inserted'] > 0]
        if successful_results:
            print(f"\n🏆 FASTEST SUCCESSFUL COMBINATIONS:")
            sorted_results = sorted(successful_results, key=lambda x: x['time_taken'])[:10]
            for i, result in enumerate(sorted_results, 1):
                print(f"   {i:2d}. {result['table']:<20} | {result['performance_mode']:<15} | {result['duplicate_strategy']:<18} | {result['time_taken']:.2f}s")
        
        # Most problematic combinations
        failed_results = [r for r in self.results if not r['success'] or r['rows_inserted'] == 0]
        if failed_results:
            print(f"\n⚠️  PROBLEMATIC COMBINATIONS:")
            print(f"     Total Failed: {len(failed_results)}")
            
            # Group by table
            table_failures = {}
            for result in failed_results:
                table = result['table']
                if table not in table_failures:
                    table_failures[table] = 0
                table_failures[table] += 1
            
            if table_failures:
                print(f"     Failures by table:")
                for table, count in sorted(table_failures.items(), key=lambda x: x[1], reverse=True):
                    print(f"       {table}: {count} failures")
        
        print(f"\n" + "="*80)
        
        # Overall conclusion
        if total_actual_insertions / total_combinations >= 0.8:
            print("🎉 OVERALL STATUS: EXCELLENT - System working well across most combinations")
        elif total_actual_insertions / total_combinations >= 0.6:
            print("✅ OVERALL STATUS: GOOD - System working for most combinations with some issues")
        elif total_actual_insertions / total_combinations >= 0.4:
            print("⚠️  OVERALL STATUS: MODERATE - System has significant issues that need attention")
        else:
            print("❌ OVERALL STATUS: POOR - System has major problems requiring immediate fixes")

    def cleanup(self):
        """Cleanup resources."""
        click.confirm = self.original_confirm

def main():
    """Main test execution."""
    suite = FocusedCompleteTestSuite()
    
    try:
        suite.run_focused_test()
    except KeyboardInterrupt:
        print(f"\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        traceback.print_exc()
    finally:
        suite.cleanup()

if __name__ == "__main__":
    main()
