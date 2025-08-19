#!/usr/bin/env python3
"""
Comprehensive Test Suite for DBMocker
Tests all combinations of PerformanceMode, MultiProcessing, and Duplicate Strategy
"""

import sys
import time
import traceback
from itertools import product
from typing import Dict, Any, List

# Add current directory to path
sys.path.insert(0, '.')

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.enhanced_cli import generate
from dbmocker.core.enhanced_models import PerformanceMode, DuplicateStrategy

class ComprehensiveTestSuite:
    """Test suite for all DBMocker combinations."""
    
    def __init__(self):
        self.db_config = DatabaseConfig(
            driver='mysql',
            host='127.0.0.1',
            port=3306,
            database='gringotts_test',
            username='root',
            password=''
        )
        
        # Test configurations
        self.performance_modes = [
            'standard',
            'high_speed', 
            'memory_efficient',
            'balanced',
            'ultra_high'
        ]
        
        self.multiprocessing_options = [
            False,  # Single-threaded
            True    # Multi-threaded
        ]
        
        self.duplicate_strategies = [
            'generate_new',
            'allow_simple', 
            'smart_duplicates',
            'cached_pool',
            'fast_data_reuse'
        ]
        
        self.test_results = []
        self.total_tests = len(self.performance_modes) * len(self.multiprocessing_options) * len(self.duplicate_strategies)
        
    def setup_test_table(self):
        """Ensure we have a clean test environment."""
        print("🔧 Setting up test environment...")
        
        try:
            with DatabaseConnection(self.db_config) as db_conn:
                # Get current row count
                result = db_conn.execute_query("SELECT COUNT(*) FROM user")
                initial_count = result[0][0] if result else 0
                print(f"   Initial user table rows: {initial_count:,}")
                
                # Analyze schema
                analyzer = SchemaAnalyzer(db_conn)
                schema = analyzer.analyze_schema(
                    include_tables=['user'],
                    analyze_data_patterns=False
                )
                
                user_table = None
                for table in schema.tables:
                    if table.name == 'user':
                        user_table = table
                        break
                
                if user_table:
                    print(f"   ✅ User table found with {len(user_table.columns)} columns")
                    
                    # Show critical column constraints
                    critical_columns = ['merchant_user_id', 'aggregator_user_id', 'mobile_number']
                    for col_name in critical_columns:
                        col = next((c for c in user_table.columns if c.name == col_name), None)
                        if col:
                            nullable = "NULL" if col.is_nullable else "NOT NULL"
                            max_len = getattr(col, 'max_length', 'N/A')
                            print(f"     {col_name}: {col.data_type} ({nullable}) max_len={max_len}")
                    
                    return True
                else:
                    print("   ❌ User table not found")
                    return False
                    
        except Exception as e:
            print(f"   ❌ Setup failed: {e}")
            return False
    
    def run_single_test(self, performance_mode: str, use_multiprocessing: bool, 
                       duplicate_strategy: str, test_num: int) -> Dict[str, Any]:
        """Run a single test configuration."""
        
        test_config = {
            'performance_mode': performance_mode,
            'multiprocessing': use_multiprocessing,
            'duplicate_strategy': duplicate_strategy,
            'test_number': test_num,
            'total_tests': self.total_tests
        }
        
        print(f"\n{'='*60}")
        print(f"🧪 TEST {test_num}/{self.total_tests}")
        print(f"   Performance Mode: {performance_mode}")
        print(f"   Multiprocessing: {use_multiprocessing}")
        print(f"   Duplicate Strategy: {duplicate_strategy}")
        print(f"{'='*60}")
        
        start_time = time.time()
        success = False
        error_message = None
        rows_generated = 0
        
        try:
            # Prepare test parameters
            rows_to_generate = 3  # Small number for quick testing
            
            # Import and configure for CLI test
            import click
            from click.testing import CliRunner
            
            # Build CLI command arguments
            args = [
                '--driver', 'mysql',
                '--host', '127.0.0.1', 
                '--port', '3306',
                '--database', 'gringotts_test',
                '--username', 'root',
                '--password', '',
                '--table', 'user',
                '--rows', str(rows_to_generate),
                '--performance-mode', performance_mode,
                '--duplicate-strategy', duplicate_strategy
            ]
            
            # Add multiprocessing flags based on strategy
            if use_multiprocessing:
                args.extend(['--max-workers', '2'])
            
            # Special handling for fast_data_reuse
            if duplicate_strategy == 'fast_data_reuse':
                args.extend([
                    '--sample-size', '100',
                    '--reuse-probability', '0.8',
                    '--progress-interval', '1000'
                ])
            
            print(f"🔧 CLI Args: {' '.join(args)}")
            
            # Create a CLI runner and test
            runner = CliRunner()
            
            # Mock the click.confirm to automatically proceed
            import click
            original_confirm = click.confirm
            click.confirm = lambda *args, **kwargs: True
            
            try:
                # Run the enhanced CLI generate command
                from dbmocker import enhanced_cli
                result = runner.invoke(enhanced_cli.generate, args, catch_exceptions=False)
                
                if result.exit_code == 0:
                    success = True
                    print("   ✅ Test completed successfully!")
                    
                    # Try to count generated rows (rough estimate)
                    output = result.output
                    if "rows generated" in output.lower():
                        # Try to extract row count from output
                        import re
                        match = re.search(r'(\d+)\s+rows?\s+generated', output.lower())
                        if match:
                            rows_generated = int(match.group(1))
                    else:
                        rows_generated = rows_to_generate  # Assume success
                        
                else:
                    error_message = f"CLI exit code: {result.exit_code}\nOutput: {result.output}"
                    print(f"   ❌ Test failed with exit code: {result.exit_code}")
                    
            finally:
                # Restore original confirm function
                click.confirm = original_confirm
                
        except Exception as e:
            error_message = str(e)
            print(f"   ❌ Test failed with exception: {e}")
            if "mysql" in str(e).lower() or "connection" in str(e).lower():
                print("   📡 Database connection issue detected")
            elif "data too long" in str(e).lower():
                print("   📏 Data length constraint issue detected")
            elif "missing" in str(e).lower() or "null" in str(e).lower():
                print("   🛡️ NULL/missing value issue detected")
            
            # Print full traceback for debugging
            print(f"   🔍 Full traceback:")
            traceback.print_exc()
        
        end_time = time.time()
        duration = end_time - start_time
        
        result = {
            **test_config,
            'success': success,
            'duration': duration,
            'rows_generated': rows_generated,
            'error_message': error_message,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        self.test_results.append(result)
        
        if success:
            print(f"   🎉 Success in {duration:.2f}s - {rows_generated} rows")
        else:
            print(f"   💥 Failed in {duration:.2f}s - {error_message}")
            
        return result
    
    def run_all_tests(self):
        """Run all test combinations."""
        print("🚀 Starting Comprehensive DBMocker Test Suite")
        print(f"📊 Total test combinations: {self.total_tests}")
        print(f"🎯 Testing: {len(self.performance_modes)} performance modes × {len(self.multiprocessing_options)} MP options × {len(self.duplicate_strategies)} duplicate strategies")
        
        if not self.setup_test_table():
            print("❌ Test environment setup failed. Aborting.")
            return
        
        start_time = time.time()
        test_num = 0
        
        # Generate all combinations
        for performance_mode, use_mp, duplicate_strategy in product(
            self.performance_modes, 
            self.multiprocessing_options, 
            self.duplicate_strategies
        ):
            test_num += 1
            
            # Run the test
            self.run_single_test(
                performance_mode=performance_mode,
                use_multiprocessing=use_mp,
                duplicate_strategy=duplicate_strategy,
                test_num=test_num
            )
            
            # Brief pause between tests
            time.sleep(0.5)
        
        total_time = time.time() - start_time
        self.generate_summary_report(total_time)
    
    def generate_summary_report(self, total_time: float):
        """Generate a comprehensive summary report."""
        print(f"\n{'='*80}")
        print("📊 COMPREHENSIVE TEST SUMMARY REPORT")
        print(f"{'='*80}")
        
        successful_tests = [r for r in self.test_results if r['success']]
        failed_tests = [r for r in self.test_results if not r['success']]
        
        print(f"🎯 Total Tests: {len(self.test_results)}")
        print(f"✅ Successful: {len(successful_tests)} ({len(successful_tests)/len(self.test_results)*100:.1f}%)")
        print(f"❌ Failed: {len(failed_tests)} ({len(failed_tests)/len(self.test_results)*100:.1f}%)")
        print(f"⏱️  Total Time: {total_time:.2f}s ({total_time/60:.1f} minutes)")
        print(f"📈 Average Time per Test: {total_time/len(self.test_results):.2f}s")
        
        # Success by category
        print(f"\n📈 SUCCESS RATES BY CATEGORY:")
        
        # By Performance Mode
        print(f"\n🚀 Performance Modes:")
        for mode in self.performance_modes:
            mode_tests = [r for r in self.test_results if r['performance_mode'] == mode]
            mode_success = [r for r in mode_tests if r['success']]
            success_rate = len(mode_success)/len(mode_tests)*100 if mode_tests else 0
            print(f"   {mode:15}: {len(mode_success):2}/{len(mode_tests):2} ({success_rate:5.1f}%)")
        
        # By Multiprocessing
        print(f"\n🧵 Multiprocessing:")
        for mp_option in [False, True]:
            mp_label = "Multi-threaded" if mp_option else "Single-threaded"
            mp_tests = [r for r in self.test_results if r['multiprocessing'] == mp_option]
            mp_success = [r for r in mp_tests if r['success']]
            success_rate = len(mp_success)/len(mp_tests)*100 if mp_tests else 0
            print(f"   {mp_label:15}: {len(mp_success):2}/{len(mp_tests):2} ({success_rate:5.1f}%)")
        
        # By Duplicate Strategy  
        print(f"\n🔄 Duplicate Strategies:")
        for strategy in self.duplicate_strategies:
            strategy_tests = [r for r in self.test_results if r['duplicate_strategy'] == strategy]
            strategy_success = [r for r in strategy_tests if r['success']]
            success_rate = len(strategy_success)/len(strategy_tests)*100 if strategy_tests else 0
            print(f"   {strategy:15}: {len(strategy_success):2}/{len(strategy_tests):2} ({success_rate:5.1f}%)")
        
        # Failed tests details
        if failed_tests:
            print(f"\n❌ FAILED TEST DETAILS:")
            for test in failed_tests:
                print(f"   Test {test['test_number']:2}: {test['performance_mode']:<12} | MP:{test['multiprocessing']:<5} | {test['duplicate_strategy']:<15}")
                if test['error_message']:
                    error_short = test['error_message'][:100] + "..." if len(test['error_message']) > 100 else test['error_message']
                    print(f"      Error: {error_short}")
        
        # Best performing combinations
        if successful_tests:
            print(f"\n🏆 FASTEST SUCCESSFUL COMBINATIONS:")
            fastest_tests = sorted(successful_tests, key=lambda x: x['duration'])[:5]
            for i, test in enumerate(fastest_tests, 1):
                print(f"   {i}. {test['performance_mode']:<12} | MP:{test['multiprocessing']:<5} | {test['duplicate_strategy']:<15} | {test['duration']:.2f}s")
        
        print(f"\n{'='*80}")
        
        # Write detailed results to file
        self.write_detailed_report()
    
    def write_detailed_report(self):
        """Write detailed test results to a file."""
        try:
            import json
            report_file = f"test_report_{int(time.time())}.json"
            
            with open(report_file, 'w') as f:
                json.dump({
                    'summary': {
                        'total_tests': len(self.test_results),
                        'successful': len([r for r in self.test_results if r['success']]),
                        'failed': len([r for r in self.test_results if not r['success']]),
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                    },
                    'detailed_results': self.test_results
                }, f, indent=2, default=str)
            
            print(f"📄 Detailed report saved to: {report_file}")
            
        except Exception as e:
            print(f"⚠️  Could not save detailed report: {e}")

def main():
    """Main test execution."""
    test_suite = ComprehensiveTestSuite()
    test_suite.run_all_tests()

if __name__ == "__main__":
    main()
