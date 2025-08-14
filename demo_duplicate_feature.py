#!/usr/bin/env python3
"""
Demo Script: New Global Duplicate Feature
This script demonstrates the new --allow-duplicates and --allow-duplicates-global options.
"""

import os
import subprocess
import sys

def run_command(cmd, description):
    """Run a command and display results."""
    print(f"\n{'='*60}")
    print(f"🎯 {description}")
    print(f"Command: {cmd}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    return result.returncode == 0

def main():
    print("🚀 DBMocker Enhanced Duplicate Features Demo")
    print("=" * 60)
    
    # Test 1: Show without duplicates (default behavior)
    run_command(
        "python -m dbmocker.cli high-performance --driver sqlite --host localhost --port 1 "
        "--database test_enhanced.db --username='' --password='' --rows 5 --dry-run",
        "Test 1: Default behavior (no duplicates)"
    )
    
    # Test 2: Show with global duplicates enabled  
    run_command(
        "python -m dbmocker.cli high-performance --driver sqlite --host localhost --port 1 "
        "--database test_enhanced.db --username='' --password='' --rows 5 --dry-run "
        "--allow-duplicates-global --global-duplicate-probability 1.0",
        "Test 2: Global duplicates enabled (100% probability)"
    )
    
    # Test 3: Show with partial probability
    run_command(
        "python -m dbmocker.cli high-performance --driver sqlite --host localhost --port 1 "
        "--database test_enhanced.db --username='' --password='' --rows 5 --dry-run "
        "--allow-duplicates-global --global-duplicate-probability 0.5",
        "Test 3: Global duplicates with 50% probability"
    )
    
    # Test 4: Regular generate command with duplicates
    run_command(
        "python -m dbmocker.cli generate --host localhost --port 1 --database test_enhanced.db "
        "--username='' --password='' --driver sqlite --rows 5 --dry-run "
        "--allow-duplicates --duplicate-probability 0.8",
        "Test 4: Regular generate command with duplicates (80% probability)"
    )
    
    print(f"\n{'='*60}")
    print("✅ Demo completed!")
    print("📝 Key Features Demonstrated:")
    print("   • --allow-duplicates-global: Global duplicate mode for high-performance command")
    print("   • --allow-duplicates: Simple duplicate mode for regular generate command") 
    print("   • --global-duplicate-probability/--duplicate-probability: Control probability")
    print("   • Constraint-aware: Respects primary keys and unique constraints")
    print("   • Performance: Works with multi-threading and multi-processing")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
