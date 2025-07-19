#!/usr/bin/env python3
"""
Unified Test Runner - Unit tests and Integration tests

This script can run different types of tests based on flags:

Usage:
    python3 run_tests.py                      # Run all tests (unit + integration)
    python3 run_tests.py --unit-only          # Run only unit tests (fast, no auth required)  
    python3 run_tests.py --integration-only   # Run only integration tests (requires Atlas CLI auth)
    python3 run_tests.py -v                   # Run all tests with verbose output
    python3 run_tests.py tests.test_common    # Run specific test module

Test Types:
    - Unit tests: Fast, mocked tests that don't require external services
    - Integration tests: Full end-to-end tests that require Atlas CLI authentication
"""

import unittest
import sys
import os
import subprocess
import tempfile
import json
from pathlib import Path


def check_atlas_auth():
    """Check if user is authenticated with Atlas CLI."""
    try:
        result = subprocess.run(['atlas', 'auth', 'whoami'], 
                              capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False


def run_unit_tests(verbosity=1, pattern="test*.py", start_dir="tests/unit"):
    """
    Run unit tests (fast, mocked, no external dependencies).
    
    Returns:
        bool: True if all tests passed, False otherwise
    """
    print("🧪 Running Unit Tests (fast, no auth required)")
    print("=" * 50)
    
    # Get the directory containing this script
    script_dir = Path(__file__).parent
    test_dir = script_dir / start_dir
    
    if not test_dir.exists():
        print(f"Error: Test directory '{test_dir}' does not exist")
        return False
    
    # Discover tests
    loader = unittest.TestLoader()
    suite = loader.discover(
        start_dir=str(test_dir),
        pattern=pattern,
        top_level_dir=str(script_dir)
    )
    
    # Count tests
    test_count = suite.countTestCases()
    if test_count == 0:
        print(f"No unit tests found in '{test_dir}' matching pattern '{pattern}'")
        return False
    
    print(f"Discovered {test_count} unit test(s)")
    print("-" * 50)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=verbosity, buffer=True)
    result = runner.run(suite)
    
    # Print summary
    print("-" * 50)
    tests_run = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped) if hasattr(result, 'skipped') else 0
    
    print(f"Unit Tests Summary:")
    print(f"  Tests run: {tests_run}")
    print(f"  Failures: {failures}")
    print(f"  Errors: {errors}")
    print(f"  Skipped: {skipped}")
    
    if failures == 0 and errors == 0:
        print("✅ All unit tests passed!")
        return True
    else:
        print("❌ Some unit tests failed")
        return False



def run_integration_tests(verbosity=1, pattern="test*.py", start_dir="tests/integration"):
    """
    Run integration tests (requires Atlas CLI authentication).
    
    Returns:
        bool: True if all tests passed, False otherwise
    """
    print("🚀 Running Integration Tests (requires Atlas CLI auth)")
    print("=" * 60)
    
    # Check authentication first
    if not check_atlas_auth():
        print("❌ AUTHENTICATION REQUIRED")
        print("You must be authenticated with Atlas CLI to run integration tests.")
        print("Please run: atlas auth login")
        print("\nAlternatively, run unit tests only: python3 run_tests.py --unit-only")
        return False
    
    print("✅ Atlas CLI authentication verified")
    
    # Get the directory containing this script
    script_dir = Path(__file__).parent
    test_dir = script_dir / start_dir
    
    if not test_dir.exists():
        print(f"Error: Integration test directory '{test_dir}' does not exist")
        return False
    
    # Discover integration tests
    loader = unittest.TestLoader()
    suite = loader.discover(
        start_dir=str(test_dir),
        pattern=pattern,
        top_level_dir=str(script_dir)
    )
    
    # Count tests
    test_count = suite.countTestCases()
    if test_count == 0:
        print(f"No integration tests found in '{test_dir}' matching pattern '{pattern}'")
        return False
    
    print(f"Discovered {test_count} integration test(s)")
    print("-" * 60)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=verbosity, buffer=True)
    result = runner.run(suite)
    
    # Print summary
    print("-" * 60)
    tests_run = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped) if hasattr(result, 'skipped') else 0
    
    print(f"Integration Tests Summary:")
    print(f"  Tests run: {tests_run}")
    print(f"  Failures: {failures}")
    print(f"  Errors: {errors}")
    print(f"  Skipped: {skipped}")
    
    if failures == 0 and errors == 0:
        print("✅ All integration tests passed!")
        return True
    else:
        print("❌ Some integration tests failed")
        return False


def main():
    """Main function to handle command line arguments and run tests."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run unit tests and/or integration tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    parser.add_argument(
        "--unit-only",
        action="store_true",
        help="Run only unit tests (fast, no auth required)"
    )
    
    parser.add_argument(
        "--integration-only",
        action="store_true",
        help="Run only integration tests (requires Atlas CLI auth)"
    )
    
    parser.add_argument(
        "-p", "--pattern",
        default="test*.py",
        help="Pattern to match test files (default: test*.py)"
    )
    
    args = parser.parse_args()
    
    verbosity = 2 if args.verbose else 1
    
    # Handle test type flags
    unit_success = True
    integration_success = True
    
    if args.unit_only:
        # Run only unit tests
        unit_success = run_unit_tests(verbosity=verbosity, pattern=args.pattern)
        success = unit_success
        
    elif args.integration_only:
        # Run only integration tests
        integration_success = run_integration_tests(verbosity=verbosity, pattern=args.pattern)
        success = integration_success
        
    else:
        # Run both unit and integration tests
        unit_success = run_unit_tests(verbosity=verbosity, pattern=args.pattern)
        
        if unit_success:
            print("\n" + "="*60)
            integration_success = run_integration_tests(verbosity=verbosity, pattern=args.pattern)
        else:
            print("\n⚠️  Skipping integration tests due to unit test failures")
            integration_success = False
        
        success = unit_success and integration_success
        
        # Overall summary
        print("\n" + "="*60)
        print("OVERALL TEST SUMMARY:")
        print(f"  Unit Tests: {'✅ PASSED' if unit_success else '❌ FAILED'}")
        print(f"  Integration Tests: {'✅ PASSED' if integration_success else '❌ FAILED'}")
        
        if success:
            print("🎉 ALL TESTS PASSED!")
        else:
            print("💥 SOME TESTS FAILED")
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()