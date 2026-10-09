"""
Test Runner for E-Commerce Data Engineering Platform.

Discovers and executes all unit tests across tests/ directory.
"""

import sys
import unittest
from pathlib import Path

# Set base project directory to Python path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


def run_all_tests():
    """Discovers and runs test suite."""
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(BASE_DIR / "tests"), pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print(f"TEST EXECUTION SUMMARY:")
    print(f"  Tests Run:  {result.testsRun}")
    print(f"  Failures:   {len(result.failures)}")
    print(f"  Errors:     {len(result.errors)}")
    print(f"  Success:    {result.wasSuccessful()}")
    print("=" * 60)

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
