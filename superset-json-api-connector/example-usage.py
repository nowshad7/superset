#!/usr/bin/env python3
"""
Example usage of the Superset JSON API Connector

This script demonstrates how to use the connector both:
1. Directly with SQLAlchemy (for testing)
2. In Apache Superset (configuration examples)
"""

import json
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError


def print_header(title):
    """Print formatted header"""
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print('=' * 60)


def test_direct_sqlalchemy():
    """Test the connector directly with SQLAlchemy"""
    print_header("Testing with SQLAlchemy Directly")

    # Example 1: Public API without authentication
    print("\n1. Testing with JSONPlaceholder (public API):")
    engine = create_engine("jsonapi://jsonplaceholder.typicode.com/posts")

    try:
        with engine.connect() as conn:
            # Get table information
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            print(f"   Available tables: {tables}")

            # Query data with LIMIT
            result = conn.execute(text("SELECT * FROM api_data LIMIT 3"))

            print(f"   Columns: {[col[0] for col in result.cursor.description]}")
            print(f"   Row count: {result.cursor.rowcount}")

            print("\n   First 3 rows:")
            for i, row in enumerate(result):
                print(f"   Row {i + 1}: ID={row['id']}, Title={row['title'][:50]}...")

            # Test with WHERE clause
            print("\n   Testing WHERE clause:")
            result2 = conn.execute(text("SELECT id, title FROM api_data WHERE userId = 1 LIMIT 2"))
            for row in result2:
                print(f"   User 1 - ID: {row['id']}, Title: {row['title'][:30]}...")

    except Exception as e:
        print(f"   ❌ Error: {e}")


def test_with_parameters():
    """Test with various URL parameters"""
    print_header("Testing with URL Parameters")

    test_cases = [
        {
            "name": "Public API with limit param",
            "uri": "jsonapi://jsonplaceholder.typicode.com/comments?limit=5"
        },
        {
            "name": "Mock API with query params",
            "uri": "jsonapi://jsonplaceholder.typicode.com/comments?postId=1"
        },
        # Add your own APIs here:
        # {
        #     "name": "Your API with auth",
        #     "uri": "jsonapi://api.example.com/data?api_key=YOUR_KEY"
        # }
    ]

    for test in test_cases:
        print(f"\n📊 {test['name']}")
        print(f"   URI: {test['uri']}")

        try:
            engine = create_engine(test['uri'])
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM api_data LIMIT 2"))
                print(f"   ✓ Connected successfully")
                print(f"   Found {result.cursor.rowcount} rows")

                # Show first row structure
                first_row = result.fetchone()
                if first_row:
                    print(f"   First row keys: {list(first_row.keys())[:5]}...")

        except Exception as e:
            print(f"   ❌ Failed: {e}")


def test_superset_configurations():
    """Show how to configure in Superset UI"""
    print_header("Superset Configuration Examples")

    configs = [
        {
            "name": "Basic JSON API",
            "database_name": "JSONPlaceholder API",
            "sqlalchemy_uri": "jsonapi://jsonplaceholder.typicode.com/posts",
            "notes": "Public API, no authentication needed"
        },
        {
            "name": "API with API Key",
            "database_name": "Secure JSON API",
            "sqlalchemy_uri": "jsonapi://api.example.com/data.json?api_key=sk_1234567890abcdef",
            "notes": "Add ?api_key=YOUR_KEY to the URI"
        },
        {
            "name": "API with Custom Headers",
            "database_name": "Custom Header API",
            "sqlalchemy_uri": "jsonapi://api.example.com/data.json?headers=" +
                              json.dumps({"X-API-Key": "your-key-here", "Accept": "application/json"}),
            "notes": "Headers must be URL-encoded JSON"
        },
        {
            "name": "API with Timeout",
            "database_name": "Slow API",
            "sqlalchemy_uri": "jsonapi://slow-api.example.com/data?timeout=60",
            "notes": "Increase timeout for slow APIs"
        }
    ]

    print("\nUse these configurations in Superset UI:")
    print("1. Go to Data → Databases → + Database")
    print("2. Fill in the form:")

    for i, config in enumerate(configs, 1):
        print(f"\n{i}. {config['name']}:")
        print(f"   • Database Name: {config['database_name']}")
        print(f"   • SQLAlchemy URI: {config['sqlalchemy_uri']}")
        print(f"   • Notes: {config['notes']}")

    print("\n⚠️  Important Superset Settings:")
    print("   ✓ Check 'Expose in SQL Lab'")
    print("   ✓ Check 'Allow CORS' if needed")
    print("   ✓ Test Connection before saving")


def test_error_cases():
    """Test error handling"""
    print_header("Testing Error Cases")

    error_cases = [
        {
            "name": "Invalid URL",
            "uri": "jsonapi://invalid-url-that-does-not-exist-12345.com/data",
            "expected": "Should fail with network error"
        },
        {
            "name": "Non-JSON response",
            "uri": "jsonapi://httpbin.org/html",  # Returns HTML, not JSON
            "expected": "Should fail with JSON parse error"
        },
        {
            "name": "404 Not Found",
            "uri": "jsonapi://jsonplaceholder.typicode.com/invalid-endpoint",
            "expected": "Should fail with 404 error"
        }
    ]

    for test in error_cases:
        print(f"\n🔧 Testing: {test['name']}")
        print(f"   URI: {test['uri']}")
        print(f"   Expected: {test['expected']}")

        try:
            engine = create_engine(test['uri'])
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM api_data LIMIT 1"))
                print(f"   ❌ UNEXPECTED: Should have failed but didn't!")
        except Exception as e:
            print(f"   ✓ Got expected error: {type(e).__name__}: {str(e)[:100]}...")


def sql_lab_examples():
    """Examples of SQL queries you can run in Superset SQL Lab"""
    print_header("SQL Lab Query Examples")

    examples = [
        {
            "query": "SELECT * FROM api_data LIMIT 100",
            "description": "Basic select with limit"
        },
        {
            "query": "SELECT id, name, email FROM api_data WHERE status = 'active'",
            "description": "Select specific columns with WHERE clause"
        },
        {
            "query": "SELECT department, COUNT(*) as employee_count FROM api_data GROUP BY department",
            "description": "Group by and aggregation"
        },
        {
            "query": "SELECT * FROM api_data ORDER BY created_at DESC LIMIT 10",
            "description": "Order by date, most recent first"
        },
        {
            "query": "SELECT user_id, COUNT(*) as post_count FROM api_data WHERE user_id IN (1, 2, 3) GROUP BY user_id",
            "description": "Filter with IN clause and group"
        }
    ]

    print("\nCopy these queries into Superset SQL Lab:")
    for i, example in enumerate(examples, 1):
        print(f"\n{i}. {example['description']}:")
        print(f"   ```sql")
        print(f"   {example['query']}")
        print(f"   ```")


def advanced_features():
    """Demonstrate advanced features"""
    print_header("Advanced Features")

    print("\n1. Pagination Support:")
    print("   The connector automatically handles pagination for APIs that:")
    print("   • Return 'next' URL in response")
    print("   • Use 'page' and 'total_pages' fields")
    print("   • Include 'Link' headers (RFC 5988)")

    print("\n2. Type Inference:")
    print("   The connector automatically detects data types:")
    print("   • Numbers → INTEGER/REAL")
    print("   • True/False → BOOLEAN")
    print("   • Arrays/Objects → JSON")
    print("   • Strings → TEXT or DATETIME (if matches date pattern)")

    print("\n3. Caching:")
    print("   For better performance in Superset:")
    print("   • Enable caching in database settings")
    print("   • Set appropriate cache timeout")
    print("   • Use 'Asynchronous Query Execution' for large datasets")


def quick_test():
    """Quick test to verify installation"""
    print_header("Quick Installation Test")

    print("\nRunning quick test...")

    try:
        # First, try to import the dialect
        from superset_json_api.dialect import JSONAPIDialect
        print("✓ Dialect module imported successfully")

        # Test SQLAlchemy registration
        from sqlalchemy.dialects import registry
        if 'jsonapi' in registry.dialects:
            print("✓ JSONAPI dialect registered with SQLAlchemy")
        else:
            print("✗ JSONAPI dialect NOT registered with SQLAlchemy")

        # Test a simple connection
        print("\nTesting connection to public API...")
        engine = create_engine("jsonapi://jsonplaceholder.typicode.com/posts")

        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM api_data LIMIT 1"))
            if result.cursor.rowcount >= 0:
                print("✓ Successfully connected to API")
                print(f"✓ Found {result.cursor.rowcount} rows available")
            else:
                print("✗ No data returned")

        print("\n✅ All tests passed! The connector is working correctly.")

    except ImportError as e:
        print(f"✗ Import failed: {e}")
        print("\nMake sure you installed the connector:")
        print("  pip install -e .")
    except Exception as e:
        print(f"✗ Test failed: {e}")
        print("\nCheck your installation and network connection.")


def main():
    """Main function"""
    print("🚀 Superset JSON API Connector - Examples & Tests")
    print("Version 0.1.0")

    # Run tests
    quick_test()
    test_direct_sqlalchemy()
    test_with_parameters()
    test_error_cases()
    sql_lab_examples()
    test_superset_configurations()
    advanced_features()

    print_header("Next Steps")
    print("\n1. Install in Superset Docker container:")
    print("   docker cp . superset:/tmp/json-api-connector")
    print("   docker exec superset pip install -e /tmp/json-api-connector")
    print("   docker restart superset")

    print("\n2. Add database connection in Superset UI:")
    print("   • Go to Data → Databases → + Database")
    print("   • Use URI: jsonapi://your-api-endpoint.com/data")

    print("\n3. Start querying in SQL Lab!")
    print("\n📝 Need help? Check the README.md file for more details.")


if __name__ == "__main__":
    main()