#!/usr/bin/env python3
# debug_api.py - Test the API directly

import requests
import json


def test_api_directly():
    """Test the API without the connector"""
    url = "https://jsonplaceholder.typicode.com/posts"

    print(f"Testing API: {url}")
    print("=" * 60)

    response = requests.get(url)
    print(f"Status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('content-type')}")

    data = response.json()
    print(f"\nData type: {type(data)}")

    if isinstance(data, list):
        print(f"List length: {len(data)}")
        if data:
            print(f"\nFirst item type: {type(data[0])}")
            print(f"First item keys: {list(data[0].keys())}")
            print(f"\nFirst item values:")
            for key, value in data[0].items():
                print(f"  {key}: {value} (type: {type(value).__name__})")

    elif isinstance(data, dict):
        print(f"Dict keys: {list(data.keys())}")
        for key, value in data.items():
            print(f"\n{key}: type={type(value).__name__}")
            if isinstance(value, list) and value:
                print(f"  First item: {value[0]}")

    print("\n" + "=" * 60)
    print("Expected: List of dicts with userId, id, title, body")
    print("Actual JSON structure shown above")


if __name__ == "__main__":
    test_api_directly()
