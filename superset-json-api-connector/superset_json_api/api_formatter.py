import json
import requests


def format_api_response_for_superset(api_url, api_key=None):
    """
    Fetch and format API response for Superset compatibility
    Returns: list of dictionaries (rows)
    """
    headers = {'Accept': 'application/json'}
    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'

    response = requests.get(api_url, headers=headers)
    data = response.json()

    # Common API response formats to handle
    if isinstance(data, dict):
        if 'results' in data:
            rows = data['results']
        elif 'data' in data:
            rows = data['data']
        elif 'items' in data:
            rows = data['items']
        elif 'records' in data:
            rows = data['records']
        else:
            # Convert dict to list of one row
            rows = [data]
    elif isinstance(data, list):
        rows = data
    else:
        rows = [{'value': data}]

    # Ensure all rows have consistent columns
    all_keys = set()
    for row in rows:
        if isinstance(row, dict):
            all_keys.update(row.keys())

    # Add missing keys to all rows
    for row in rows:
        if isinstance(row, dict):
            for key in all_keys:
                if key not in row:
                    row[key] = None

    return rows


# Example usage
if __name__ == "__main__":
    # Test with public API
    url = "https://jsonplaceholder.typicode.com/posts"
    rows = format_api_response_for_superset(url)

    print(f"Total rows: {len(rows)}")
    print(f"Columns: {list(rows[0].keys()) if rows else []}")
    print("\nFirst row:")
    print(json.dumps(rows[0], indent=2))
