import json
import requests
from typing import Any, List, Dict, Optional, Tuple
import time
from urllib.parse import urlencode

# ========== DBAPI 2.0 REQUIRED MODULE-LEVEL ATTRIBUTES ==========
# These MUST be defined at the module level (outside any class)

# DBAPI 2.0 required string constants
apilevel = "2.0"
threadsafety = 1  # 1 = Threads may share the module but not connections
paramstyle = "pyformat"  # or 'named', 'format', 'qmark'

# DBAPI 2.0 required type objects (these are actually integers in DBAPI spec)
STRING = 1
BINARY = 2
NUMBER = 3
DATETIME = 4
ROWID = 5


# DBAPI 2.0 required exception classes
class JSONAPIError(Exception):
    """Base exception for JSON API errors"""
    pass


class Warning(Exception):
    """Exception for important warnings"""
    pass


class Error(JSONAPIError):
    """Base class for all other error exceptions"""
    pass


class InterfaceError(Error):
    """Errors related to the database interface"""
    pass


class DatabaseError(Error):
    """Errors related to the database"""
    pass


class DataError(DatabaseError):
    """Errors due to problems with the processed data"""
    pass


class OperationalError(DatabaseError):
    """Errors related to database operation"""
    pass


class IntegrityError(DatabaseError):
    """Errors when relational integrity is affected"""
    pass


class InternalError(DatabaseError):
    """Errors internal to the database module"""
    pass


class ProgrammingError(DatabaseError):
    """Programming errors like table not found, syntax error"""
    pass


class NotSupportedError(DatabaseError):
    """Method or API not supported"""
    pass


# ========== DBAPI 2.0 REQUIRED FUNCTIONS ==========

def connect(endpoint: str, **kwargs) -> 'JSONAPIConnection':
    """DBAPI 2.0 required connect() function"""
    return JSONAPIConnection(endpoint, **kwargs)


# ========== MAIN CLASSES ==========

class JSONAPIConnection:
    """DBAPI connection to JSON API"""

    def __init__(self, endpoint: str, api_key: str = None, headers: Dict = None,
                 timeout: int = 30, verify_ssl: bool = True):
        self.endpoint = endpoint
        self.api_key = api_key
        self.headers = headers or {}
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        # Set default headers
        self.headers.setdefault('User-Agent', 'Superset-JSON-API-Connector/0.1.0')
        self.headers.setdefault('Accept', 'application/json')

        if api_key:
            self.headers['Authorization'] = f'Bearer {api_key}'

        # Create session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def cursor(self):
        return JSONAPICursor(self)

    def close(self):
        self.session.close()

    def commit(self):
        """Read-only API, nothing to commit"""
        pass

    def rollback(self):
        """Read-only API, nothing to rollback"""
        pass


class JSONAPICursor:
    """DBAPI cursor for fetching data from API"""

    def __init__(self, connection: JSONAPIConnection):
        self.connection = connection
        self._results = []
        self._description = None
        self.rowcount = -1
        self._row_index = 0
        self.arraysize = 1  # Default fetchmany size

    def execute(self, operation: str = None, parameters: Dict = None):
        """Execute API call with optional SQL-like filters"""
        try:
            # Parse operation for LIMIT and basic WHERE clauses
            limit = 1000  # Default limit
            filters = {}

            if operation:
                limit = self._parse_limit(operation)
                filters = self._parse_filters(operation)

            # Merge with explicit parameters
            query_params = {**(parameters or {}), **filters}

            # Make API request
            start_time = time.time()
            response = self.connection.session.get(
                self.connection.endpoint,
                params=query_params,
                timeout=self.connection.timeout,
                verify=self.connection.verify_ssl
            )

            # Handle HTTP errors
            if response.status_code != 200:
                raise OperationalError(
                    f"API returned {response.status_code}: {response.text}")

            # Parse JSON response
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                raise DataError(f"Invalid JSON response: {str(e)}")

            # Convert to list of rows
            rows = self._normalize_data(data)

            # Apply limit
            if limit > 0 and len(rows) > limit:
                rows = rows[:limit]

            self._results = rows
            self.rowcount = len(rows)
            self._row_index = 0

            # Create column description from first row
            if rows:
                self._description = self._create_description(rows[0])
            else:
                # Create empty description if no data
                self._description = []

            print(f"Fetched {self.rowcount} rows from {self.connection.endpoint}")

        except requests.RequestException as e:
            raise OperationalError(f"Network error: {str(e)}")
        except Exception as e:
            raise DatabaseError(f"Execution error: {str(e)}")

    def executemany(self, operation: str, seq_of_parameters: List[Dict]):
        """Not supported for read-only API"""
        raise NotSupportedError("executemany not supported for JSON API")

    def _parse_limit(self, operation: str) -> int:
        """Extract LIMIT clause from SQL operation"""
        import re
        limit_match = re.search(r'LIMIT\s+(\d+)', operation, re.IGNORECASE)
        if limit_match:
            return int(limit_match.group(1))
        return 1000  # Default limit

    def _parse_filters(self, operation: str) -> Dict:
        """Extract WHERE clause conditions"""
        filters = {}
        import re

        # Find WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+LIMIT\s+\d+|\s*$)', operation,
                                re.IGNORECASE | re.DOTALL)
        if where_match:
            conditions = where_match.group(1)

            # Parse simple conditions (key = 'value' or key = value)
            pattern = r'(\w+)\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|(\w+))'
            matches = re.findall(pattern, conditions)

            for match in matches:
                key = match[0]
                # Get value from one of the capture groups
                value = next((v for v in match[1:] if v), None)
                if value is not None:
                    filters[key] = value

        return filters

    def _normalize_data(self, data: Any) -> List[Dict]:
        """Convert various JSON structures to list of dictionaries"""
        rows = []

        if isinstance(data, dict):
            # Check common API response formats
            if 'results' in data:
                data = data['results']
            elif 'data' in data:
                data = data['data']
            elif 'items' in data:
                data = data['items']
            elif 'records' in data:
                data = data['records']
            else:
                # Treat the entire dict as a single row
                data = [data]

        if isinstance(data, list):
            # Ensure each item is a dict
            for item in data:
                if isinstance(item, dict):
                    rows.append(item)
                else:
                    # Convert non-dict items to dict
                    rows.append({'value': item})
        else:
            # Convert single value to list
            rows.append({'value': data})

        return rows

    def _create_description(self, row: Dict) -> List[Tuple]:
        """Create DBAPI description from row data"""
        description = []

        for key, value in row.items():
            # Determine type code (DBAPI integer codes)
            if isinstance(value, int):
                type_code = NUMBER
            elif isinstance(value, float):
                type_code = NUMBER
            elif isinstance(value, bool):
                type_code = NUMBER  # Some DBAPIs use 16 for BOOLEAN
            elif isinstance(value, dict) or isinstance(value, list):
                type_code = STRING  # JSON as string
            else:
                type_code = STRING

            description.append((
                key,  # name
                type_code,  # type_code (DBAPI integer)
                None,  # display_size
                None,  # internal_size
                None,  # precision
                None,  # scale
                True  # nullable
            ))

        return description

    def fetchone(self) -> Optional[Dict]:
        """Fetch single row"""
        if self._row_index >= len(self._results):
            return None

        row = self._results[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: int = None) -> List[Dict]:
        """Fetch multiple rows"""
        if size is None:
            size = self.arraysize

        end_index = min(self._row_index + size, len(self._results))
        result = self._results[self._row_index:end_index]
        self._row_index = end_index

        return result

    def fetchall(self) -> List[Dict]:
        """Fetch all remaining rows"""
        result = self._results[self._row_index:]
        self._row_index = len(self._results)
        return result

    @property
    def description(self):
        return self._description

    def close(self):
        """Cleanup cursor"""
        self._results = []
        self._description = None
        self.rowcount = -1

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


# DBAPI 2.0 requires Binary type
Binary = bytes

# For SQLAlchemy compatibility
Date = str
Time = str
Timestamp = str
DateFromTicks = None
TimeFromTicks = None
TimestampFromTicks = None
