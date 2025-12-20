import json
import requests
from typing import Any, List, Dict, Optional, Tuple

# ================= DBAPI 2.0 METADATA =================

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

STRING = 1
BINARY = 2
NUMBER = 3
DATETIME = 4
ROWID = 5


# ================= DBAPI EXCEPTIONS =================

class JSONAPIError(Exception): pass
class Warning(Exception): pass
class Error(JSONAPIError): pass
class InterfaceError(Error): pass
class DatabaseError(Error): pass
class DataError(DatabaseError): pass
class OperationalError(DatabaseError): pass
class IntegrityError(DatabaseError): pass
class InternalError(DatabaseError): pass
class ProgrammingError(DatabaseError): pass
class NotSupportedError(DatabaseError): pass


# ================= DBAPI connect() =================

def connect(endpoint: str, **kwargs):
    return JSONAPIConnection(endpoint, **kwargs)


# ================= CONNECTION =================

class JSONAPIConnection:
    def __init__(
        self,
        endpoint: str,
        api_key: str = None,
        headers: Dict = None,
        timeout: int = 30,
        verify_ssl: bool = True,
    ):
        self.endpoint = endpoint
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        self.headers = headers or {}
        self.headers.setdefault("Accept", "application/json")
        self.headers.setdefault("User-Agent", "Superset-JSON-API-DBAPI/1.0")

        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def cursor(self):
        return JSONAPICursor(self)

    def close(self):
        self.session.close()

    def commit(self): pass
    def rollback(self): pass


# ================= CURSOR =================

class JSONAPICursor:
    def __init__(self, connection: JSONAPIConnection):
        self.connection = connection
        self._results: List[Tuple] = []
        self._columns: List[str] = []
        self._description = []
        self._row_index = 0
        self.rowcount = -1
        self.arraysize = 1000

    # ---------------- execute ----------------

    def execute(self, operation: str = None, parameters: Dict = None):
        try:
            limit = self._parse_limit(operation)
            filters = self._parse_filters(operation)

            params = {}
            params.update(parameters or {})
            params.update(filters)

            response = self.connection.session.get(
                self.connection.endpoint,
                params=params,
                timeout=self.connection.timeout,
                verify=self.connection.verify_ssl,
            )

            if response.status_code != 200:
                raise OperationalError(
                    f"HTTP {response.status_code}: {response.text[:200]}"
                )

            try:
                data = response.json()
            except json.JSONDecodeError as e:
                raise DataError(f"Invalid JSON: {e}")

            rows = self._normalize_data(data)

            if not rows:
                self._results = []
                self._columns = []
                self._description = []
                self.rowcount = 0
                return

            # ✅ COLUMN ORDER (CRITICAL)
            self._columns = list(rows[0].keys())

            # ✅ DBAPI description
            self._description = self._create_description(rows[0])

            # ✅ CONVERT DICTS → TUPLES
            self._results = [
                tuple(row.get(col) for col in self._columns)
                for row in rows[:limit]
            ]

            self.rowcount = len(self._results)
            self._row_index = 0

        except requests.RequestException as e:
            raise OperationalError(f"Network error: {e}")

    # ---------------- fetch methods ----------------

    def fetchone(self):
        if self._row_index >= len(self._results):
            return None
        row = self._results[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size=None):
        size = size or self.arraysize
        end = min(self._row_index + size, len(self._results))
        rows = self._results[self._row_index:end]
        self._row_index = end
        return rows

    def fetchall(self):
        rows = self._results[self._row_index:]
        self._row_index = len(self._results)
        return rows

    @property
    def description(self):
        return self._description

    def close(self):
        self._results = []
        self._description = []
        self._columns = []
        self.rowcount = -1

    def executemany(self, *args):
        raise NotSupportedError("executemany not supported")

    def setinputsizes(self, sizes): pass
    def setoutputsize(self, size, column=None): pass

    # ---------------- helpers ----------------

    def _parse_limit(self, sql: str) -> int:
        import re
        if not sql:
            return 1000
        m = re.search(r"LIMIT\s+(\d+)", sql, re.I)
        return int(m.group(1)) if m else 1000

    def _parse_filters(self, sql: str) -> Dict:
        import re
        filters = {}
        if not sql:
            return filters

        m = re.search(r"WHERE\s+(.+?)(?:LIMIT|$)", sql, re.I | re.S)
        if not m:
            return filters

        for key, val1, val2, val3 in re.findall(
            r"(\w+)\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|(\w+))", m.group(1)
        ):
            filters[key] = val1 or val2 or val3

        return filters

    def _normalize_data(self, data: Any) -> List[Dict]:
        if isinstance(data, dict):
            for key in ("data", "results", "items", "records"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                return [data]

        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]

        return []

    def _create_description(self, row: Dict) -> List[Tuple]:
        desc = []
        for k, v in row.items():
            if isinstance(v, (int, float, bool)):
                t = NUMBER
            else:
                t = STRING

            desc.append((k, t, None, None, None, None, True))
        return desc


# ================= TYPE ALIASES =================

Binary = bytes
Date = Time = Timestamp = str
DateFromTicks = TimeFromTicks = TimestampFromTicks = None
