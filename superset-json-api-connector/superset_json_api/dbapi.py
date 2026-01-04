import json
import duckdb
import pandas as pd
import requests
from typing import Any, List, Dict, Tuple
from flask import session

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

STRING = 1
BINARY = 2
NUMBER = 3
DATETIME = 4
ROWID = 5

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

def connect(endpoint: str, **kwargs):
    return JSONAPIConnection(endpoint, **kwargs)


def get_access_token():
    oauth = session.get("oauth")
    if not oauth:
        print('access_token: ---> none')
        return None

    access_token = oauth[0]
    print('access_token: --->', access_token)
    # check it's a valid token, else using refresh token get new token
    #
    return access_token

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
        else:
            self.headers["Authorization"] = f"Bearer {get_access_token()}"

        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.duck = duckdb.connect(database=":memory:")

    def cursor(self):
        return JSONAPICursor(self)

    def close(self):
        self.session.close()
        self.duck.close()

    def commit(self): pass
    def rollback(self): pass

class JSONAPICursor:
    def __init__(self, connection: JSONAPIConnection):
        self.connection = connection
        self._results: List[Tuple] = []
        self._description: List[Tuple] = []
        self._row_index = 0
        self.rowcount = -1
        self.arraysize = 1000

    def execute(self, operation: str = None, parameters: Dict = None):
        if not operation:
            raise ProgrammingError("No SQL provided")

        try:
            print("=== JSONAPI REQUEST ===")
            print("Endpoint:", self.connection.endpoint)
            print("Headers:")
            for k, v in self.connection.session.headers.items():
                print(f"  {k}: {v}")
            print("=======================")


            response = self.connection.session.get(
                self.connection.endpoint,
                timeout=self.connection.timeout,
                verify=self.connection.verify_ssl,
            )

            print("=== JSONAPI RESPONSE ===")
            print("Status Code:", response.status_code)
            print("Response Headers:")
            for k, v in response.headers.items():
                print(f"  {k}: {v}")

            # Print body safely (truncate large payloads)
            body_preview = response.text[:2000]
            print("Response Body (preview):")
            print(body_preview)
            print("========================")
            if response.status_code != 200:
                raise OperationalError(
                    f"HTTP {response.status_code}: {response.text[:200]}"
                )

            try:
                payload = response.json()
            except json.JSONDecodeError as e:
                raise DataError(f"Invalid JSON response: {e}")

            rows = self._normalize_data(payload)
            self.connection.duck.execute("DROP TABLE IF EXISTS api_data")

            if rows:
                df = pd.DataFrame(rows)
                self.connection.duck.register("df_api_data", df)
                self.connection.duck.execute(
                    "CREATE TABLE api_data AS SELECT * FROM df_api_data"
                )
            else:
                self.connection.duck.execute(
                    "CREATE TABLE api_data (data VARCHAR)"
                )
            result = self.connection.duck.execute(operation)

            self._results = result.fetchall()
            self.rowcount = len(self._results)
            self._row_index = 0
            self._description = []

            for col in result.description:
                self._description.append(
                    (col[0], STRING, None, None, None, None, True)
                )

        except Exception as e:
            raise DatabaseError(str(e))

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
        self.rowcount = -1

    def executemany(self, *args):
        raise NotSupportedError("executemany not supported")

    def setinputsizes(self, sizes): pass
    def setoutputsize(self, size, column=None): pass

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

Binary = bytes
Date = Time = Timestamp = str
DateFromTicks = TimeFromTicks = TimestampFromTicks = None
