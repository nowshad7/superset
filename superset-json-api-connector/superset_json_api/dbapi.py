import json
import time
import base64
from typing import Any, Dict, List, Tuple

# Optional third-party dependencies
try:
    import duckdb
except Exception:
    duckdb = None

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import requests
except Exception:
    requests = None


# ----------------------------------------------------------------------
# DB-API metadata
# ----------------------------------------------------------------------

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


# ----------------------------------------------------------------------
# DB-API entrypoint
# ----------------------------------------------------------------------

def connect(endpoint: str, **kwargs):
    """
    DB-API connection factory.
    """
    return JSONAPIConnection(endpoint, **kwargs)


# ----------------------------------------------------------------------
# Superset OAuth delegation helper
# ----------------------------------------------------------------------

def get_access_token():
    """
    Retrieve OAuth access token from Flask session if available.
    Used for Superset Auth delegation.
    """
    try:
        import importlib
        flask = importlib.import_module("flask")
        session = getattr(flask, "session", None)
        if session is None:
            return None
    except Exception:
        return None

    oauth = session.get("oauth")
    return oauth[0] if oauth else None


# ----------------------------------------------------------------------
# OAuth2 token manager
# ----------------------------------------------------------------------

class OAuth2TokenManager:
    """
    Handles OAuth2 client-credentials token lifecycle.
    """

    def __init__(self, oauth2_config: Dict | None = None):
        self.oauth2_config = oauth2_config or {}
        self.access_token = self.oauth2_config.get("oauth2_access_token")
        self.token_expiry: float | None = None

    def get_token(self) -> str | None:
        """
        Return a valid access token, refreshing if expired.
        """
        if not self.oauth2_config:
            return None

        # If we have a static token from config, use it
        if self.access_token and not self.token_expiry:
            return self.access_token

        # If token is expired, refresh it
        if self.token_expiry and time.time() > self.token_expiry:
            print("Token expired, refreshing...")
            return self._refresh_token()

        # If no token at all, fetch one now
        if not self.access_token:
            print("No access token, fetching initial token...")
            return self._refresh_token()

        return self.access_token

    def _refresh_token(self) -> str | None:
        """
        Refresh token using OAuth2 client credentials flow.
        """
        try:
            token_url = self.oauth2_config.get("oauth2_token_url")
            client_id = self.oauth2_config.get("oauth2_client_id")
            client_secret = self.oauth2_config.get("oauth2_client_secret")
            scopes = self.oauth2_config.get("oauth2_scopes")

            print(
                "=== OAuth2 Token Request ===",
                f"token_url={token_url}",
                f"client_id={client_id}",
                f"scopes={scopes}",
                sep="\n"
            )

            if not all([token_url, client_id, client_secret]):
                print("Missing OAuth2 config: token_url, client_id, or client_secret")
                return None

            # Prepare payload
            payload = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }

            if scopes:
                payload["scope"] = scopes

            print(f"Payload: {payload}")

            # Try JSON first (most common for OAuth2 servers)
            print("Attempting JSON request...")
            response = requests.post(
                token_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
                verify=True,
            )

            print(f"Token response status: {response.status_code}")
            print(f"Token response: {response.text}")

            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get("access_token")
                expires_in = token_data.get("expires_in", 3600)
                self.token_expiry = time.time() + expires_in - 60
                print(f"Token refreshed, expires in {expires_in}s")
                return self.access_token
            else:
                raise InterfaceError("Unable to obtain OAuth2 access token")

        except Exception as e:
            print(f"OAuth2 token refresh error: {e}")
            raise InterfaceError("Unable to obtain OAuth2 access token")

        return None


# ----------------------------------------------------------------------
# Connection
# ----------------------------------------------------------------------

class JSONAPIConnection:
    """
    Represents a JSON API backed DB-API connection.
    """

    def __init__(
        self,
        endpoint: str,
        encrypted_extra: dict | None = None,
        api_key: str | None = None,
        headers: Dict | None = None,
        timeout: int = 30,
        verify_ssl: bool = True,
        oauth2: Dict | None = None,
        oauth2_access_token: str | None = None,
        auth_config_type: str | None = None,
        **kwargs,
    ):
        if requests is None:
            raise ImportError("requests is required by superset-json-api-connector")

        if duckdb is None or pd is None:
            raise ImportError(
                "duckdb and pandas are required by the JSON API connector"
            )

        self.endpoint = endpoint
        self.timeout = timeout or 30
        self.verify_ssl = verify_ssl if verify_ssl is not None else True

        self.headers: Dict[str, str] = headers or {}
        self.headers.setdefault("Accept", "application/json")
        self.headers.setdefault(
            "User-Agent",
            "Superset-JSON-API-DBAPI/1.0",
        )

        # Normalize encrypted_extra
        if isinstance(encrypted_extra, str):
            try:
                encrypted_extra = json.loads(encrypted_extra)
            except Exception:
                encrypted_extra = {}
        elif not isinstance(encrypted_extra, dict):
            encrypted_extra = {}

        auth_type = encrypted_extra.get(
            "auth_config_type",
            auth_config_type or "no_auth",
        )

        # OAuth2 manager
        self.oauth2_manager: OAuth2TokenManager | None = None
        oauth2_config = oauth2 or encrypted_extra

        if oauth2_access_token:
            oauth2_config = oauth2_config or {}
            oauth2_config["oauth2_access_token"] = oauth2_access_token

        if oauth2_config:
            self.oauth2_manager = OAuth2TokenManager(oauth2_config)

        # Authentication header
        self._set_auth_header(
            auth_type=auth_type,
            api_key=api_key or encrypted_extra.get("api_key"),
            encrypted_extra=encrypted_extra,
        )

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        self.duck = duckdb.connect(database=":memory:")

    def _set_auth_header(
        self,
        auth_type: str = "no_auth",
        api_key: str | None = None,
        encrypted_extra: dict | None = None,
    ):
        """
        Configure Authorization header.

        Supported:
        - oauth2
        - api_key
        - basic_auth
        - superset_auth
        - no_auth
        """
        encrypted_extra = encrypted_extra or {}

        if "Authorization" in self.headers:
            return

        if auth_type == "oauth2":
            print('Using OAuth2 authentication')
            if not self.oauth2_manager:
                self.oauth2_manager = OAuth2TokenManager(encrypted_extra)
            token = self.oauth2_manager.get_token()
            if token:
                self.headers["Authorization"] = f"Bearer {token}"

        elif auth_type == "api_key":
            key = api_key or encrypted_extra.get("api_key")
            if key:
                self.headers["Authorization"] = f"Bearer {key}"

        elif auth_type == "basic_auth":
            user = encrypted_extra.get("basic_auth_username", "")
            pwd = encrypted_extra.get("basic_auth_password", "")
            if user or pwd:
                encoded = base64.b64encode(f"{user}:{pwd}".encode()).decode()
                self.headers["Authorization"] = f"Basic {encoded}"

        elif auth_type == "superset_auth":
            token = get_access_token()
            if token:
                self.headers["Authorization"] = f"Bearer {token}"

    def cursor(self):
        return JSONAPICursor(self)

    def close(self):
        self.session.close()
        self.duck.close()

    def commit(self):
        pass

    def rollback(self):
        pass


# ----------------------------------------------------------------------
# Cursor
# ----------------------------------------------------------------------

class JSONAPICursor:
    """
    DB-API cursor implementation.
    """

    def __init__(self, connection: JSONAPIConnection):
        self.connection = connection
        self._results: List[Tuple] = []
        self._description: List[Tuple] = []
        self._row_index = 0
        self.rowcount = -1
        self.arraysize = 1000

    def execute(self, operation: str | None = None, parameters: Dict | None = None):
        if not operation:
            raise ProgrammingError("No SQL provided")

        try:
            # Refresh OAuth token if required
            if self.connection.oauth2_manager:
                token = self.connection.oauth2_manager.get_token()
                if token:
                    self.connection.headers["Authorization"] = f"Bearer {token}"
                    self.connection.session.headers.update(self.connection.headers)

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

            body_preview = response.text[:2000]
            print("Response Body (preview):")
            print(body_preview)
            print("========================")

            if response.status_code == 401:
                # Try OAuth2 token refresh first
                if self.connection.oauth2_manager:
                    print("401 Unauthorized - attempting OAuth2 token refresh...")
                    token = self.connection.oauth2_manager._refresh_token()
                    if token:
                        self.connection.headers["Authorization"] = f"Bearer {token}"
                        self.connection.session.headers.update(
                            self.connection.headers
                        )
                        print("Token refreshed, retrying request...")
                        return self.execute(operation, parameters)
                    else:
                        print("OAuth2 token refresh failed")
                
                # For superset_auth, don't clear session - just fail
                raise InterfaceError(
                    "Authentication expired or invalid. Please check your credentials."
                )

            if response.status_code != 200:
                raise OperationalError(
                    f"HTTP {response.status_code}: {response.text[:200]}"
                )

            try:
                payload = response.json()
            except json.JSONDecodeError as exc:
                raise DataError(f"Invalid JSON response: {exc}")

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
            self._description = [
                (col[0], STRING, None, None, None, None, True)
                for col in result.description
            ]

        except Exception as exc:
            raise DatabaseError(str(exc))

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

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def _normalize_data(self, data: Any) -> List[Dict]:
        """
        Normalize API payload into list of dicts.
        """
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


# ----------------------------------------------------------------------
# DB-API type helpers
# ----------------------------------------------------------------------

Binary = bytes
Date = Time = Timestamp = str
DateFromTicks = TimeFromTicks = TimestampFromTicks = None
