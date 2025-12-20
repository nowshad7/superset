from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql import compiler
from sqlalchemy import types as sqltypes, exc
from sqlalchemy.engine.reflection import Inspector
from typing import Optional, List, Dict, Any
import re
from .dbapi import JSONAPIConnection


class JSONAPIIdentifierPreparer(compiler.IdentifierPreparer):
    """Identifier preparer for JSON API dialect"""

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="", final_quote="")


class JSONAPIDialect(DefaultDialect):
    """SQLAlchemy Dialect for JSON API"""

    name = 'jsonapi'
    driver = 'jsonapi'

    # DBAPI module
    dbapi_class = JSONAPIConnection

    # Disable transactions for read-only API
    supports_native_decimal = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_boolean = True
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = False
    supports_statement_cache = False
    supports_sequences = False
    supports_isolation_level = False

    # Use custom identifier preparer
    preparer = JSONAPIIdentifierPreparer

    # Max identifier length
    max_identifier_length = 255

    # Default statement
    default_paramstyle = 'pyformat'

    @classmethod
    def dbapi(cls):
        """Return DBAPI module"""
        from . import dbapi
        return dbapi

    def create_connect_args(self, url):
        """Parse connection URL and create connection arguments"""

        # Extract endpoint from URL
        scheme = 'https' if url.port == 443 else 'http'
        if url.host:
            endpoint = f"{scheme}://{url.host}"
            if url.port and url.port not in [80, 443]:
                endpoint = f"{endpoint}:{url.port}"
            if url.database:
                endpoint = f"{endpoint}/{url.database}"
        else:
            endpoint = url.database or ''

        # Parse query parameters for configuration
        params = {}
        if url.query:
            for key, value in url.query.items():
                if key in ['api_key', 'api_token', 'token']:
                    params['api_key'] = value
                elif key == 'headers':
                    import json
                    try:
                        params['headers'] = json.loads(value)
                    except:
                        params['headers'] = {'Custom-Header': value}
                elif key == 'timeout':
                    params['timeout'] = int(value)
                elif key == 'verify_ssl':
                    params['verify_ssl'] = value.lower() in ['true', '1', 'yes']
                else:
                    # Pass other params as headers
                    params.setdefault('headers', {})[key] = value

        return [endpoint], params

    def connect(self, *args, **kwargs):
        """Create a DBAPI connection"""
        return self.dbapi_class(*args, **kwargs)

    def get_columns(self, connection, table_name: str, schema: str = None, **kwargs):
        """Fetch column information from API - MUST return proper format"""
        try:
            # Create a cursor
            cursor = connection.connection.cursor()

            # Make a test request with small limit
            cursor.execute("SELECT * FROM data LIMIT 5")
            rows = cursor.fetchmany(5)

            columns = []
            if rows:
                # Use first row to infer schema
                first_row = rows[0]

                for col_name, value in first_row.items():
                    # Determine SQLAlchemy type
                    if isinstance(value, int):
                        coltype = sqltypes.Integer()
                    elif isinstance(value, float):
                        coltype = sqltypes.Float()
                    elif isinstance(value, bool):
                        coltype = sqltypes.Boolean()
                    elif isinstance(value, (dict, list)):
                        coltype = sqltypes.JSON()
                    else:
                        coltype = sqltypes.Text()

                    columns.append({
                        'name': col_name,
                        'type': coltype,
                        'nullable': True,
                        'default': None,
                        'autoincrement': False,
                        'primary_key': False
                    })

            # If no columns found, create a default column
            if not columns:
                columns = [{
                    'name': 'data',
                    'type': sqltypes.Text(),
                    'nullable': True,
                    'default': None
                }]

            return columns

        except Exception as e:
            print(f"Error getting columns: {e}")
            # Return minimal column info
            return [{
                'name': 'data',
                'type': sqltypes.Text(),
                'nullable': True,
                'default': None
            }]

    def _get_sqlalchemy_type(self, value: Any):
        """Map Python type to SQLAlchemy type"""
        if value is None:
            return sqltypes.Text()
        elif isinstance(value, int):
            return sqltypes.Integer()
        elif isinstance(value, float):
            return sqltypes.Float()
        elif isinstance(value, bool):
            return sqltypes.Boolean()
        elif isinstance(value, dict) or isinstance(value, list):
            return sqltypes.JSON()
        elif isinstance(value, str):
            # Check if it's a date/time string
            date_patterns = [
                r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
                r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}',  # ISO date
            ]
            for pattern in date_patterns:
                if re.match(pattern, value):
                    return sqltypes.DateTime()
            return sqltypes.Text()
        else:
            return sqltypes.Text()

    def has_table(self, connection, table_name: str, schema: str = None, **kwargs):
        """APIs don't have traditional tables, so always return True for default table"""
        return table_name.lower() in ['api_data']

    def get_table_names(self, connection, schema: str = None, **kwargs):
        """Return table names - Superset needs this for dataset creation"""
        return ['api_data', 'json_data', 'data']

    def get_schema_names(self, connection, **kwargs):
        """Return schema names - Superset expects this"""
        return ['default', 'public']

    def get_pk_constraint(self, connection, table_name: str, schema: str = None, **kwargs):
        """No primary keys in API data"""
        return {'constrained_columns': [], 'name': None}

    def get_foreign_keys(self, connection, table_name: str, schema: str = None, **kwargs):
        """No foreign keys in API data"""
        return []

    def get_indexes(self, connection, table_name: str, schema: str = None, **kwargs):
        """No indexes in API data"""
        return []

    def do_ping(self, dbapi_connection):
        """Test if API is reachable"""
        try:
            cursor = dbapi_connection.cursor()
            # Try to fetch a small amount of data
            cursor.execute("SELECT 1 LIMIT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False

    def get_check_constraints(self, connection, table_name: str, schema: str = None, **kwargs):
        return []

    def normalize_name(self, name):
        """Normalize identifier names"""
        if name is None:
            return None
        return name.lower()

    def denormalize_name(self, name):
        """Denormalize identifier names"""
        if name is None:
            return None
        return name

    def get_view_names(self, connection, schema=None, **kwargs):
        """Return empty list for views - required by Superset"""
        return []

    def get_temp_view_names(self, connection, schema=None, **kwargs):
        """Return empty list for temp views"""
        return []
