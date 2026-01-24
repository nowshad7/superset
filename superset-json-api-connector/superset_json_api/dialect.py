from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import compiler
from sqlalchemy import types as sqltypes
from urllib.parse import unquote
import json
import base64


class JSONAPIIdentifierPreparer(compiler.IdentifierPreparer):
    """Ensures identifiers are quoted safely."""

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote='"', final_quote='"')


class JSONAPIDialect(DefaultDialect):
    """
    SQLAlchemy dialect for JSON-based REST APIs.

    Uses DuckDB as an in-memory execution engine.
    """

    name = "jsonapi"
    driver = "apsw"

    preparer = JSONAPIIdentifierPreparer

    supports_alter = False
    supports_sequences = False
    supports_pk_autoincrement = False
    supports_statement_cache = False
    supports_native_boolean = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_identifier_quoting = True

    default_paramstyle = "pyformat"
    max_identifier_length = 255

    @classmethod
    def dbapi(cls):
        from . import dbapi
        return dbapi

    def create_connect_args(self, url):
        """
        Extract endpoint and encrypted extras from SQLAlchemy URL.
        """
        params = {}
        endpoint = None

        if url.database:
            endpoint = unquote(url.database)

        if not endpoint:
            scheme = "https" if url.port == 443 else "http"
            endpoint = f"{scheme}://{url.host}"
            if url.port and url.port not in (80, 443):
                endpoint += f":{url.port}"

        if "encrypted_extra_b64" in url.query:
            try:
                encrypted_json = base64.b64decode(
                    url.query["encrypted_extra_b64"]
                ).decode()
                params["encrypted_extra"] = json.loads(encrypted_json)
            except Exception:
                params["encrypted_extra"] = {}

        for key, value in url.query.items():
            if key != "encrypted_extra_b64":
                params[key] = value

        return [endpoint], params

    def get_table_names(self, connection, schema=None, **kw):
        return ["api_data"]

    def has_table(self, connection, table_name, schema=None, **kw):
        return table_name == "api_data"

    def get_columns(self, connection, table_name, schema=None, **kw):
        cursor = connection.connection.cursor()
        cursor.execute("SELECT * FROM api_data LIMIT 1")

        columns = []
        for col in cursor.description or []:
            columns.append(
                {
                    "name": col[0],
                    "type": self._map_type(col[1]),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "primary_key": False,
                }
            )

        if not columns:
            columns.append(
                {
                    "name": "data",
                    "type": sqltypes.Text(),
                    "nullable": True,
                }
            )

        return columns

    def _map_type(self, type_code):
        from .dbapi import NUMBER
        return sqltypes.Float() if type_code == NUMBER else sqltypes.Text()

    def get_pk_constraint(self, *_, **__):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, *_, **__):
        return []

    def get_indexes(self, *_, **__):
        return []

    def get_schema_names(self, *_, **__):
        return ["memory"]

    def get_view_names(self, *_, **__):
        return []

    def get_temp_view_names(self, *_, **__):
        return []

    def do_ping(self, dbapi_connection):
        try:
            cur = dbapi_connection.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return True
        except Exception:
            return False

    def normalize_name(self, name):
        return name.lower() if name else name

    def denormalize_name(self, name):
        return name
