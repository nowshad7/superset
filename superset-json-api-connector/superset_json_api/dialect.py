from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import compiler
from sqlalchemy import types as sqltypes


class JSONAPIIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="", final_quote="")


class JSONAPIDialect(DefaultDialect):
    name = "jsonapi"
    driver = "jsonapi"

    preparer = JSONAPIIdentifierPreparer

    supports_alter = False
    supports_sequences = False
    supports_pk_autoincrement = False
    supports_statement_cache = False
    supports_native_boolean = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    default_paramstyle = "pyformat"
    max_identifier_length = 255

    # ---------------- DBAPI ----------------

    @classmethod
    def dbapi(cls):
        from . import dbapi
        return dbapi

    def create_connect_args(self, url):
        scheme = "https" if url.port == 443 else "http"
        endpoint = f"{scheme}://{url.host}"

        if url.port and url.port not in (80, 443):
            endpoint += f":{url.port}"

        if url.database:
            endpoint += f"/{url.database}"

        params = {}

        for key, value in url.query.items():
            if key in ("api_key", "token"):
                params["api_key"] = value
            elif key == "timeout":
                params["timeout"] = int(value)
            elif key == "verify_ssl":
                params["verify_ssl"] = value.lower() in ("1", "true", "yes")
            else:
                params.setdefault("headers", {})[key] = value

        return [endpoint], params

    # ---------------- Schema ----------------

    def get_table_names(self, connection, schema=None, **kw):
        return ["api_data"]

    def has_table(self, connection, table_name, schema=None, **kw):
        return table_name == "api_data"

    def get_columns(self, connection, table_name, schema=None, **kw):
        # 🔑 IMPORTANT FIX
        dbapi_conn = connection.connection
        cursor = dbapi_conn.cursor()

        cursor.execute("SELECT * FROM api_data LIMIT 1")

        description = cursor.description or []

        columns = []
        for col in description:
            name = col[0]
            type_code = col[1]

            columns.append({
                "name": name,
                "type": self._map_type(type_code),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "primary_key": False,
            })

        if not columns:
            columns.append({
                "name": "data",
                "type": sqltypes.Text(),
                "nullable": True,
                "default": None,
            })

        return columns

    def _map_type(self, type_code):
        from .dbapi import NUMBER
        if type_code == NUMBER:
            return sqltypes.Float()
        return sqltypes.Text()

    # ---------------- Constraints ----------------

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_schema_names(self, connection, **kw):
        return ["default"]

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    # ---------------- Health Check ----------------

    def do_ping(self, dbapi_connection):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1 LIMIT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False

    # ---------------- Name Handling ----------------

    def normalize_name(self, name):
        return name.lower() if name else name

    def denormalize_name(self, name):
        return name
