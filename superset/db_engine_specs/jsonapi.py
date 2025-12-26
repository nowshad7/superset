from superset.db_engine_specs.base import BaseEngineSpec


class JSONAPIEngineSpec(BaseEngineSpec):
    engine = "jsonapi"
    engine_name = "Report Engine (API)"

    allows_joins = True
    allows_subqueries = True
    allows_alias_in_select = True
    allows_sql_comments = True
    allows_sql_validation = False
    sqlalchemy_uri_placeholder = "jsonapi://"

