from superset.db_engine_specs.gsheets import GSheetsEngineSpec


class JSONAPIEngineSpec(GSheetsEngineSpec):
    engine = "jsonapi"
    engine_name = "Report Engine (API)"
    sqlalchemy_uri_placeholder = "jsonapi://"
