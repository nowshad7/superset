from sqlalchemy import types


class JSONText(types.TypeDecorator):
    """Handle JSON string conversion"""
    impl = types.Text

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return str(value) if not isinstance(value, str) else value


class JSONDict(types.TypeDecorator):
    """Handle dictionary/object JSON"""
    impl = types.Text

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        import json
        if isinstance(value, dict):
            return value
        try:
            return json.loads(value) if isinstance(value, str) else value
        except:
            return {"data": value}