from typing import Any, Dict
import json
import base64
from urllib.parse import quote

from superset.db_engine_specs.gsheets import GSheetsEngineSpec


class JSONAPIEngineSpec(GSheetsEngineSpec):
    """
    SQLAlchemy EngineSpec for REST/JSON APIs.

    This engine encodes API configuration into the SQLAlchemy URI and
    passes authentication details securely via encrypted extras.
    """

    engine = "jsonapi"
    engine_name = "Report Engine (API)"
    sqlalchemy_uri_placeholder = "jsonapi://"

    parameters_schema = None

    @classmethod
    def parameters_json_schema(cls) -> dict[str, Any] | None:
        """
        JSON schema describing connection parameters.
        """
        schema: dict[str, Any] = {
            "type": "object",
            "properties": {
                "endpoint": {
                    "type": "string",
                    "description": "API endpoint URL",
                },
                "auth_config_type": {
                    "type": "string",
                    "description": "Authentication type",
                    "enum": [
                        "no_auth",
                        "superset_auth",
                        "basic_auth",
                        "api_key",
                        "oauth2",
                    ],
                },
                "api_key": {
                    "type": "string",
                    "description": "API key (for api_key auth)",
                    "x-encrypted-extra": True,
                },
                "oauth2_client_id": {
                    "type": "string",
                    "description": "OAuth2 client ID",
                },
                "oauth2_client_secret": {
                    "type": "string",
                    "description": "OAuth2 client secret",
                    "x-encrypted-extra": True,
                },
                "oauth2_token_url": {
                    "type": "string",
                    "description": "OAuth2 token endpoint",
                },
                "oauth2_scopes": {
                    "type": "string",
                    "description": "Space-separated OAuth2 scopes",
                },
                "oauth2_access_token": {
                    "type": "string",
                    "description": "Static OAuth2 access token",
                    "x-encrypted-extra": True,
                },
                "basic_auth_username": {
                    "type": "string",
                    "description": "Basic auth username",
                },
                "basic_auth_password": {
                    "type": "string",
                    "description": "Basic auth password",
                    "x-encrypted-extra": True,
                },
            },
            "required": ["endpoint", "auth_config_type"],
            "oneOf": [
                {
                    "properties": {"auth_config_type": {"enum": ["api_key"]}},
                    "required": ["api_key"],
                },
                {
                    "properties": {"auth_config_type": {"enum": ["basic_auth"]}},
                    "required": [
                        "basic_auth_username",
                        "basic_auth_password",
                    ],
                },
                {
                    "properties": {"auth_config_type": {"enum": ["oauth2"]}},
                    "required": [
                        "oauth2_client_id",
                        "oauth2_client_secret",
                        "oauth2_token_url",
                    ],
                },
                {
                    "properties": {
                        "auth_config_type": {
                            "enum": ["no_auth", "superset_auth"]
                        }
                    }
                },
            ],
        }

        return schema

    @staticmethod
    def update_params_from_encrypted_extra(
        database: Any,
        params: dict[str, Any],
    ) -> None:
        """
        Intentionally no-op.

        All DBAPI configuration is embedded in the SQLAlchemy URI
        to avoid SQLAlchemy validation issues.
        """
        return None

    @classmethod
    def build_sqlalchemy_uri(
        cls,
        parameters: dict[str, Any],
        encrypted_extra: dict[str, Any] | None = None,
    ) -> str:
        """
        Build SQLAlchemy URI with encoded endpoint and encrypted extras.
        """
        endpoint = ""

        if isinstance(parameters, dict):
            endpoint = parameters.get("endpoint", "")

        if not endpoint and isinstance(encrypted_extra, dict):
            endpoint = encrypted_extra.get("endpoint", "")

        if not endpoint:
            return "jsonapi:///"

        encoded_endpoint = quote(
            endpoint,
            safe=":/?#[]@!$&'()*+,;=",
        )

        uri = f"jsonapi:///{encoded_endpoint}"

        if isinstance(parameters, dict):
            encrypted_json = json.dumps(parameters)
            encrypted_b64 = base64.b64encode(
                encrypted_json.encode()
            ).decode()
            uri += f"?encrypted_extra_b64={quote(encrypted_b64)}"

        return uri
