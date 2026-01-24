# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from superset.databases.schemas import DatabasePostSchema


@dataclass
class DummySpec:
    # Simulate an engine spec that intentionally does not provide
    # a Marshmallow ``parameters_schema`` but does provide
    # ``build_sqlalchemy_uri`` used to construct the URI.
    parameters_schema: Any = None

    @staticmethod
    def build_sqlalchemy_uri(params: dict[str, Any], encrypted_extra: dict[str, Any] | None) -> str:
        # Return a deterministic placeholder URI for test purposes
        return "jsonapi://"


def test_dynamic_form_with_no_parameters_schema(monkeypatch):
    schema = DatabasePostSchema()

    # Ensure get_engine_spec returns the dummy spec that has parameters_schema=None
    import superset.db_engine_specs as engine_specs

    monkeypatch.setattr(engine_specs, "get_engine_spec", lambda engine, driver=None: DummySpec())

    # Payload using the dynamic_form configuration method. It should not raise
    # AttributeError even though DummySpec.parameters_schema is None.
    payload = {
        "database_name": "Report Engine (API)",
        "configuration_method": "dynamic_form",
        "engine": "jsonapi",
        "parameters": {"endpoint": "https://jsonplaceholder.typicode.com/posts"},
        "masked_encrypted_extra": "{}",
    }

    # Should not raise
    result = schema.load(payload)
    assert result["sqlalchemy_uri"] == "jsonapi://"

