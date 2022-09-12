from pathlib import Path

import pytest
from cognite.client.testing import monkeypatch_cognite_client

from cognite.replicator.__main__ import (
    ENV_VAR_FOR_CONFIG_FILE_PATH,
    _get_config_path,
    _validate_login_apikey,
    create_cli_parser,
)


def test_validate_login():
    with monkeypatch_cognite_client() as client:
        valid = _validate_login_apikey(client, client, None, None, None, None)
        assert valid is True


def test_cli_parser():
    parser = create_cli_parser()
    args = parser.parse_args(args=["config/test.yml"])
    assert args.config == "config/test.yml"

    args = parser.parse_args(args=[])
    assert args.config is None
