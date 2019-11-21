from pathlib import Path

import pytest
import datetime
import time

from cognite.client.testing import monkeypatch_cognite_client
from cognite.replicator.__main__ import (
    ENV_VAR_FOR_CONFIG_FILE_PATH,
    _get_config_path,
    _validate_login,
    create_cli_parser,
    _get_datapoints_end_timestamp,
)


def test_get_datapoints_end_timestamp():
    now = int(datetime.datetime(2019, 1, 31, 12).timestamp())
    timeseries_delay_seconds = 604800
    datapoints_end_timestamp = _get_datapoints_end_timestamp(now, timeseries_delay_seconds)

    assert datapoints_end_timestamp == int(datetime.datetime(2019, 1, 24, 12).timestamp()) * 1000

    datapoints_end_timestamp = _get_datapoints_end_timestamp(now, None)
    assert datapoints_end_timestamp is None


def test_validate_login():
    with monkeypatch_cognite_client() as client:
        valid = _validate_login(client, client, None, None)
        assert valid is True
        wrong_src = _validate_login(client, client, "src", None)
        assert wrong_src is False
        wrong_dest = _validate_login(client, client, None, "dest")
        assert wrong_dest is False


def test_cli_parser():
    parser = create_cli_parser()
    args = parser.parse_args(args=["config/test.yml"])
    assert args.config == "config/test.yml"

    args = parser.parse_args(args=[])
    assert args.config is None


def test_get_config_path(mocker):
    with pytest.raises(SystemExit):
        _get_config_path(None)

    config_path = _get_config_path(__file__)
    assert config_path.is_file()

    mocker.patch.dict("os.environ", {ENV_VAR_FOR_CONFIG_FILE_PATH: str(__file__)})
    config_path = _get_config_path(None)
    assert config_path == Path(__file__)

    config_path = _get_config_path(__file__)
    assert config_path == Path(__file__)
