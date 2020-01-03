#!/usr/bin/env python
"""
A tool for replicating data from one CDF tenant to another.

To run, configure the source and destination CogniteClient with project and api_key.
API keys can be set as environment variables COGNITE_SOURCE_API_KEY and
COGNITE_DESTINATION_API_KEY or through command line arguments.

You must provide a config file, give the path either in environment variable
COGNITE_CONFIG_FILE or as command line argument.

Example usage: poetry run replicator
"""
import argparse
import logging
import os
import sys
from enum import Enum, auto, unique
from pathlib import Path
from typing import Optional

import yaml

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

from . import assets, configure_logger, datapoints, events, files, raw, time_series

ENV_VAR_FOR_CONFIG_FILE_PATH = "COGNITE_CONFIG_FILE"


@unique
class Resource(Enum):
    """CDF Resource types that can be replicated."""

    ALL = auto()
    ASSETS = auto()
    EVENTS = auto()
    RAW = auto()
    TIMESERIES = auto()
    DATAPOINTS = auto()
    FILES = auto()


def create_cli_parser() -> argparse.ArgumentParser:
    """Returns ArgumentParser for command line interface."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config", nargs="?", help="path to yaml configuration file")
    return parser


def _validate_login(src_client: CogniteClient, dst_client: CogniteClient, src_project: str, dst_project: str) -> bool:
    """Login with CogniteClients and validate projects if set."""
    try:
        src_login_status = src_client.login.status()
        dst_login_status = dst_client.login.status()
    except CogniteAPIError as exc:
        logging.fatal("Failed to login with CogniteClient {!s}".format(exc))
        return False
    if src_project and src_login_status.project != src_project:
        logging.fatal("Source project don't match with API key configuration")
        return False
    if dst_project and dst_login_status.project != dst_project:
        logging.fatal("Destination project don't match with API key configuration")
        return False
    return True


def _get_config_path(config_arg: Optional[str]) -> Path:
    """Get the config file, first either from given path or from env variable."""
    if config_arg:
        config_file = Path(config_arg)
    elif os.environ.get(ENV_VAR_FOR_CONFIG_FILE_PATH):
        config_file = Path(os.environ[ENV_VAR_FOR_CONFIG_FILE_PATH])
    else:
        config_file = None

    if not config_file or not config_file.is_file():
        logging.fatal(f"Config file not found: {config_file}")
        sys.exit(1)
    return config_file


def main():
    args = create_cli_parser().parse_args()
    with open(_get_config_path(args.config)) as config_file:
        config = yaml.safe_load(config_file.read())

    configure_logger(config.get("log_level", "INFO").upper(), Path(config.get("log_path", "log")))

    delete_replicated_if_not_in_src = config.get("delete_if_removed_in_source", False)
    delete_not_replicated_in_dst = config.get("delete_if_not_replicated", False)

    src_api_key = os.environ.get(config.get("src_api_key_env_var", "COGNITE_SOURCE_API_KEY"))
    dst_api_key = os.environ.get(config.get("dst_api_key_env_var", "COGNITE_DESTINATION_API_KEY"))

    src_client = CogniteClient(
        api_key=src_api_key,
        project=config.get("src_project"),
        client_name=config.get("client_name"),
        base_url=config.get("src_baseurl", "https://api.cognitedata.com"),
        timeout=config.get("client_timeout"),
    )
    dst_client = CogniteClient(
        api_key=dst_api_key,
        project=config.get("dst_project"),
        client_name=config.get("client_name"),
        base_url=config.get("dst_baseurl", "https://api.cognitedata.com"),
        timeout=config.get("client_timeout"),
    )

    if not _validate_login(src_client, dst_client, config.get("src_project"), config.get("dst_project")):
        sys.exit(2)

    resources_to_replicate = {Resource[resource.upper()] for resource in config.get("resources")}
    if Resource.ALL in resources_to_replicate:
        resources_to_replicate.update({resource for resource in Resource})

    if Resource.ASSETS in resources_to_replicate:
        assets.replicate(
            src_client,
            dst_client,
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
        )

    if Resource.EVENTS in resources_to_replicate:
        events.replicate(
            src_client,
            dst_client,
            config.get("batch_size"),
            config.get("number_of_threads"),
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
        )

    if Resource.TIMESERIES in resources_to_replicate:
        time_series.replicate(
            src_client,
            dst_client,
            config.get("batch_size"),
            config.get("number_of_threads"),
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
            target_external_ids=config.get("timeseries_external_ids"),
            exclude_pattern=config.get("timeseries_exclude_pattern"),
        )

    if Resource.FILES in resources_to_replicate:
        files.replicate(
            src_client,
            dst_client,
            config.get("batch_size"),
            config.get("number_of_threads"),
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
        )

    if Resource.RAW in resources_to_replicate:
        raw.replicate(src_client, dst_client, config.get("batch_size"))

    if Resource.DATAPOINTS in resources_to_replicate:
        datapoints.replicate(
            client_src=src_client,
            client_dst=dst_client,
            batch_size=config.get("batch_size_datapoints"),
            num_threads=config.get("number_of_threads"),
            limit=config.get("datapoint_limit"),
            external_ids=config.get("timeseries_external_ids"),
            start=config.get("datapoints_start"),
            end=config.get("datapoints_end"),
            exclude_pattern=config.get("timeseries_exclude_pattern"),
        )


if __name__ == "__main__":
    main()
