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
import time
from enum import Enum, auto, unique
from pathlib import Path
from typing import Optional
import jwt
import msal
import getpass
import yaml
from cognite.client import CogniteClient, ClientConfig
from cognite.client.exceptions import CogniteAPIError
from cognite.client.credentials import OAuthClientCredentials, Token, OAuthInteractive, APIKey
from cognite.client.data_classes import assets, datapoints, events, files, raw, time_series

# import __init__
import cognite.replicator.__init__
import cognite.replicator.assets
import cognite.replicator.datapoints
import cognite.replicator.events
import cognite.replicator.files
import cognite.replicator.raw
import cognite.replicator.replication
import cognite.replicator.sequences
import cognite.replicator.time_series

ENV_VAR_FOR_CONFIG_FILE_PATH = "COGNITE_CONFIG_FILE"

src_dst_dataset_mapping = {}


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


def _validate_login_apikey(
    src_client: CogniteClient,
    dst_client: CogniteClient,
    src_project: str,
    dst_project: str,
    src_api_authentication: str,
    dst_api_authentication: str,
) -> bool:
    """Login with CogniteClients and validate projects if set."""
    try:
        if src_api_authentication:
            src_login_status = src_client.login.status()
            if src_project and src_login_status.project != src_project:
                logging.fatal("Source project don't match with API key configuration")
                return False
        if dst_api_authentication:
            dst_login_status = dst_client.login.status()
            if dst_project and dst_login_status.project != dst_project:
                logging.fatal("Destination project don't match with API key configuration")
                return False

    except CogniteAPIError as exc:
        logging.fatal("Failed to login with CogniteClient {!s}".format(exc))
        return False

    return True


def _validate_capabilities_oidc(
    src_client: CogniteClient,
    dst_client: CogniteClient,
    needed_capabilities: [str],
    src_api_authentication: str,
    dst_api_authentication: str,
) -> bool:
    """Login with CogniteClients and validate projects if set."""

    # print(needed_capabilities)                                                                                       # check which resources are going to be replicated - need read access for source and write access for destination

    # which capabilities to check for in the capabilities list
    check_for_capabilities = []

    # creating the list of which capabilities to search for in the cognite client object
    if "assets" in needed_capabilities:
        check_for_capabilities.append("assetsAcl")
    if "events" in needed_capabilities:
        check_for_capabilities.append("eventsAcl")
    if "timeseries" in needed_capabilities:
        check_for_capabilities.append("timeSeriesAcl")
    if "sequences" in needed_capabilities:
        check_for_capabilities.append("sequencesAcl")
    if "files" in needed_capabilities:
        check_for_capabilities.append("filesAcl")
    if "raw" in needed_capabilities:
        check_for_capabilities.append("rawAcl")

    # TODO: add, so that these resources can be replicated as well
    # if 'relationships' in needed_capabilities:
    #    check_for_capabilities.append('relationshipsAcl')
    # if 'labels' in needed_capabilities:
    #    check_for_capabilities.append('labelsAcl')
    # if 'types' in needed_capabilities:
    #    check_for_capabilities.append('typesAcl')
    # if 'datasets' in needed_capabilities:
    #    check_for_capabilities.append('datasetsAcl')

    try:
        if src_api_authentication:
            # check that src_capabilities are read for all the mentioned resources
            src_capabilities = src_client.iam.token.inspect().capabilities
            for check_capability in check_for_capabilities:
                for capability in src_capabilities:
                    if check_capability in capability.keys():
                        if not "READ" in capability[check_capability]["actions"]:
                            return False
        if dst_api_authentication:
            dst_capabilities = dst_client.iam.token.inspect().capabilities
            for check_capability in check_for_capabilities:
                for capability in dst_capabilities:
                    if check_capability in capability.keys():
                        if not "WRITE" in capability[check_capability]["actions"]:
                            return False

    except CogniteAPIError as exc:
        logging.fatal(
            "Mismatch in needed capabilities with project capabilities with the following message: ".format(exc)
        )
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


def get_lines_in_file(config_file):
    """
    Convert and return file to array lines. It excludes comment lines.
    Args:
        config_file: Config file
    Returns:
        Array of lines
    """
    lines = []

    line = config_file.readline()
    lines.append([1, line])

    line_counter = 1
    while line:
        line = config_file.readline()
        if not (line.lstrip().startswith("#")):
            lines.append([line_counter, line])

        line_counter += 1

    print(len(lines))

    return lines


def get_repeat_line_numbers(lines):
    """
    Return repeated lines
    Args:
        lines: Array lines
    Returns:
        Array if line number
    """
    repeat_line_numbers = []

    for line in lines:
        lines_number_found = [x[0] for x in lines if x[1] == line[1]]
        if len(lines_number_found) > 1:
            repeat_line_numbers.append(line[0])

    return repeat_line_numbers


def get_no_repeat_lines_as_string(lines):
    """
    Read array lines and return lines without duplicates in single string
    Args:
        lines: Array lines
    Returns:
        single string
    """
    unique_lines = []

    for line in lines:
        if len(line[1]) > 0 and not (line[1] in unique_lines):
            unique_lines.append(line[1])

    return "".join(unique_lines)


# src_SCOPES = [f"https://{src_CDF_CLUSTER}.cognitedata.com/.default"]
# src_AUTHORITY_URI: src_AUTHORITY_HOST_URI + "/" + src_TENANT_ID
# dst_SCOPES: [f"https://{dst_CDF_CLUSTER}.cognitedata.com/.default"]
# dst_AUTHORITY_URI: dst_AUTHORITY_HOST_URI + "/" + dst_TENANT_ID


def main():
    args = create_cli_parser().parse_args()
    with open(_get_config_path(args.config)) as config_file:
        config_file_lines = get_lines_in_file(config_file)
        repeat_line_numbers = get_repeat_line_numbers(config_file_lines)
        config_file_str = get_no_repeat_lines_as_string(config_file_lines)
        config = yaml.safe_load(config_file_str)

    cognite.replicator.__init__.configure_logger(
        config.get("log_level", "INFO").upper(), Path(config.get("log_path", "log"))
    )

    if len(repeat_line_numbers) > 0:
        for line_number in repeat_line_numbers:
            line_found = [x for x in config_file_lines if x[0] == line_number][0]
            logging.info(f"Config file - Repeat line {str(line_found[0])}: { line_found[1]}")

    delete_replicated_if_not_in_src = config.get("delete_if_removed_in_source", False)
    delete_not_replicated_in_dst = config.get("delete_if_not_replicated", False)

    if config.get("src_authenticate_api_key"):
        # create source Cognite client with API Key authentication
        src_api_key = os.environ.get(config.get("src_api_key_env_var", "COGNITE_SOURCE_API_KEY"))
        src_client = CogniteClient(
            ClientConfig(
                credentials=APIKey(src_api_key),
                project=config.get("src_COGNITE_PROJECT"),
                client_name=config.get("client_name"),
                base_url=config.get("src_baseurl", "https://api.cognitedata.com"),
                timeout=config.get("client_timeout"),
            )
        )
    else:
        # create source Cognite client with OIDC authentication
        src_cluster = config.get("src_CDF_CLUSTER")
        src_tenant_id = config.get("src_TENANT_ID")
        if not config.get("src_boolean_client_secret"):
            # authenticate by implicit login
            src_uri = config.get("src_AUTHORITY_HOST_URI") + "/" + src_tenant_id
            oauth_provider = OAuthInteractive(
                authority_url=src_uri,
                client_id=config.get("src_CLIENT_ID"),
                scopes=[f"https://{src_cluster}.cognitedata.com/.default"],
            )

            src_client = CogniteClient(
                ClientConfig(
                    credentials=oauth_provider,
                    project=config.get("src_COGNITE_PROJECT"),
                    base_url=f"https://{src_cluster}.cognitedata.com",
                    client_name="cognite-replicator",
                )
            )

        else:
            # authenticate by client secret
            creds = OAuthClientCredentials(
                token_url=f"https://login.microsoftonline.com/{src_tenant_id}/oauth2/v2.0/token",
                client_id=config.get("src_CLIENT_ID"),
                scopes=[f"https://{src_cluster}.cognitedata.com/.default"],
                client_secret=os.environ.get(config.get("src_client_secret", "COGNITE_SOURCE_CLIENT_SECRET")),
            )
            src_client = CogniteClient(
                ClientConfig(
                    credentials=creds,
                    project=config.get("src_COGNITE_PROJECT"),
                    base_url=f"https://{src_cluster}.cognitedata.com",
                    client_name="cognite-replicator",
                )
            )
        # print(src_client.iam.token.inspect())

    if config.get("dst_authenticate_api_key"):
        # create source Cognite client with API Key authentication
        dst_api_key = os.environ.get(config.get("dst_api_key_env_var", "COGNITE_DESTINATION_API_KEY"))
        dst_client = CogniteClient(
            ClientConfig(
                credentials=APIKey(dst_api_key),
                project=config.get("dst_COGNITE_PROJECT"),
                client_name=config.get("client_name"),
                base_url=config.get("dst_baseurl", "https://api.cognitedata.com"),
                timeout=config.get("client_timeout"),
            )
        )

    else:
        # create destination Cognite client with OIDC authentication
        dst_cluster = config.get("dst_CDF_CLUSTER")
        dst_tenant_id = config.get("dst_TENANT_ID")
        if not config.get("dst_boolean_client_secret"):
            # authenticate by implicit login
            dst_uri = config.get("dst_AUTHORITY_HOST_URI") + "/" + config.get("dst_TENANT_ID")

            oauth_provider = OAuthInteractive(
                authority_url=dst_uri,
                client_id=config.get("dst_CLIENT_ID"),
                scopes=[f"https://{dst_cluster}.cognitedata.com/.default"],
            )

            dst_client = CogniteClient(
                ClientConfig(
                    credentials=oauth_provider,
                    project=config.get("dst_COGNITE_PROJECT"),
                    base_url=f"https://{dst_cluster}.cognitedata.com",
                    client_name="cognite-replicator",
                )
            )

        else:
            # authenticate by client secret
            creds = OAuthClientCredentials(
                token_url=f"https://login.microsoftonline.com/{dst_tenant_id}/oauth2/v2.0/token",
                client_id=config.get("dst_CLIENT_ID"),
                scopes=[f"https://{dst_cluster}.cognitedata.com/.default"],
                client_secret=os.environ.get(config.get("dst_client_secret", "COGNITE_DESTINATION_CLIENT_SECRET")),
            )
            dst_client = CogniteClient(
                ClientConfig(
                    credentials=creds,
                    project=config.get("dst_COGNITE_PROJECT"),
                    base_url=f"https://{dst_cluster}.cognitedata.com",
                    client_name="cognite-replicator",
                )
            )
            # print(dst_client.iam.token.inspect())

    # check that all capabilities / login is in place

    # if at least one project authenticates with API keys
    if config.get("src_authenticate_api_key") or config.get("dst_authenticate_api_key"):
        if not _validate_login_apikey(
            src_client,
            dst_client,
            config.get("src_COGNITE_PROJECT"),
            config.get("dst_COGNITE_PROJECT"),
            config.get("src_authenticate_api_key"),
            config.get("dst_authenticate_api_key"),
        ):
            sys.exit(2)

    # if at least one project authenticates with OIDC
    if not (config.get("src_authenticate_api_key") or config.get("dst_authenticate_api_key")):
        if not _validate_capabilities_oidc(
            src_client,
            dst_client,
            config.get("resources"),
            config.get("str_authenticate_api_key"),
            config.get("dst_authenticate_api_key"),
        ):
            sys.exit(2)

    # REPLICATION PROCESS
    print("Starting replication of resources")

    resources_to_replicate = {Resource[resource.upper()] for resource in config.get("resources")}
    if Resource.ALL in resources_to_replicate:
        resources_to_replicate.update({resource for resource in Resource})

    if Resource.ASSETS in resources_to_replicate:
        print("Replicating assets...")
        cognite.replicator.assets.replicate(
            src_client,
            dst_client,
            src_dst_dataset_mapping,
            config,
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
        )

    if Resource.EVENTS in resources_to_replicate:
        print("Replicating events...")
        cognite.replicator.events.replicate(
            src_client,
            dst_client,
            src_dst_dataset_mapping,
            config,
            config.get("batch_size"),
            config.get("number_of_threads"),
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
            target_external_ids=config.get("events_external_ids"),
            exclude_pattern=config.get("events_exclude_pattern"),
        )

    if Resource.TIMESERIES in resources_to_replicate:
        print("Replicating time series...")
        cognite.replicator.time_series.replicate(
            src_client,
            dst_client,
            src_dst_dataset_mapping,
            config,
            config.get("batch_size"),
            config.get("number_of_threads"),
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
            target_external_ids=config.get("timeseries_external_ids"),
            exclude_pattern=config.get("timeseries_exclude_pattern"),
            exclude_fields=config.get("timeseries_exclude_fields"),
        )

    if Resource.FILES in resources_to_replicate:
        print("Replicating files...")
        cognite.replicator.files.replicate(
            src_client,
            dst_client,
            src_dst_dataset_mapping,
            config,
            config.get("batch_size"),
            config.get("number_of_threads"),
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
            target_external_ids=config.get("files_external_ids"),
            exclude_pattern=config.get("files_exclude_pattern"),
        )

    if Resource.RAW in resources_to_replicate:
        print("Replicating raw...")
        cognite.replicator.raw.replicate(src_client, dst_client, config.get("batch_size"))

    if Resource.DATAPOINTS in resources_to_replicate:
        print("Replicating datapoints...")
        cognite.replicator.datapoints.replicate(
            client_src=src_client,
            client_dst=dst_client,
            limit=config.get("datapoint_limit"),
            external_ids=config.get("timeseries_external_ids"),
            start=config.get("datapoints_start"),
            end=config.get("datapoints_end"),
            exclude_pattern=config.get("timeseries_exclude_pattern"),
            value_manipulation_lambda_fnc=config.get("value_manipulation_lambda_fnc"),
        )


if __name__ == "__main__":
    start = time.time()
    main()
    logging.info(f"Replication finished at: {time.ctime()}")
    logging.info(f"Minutes spent in the replication: {((time.time()-start)/60)}")
