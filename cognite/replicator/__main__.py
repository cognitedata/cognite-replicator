#!/usr/bin/env python
"""
A tool for replicating data from one CDF tenant to another.

To run, configure the source and destination CogniteClient with project and api_key.
API keys can be set as environment variables COGNITE_SOURCE_API_KEY and
COGNITE_DESTINATION_API_KEY or through command line arguments.

Example usage: poetry run replicator assets
"""
import argparse
import logging
import os
import sys
from enum import Enum, auto, unique
from pathlib import Path

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

from . import assets, configure_logger, events, raw, time_series

CLIENT_NAME = "cognite-replicator"
CLIENT_TIMEOUT = 120


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
    """Returns Argumentparser for command line interface."""
    parser = argparse.ArgumentParser()
    options = [i.name.lower() for i in Resource]
    parser.add_argument("resource", nargs="+", choices=options, help="Which resource types to replicate")

    parser.add_argument("--src-api-key", help="CDF API KEY of the source project to copy objects from")
    parser.add_argument("--dest-api-key", help="CDF API KEY of the destination project")
    parser.add_argument("--src-project", help="Project that src-api-key belongs to")
    parser.add_argument("--dest-project", help="Project that dest-api-key belongs to")

    parser.add_argument(
        "--resync-destination-tenant",
        action="store_true",
        help="Remove objects so destination is in sync with source tenant",
    )
    parser.add_argument(
        "--delete-if-removed-in-source",
        action="store_true",
        help="Remove objects that was replicated and are deleted in source now",
    )
    parser.add_argument(
        "--delete-if-not-replicated",
        action="store_true",
        help="Remove all objects in destination which was created/updated by replication",
    )

    parser.add_argument("--batch-size", default=10000, type=int, help="Number of items in each batch, if relevant")
    parser.add_argument("--number-of-threads", default=1, type=int, help="Number of thread to use, if relevant")

    parser.add_argument("--client-timeout", default=CLIENT_TIMEOUT, type=int, help="Seconds for clients to timeout")
    parser.add_argument("--client-name", default=CLIENT_NAME, help="Name of client, default: " + CLIENT_NAME)
    parser.add_argument("--log-path", default="log", help="Folder to save logs to")
    parser.add_argument("--log-level", default="info", help="Logging level")
    return parser


def _validate_login(src_client: CogniteClient, dest_client: CogniteClient, src_project: str, dest_project: str) -> bool:
    """Login with CogniteClients and validate projects if set."""
    try:
        src_login_status = src_client.login.status()
        dest_login_status = dest_client.login.status()
    except CogniteAPIError as exc:
        logging.fatal("Failed to login with CogniteClient {!s}".format(exc))
        return False
    if src_project and src_login_status.project != src_project:
        logging.fatal("Source project don't match with API key configuration")
        return False
    if dest_project and dest_login_status.project != dest_project:
        logging.fatal("Destination project don't match with API key configuration")
        return False
    return True


def main():
    args = create_cli_parser().parse_args()
    configure_logger(args.log_level, Path(args.log_path))

    delete_replicated_if_not_in_src = True if args.resync_destination_tenant else args.delete_if_removed_in_source
    delete_not_replicated_in_dst = True if args.resync_destination_tenant else args.delete_if_not_replicated
    src_api_key = args.src_api_key if args.src_api_key else os.environ.get("COGNITE_SOURCE_API_KEY")
    dest_api_key = args.src_api_key if args.dest_api_key else os.environ.get("COGNITE_DESTINATION_API_KEY")

    src_client = CogniteClient(
        api_key=src_api_key, project=args.src_project, client_name=args.client_name, timeout=args.client_timeout
    )
    dest_client = CogniteClient(
        api_key=dest_api_key, project=args.dest_project, client_name=args.client_name, timeout=args.client_timeout
    )

    if not _validate_login(src_client, dest_client, args.src_project, args.dest_project):
        sys.exit(2)

    resources_to_replicate = {Resource[i.upper()] for i in args.resource}
    if Resource.ALL in resources_to_replicate:
        resources_to_replicate.update({i for i in Resource})

    if Resource.ASSETS in resources_to_replicate:
        assets.replicate(
            src_client,
            dest_client,
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
        )

    if Resource.EVENTS in resources_to_replicate:
        events.replicate(
            src_client,
            dest_client,
            args.batch_size,
            args.number_of_threads,
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
        )

    if Resource.TIMESERIES in resources_to_replicate:
        time_series.replicate(
            src_client,
            dest_client,
            args.batch_size,
            args.number_of_threads,
            delete_replicated_if_not_in_src=delete_replicated_if_not_in_src,
            delete_not_replicated_in_dst=delete_not_replicated_in_dst,
        )

    if Resource.FILES in resources_to_replicate:
        pass

    if Resource.RAW in resources_to_replicate:
        raw.replicate(src_client, dest_client, args.batch_size)

    if Resource.DATAPOINTS in resources_to_replicate:
        pass


if __name__ == "__main__":
    main()
