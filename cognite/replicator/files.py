import copy
import logging
import time
from typing import Dict, List

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes import Event

from . import replication


def replicate_files(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    batch_size: int = 10000,
    num_threads: int = 1,
    delete_replicated_if_not_in_src: bool = False,
    delete_not_replicated_in_dst: bool = False,
):
    """
    Replicates all the events from the source project into the destination project.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        batch_size: The biggest batch size to post chunks in.
        num_threads: The number of threads to be used.
        delete_replicated_if_not_in_src: If True, will delete replicated events that are in the destination,
        but no longer in the source project (Default=False).
        delete_not_replicated_in_dst: If True, will delete events from the destination if they were not replicated
        from the source (Default=False).
    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    files_src = client_src.files.list(limit=None)
    files_dst = client_dst.files.list(limit=None)

    logging.info(f"There are {len(files_src)} existing events in source ({project_src}).")
    logging.info(f"There are {len(files_dst)} existing events in destination ({project_dst}).")

    src_id_dst_event = replication.make_id_object_map(files_dst)

    assets_dst = client_dst.assets.list(limit=None)
    src_dst_ids_assets = replication.existing_mapping(*assets_dst)
    logging.info(
        f"If an events asset ids is one of the {len(src_dst_ids_assets)} assets "
        f"that have been replicated then it will be linked."
    )

    replicated_runtime = int(time.time()) * 1000
    logging.info(f"These copied/updated events will have a replicated run time of: {replicated_runtime}.")

    logging.info(
        f"Starting to copy and update {len(files_src)} events from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if len(files_src) > batch_size:
        replication.thread(
            num_threads=num_threads,
            copy=copy_events,
            src_objects=files_src,
            src_id_dst_obj=src_id_dst_event,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            replicated_runtime=replicated_runtime,
            client=client_dst,
        )
    else:
        copy_events(
            src_events=files_src,
            src_id_dst_event=src_id_dst_event,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            runtime=replicated_runtime,
            client=client_dst,
        )

    logging.info(
        f"Finished copying and updating {len(files_src)} events from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if delete_replicated_if_not_in_src:
        ids_to_delete = replication.find_objects_to_delete_if_not_in_src(files_src, files_dst)
        client_dst.events.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} events in destination ({project_dst})"
            f" because they were no longer in source ({project_src})   "
        )
    if delete_not_replicated_in_dst:
        ids_to_delete = replication.find_objects_to_delete_not_replicated_in_dst(files_dst)
        client_dst.events.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} events in destination ({project_dst}) because"
            f"they were not replicated from source ({project_src})   "
        )


def create_file(src_file: FileMetadata, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int) -> FileMetadata:
    """
    Makes a new copy of the FileMetadata to be replicated based on a source event.

    Args:
        src_file: The FileMetadata from the source to be replicated to destination.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The replicated FileMetaData to be created in the destination.
    """
    logging.debug(f"Creating a new event based on source event id {src_file.id}")

    copied_filemetadata = copy.copy(src_file)
    copied_filemetadata.metadata = replication.new_metadata(src_file, project_src, runtime)
    copied_filemetadata.asset_ids = replication.get_asset_ids(src_file.asset_ids, src_dst_ids_assets)

    return copied_filemetadata


def update_file(
    src_file: FileMetadata, dst_file: FileMetadata, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> FileMetadata:
    """
    Makes an updated version of the destination file based on the corresponding source file.

    Args:
        src_file: The event from the source to be replicated.
        dst_file: The event from the destination that needs to be updated to reflect changes made to its source event.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The updated FileMetadata object for the replication destination.
    """
    logging.debug(f"Updating existing event {dst_file.id} based on source FileMetadata id {src_file.id}")

    dst_file.external_id = src_file.external_id
    dst_file.start_time = (
        src_file.start_time
        if src_file.start_time and src_file.end_time and src_file.start_time < src_file.end_time
        else src_file.end_time
    )
    dst_file.end_time = (
        src_file.end_time
        if src_file.start_time and src_file.end_time and src_file.start_time > src_file.end_time
        else src_file.end_time
    )
    dst_file.type = src_file.type
    dst_file.subtype = src_file.subtype
    dst_file.description = src_file.description
    dst_file.metadata = replication.new_metadata(src_file, project_src, runtime)
    dst_file.asset_ids = replication.get_asset_ids(src_file.asset_ids, src_dst_ids_assets)
    dst_file.source = src_file.source
    return dst_file


def copy_events(
    src_events: List[Event],
    src_id_dst_event: Dict[int, Event],
    src_dst_ids_assets: Dict[int, int],
    project_src: str,
    runtime: int,
    client: CogniteClient,
):
    """
    Creates/updates event objects and then attempts to create and update these objects in the destination.

    Args:
        src_events: A list of the events that are in the source.
        src_id_dst_event:  A dictionary of an events source id to it's matching destination object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        client: The client corresponding to the destination project.

    """
    logging.debug(f"Starting to replicate {len(src_events)} events.")

    create_files, update_files, unchanged_files = replication.make_objects_batch(
        src_events, src_id_dst_event, src_dst_ids_assets, create_file, update_file, project_src, runtime
    )

    logging.info(f"Creating {len(create_files)} new events and updating {len(update_files)} existing events.")

    if create_files:
        logging.debug(f"Attempting to create {len(create_files)} files.")
        create_files = replication.retry(client.events.create, create_files)
        logging.debug(f"Successfully created {len(create_files)} files.")

    if update_files:
        logging.debug(f"Attempting to update {len(update_files)} files.")
        update_files = replication.retry(client.events.update, update_files)
        logging.debug(f"Successfully updated {len(update_files)} files.")

    logging.info(f"Created {len(create_files)} new events and updated {len(update_files)} existing files.")
