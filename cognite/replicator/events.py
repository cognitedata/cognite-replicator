import logging
import time
from typing import Dict, List

from cognite.client import CogniteClient
from cognite.client.data_classes import Event

from . import replication


def create_event(src_event: Event, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int) -> Event:
    """
    Makes a new copy of the event to be replicated based on a source event.

    Args:
        src_event: The event from the source to be replicated to destination.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The replicated event to be created in the destination.
    """
    logging.debug(f"Creating a new event based on source event id {src_event.id}")

    return Event(
        external_id=src_event.external_id,
        start_time=src_event.start_time
        if src_event.start_time and src_event.end_time and src_event.start_time < src_event.end_time
        else src_event.end_time,
        end_time=src_event.end_time,
        type=src_event.type,
        subtype=src_event.subtype,
        description=src_event.description,
        metadata=replication.new_metadata(src_event, project_src, runtime),
        asset_ids=replication.get_asset_ids(src_event.asset_ids, src_dst_ids_assets),
        source=src_event.source,
    )


def update_event(
    src_event: Event, dst_event: Event, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> Event:
    """
    Makes an updated version of the destination event based on the corresponding source event.

    Args:
        src_event: The event from the source to be replicated.
        dst_event: The event from the destination that needs to be updated to reflect changes made to its source event.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The updated event object for the replication destination.
    """
    logging.debug(f"Updating existing event {dst_event.id} based on source event id {src_event.id}")

    dst_event.external_id = src_event.external_id
    dst_event.start_time = (
        src_event.start_time
        if src_event.start_time and src_event.end_time and src_event.start_time < src_event.end_time
        else src_event.end_time
    )
    dst_event.end_time = src_event.end_time
    dst_event.type = src_event.type
    dst_event.subtype = src_event.subtype
    dst_event.description = src_event.description
    dst_event.metadata = replication.new_metadata(src_event, project_src, runtime)
    dst_event.asset_ids = replication.get_asset_ids(src_event.asset_ids, src_dst_ids_assets)
    dst_event.source = src_event.source
    return dst_event


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

    create_events, update_events, unchanged_events = replication.make_objects_batch(
        src_events, src_id_dst_event, src_dst_ids_assets, create_event, update_event, project_src, runtime
    )

    logging.info(f"Creating {len(create_events)} new events and updating {len(update_events)} existing events.")

    if create_events:
        logging.debug(f"Attempting to create {len(create_events)} events.")
        create_events = replication.retry(client.events.create, create_events)
        logging.debug(f"Successfully created {len(create_events)} events.")

    if update_events:
        logging.debug(f"Attempting to update {len(update_events)} events.")
        update_events = replication.retry(client.events.update, update_events)
        logging.debug(f"Successfully updated {len(update_events)} events.")

    logging.info(f"Created {len(create_events)} new events and updated {len(update_events)} existing events.")


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    batch_size: int = 10000,
    num_threads: int = 1,
    delete_replicated_if_not_in_src: bool = False,
    delete_not_replicated_in_dst: bool = False,
    skip_unlinkable: bool = False,
    skip_nonasset: bool = False,
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
        skip_unlinkable: If no assets exist in the destination for an event, do not replicate it
        skip_nonasset: If an event has no associated assets, do not replicate it
    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    events_src = client_src.events.list(limit=None)
    events_dst = client_dst.events.list(limit=None)
    logging.info(f"There are {len(events_src)} existing events in source ({project_src}).")
    logging.info(f"There are {len(events_dst)} existing events in destination ({project_dst}).")

    src_id_dst_event = replication.make_id_object_map(events_dst)

    assets_dst = client_dst.assets.list(limit=None)
    src_dst_ids_assets = replication.existing_mapping(*assets_dst)
    logging.info(
        f"If an events asset ids is one of the {len(src_dst_ids_assets)} assets "
        f"that have been replicated then it will be linked."
    )

    if skip_unlinkable or skip_nonasset:
        pre_filter_length = len(events_src)
        events_src = replication.filter_objects(events_src, src_dst_ids_assets, skip_unlinkable, skip_nonasset)
        logging.info(f"Filtered out {pre_filter_length - len(events_src)} events. {len(events_src)} events remain.")

    replicated_runtime = int(time.time()) * 1000
    logging.info(f"These copied/updated events will have a replicated run time of: {replicated_runtime}.")

    logging.info(
        f"Starting to copy and update {len(events_src)} events from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if len(events_src) > batch_size:
        replication.thread(
            num_threads=num_threads,
            copy=copy_events,
            src_objects=events_src,
            src_id_dst_obj=src_id_dst_event,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            replicated_runtime=replicated_runtime,
            client=client_dst,
        )
    else:
        copy_events(
            src_events=events_src,
            src_id_dst_event=src_id_dst_event,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            runtime=replicated_runtime,
            client=client_dst,
        )

    logging.info(
        f"Finished copying and updating {len(events_src)} events from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if delete_replicated_if_not_in_src:
        ids_to_delete = replication.find_objects_to_delete_if_not_in_src(events_src, events_dst)
        client_dst.events.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} events in destination ({project_dst})"
            f" because they were no longer in source ({project_src})   "
        )
    if delete_not_replicated_in_dst:
        ids_to_delete = replication.find_objects_to_delete_not_replicated_in_dst(events_dst)
        client_dst.events.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} events in destination ({project_dst}) because"
            f"they were not replicated from source ({project_src})   "
        )
