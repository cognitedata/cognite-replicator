import time
import logging
from typing import List, Dict

from cognite.client import CogniteClient
from cognite.client.data_classes import Event

# from . import replication
import replication


def create_event(
    src_event: Event, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> Event:
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
        if src_event.start_time and src_event.start_time < src_event.end_time
        else src_event.end_time,
        end_time=src_event.end_time
        if src_event.end_time and src_event.start_time > src_event.end_time
        else src_event.end_time,
        type=src_event.type,
        subtype=src_event.subtype,
        description=src_event.description,
        metadata=replication.new_metadata(src_event, project_src, runtime),
        asset_ids=replication.get_asset_ids(src_event.asset_ids, src_dst_ids_assets),
        source=src_event.source,
    )


def update_event(
    src_event: Event,
    dst_event: Event,
    src_dst_ids_assets: Dict[int, int],
    project_src: str,
    runtime: int,
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

    logging.debug(
        f"Updating existing event {dst_event.id} based on source event id {src_event.id}"
    )

    dst_event.external_id = src_event.external_id
    dst_event.start_time = (
        src_event.start_time
        if src_event.start_time and src_event.start_time < src_event.end_time
        else src_event.end_time
    )
    dst_event.end_time = (
        src_event.end_time
        if src_event.end_time and src_event.start_time > src_event.end_time
        else src_event.end_time
    )
    dst_event.type = src_event.type
    dst_event.subtype = src_event.subtype
    dst_event.description = src_event.description
    dst_event.metadata = replication.new_metadata(src_event, project_src, runtime)
    dst_event.asset_ids = replication.get_asset_ids(
        src_event.asset_ids, src_dst_ids_assets
    )
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
    create_events, update_events = replication.make_objects_batch(
        src_objects=src_events,
        src_id_dst_obj=src_id_dst_event,
        src_dst_ids_assets=src_dst_ids_assets,
        create=create_event,
        update=update_event,
        project_src=project_src,
        replicated_runtime=runtime,
    )

    logging.info(
        f"Creating {len(create_events)} new events and updating {len(update_events)} existing events."
    )

    if create_events:
        logging.debug(f"Attempting to create {len(create_events)} events.")
        create_events = replication.retry(create_events, client.events.create)
        logging.debug(f"Successfully created {len(create_events)} events.")

    if update_events:
        logging.debug(f"Attempting to update {len(update_events)} events.")
        update_events = replication.retry(update_events, client.events.update)
        logging.debug(f"Successfully updated {len(update_events)} events.")

    logging.info(
        f"Created {len(create_events)} new events and updated {len(update_events)} existing events."
    )


def replicate(
    project_src: str,
    client_src: CogniteClient,
    project_dst: str,
    client_dst: CogniteClient,
    batch_size: int,
    num_threads: int,
):
    """
    Replicates all the assets from the source project into the destination project.

    Args:
        project_src: The name of the project the object is being replicated from.
        client_src: The client corresponding to the source project.
        project_dst: The name of the project the object is being replicated to.
        client_dst: The client corresponding to the destination project.
        batch_size: The biggest batch size to post chunks in.
        num_threads: The number of threads to be used.

    """

    events_src = client_src.events.list(limit=None)
    events_dst = client_dst.events.list(limit=None)
    logging.info(
        f"There are {len(events_src)} existing assets in source ({project_src})."
    )
    logging.info(
        f"There are {len(events_dst)} existing assets in destination ({project_dst})."
    )

    src_id_dst_event = replication.make_id_object_dict(events_dst)

    assets_dst = client_dst.assets.list(limit=None)
    src_dst_ids_assets = replication.existing_mapping(*assets_dst)
    logging.info(
        f"If an events asset ids is one of the {len(src_dst_ids_assets)} assets "
        f"that have been replicated then it will be linked."
    )

    replicated_runtime = int(time.time()) * 1000
    logging.info(
        f"These copied/updated events will have a replicated run time of: {replicated_runtime}."
    )

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
