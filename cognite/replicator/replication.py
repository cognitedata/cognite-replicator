import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import requests

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, FileMetadata, TimeSeries
from cognite.client.data_classes.assets import Asset
from cognite.client.data_classes.raw import Row


def make_id_object_map(
    objects: List[Union[Asset, Event, FileMetadata, TimeSeries]]
) -> Dict[int, Union[Asset, Event, FileMetadata, TimeSeries]]:
    """
    Makes a dictionary with the source object id as the key and the object as the value for objects
    that have been replicated.

    Args:
        objects: A list of objects that are from the replication destination.

    Returns:
        A dictionary of source object id to destination object for objects that have been replicated.
    """
    return {
        int(obj.metadata["_replicatedInternalId"]): obj
        for obj in objects
        if obj.metadata and obj.metadata.get("_replicatedInternalId")
    }


def filter_objects(
    objects: Union[List[Event], List[FileMetadata], List[TimeSeries]],
    src_dst_ids_assets: Dict[int, int],
    skip_unlinkable: bool = False,
    skip_nonasset: bool = False,
    filter_fn: Optional[Callable[[Union[Event, FileMetadata, TimeSeries]], bool]] = None,
) -> Union[List[Event], List[FileMetadata], List[TimeSeries]]:
    """Filters out objects based on their assets and optionally custom filter logic

    Args:
        objects: A list of all the objects to filter.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        skip_unlinkable: If True, excludes objects whose assets haven't been replicated in the destination
        skip_nonasset: If True, excludes objects without any associated assets
        filter_fn: If specified, is used to filter the objects in addition to the existing asset filters.

    Returns:
        A list of objects that meet the criteria.
    """
    filtered_objs = []

    def has_assets(obj):
        is_multi_asset = hasattr(obj, "asset_ids")
        return (is_multi_asset and getattr(obj, "asset_ids", None) is not None) or (
            not is_multi_asset and getattr(obj, "asset_id", None) is not None
        )

    for object in objects:
        if filter_fn is not None and not filter_fn(object):
            continue

        if skip_nonasset and not has_assets(object):
            continue

        if skip_unlinkable and has_assets(object):
            linkable = False
            asset_id_list = getattr(object, "asset_ids", [getattr(object, "asset_id", -1)])
            for asset_id in asset_id_list:
                if asset_id in src_dst_ids_assets:
                    linkable = True
                    break

            if not linkable:
                continue
        filtered_objs.append(object)

    return filtered_objs


def existing_mapping(*objects: List[Asset], ids: Dict[int, int] = None) -> Dict[int, int]:
    """
    Updates a dictionary with all the source id to destination id pairs for the objects that have been replicated.

    Args:
        *objects: A list of objects to make a mapping of.
        ids: A dictionary of all the mappings of source object id to destination object id.

    Returns:
        The updated dictionary with the ids from new objects that have been replicated.
    """
    if not ids:
        ids = {}

    for obj in objects:
        if obj.metadata and obj.metadata.get("_replicatedInternalId"):
            ids[int(obj.metadata["_replicatedInternalId"])] = obj.id

    return ids


def get_asset_ids(ids: List[int], src_dst_ids_assets: Dict[int, int]) -> Union[List[int], None]:
    """
    Create the list of destination asset ids from the list of source asset ids.

    Args:
        ids: A list of the source asset ids from the source object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.

    Returns:
        A list of the destination asset ids for the destination object.
    """
    if ids is None:
        return None

    return [src_dst_ids_assets[src_asset_id] for src_asset_id in ids if src_asset_id in src_dst_ids_assets]


def new_metadata(
    obj: Union[Asset, Event, FileMetadata, TimeSeries], project_src: str, replicated_runtime: int
) -> Dict[str, Union[int, str]]:
    """
    Copies the objects metadata and adds three new fields to it providing information about the objects replication.

    Fields Created
        - **_replicatedSource**: The name of the project this object is replicated from.
        - **_replicatedTime**: The timestamp of when the object was replicated, all objects created/updated in the same
          execution will have the same timestamp.
        - **_replicatedInternalId**: The internal id of the source object that the destination object
          is being replicated from.

    Args:
        obj: The source object that is being replicated to the destination.
        project_src: The name of the project the object is being replicated from.
        replicated_runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The metadata dictionary for the replicated destination object based on the source object.

    """
    metadata: Dict[str, Any] = dict(obj.metadata if obj.metadata else {})
    metadata["_replicatedSource"] = project_src
    metadata["_replicatedTime"] = replicated_runtime
    metadata["_replicatedInternalId"] = obj.id
    return metadata


def make_objects_batch(
    src_objects: List[Union[Asset, Event, FileMetadata, TimeSeries]],
    src_id_dst_map: Dict[int, Union[Asset, Event, FileMetadata, TimeSeries]],
    src_dst_ids_assets: Dict[int, int],
    create,
    update,
    project_src: str,
    replicated_runtime: int,
    depth: Optional[int] = None,
    dst_ts: Optional[TimeSeries] = None,
) -> Tuple[
    List[Union[Asset, Event, FileMetadata, TimeSeries]],
    List[Union[Asset, Event, FileMetadata, TimeSeries]],
    List[Union[Asset, Event, FileMetadata, TimeSeries]],
]:
    """
    Create a batch of new objects from a list of source objects or update existing destination objects to their
    corresponding source object.

    Args:
        src_objects: A list of objects to be replicated from a source.
        src_id_dst_map: A dictionary of source object ids to the matching destination object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        create: The function to be used in order to create all the objects in CDF.
        update: The function to be used in order to update the existing objects in CDF.
        project_src: The name of the project the object is being replicated from.
        replicated_runtime: The timestamp to be used in the new replicated metadata.
        depth: The depth of the asset within the asset hierarchy, only used for making assets.
        dst_ts: List of timeseries in the destination - Will be used for comparison if current timeseries where not copied by the replicator
    Returns:
        create_objects: A list of all the new objects to be posted to CDF.
        update_objects: A list of all the updated objects to be updated in CDF.
        unchanged_objects: A list of all the objects that don't need to be updated.
    """

    create_objects = []
    update_objects = []
    unchanged_objects = []

    kwargs = {"depth": depth} if depth is not None else {}  # Only used on assets

    # make a set of external ids to loop through
    if dst_ts:
        dst_ts_ext_id_set = {ts.external_id for ts in dst_ts}
    else:
        dst_ts_ext_id_set = set()

    for src_obj in src_objects:
        dst_obj = src_id_dst_map.get(src_obj.id)
        if dst_obj:
            if not src_obj.last_updated_time or src_obj.last_updated_time > int(dst_obj.metadata["_replicatedTime"]):
                dst_obj = update(src_obj, dst_obj, src_dst_ids_assets, project_src, replicated_runtime, **kwargs)
                update_objects.append(dst_obj)
            else:
                unchanged_objects.append(dst_obj)
        elif dst_ts:
            if src_obj.external_id in dst_ts_ext_id_set:
                unchanged_objects.append(dst_obj)
        else:
            new_asset = create(src_obj, src_dst_ids_assets, project_src, replicated_runtime, **kwargs)
            create_objects.append(new_asset)

    return create_objects, update_objects, unchanged_objects


def retry(
    function, objects: List[Union[Asset, Event, FileMetadata, TimeSeries, Row]], **kwargs
) -> List[Union[Asset, Event, TimeSeries]]:
    """
    Attempt to either create/update the objects, if it fails retry creating/updating the objects. This will retry up
    to three times.

    Args:
        function: The function that will be applied to objects, either creating_objects or updating_objects.
        objects: A list of all the new objects or updated objects.

    Returns:
        A list of all the objects that were created or updated in CDF.

    """
    ret: List[Union[Asset, Event, FileMetadata, TimeSeries, Row]] = []
    if objects:
        tries = 3
        for i in range(tries):
            logging.info("Current try: %d" % i)
            try:
                ret = function(objects, **kwargs)
                break
            except requests.exceptions.ReadTimeout as e:
                logging.warning(f"Retrying due to {e}")

    return ret


def thread(
    num_threads: int,
    copy,
    src_objects: List[Union[Event, FileMetadata, TimeSeries]],
    src_id_dst_obj: Dict[int, Union[Event, FileMetadata, TimeSeries]],
    src_dst_ids_assets: Dict[int, int],
    project_src: str,
    replicated_runtime: int,
    client: CogniteClient,
):
    """
    Split up objects to replicate them in batches and thread each batch.

    Args:
        num_threads: The number of threads to be used.
        copy: The function used to copy objects.
        src_objects: A list of all the objects in the source to be replicated.
        src_id_dst_obj: A dictionary of source object id to destination object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        replicated_runtime: The timestamp to be used in the new replicated metadata.
        client: The Cognite Client for the destination project.

    """

    threads = []
    chunk_size = len(src_objects) // num_threads
    logging.info(f"Thread chunk size: {chunk_size}")
    remainder = len(src_objects) % num_threads
    logging.info(f"Thread remainder size: {remainder}\n")

    i = 0
    for t in range(num_threads):
        cs = chunk_size + int(t < remainder)
        threads.append(
            threading.Thread(
                target=copy,
                args=(
                    src_objects[i : i + cs],
                    src_id_dst_obj,
                    src_dst_ids_assets,
                    project_src,
                    replicated_runtime,
                    client,
                ),
            )
        )
        i += cs

    assert i == len(src_objects)

    for count, t in enumerate(threads, start=1):
        t.start()
        logging.info(f"Started thread: {count}")

    for count, t in enumerate(threads, start=1):
        t.join()
        logging.info(f"Joined thread: {count}")


def remove_replication_metadata(objects: Union[List[Asset], List[Event], List[TimeSeries]]):
    """Removes the replication metadata from the passed resource list, so that the resources will look original.
    See also clear_replication_metadata.

    Parameters:
        objects: The list of objects to strip replication metadata away from
    """
    for obj in objects:
        if obj.metadata:
            obj.metadata.pop("_replicatedInternalId", None)
            obj.metadata.pop("_replicatedSource", None)
            obj.metadata.pop("_replicatedTime", None)


def clear_replication_metadata(client: CogniteClient):
    """Removes the replication metadata from all resources, so that the replicated tenant will look like an original.
    Sample use-case: clean up a demo-tenant so that extra data is not present.

    Caution: After clearing replication metadata, the delete_replicated_if_not_in_src option will delete everything.

    Parameters:
        client: The client to strip replication metadata away from
    """

    logging.info("Starting to clear replication metadata...")
    events_dst = client.events.list(limit=None)
    ts_dst = client.time_series.list(limit=None)
    assets_dst = client.assets.list(limit=None)
    client.events.update(events_dst)
    client.time_series.update(ts_dst)
    client.assets.update(assets_dst)
    logging.info("Replication metadata cleared!")


def find_objects_to_delete_not_replicated_in_dst(
    dst_objects: List[Union[Asset, Event, FileMetadata, TimeSeries]]
) -> List[Asset]:
    """
    Deleting all the assets in the destination that do not have the "_replicatedSource" in metadata, which
    means that is was not copied from the source, but created in the destination.

    Parameters:
        dst_objects: The list of objects from the dst destination.
    """
    obj_ids_to_remove = []
    for obj in dst_objects:
        if not obj.metadata or not obj.metadata.get("_replicatedSource"):
            obj_ids_to_remove.append(obj.id)
    return obj_ids_to_remove


def find_objects_to_delete_if_not_in_src(
    src_objects: List[Union[Asset, Event, FileMetadata, TimeSeries]],
    dst_objects: List[Union[Asset, Event, FileMetadata, TimeSeries]],
) -> List[Union[Asset, Event, FileMetadata, TimeSeries]]:
    """
    Compare the destination and source assets and delete the ones that are no longer in the source.

    Parameters:
        src_objects: The list of objects from the src destination.
        dst_objects: The list of objects from the dst destination.
    """

    src_obj_ids = {obj.id for obj in src_objects}

    obj_ids_to_remove = []
    for obj in dst_objects:
        if obj.metadata and obj.metadata.get("_replicatedInternalId"):
            if int(obj.metadata["_replicatedInternalId"]) not in src_obj_ids:
                obj_ids_to_remove.append(obj.id)
    return obj_ids_to_remove
