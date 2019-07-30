import threading
import logging
from typing import List, Dict, Tuple, Union, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes.assets import Asset
from cognite.client.data_classes import Event, TimeSeries

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def make_id_object_dict(
    objects: List[Union[Asset, Event, TimeSeries]]
) -> Dict[int, Union[Asset, Event, TimeSeries]]:
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
        if obj.metadata and obj.metadata["_replicatedInternalId"]
    }


def existing_mapping(*objects: List[Asset], ids={}) -> Dict[int, int]:
    """
    Updates a dictionary with all the source id to destination id pairs for the objects that have been replicated.

    Args:
        *objects: A list of objects to make a mapping of.
        ids: A dictionary of all the mappings of source object id to destination object id.

    Returns:
        The updated dictionary with the ids from new objects that have been replicated.

    """

    for obj in objects:
        if obj.metadata and obj.metadata["_replicatedInternalId"]:
            ids[int(obj.metadata["_replicatedInternalId"])] = obj.id

    return ids


def get_asset_ids(ids: List[int], src_dst_ids_assets: Dict[int, int]) -> List[int]:
    """
    Create the list of destination asset ids from the list of source asset ids.

    Args:
        ids: A list of the source asset ids from the source object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.

    Returns:
        A list of the destination asset ids for the destination object.

    """

    return [
        src_dst_ids_assets[src_asset_id]
        for src_asset_id in ids
        if src_asset_id in src_dst_ids_assets
    ]


def new_metadata(
    obj: Union[Asset, Event, TimeSeries], project_src: str, replicated_runtime: int
) -> Dict[str, Union[int, str]]:
    """
    Copies the objects metadata and adds three new fields to it providing information about the objects replication.
    Three extra fields are added to an objects metadata (_replicatedSource, _replicatedTime, _replicatedInternalId).
        _replicatedSource: The name of the project this object is replicated from.
        _replicatedTime: The timestamp of when the object was replicated, all objects created/updated in the same
                         execution will have the same timestamp.
        _replicatedInternalId: The internal id of the source object that the destination object is being replicated
                               from.

    Args:
        obj: The source object that is being replicated to the destination.
        project_src: The name of the project the object is being replicated from.
        replicated_runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The metadata dictionary for the replicated destination object based on the source object.

    """

    metadata = dict(obj.metadata if obj.metadata else dict())
    metadata["_replicatedSource"] = project_src
    metadata["_replicatedTime"] = replicated_runtime
    metadata["_replicatedInternalId"] = obj.id
    return metadata


def make_objects_batch(
    src_objects: List[Union[Asset, Event, TimeSeries]],
    src_id_dst_obj: Dict[int, Union[Asset, Event, TimeSeries]],
    src_dst_ids_assets: Dict[int, int],
    create,
    update,
    project_src: str,
    replicated_runtime: int,
    depth: Optional[int] = -1,
) -> Tuple[
    List[Union[Asset, Event, TimeSeries]], List[Union[Asset, Event, TimeSeries]]
]:
    """
    Create a batch of new objects from a list of source objects or update existing destination objects to their
    corresponding source object.

    Args:
        src_objects: A list of objects to be replicated from a source.
        src_id_dst_obj: A dictionary of source object ids to the matching destination object.
        create: The function to be used in order to create all the objects in CDF.
        update: The function to be used in order to update the existing objects in CDF.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        replicated_runtime: The timestamp to be used in the new replicated metadata.
        depth: The depth of the asset within the asset hierarchy, only used for making assets.

    Returns:
        create_objects: A list of all the new objects to be posted to CDF.
        update_objects: A list of all the updated objects to be updated in CDF.

    """

    create_objects = []
    update_objects = []

    for src_obj in src_objects:
        if src_obj.id in [*src_id_dst_obj]:
            if depth == -1:
                dst_obj = update(
                    src_obj,
                    src_id_dst_obj[src_obj.id],
                    src_dst_ids_assets,
                    project_src,
                    replicated_runtime,
                )
            else:
                dst_obj = update(
                    src_obj,
                    src_id_dst_obj[src_obj.id],
                    src_dst_ids_assets,
                    project_src,
                    replicated_runtime,
                    depth,
                )
            update_objects.append(dst_obj)
        else:
            if depth == -1:
                new_asset = create(
                    src_obj, src_dst_ids_assets, project_src, replicated_runtime
                )
            else:
                new_asset = create(
                    src_obj, src_dst_ids_assets, project_src, replicated_runtime, depth
                )
            create_objects.append(new_asset)

    return create_objects, update_objects


def retry(
    objects: List[Union[Asset, Event, TimeSeries]], function
) -> List[Union[Asset, Event, TimeSeries]]:
    """
    Attempt to either create/update the objects, if it fails retry creating/updating the objects. This will retry up
    to three times.

    Args:
        objects: A list of all the new objects or updated objects.
        function: The function that will be applied to objects, either creating_objects or updating_objects.

    Returns:
        A list of all the objects that were created or updated in CDF.

    """

    ret: List[Union[Asset, Event, TimeSeries]] = []
    if objects:
        tries = 4
        for i in range(1, tries):
            logging.info("Current try: %d" % i)
            try:
                ret = function(objects)
            except Exception as e:
                logging.info(e)
                if i < tries:
                    continue
                else:
                    raise
            break

    return ret


def thread(
    num_threads: int,
    copy,
    src_objects: List[Union[Event, TimeSeries]],
    src_id_dst_obj: Dict[int, Union[Event, TimeSeries]],
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
    chunk_size = int(len(src_objects) / num_threads)
    logging.info(f"Thread chunk size: {chunk_size}")
    remainder = len(src_objects) - (chunk_size * num_threads)
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

    count = 0
    for t in threads:
        t.start()
        count += 1
        logging.info(f"Started thread: {count}")

    count = 0
    for t in threads:
        t.join()
        count += 1
        logging.info(f"Joined thread: {count}")
