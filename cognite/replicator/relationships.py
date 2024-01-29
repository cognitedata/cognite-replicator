import logging
import queue
import re
import time
from typing import Dict, List, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes import Relationship, RelationshipList
from cognite.client.exceptions import CogniteNotFoundError

from . import replication, datasets


def create_relationship(
    src_relationship: Relationship,
    src_client: CogniteClient,
    dst_client: CogniteClient,
    src_dst_dataset_mapping: dict[int, int],
    config: Dict,
) -> Relationship:
    """
    Makes a new copy of the relationship to be replicated based on a source relationship.

    Args:
        src_relationship: The relationship from the source to be replicated to destination.
        src_client: The client corresponding to the source project.
        dst_client: The client corresponding to the destination project.
        src_dst_dataset_mapping: Dictionary that maps source dataset ids to destination dataset ids
        config: dict corresponding to the selected yaml config file

    Returns:
        The replicated relationship to be created in the destination.
    """
    logging.debug(f"Creating a new relationship based on source relationship id {src_relationship.external_id}")

    return Relationship(
        external_id=src_relationship.external_id,
        source_external_id=src_relationship.source_external_id,
        source_type=src_relationship.source_type,
        source=src_relationship.source,
        target_external_id=src_relationship.target_external_id,
        target_type=src_relationship.target_type,
        target=src_relationship.target,
        start_time=(
            src_relationship.start_time
            if src_relationship.start_time
            and src_relationship.end_time
            and src_relationship.start_time < src_relationship.end_time
            else src_relationship.end_time
        ),
        end_time=src_relationship.end_time,
        confidence=src_relationship.confidence,
        data_set_id=(
            datasets.replicate(src_client, dst_client, src_relationship.data_set_id, src_dst_dataset_mapping)
            if config and config.get("dataset_support", False)
            else None
        ),
    )


def update_relationship(
    src_relationship: Relationship,
    dst_relationship: Relationship,
    src_client: CogniteClient,
    dst_client: CogniteClient,
    src_dst_dataset_mapping: dict[int, int],
    config: Dict,
) -> Relationship:
    """
    Makes an updated version of the destination relationship based on the corresponding source relationship.

    Args:
        src_relationship: The relationship from the source to be replicated.
        dst_relationship: The relationship from the destination that needs to be updated to reflect changes made to its source relationship.
        src_client: The client corresponding to the source project.
        dst_client: The client corresponding to the destination project.
        src_dst_dataset_mapping: Dictionary that maps source dataset ids to destination dataset ids
        config: dict corresponding to the selected yaml config file

    Returns:
        The updated relationship object for the replication destination.
    """
    logging.debug(
        f"Updating existing relationship {dst_relationship.external_id} based on source relationship id {src_relationship.external_id}"
    )

    dst_relationship.external_id = src_relationship.external_id
    dst_relationship.source_external_id = src_relationship.source_external_id
    dst_relationship.source_type = src_relationship.source_type
    dst_relationship.source = src_relationship.source
    dst_relationship.target_external_id = src_relationship.target_external_id
    dst_relationship.target_type = src_relationship.target_type
    dst_relationship.target = src_relationship.target
    dst_relationship.start_time = (
        src_relationship.start_time
        if src_relationship.start_time
        and src_relationship.end_time
        and src_relationship.start_time < src_relationship.end_time
        else src_relationship.end_time
    )
    dst_relationship.end_time = src_relationship.end_time
    dst_relationship.confidence = src_relationship.confidence
    dst_relationship.data_set_id = (
        (
            datasets.replicate(src_client, dst_client, src_relationship.data_set_id, src_dst_dataset_mapping)
            if config and config.get("dataset_support", False)
            else None
        ),
    )
    return dst_relationship


def copy_relationships(
    src_relationships: List[Relationship],
    src_id_dst_relationship: Dict[int, Relationship],
    project_src: str,
    runtime: int,
    src_client: CogniteClient,
    dst_client: CogniteClient,
    src_dst_dataset_mapping: Dict[int, int],
    config: Dict,
    src_filter: List[Relationship],
    jobs: queue.Queue = None,
):
    """
    Creates/updates relationship objects and then attempts to create and update these objects in the destination.

    Args:
        src_relationships: A list of the relationships that are in the source.
        src_id_dst_relationship:  A dictionary of an relationships source id to it's matching destination object.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        src_client: The client corresponding to the source project.
        dst_client: The client corresponding to the destination project.
        src_dst_dataset_mapping: dictionary mapping the source dataset ids to the destination ones
                config: dict corresponding to the selected yaml config file
        src_filter: List of relationships in the destination - Will be used for comparison if current relationships were not copied by the replicator.
        jobs: Shared job queue, this is initialized and managed by replication.py.
    """

    if jobs:
        use_queue_logic = True
        do_while = not jobs.empty()
    else:
        use_queue_logic = False
        do_while = True

    while do_while:
        if use_queue_logic:
            chunk = jobs.get()
            chunk_relationships = src_relationships[chunk[0] : chunk[1]]
        else:
            chunk_relationships = src_relationships

        logging.debug(f"Starting to replicate {len(chunk_relationships)} relationships.")

        create_relationships, update_relationships, unchanged_relationships = replication.make_objects_batch(
            src_objects=chunk_relationships,
            src_id_dst_map=src_id_dst_relationship,
            src_dst_ids_assets=None,
            create=create_relationship,
            update=update_relationship,
            project_src=project_src,
            replicated_runtime=runtime,
            src_client=src_client,
            dst_client=dst_client,
            src_dst_dataset_mapping=src_dst_dataset_mapping,
            config=config,
            src_filter=src_filter,
        )

        logging.info(
            f"Creating {len(create_relationships)} new relationships and updating {len(update_relationships)} existing relationships."
        )

        if create_relationships:
            logging.debug(f"Attempting to create {len(create_relationships)} relationships.")
            create_relationships = replication.retry(dst_client.relationships.create, create_relationships)
            logging.debug(f"Successfully created {len(create_relationships)} relationships.")

        if update_relationships:
            logging.debug(f"Attempting to update {len(update_relationships)} relationships.")
            update_relationships = replication.retry(dst_client.relationships.update, update_relationships)
            logging.debug(f"Successfully updated {len(update_relationships)} relationships.")

        logging.info(
            f"Created {len(create_relationships)} new relationships and updated {len(update_relationships)} existing relationships."
        )

        if use_queue_logic:
            jobs.task_done()
            do_while = not jobs.empty()
        else:
            do_while = False


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    batch_size: int = 10000,
    num_threads: int = 1,
    config: Dict = None,
    src_dst_dataset_mapping: Dict[int, int] = {},
    delete_replicated_if_not_in_src: bool = False,
    delete_not_replicated_in_dst: bool = False,
    target_external_ids: Optional[List[str]] = None,
):
    """
    Replicates all the relationships from the source project into the destination project.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        batch_size: The biggest batch size to post chunks in.
        num_threads: The number of threads to be used.
        config: dict corresponding to the selected yaml config file
        src_dst_dataset_mapping: dictionary mapping the source dataset ids to the destination ones
        delete_replicated_if_not_in_src: If True, will delete replicated relationships that are in the destination,
        but no longer in the source project (Default=False).
        delete_not_replicated_in_dst: If True, will delete relationships from the destination if they were not replicated
        from the source (Default=False).
        target_external_ids: List of specific relationships external ids to replicate
    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    if target_external_ids:
        relationships_src = client_src.relationships.retrieve_multiple(
            external_ids=target_external_ids, ignore_unknown_ids=True
        )
        try:
            relationships_dst = client_dst.relationships.retrieve_multiple(
                external_ids=target_external_ids, ignore_unknown_ids=True
            )
        except CogniteNotFoundError:
            relationships_dst = RelationshipList([])
    else:
        relationships_src = client_src.relationships.list(limit=None)
        relationships_dst = client_dst.relationships.list(limit=None)
        logging.info(f"There are {len(relationships_src)} existing relationships in source ({project_src}).")
        logging.info(f"There are {len(relationships_dst)} existing relationships in destination ({project_dst}).")

    src_id_dst_relationship = replication.make_id_object_map(relationships_dst)

    replicated_runtime = int(time.time()) * 1000
    logging.info(f"These copied/updated relationships will have a replicated run time of: {replicated_runtime}.")

    logging.info(
        f"Starting to copy and update {len(relationships_src)} relationships from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if len(relationships_src) > batch_size:
        replication.thread(
            num_threads=num_threads,
            batch_size=batch_size,
            copy=copy_relationships,
            src_objects=relationships_src,
            src_id_dst_obj=src_id_dst_relationship,
            src_dst_ids_assets={},
            project_src=project_src,
            replicated_runtime=replicated_runtime,
            src_client=client_src,
            dst_client=client_dst,
            src_dst_dataset_mapping=src_dst_dataset_mapping,
            config=config,
            src_filter=relationships_dst,
        )
    else:
        copy_relationships(
            src_relationships=relationships_src,
            src_id_dst_relationship=src_id_dst_relationship,
            project_src=project_src,
            runtime=replicated_runtime,
            src_client=client_src,
            dst_client=client_dst,
            src_dst_dataset_mapping=src_dst_dataset_mapping,
            config=config,
            src_filter=relationships_dst,
        )

    logging.info(
        f"Finished copying and updating {len(relationships_src)} relationships from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if delete_replicated_if_not_in_src:
        ids_to_delete = replication.find_objects_to_delete_if_not_in_src(relationships_src, relationships_dst)
        if ids_to_delete:
            client_dst.relationships.delete(id=ids_to_delete)
            logging.info(
                f"Deleted {len(ids_to_delete)} relationships in destination ({project_dst})"
                f" because they were no longer in source ({project_src})   "
            )
    if delete_not_replicated_in_dst:
        ids_to_delete = replication.find_objects_to_delete_not_replicated_in_dst(relationships_dst)
        if ids_to_delete:
            client_dst.relationships.delete(id=ids_to_delete)
            logging.info(
                f"Deleted {len(ids_to_delete)} relationships in destination ({project_dst}) because"
                f"they were not replicated from source ({project_src})   "
            )
