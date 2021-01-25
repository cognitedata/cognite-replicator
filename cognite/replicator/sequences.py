import logging
import time
from typing import Dict, List

from cognite.client import CogniteClient
from cognite.client.data_classes import Sequence

from . import replication


def create_sequence(src_seq: Sequence, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int) -> Sequence:
    """
    Make a new copy of the sequence to be replicated based on a source sequence.

    Args:
        src_seq: The sequence from the source to be replicated to the destination.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The replicated sequence to be created in the destination.
    """
    logging.debug(f"Creating a new sequence based on source sequence id {src_seq.id}")
    # TODO: Bug happens at line 29 get_asset_ids returns empty list
    return Sequence(
        name=src_seq.name,
        description=src_seq.description,
        asset_id=replication.get_asset_ids([src_seq.asset_id], src_dst_ids_assets)[0] if src_seq.asset_id else None,
        external_id=src_seq.external_id,
        metadata=replication.new_metadata(src_seq, project_src, runtime),
        columns=src_seq.columns,
    )


def update_sequence(
    src_seq: Sequence, dst_seq: Sequence, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> Sequence:
    """
    Makes an updated version of the destination sequence based on the corresponding source sequence.

    Args:
        src_seq: The sequence from the source to be replicated.
        dst_seq: The sequence from the destination that needs to be updated to reflect changes made to iseq
                source sequence.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The updated sequence object for the replication destination.
    """
    logging.debug(f"Updating existing sequence {dst_seq.id} based on source sequence id {src_seq.id}")

    dst_seq.name = src_seq.name
    dst_seq.description = src_seq.description
    dst_seq.asset_id = (
        replication.get_asset_ids([src_seq.asset_id], src_dst_ids_assets)[0] if src_seq.asset_id else None
    )
    dst_seq.external_id = src_seq.external_id
    dst_seq.metadata = replication.new_metadata(src_seq, project_src, runtime)
    return dst_seq


def copy_seq(
    src_seq: List[Sequence],
    src_id_dst_seq: Dict[int, Sequence],
    src_dst_ids_assets: Dict[int, int],
    project_src: str,
    runtime: int,
    client: CogniteClient,
):
    """
    Creates/updates sequence objects and then attempts to create and update these sequence in the destination.

    Args:
        src_seq: A list of the sequence that are in the source.
        src_id_dst_seq: A dictionary of a sequence source id to it's matching destination object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        client: The client corresponding to the destination project.
    """
    logging.info(f"Starting to replicate {len(src_seq)} sequence.")
    create_seq, update_seq, unchanged_seq = replication.make_objects_batch(
        src_seq, src_id_dst_seq, src_dst_ids_assets, create_sequence, update_sequence, project_src, runtime
    )

    logging.info(f"Creating {len(create_seq)} new sequence and updating {len(update_seq)} existing sequence.")

    if create_seq:
        logging.info(f"Creating {len(create_seq)} sequence.")
        created_seq = replication.retry(client.sequences.create, create_seq)
        logging.info(f"Successfully created {len(created_seq)} sequence.")

    if update_seq:
        logging.info(f"Updating {len(update_seq)} sequence.")
        updated_seq = replication.retry(client.sequences.update, update_seq)
        logging.info(f"Successfully updated {len(updated_seq)} sequence.")


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
    Replicates all the sequence from the source project into the destination project.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        batch_size: The biggest batch size to post chunks in.
        num_threads: The number of threads to be used.
        delete_replicated_if_not_in_src: If True, will delete replicated assets that are in the destination,
        but no longer in the source project (Default=False).
        delete_not_replicated_in_dst: If True, will delete assets from the destination if they were not replicated
        from the source (Default=False).
        skip_unlinkable: If no assets exist in the destination for a sequence, do not replicate it
        skip_nonasset: If a sequence has no associated assets, do not replicate it
    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    seq_src = client_src.sequences.list(limit=None)
    seq_dst = client_dst.sequences.list(limit=None)
    logging.info(f"There are {len(seq_src)} existing sequences in source ({project_src}).")
    logging.info(f"There are {len(seq_dst)} existing sequences in destination ({project_dst}).")

    src_id_dst_seq = replication.make_id_object_map(seq_dst)

    assets_dst = client_dst.assets.list(limit=None)
    src_dst_ids_assets = replication.existing_mapping(*assets_dst)

    if not src_dst_ids_assets:
        assets_src = client_src.assets.list(limit=None)
        src_assets_map = replication.make_external_id_obj_map(assets_src)
        src_dst_ids_assets = replication.map_ids_from_external_ids(src_assets_map, assets_dst)

    logging.info(
        f"If a sequence asset id is one of the {len(src_dst_ids_assets)} assets "
        f"that have been replicated then it will be linked."
    )

    seq_src_filtered = replication.filter_objects(seq_src, src_dst_ids_assets, skip_unlinkable, skip_nonasset)
    logging.info(
        f"Filtered out {len(seq_src) - len(seq_src_filtered)} sequences. {len(seq_src_filtered)} sequences remain."
    )

    replicated_runtime = int(time.time()) * 1000
    logging.info(f"These copied/updated sequences will have a replicated run time of: {replicated_runtime}.")

    logging.info(
        f"Starting to copy and update {len(seq_src_filtered)} sequences from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if len(seq_src_filtered) > batch_size:
        replication.thread(
            num_threads=num_threads,
            copy=copy_seq,
            src_objects=seq_src_filtered,
            src_id_dst_obj=src_id_dst_seq,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            replicated_runtime=replicated_runtime,
            client=client_dst,
        )
    else:
        copy_seq(
            src_seq=seq_src_filtered,
            src_id_dst_seq=src_id_dst_seq,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            runtime=replicated_runtime,
            client=client_dst,
        )

    logging.info(
        f"Finished copying and updating {len(seq_src_filtered)} sequence from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if delete_replicated_if_not_in_src:
        ids_to_delete = replication.find_objects_to_delete_if_not_in_src(seq_src, seq_dst)
        client_dst.sequences.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} sequence destination ({project_dst})"
            f" because they were no longer in source ({project_src})   "
        )
    if delete_not_replicated_in_dst:
        ids_to_delete = replication.find_objects_to_delete_not_replicated_in_dst(seq_dst)
        client_dst.sequences.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} sequence in destination ({project_dst}) because"
            f"they were not replicated from source ({project_src})   "
        )


def replicate_rows(client_src, client_dst):
    seq_src = client_src.sequences.list(limit=None)
    seq_dst = client_dst.sequences.list(limit=None)

    dst_sequence_map = replication.make_external_id_obj_map(seq_dst)

    for sequence in seq_src:
        src_rows = client_src.sequences.data.retrieve(id=sequence.id, start=0, end=None)

        # if nothing to copy continue to next sequence
        if not src_rows.values:
            continue

        dst_rows = client_dst.sequences.data.retrieve(id=dst_sequence_map[sequence.external_id].id, start=0, end=None)

        if not dst_rows.values:
            client_dst.sequences.data.insert(
                rows=src_rows,
                id=dst_sequence_map[sequence.external_id].id,
                column_external_ids=src_rows.column_external_ids,
            )
