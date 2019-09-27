import logging
import time
from typing import Dict, List

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries

from . import replication


def create_time_series(
    src_ts: TimeSeries, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> TimeSeries:
    """
    Make a new copy of the time series to be replicated based on a source time series.

    Args:
        src_ts: The time series from the source to be replicated to the destination.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The replicated time series to be created in the destination.
    """
    logging.debug(f"Creating a new time series based on source time series id {src_ts.id}")

    return TimeSeries(
        external_id=src_ts.external_id,
        name=src_ts.name,
        is_string=src_ts.is_string,
        metadata=replication.new_metadata(src_ts, project_src, runtime),
        unit=src_ts.unit,
        asset_id=replication.get_asset_ids([src_ts.asset_id], src_dst_ids_assets)[0] if src_ts.asset_id else None,
        is_step=src_ts.is_step,
        description=src_ts.description,
        security_categories=src_ts.security_categories,
        legacy_name=src_ts.external_id,
    )


def update_time_series(
    src_ts: TimeSeries, dst_ts: TimeSeries, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> TimeSeries:
    """
    Makes an updated version of the destination time series based on the corresponding source time series.

    Args:
        src_ts: The time series from the source to be replicated.
        dst_ts: The time series from the destination that needs to be updated to reflect changes made to its
                source time series.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The updated time series object for the replication destination.
    """
    logging.debug(f"Updating existing time series {dst_ts.id} based on source time series id {src_ts.id}")

    dst_ts.external_id = src_ts.external_id
    dst_ts.name = src_ts.name
    dst_ts.is_string = src_ts.is_string
    dst_ts.metadata = replication.new_metadata(src_ts, project_src, runtime)
    dst_ts.unit = src_ts.unit
    dst_ts.asset_id = replication.get_asset_ids([src_ts.asset_id], src_dst_ids_assets)[0] if src_ts.asset_id else None
    dst_ts.is_step = src_ts.is_step
    dst_ts.description = src_ts.description
    dst_ts.security_categories = src_ts.security_categories
    return dst_ts


def _has_security_category(ts: TimeSeries) -> bool:
    return ts.security_categories is not None and len(ts.security_categories) > 0


def _is_service_account_metric(ts: TimeSeries) -> bool:
    return "service_account_metrics" in ts.name


def _is_copyable(ts: TimeSeries) -> bool:
    return not _is_service_account_metric(ts) and not _has_security_category(ts)


def copy_ts(
    src_ts: List[TimeSeries],
    src_id_dst_ts: Dict[int, TimeSeries],
    src_dst_ids_assets: Dict[int, int],
    project_src: str,
    runtime: int,
    client: CogniteClient,
):
    """
    Creates/updates time series objects and then attempts to create and update these time series in the destination.

    Args:
        src_ts: A list of the time series that are in the source.
        src_id_dst_ts: A dictionary of a time series source id to it's matching destination object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        client: The client corresponding to the destination project.
    """
    logging.info(f"Starting to replicate {len(src_ts)} time series.")
    create_ts, update_ts, unchanged_ts = replication.make_objects_batch(
        src_ts, src_id_dst_ts, src_dst_ids_assets, create_time_series, update_time_series, project_src, runtime
    )

    logging.info(f"Creating {len(create_ts)} new time series and updating {len(update_ts)} existing time series.")

    if create_ts:
        logging.info(f"Creating {len(create_ts)} time series.")
        created_ts = replication.retry(client.time_series.create, create_ts)
        logging.info(f"Successfully created {len(created_ts)} time series.")

    if update_ts:
        logging.info(f"Updating {len(update_ts)} time series.")
        updated_ts = replication.retry(client.time_series.update, update_ts)
        logging.info(f"Successfully updated {len(updated_ts)} time series.")


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
    Replicates all the time series from the source project into the destination project.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        batch_size: The biggest batch size to post chunks in.
        num_threads: The number of threads to be used.
        delete_replicated_if_not_in_src: If True, will delete replicated assets that are in the destination,
        but no longer in the source project (Default=False).
        delete_not_replicated_in_dst: If True, will delete assets from the destination if they were not replicated
        from the source (Default=False).
        skip_unlinkable: If no assets exist in the destination for a time series, do not replicate it
        skip_nonasset: If a time series has no associated assets, do not replicate it
    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    ts_src = client_src.time_series.list(limit=None)
    ts_dst = client_dst.time_series.list(limit=None)
    logging.info(f"There are {len(ts_src)} existing time series in source ({project_src}).")
    logging.info(f"There are {len(ts_dst)} existing time series in destination ({project_dst}).")

    src_id_dst_ts = replication.make_id_object_map(ts_dst)

    assets_dst = client_dst.assets.list(limit=None)
    src_dst_ids_assets = replication.existing_mapping(*assets_dst)
    logging.info(
        f"If a time series asset id is one of the {len(src_dst_ids_assets)} assets "
        f"that have been replicated then it will be linked."
    )

    ts_src_filtered = replication.filter_objects(
        ts_src, src_dst_ids_assets, skip_unlinkable, skip_nonasset, _is_copyable
    )
    logging.info(
        f"Filtered out {len(ts_src) - len(ts_src_filtered)} time series. {len(ts_src_filtered)} time series remain."
    )

    replicated_runtime = int(time.time()) * 1000
    logging.info(f"These copied/updated time series will have a replicated run time of: {replicated_runtime}.")

    logging.info(
        f"Starting to copy and update {len(ts_src_filtered)} time series from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if len(ts_src_filtered) > batch_size:
        replication.thread(
            num_threads=num_threads,
            copy=copy_ts,
            src_objects=ts_src_filtered,
            src_id_dst_obj=src_id_dst_ts,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            replicated_runtime=replicated_runtime,
            client=client_dst,
        )
    else:
        copy_ts(
            src_ts=ts_src_filtered,
            src_id_dst_ts=src_id_dst_ts,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            runtime=replicated_runtime,
            client=client_dst,
        )

    logging.info(
        f"Finished copying and updating {len(ts_src_filtered)} time series from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if delete_replicated_if_not_in_src:
        ids_to_delete = replication.find_objects_to_delete_if_not_in_src(ts_src, ts_dst)
        client_dst.time_series.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} time series destination ({project_dst})"
            f" because they were no longer in source ({project_src})   "
        )
    if delete_not_replicated_in_dst:
        ids_to_delete = replication.find_objects_to_delete_not_replicated_in_dst(ts_dst)
        client_dst.time_series.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} time series in destination ({project_dst}) because"
            f"they were not replicated from source ({project_src})   "
        )
