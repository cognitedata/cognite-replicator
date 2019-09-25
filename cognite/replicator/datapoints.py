#!/usr/bin/env python3

import logging
import multiprocessing as mp
from typing import Any, List, Optional

from cognite.client import CogniteClient


def retrieve_insert(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    thread_id: int,
    ext_ids: List[str],
    limit: int,
    mock_run: bool = False,
):
    """
    Copies data points from the source time series specified by the external id list into the destination project.

    If data points already exist in the destination for a time series, only the newer data points in the source are
    copied over.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        thread_id: The int name of the thread processing the batch
        ext_ids: The list of external ids for time series to copy over
        limit: The maximum number of data points to copy per time series
        mock_run: If true, only retrieves data points from source and does not insert into destination
    """

    logging.info(f"Thread {thread_id}: Starting datapoint replication...")
    updated_timeseries_count = 0
    total_datapoints_copied = 0
    for ext_id in ext_ids:
        # SOURCE
        latest_src_dp = client_src.datapoints.retrieve_latest(external_id=ext_id)
        if not latest_src_dp:
            logging.info(
                f"Thread {thread_id}: No datapoints found in source -- "
                "skipping time series associated with: {src_ext_id}"
            )
            continue

        logging.debug(f"Thread {thread_id}: Latest timestamp source with ext_id {ext_id}: {latest_src_dp[0].timestamp}")
        latest_src_time = latest_src_dp[0].timestamp + 1  # +1 because end time is exclusive for retrieve()

        # DESTINATION
        latest_destination_dp = client_dst.datapoints.retrieve_latest(external_id=ext_id)
        latest_dst_time = 0
        if not latest_destination_dp:
            logging.debug(
                f"Thread {thread_id}: No datapoints in destination, "
                "starting copying from time(epoch): {latest_dst_time}"
            )
        elif latest_destination_dp:
            latest_dst_time = latest_destination_dp[0].timestamp + 1  # +1 because start time is inclusive for retrieve

        if latest_dst_time >= latest_src_time:
            logging.debug(f"Skipping {ext_id} because already up-to-date")
            continue

        # Retrieve and insert missing datapoints
        logging.debug(f"Thread {thread_id} is retrieving datapoints between {latest_dst_time} and {latest_src_time}")

        datapoints = client_src.datapoints.retrieve(
            external_id=ext_id, start=latest_dst_time, end=latest_src_time, limit=limit
        )
        logging.debug(f"Thread {thread_id}: Number of datapoints: {len(datapoints)}")
        if not mock_run:
            client_dst.datapoints.insert(datapoints, external_id=ext_id)
        updated_timeseries_count += 1
        total_datapoints_copied += len(datapoints)

    logging.info(
        f"Thread {thread_id}: Done! {updated_timeseries_count} time series updated, "
        f"{len(ext_ids) - updated_timeseries_count} time series up-to-date. "
        f"{total_datapoints_copied} datapoints copied in total"
    )


def _get_chunk(lst: List[Any], num_chunks: int, chunk_number: int) -> List[Any]:
    """Returns a slice of the given list such that all slices are as even in size as possible.

    Args:
        lst: The list to slice
        num_chunks: The amount of chunks that the list should be split into
        chunk_number: Which chunk of the lst to return

    Returns:
        The chunk_number-th chunk of lst such that the concat of all num_chunks chunks is equivalent to the full lst,
        and each chunk has equal size +-1
    """
    chunk_size = len(lst) // num_chunks
    remainder = len(lst) % num_chunks
    return lst[
        chunk_number * chunk_size
        + min(chunk_number, remainder) : (chunk_number + 1) * chunk_size
        + int(chunk_number < remainder)
        + min(chunk_number, remainder)
    ]


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    num_threads: int = 10,
    limit: Optional[int] = None,
    external_ids: Optional[List[str]] = None,
    mock_run: Optional[bool] = False,
):
    """
    Replicates data points from the source project into the destination project for all time series that
    exist in both environments.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        num_threads: The number of threads to be used.
        limit: The maximum number of data points to copy per time series
        external_ids: A list of time series to replicate data points for
        mock_run: If true, runs the replication without insert, printing what would happen
    """
    if external_ids is not None:
        ts_src = client_src.time_series.retrieve_multiple(external_ids=external_ids)
        ts_dst = client_dst.time_series.retrieve_multiple(external_ids=external_ids)
    else:
        ts_src = client_src.time_series.list(limit=None)
        ts_dst = client_dst.time_series.list(limit=None)

    logging.info(f"Number of time series in source: {len(ts_src)}")
    logging.info(f"Number of time series in destination: {len(ts_dst)}")

    src_ext_id_list = set([ts_id.external_id for ts_id in ts_src])
    dst_ext_id_list = set([ts_id.external_id for ts_id in ts_dst])
    shared_external_ids = [ext_id for ext_id in src_ext_id_list if ext_id in dst_ext_id_list]
    logging.info(
        f"Number of common time series external ids between destination " f"and source: {len(shared_external_ids)}"
    )
    num_batches = num_threads

    arg_list = [
        (client_src, client_dst, thread_id, _get_chunk(shared_external_ids, num_batches, thread_id), limit, mock_run)
        for thread_id in range(num_threads)
    ]

    with mp.Pool(num_threads) as pool:
        pool.starmap(retrieve_insert, arg_list)
