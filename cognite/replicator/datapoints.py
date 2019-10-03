import logging
import multiprocessing as mp
from datetime import datetime
from enum import Enum
from math import ceil, floor
from typing import Any, List, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoints
from cognite.client.exceptions import CogniteAPIError


class Replication(Enum):
    SUCCESS = 0
    FAILURE = 1
    EMPTY = 2


def _get_time_range(src_datapoint: Datapoints, dst_datapoint: Datapoints):
    # +1 because datapoint time ranges are inclusive on start and exclusive on end,
    # and we want the last one but not the first
    start_time = 0
    if dst_datapoint:
        start_time = dst_datapoint.timestamp[0] + 1

    end_time = None if not src_datapoint else src_datapoint.timestamp[0] + 1
    return start_time, end_time


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


def replicate_datapoints(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    ts_external_id: str,
    limit: Optional[int] = None,
    partition_size: int = 100000,
    mock_run: bool = False,
    job_id: int = 1,
):
    """
    Copies data points from the source tenant into the destination project, for the given time series.

    If data points already exist in the destination for the time series, only the newer data points in the source are
    copied over.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        ts_external_id: The external id of the time series to replicate datapoints for
        limit: The maximum number of data points to copy
        partition_size: The maximum number of datapoints to retrieve per request
        mock_run: If true, only retrieves data points from source and does not insert into destination
        job_id: The batch number being processed
    """
    try:
        latest_dst_dp = client_dst.datapoints.retrieve_latest(external_id=ts_external_id)
        latest_src_dp = client_src.datapoints.retrieve_latest(external_id=ts_external_id)
    except CogniteAPIError as exc:
        logging.error(f"Job {job_id}: Failed for external id {ts_external_id}. {exc}")
        return Replication.FAILURE, 0

    start, end = _get_time_range(latest_src_dp, latest_dst_dp)
    if not end:
        return Replication.EMPTY, 0

    logging.debug(f"Job {job_id}: Ext_id: {ts_external_id} Retrieving datapoints between {start} and {end}")
    datapoints_count = 0
    while start < end:
        num_to_fetch = partition_size if limit is None else min(partition_size, limit - datapoints_count)
        if num_to_fetch == 0:
            break

        try:
            datapoints = client_src.datapoints.retrieve(
                external_id=ts_external_id, start=start, end=end, limit=num_to_fetch
            )
            datapoints_count += len(datapoints)
            if len(datapoints):
                start = datapoints[-1].timestamp + 1
                if not mock_run:
                    client_dst.datapoints.insert(datapoints, external_id=ts_external_id)
        except CogniteAPIError as exc:
            logging.error(f"Job {job_id}: Failed for external id {ts_external_id}. {exc}")
            return Replication.FAILURE, datapoints_count

    logging.debug(f"Job {job_id}: Ext_id: {ts_external_id} Number of datapoints: {datapoints_count}")
    return Replication.SUCCESS, datapoints_count


def batch_replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    job_id: int,
    ext_ids: List[str],
    limit: int,
    mock_run: bool = False,
    partition_size: int = 100000,
):
    """
    Replicates datapoints for each time series specified by the external id list.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        job_id: The batch number being processed
        ext_ids: The list of external ids for time series to copy over
        limit: The maximum number of data points to copy per time series
        mock_run: If true, only retrieves data points from source and does not insert into destination
        partition_size: The maximum number of datapoints to retrieve per request
    """

    def log_status(total_ts_count):
        logging.info(
            f"Job {job_id}: Current results: {updated_timeseries_count} time series updated, "
            f"{total_ts_count - updated_timeseries_count - len(failed_external_ids) - len(empty_external_ids)} "
            f"time series up-to-date. {total_datapoints_copied} datapoints copied. "
            f"{len(failed_external_ids)} failure(s). {len(empty_external_ids)} time series without datapoints."
        )

    logging.info(f"Job {job_id}: Starting datapoint replication for {len(ext_ids)} time series...")
    updated_timeseries_count = 0
    total_datapoints_copied = 0
    empty_external_ids = []
    failed_external_ids = []
    start_time = datetime.now()

    for i, ext_id in enumerate(ext_ids):
        if i % ceil(len(ext_ids) / 10) == 0:
            logging.info(
                f"Job {job_id}: Progress: On time series {i+1}/{len(ext_ids)} "
                f"({floor(100 * i / len(ext_ids))}% complete) in {datetime.now()-start_time}"
            )
            log_status(i)

        status, datapoints_copied_count = replicate_datapoints(
            client_src, client_dst, ext_id, partition_size=partition_size, mock_run=mock_run, job_id=job_id, limit=limit
        )

        if status == Replication.EMPTY:
            empty_external_ids.append(ext_id)
        elif status == Replication.FAILURE:
            failed_external_ids.append(ext_id)
        else:
            updated_timeseries_count += 1
        total_datapoints_copied += datapoints_copied_count

    log_status(len(ext_ids))

    logging.info(f"Total elapsed time: {datetime.now() - start_time}")
    logging.info(f"Job {job_id}: Sample of failed ids: {failed_external_ids[:10]}")
    logging.info(f"Job {job_id}: Sample of empty time series: {empty_external_ids[:10]}")


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    batch_size: Optional[int] = None,
    num_threads: int = 10,
    limit: Optional[int] = None,
    external_ids: Optional[List[str]] = None,
    mock_run: bool = False,
    partition_size: int = 100000,
):
    """
    Replicates data points from the source project into the destination project for all time series that
    exist in both environments.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        batch_size: The size of batches to split the external id list into. Defaults to num_threads.
        num_threads: The number of threads to be used.
        limit: The maximum number of data points to copy per time series
        external_ids: A list of time series to replicate data points for
        mock_run: If true, runs the replication without insert, printing what would happen
        partition_size: The maximum number of datapoints to retrieve per request
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
    shared_external_ids = [ext_id for ext_id in src_ext_id_list if ext_id in dst_ext_id_list and ext_id]
    logging.info(
        f"Number of common time series external ids between destination and source: {len(shared_external_ids)}"
    )
    if batch_size is None:
        batch_size = ceil(len(shared_external_ids) / num_threads)
    num_batches = ceil(len(shared_external_ids) / batch_size)

    arg_list = [
        (
            client_src,
            client_dst,
            job_id,
            _get_chunk(shared_external_ids, num_batches, job_id),
            limit,
            mock_run,
            partition_size,
        )
        for job_id in range(num_batches)
    ]

    if num_threads > 1:
        with mp.Pool(num_threads) as pool:
            pool.starmap(batch_replicate, arg_list)
    else:
        batch_replicate(*arg_list[0])
