import logging
import multiprocessing as mp
import re
from datetime import datetime
from math import ceil, floor
from typing import Any, Callable, List, Optional, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoint, Datapoints
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._time import timestamp_to_ms


def _get_time_range(src_datapoint: Datapoints, dst_datapoint: Datapoints) -> Tuple[int, int]:
    # +1 because datapoint retrieval time ranges are inclusive on start and exclusive on end
    start_time = 0 if not dst_datapoint else dst_datapoint.timestamp[0] + 1
    end_time = 0 if not src_datapoint else src_datapoint.timestamp[0] + 1
    return start_time, end_time


def _get_chunk(lst: List[Any], num_chunks: int, chunk_number: int) -> List[Any]:
    """Returns a slice of the given list such that all slices are as even in size as possible.

    Args:
        lst: The list to slice
        num_chunks: The amount of chunks that the list should be split into
        chunk_number: Which chunk of the lst to return (0-indexed)

    Returns:
        The chunk_number-th chunk of lst such that the concat of all num_chunks chunks is equivalent to the full lst,
        and each chunk has equal size +-1
    """
    chunk_size = len(lst) // num_chunks
    num_excess_elements = len(lst) % num_chunks

    start_index = chunk_number * chunk_size
    start_index += min(chunk_number, num_excess_elements)  # offset by amount of excess elements used in previous chunks

    end_index = start_index + chunk_size
    if chunk_number < num_excess_elements:  # if we need to include an extra element
        end_index += 1

    return lst[start_index:end_index]


def evaluate_lambda_function(lambda_fnc_str: str):
    """Returns callable object by evaluating lambda function string.

    Args:
        lambda_fnc_str: lambda function string for datapoint.value manipulation

    Returns:
        Callable function

    """
    try:
        lambda_fnc = eval(lambda_fnc_str)
        return lambda_fnc
    except Exception as e:
        logging.error(f"An error occurred when using value manipulation " f"lambda function. {lambda_fnc_str}")
        logging.error(e)
        return None


def replicate_datapoints(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    ts_external_id: str,
    limit: Optional[int] = None,
    partition_size: int = 100000,
    mock_run: bool = False,
    job_id: int = 1,
    src_datapoint_transform: Optional[Callable[[Datapoint], Datapoint]] = None,
    timerange_transform: Optional[Callable[[Tuple[int, int]], Tuple[int, int]]] = None,
    start: Union[int, str] = None,
    end: Union[int, str] = None,
    value_manipulation_lambda_fnc: str = None,
) -> Tuple[bool, int]:
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
        src_datapoint_transform: Function to apply to all source datapoints before inserting into destination
        timerange_transform: Function to set the time range boundaries (start, end) arbitrarily.
        start: Timestamp to start replication onwards from; if not specified starts at most recent datapoint
        end: If specified, limits replication to datapoints earlier than the end time
        value_manipulation_lambda_fnc: A basic lambda function can be provided to manipulate datapoints as a string.

    Returns:
        A tuple of the success status (True if no failures) and the number of datapoints successfully replicated
    """
    try:
        latest_dst_dp = client_dst.datapoints.retrieve_latest(external_id=ts_external_id)
        latest_src_dp = client_src.datapoints.retrieve_latest(external_id=ts_external_id)
    except CogniteAPIError as exc:
        logging.error(f"Job {job_id}: Failed for external id {ts_external_id}. {exc}")
        return False, 0

    if not latest_src_dp:
        return True, 0

    if src_datapoint_transform:
        latest_src_dp = Datapoints(timestamp=[src_datapoint_transform(latest_src_dp[0]).timestamp])

    _start, _end = _get_time_range(latest_src_dp, latest_dst_dp)

    start = _start if start is None else timestamp_to_ms(start)
    end = _end if end is None else timestamp_to_ms(end)

    if timerange_transform:
        start, end = timerange_transform(start, end)

    # Api Restrictions
    start = max(start, 31536000000)  # 1971

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
            if not datapoints:
                break

            if src_datapoint_transform:
                transformed_values = []
                transformed_timestamps = []
                for src_datapoint in datapoints:
                    transformed_datapoint = src_datapoint_transform(src_datapoint)
                    transformed_timestamps.append(transformed_datapoint.timestamp)
                    transformed_values.append(transformed_datapoint.value)
                datapoints = Datapoints(timestamp=transformed_timestamps, value=transformed_values)

            if value_manipulation_lambda_fnc:
                transformed_values = []
                transformed_timestamps = []
                lambda_fnc = evaluate_lambda_function(value_manipulation_lambda_fnc)
                if lambda_fnc:
                    for src_datapoint in datapoints:
                        try:
                            transformed_timestamps.append(src_datapoint.timestamp)
                            transformed_values.append(lambda_fnc(src_datapoint.value))
                        except Exception as e:
                            logging.error(
                                f"Could not manipulate the datapoint (value={src_datapoint.value},"
                                + f" timestamp={src_datapoint.timestamp}). Error: {e}"
                            )
                    datapoints = Datapoints(timestamp=transformed_timestamps, value=transformed_values)

            if not mock_run:
                client_dst.datapoints.insert(datapoints, external_id=ts_external_id)
        except CogniteAPIError as exc:
            logging.error(f"Job {job_id}: Failed for external id {ts_external_id}. {exc}")
            return False, datapoints_count
        else:
            datapoints_count += len(datapoints)
            start = datapoints[-1].timestamp + 1

    logging.debug(f"Job {job_id}: Ext_id: {ts_external_id} Number of datapoints: {datapoints_count}")
    return True, datapoints_count


def batch_replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    job_id: int,
    ext_ids: List[str],
    limit: int,
    mock_run: bool = False,
    partition_size: int = 100000,
    src_datapoint_transform: Optional[Callable[[Datapoint], Datapoint]] = None,
    timerange_transform: Optional[Callable[[Tuple[int, int]], Tuple[int, int]]] = None,
    start: Union[int, str] = None,
    end: Union[int, str] = None,
    value_manipulation_lambda_fnc: str = None,
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
        src_datapoint_transform: Function to apply to all source datapoints before inserting into destination
        timerange_transform: Function to set the time range boundaries (start, end) arbitrarily.
        start: Timestamp to start replication onwards from; if not specified starts at most recent datapoint
        end: If specified, limits replication to datapoints earlier than the end time
        value_manipulation_lambda_fnc: A basic lambda function can be provided to manipulate datapoints as a string.
    """

    def log_status(total_ts_count):
        logging.info(
            f"Job {job_id}: Current results: {updated_timeseries_count} time series updated, "
            f"{total_ts_count - updated_timeseries_count - len(failed_external_ids)} "
            f"time series up-to-date. {total_datapoints_copied} datapoints copied. "
            f"{len(failed_external_ids)} failure(s)."
        )

    logging.info(f"Job {job_id}: Starting datapoint replication for {len(ext_ids)} time series...")
    updated_timeseries_count = 0
    total_datapoints_copied = 0
    failed_external_ids = []
    start_time = datetime.now()

    for i, ext_id in enumerate(ext_ids):
        if i % ceil(len(ext_ids) / 10) == 0:
            logging.info(
                f"Job {job_id}: Progress: On time series {i+1}/{len(ext_ids)} "
                f"({floor(100 * i / len(ext_ids))}% complete) in {datetime.now()-start_time}"
            )
            log_status(i)

        success_status, datapoints_copied_count = replicate_datapoints(
            client_src,
            client_dst,
            ext_id,
            partition_size=partition_size,
            mock_run=mock_run,
            job_id=job_id,
            limit=limit,
            src_datapoint_transform=src_datapoint_transform,
            timerange_transform=timerange_transform,
            start=start,
            end=end,
            value_manipulation_lambda_fnc=value_manipulation_lambda_fnc,
        )

        if not success_status:
            failed_external_ids.append(ext_id)
        else:
            updated_timeseries_count += 1
        total_datapoints_copied += datapoints_copied_count

    log_status(len(ext_ids))

    logging.info(f"Total elapsed time: {datetime.now() - start_time}")
    logging.info(f"Job {job_id}: Sample of failed ids: {failed_external_ids[:10]}")


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    batch_size: Optional[int] = None,
    num_threads: int = 10,
    limit: Optional[int] = None,
    external_ids: Optional[List[str]] = None,
    mock_run: bool = False,
    partition_size: int = 100000,
    src_datapoint_transform: Optional[Callable[[Datapoint], Datapoint]] = None,
    timerange_transform: Optional[Callable[[Tuple[int, int]], Tuple[int, int]]] = None,
    start: Union[int, str] = None,
    end: Union[int, str] = None,
    exclude_pattern: str = None,
    value_manipulation_lambda_fnc: str = None,
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
        src_datapoint_transform: Function to apply to all source datapoints before inserting into destination
        timerange_transform: Function to set the time range boundaries (start, end) arbitrarily.
        start: Timestamp to start replication onwards from; if not specified starts at most recent datapoint
        end: If specified, limits replication to datapoints earlier than the end time
        exclude_pattern: Regex pattern; time series whose names match will not be replicated from
        value_manipulation_lambda_fnc: A basic lambda function can be provided to manipulate datapoints as a string.
                                        It will be applied to the value of each datapoint in the timeseries.
    """

    if external_ids and exclude_pattern:
        raise ValueError(
            f"List of time series AND a regex exclusion rule was given! Either remove the filter {exclude_pattern} or the list of time series {external_ids}"
        )
    elif external_ids is not None:  # Specified list of time series is given
        ts_src = client_src.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
        ts_dst = client_dst.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
        src_ext_id_list = [ts_obj.external_id for ts_obj in ts_src]
    else:
        ts_src = client_src.time_series.list(limit=None)
        ts_dst = client_dst.time_series.list(limit=None)
        filtered_ts_src = []
        skipped_ts = []
        if exclude_pattern:  # Filtering based on regex rule given
            compiled_re = re.compile(exclude_pattern)
            for ts in ts_src:
                if compiled_re.search(ts.external_id):
                    skipped_ts.append(ts.external_id)
                else:
                    filtered_ts_src.append(ts.external_id)
            src_ext_id_list = filtered_ts_src
            logging.info(
                f"Excluding datapoints from {len(skipped_ts)} time series, due to regex rule: {exclude_pattern}. Sample: {skipped_ts[:5]}"
            )
            # Should probably change to logging.debug after a while
        else:  # Expects to replicate all shared time series
            src_ext_id_list = [ts_obj.external_id for ts_obj in ts_src]
    logging.info(f"Number of time series in source: {len(ts_src)}")
    logging.info(f"Number of time series in destination: {len(ts_dst)}")

    dst_ext_id_list = set([ts_obj.external_id for ts_obj in ts_dst])
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
            src_datapoint_transform,
            timerange_transform,
            start,
            end,
            value_manipulation_lambda_fnc,
        )
        for job_id in range(num_batches)
    ]

    if num_threads > 1:
        with mp.Pool(num_threads) as pool:
            pool.starmap(batch_replicate, arg_list)
    else:
        batch_replicate(*arg_list[0])
