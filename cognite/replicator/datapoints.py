import logging
import multiprocessing as mp
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from math import ceil, floor
from typing import Any, Callable, List, Optional, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoint, Datapoints, DatapointsQuery
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._time import timestamp_to_ms


""" This is useful if there are many time series coming in at very different frequences """
# def _get_time_range(src_datapoint: Datapoints, dst_datapoint: Datapoints) -> Tuple[int, int]:
#    # +1 because datapoint retrieval time ranges are inclusive on start and exclusive on end
#    start_time = 0 if not dst_datapoint else dst_datapoint.timestamp[0] + 1
#    end_time = 0 if not src_datapoint else src_datapoint.timestamp[0] + 1
#    return start_time, end_time


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


def replicate_datapoints_several_ts(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    job_id: int,  # = 1,
    ext_ids: List[str],
    limit: Optional[int] = None,
    mock_run: bool = False,
    partition_size: int = 100000,
    src_datapoint_transform: Optional[Callable[[Datapoint], Datapoint]] = None,
    timerange_transform: Optional[Callable[[Tuple[int, int]], Tuple[int, int]]] = None,
    start: Union[int, str] = None,
    end: Union[int, str] = None,
    value_manipulation_lambda_fnc: str = None,
) -> Tuple[bool, int]:
    """
    Copies data points from the source tenant into the destination project, for the time series in the list.
    Fetches the last datapoints from a list of time series and starts replication at that timestamp for each respective time series. June 2022 update according to updates in sdk.
    """

    logging.info(f"Number of time series into replicate function: {len(ext_ids)}")

    # Logging the beginning of the process
    logging.info(f"Job {job_id}: Starting datapoint replication for {len(ext_ids)} time series...")
    logging.info(f"The timeseries included in the job are: {ext_ids}")
    start_time = datetime.now()

    try:
        dst_latest_datapoints = client_dst.datapoints.retrieve_latest(
            external_id=ext_ids
        )  # getting the latest datapoints from the destination, timestamps
        src_datapoint_queries = [
            DatapointsQuery(
                external_id=dst_latest_dp.external_id,
                start=dst_latest_dp[0].timestamp if len(dst_latest_dp) > 0 else start,  # 4 years
                end=end,
            )
            for dst_latest_dp in dst_latest_datapoints
        ]  # creating queries for the datapoints starting with the latest datapoint
        print("Queries ready: ", time.ctime())
        src_datapoints_to_insert = client_src.datapoints.query(
            src_datapoint_queries
        )  # querying the source for the datapoints matching this query
        print("Datapoints to insert ready", time.ctime())
        insert_format_datapoints = []

        # Written this way as the insert_multiple function in the sdk does not support to insert a Datapoints object directly
        for dplist in src_datapoints_to_insert:
            dict_to_insert = {}
            for datapoints in dplist:
                # if there is not yet an entry for this specific time series
                if "externalId" not in dict_to_insert.keys():
                    dict_to_insert["externalId"] = datapoints.external_id

                # If datapoints should be transformed
                transformed_dps = None
                if src_datapoint_transform:
                    transformed_values = []
                    transformed_timestamps = []
                    for src_datapoint in datapoints:
                        transformed_datapoint = src_datapoint_transform(src_datapoint)
                        transformed_timestamps.append(transformed_datapoint.timestamp)
                        transformed_values.append(transformed_datapoint.value)
                    transformed_dps = Datapoints(timestamp=transformed_timestamps, value=transformed_values)

                # If datapoints should get applied a lambda function
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
                if transformed_dps is not None:
                    list_of_datapoints = transformed_dps
                else:
                    list_of_datapoints = [
                        {"timestamp": datapoints.timestamp[i], "value": datapoints.value[i]}
                        for i in range(len(datapoints.timestamp))
                    ]
                logging.info(f"Ext id:  {datapoints.external_id} Number of datapoints: {len(list_of_datapoints)}")

            # This assertion needs to be in place, because the API call crashes if one ts has no datapoints to insert
            if len(list_of_datapoints) > 0:
                dict_to_insert["datapoints"] = list_of_datapoints
                insert_format_datapoints.append(dict_to_insert)

        print("Ready to insert datapoints...", time.ctime())
        # insert the multiple lists of datapoints into CDF
        if not mock_run:
            client_dst.datapoints.insert_multiple(insert_format_datapoints)
            print("DATAPOINTS INSERTED AT: ", time.ctime())

    except CogniteAPIError as exc:
        logging.error(f"Job {job_id}: Failed for external ids {ext_ids}. {exc}")
        return (False, len(ext_ids))

    return (True, len(ext_ids))

    # actually appending

    # After-replication handling

    # if not success_status:
    #    ....
    # else:
    #    updated_timeseries_count += 1
    # total_datapoints_copied


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
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

    # Confusement in which method to use
    if external_ids and exclude_pattern:
        raise ValueError(
            f"List of time series AND a regex exclusion rule was given! Either remove the filter {exclude_pattern} or the list of time series {external_ids}"
        )

    # Replicate based on list of external ids
    elif external_ids is not None:  # Specified list of time series is given
        ts_src = client_src.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
        ts_dst = client_dst.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
        src_ext_id_list = [ts_obj.external_id for ts_obj in ts_src]

    # Replicate based on regex expression
    else:
        ts_src = client_src.time_series.list(limit=None)
        ts_dst = client_dst.time_series.list(limit=None)
        filtered_ts_src = []
        skipped_ts = []
        if exclude_pattern:  # Filtering based on regex rule given
            compiled_re = re.compile(exclude_pattern)
            for ts in ts_src:
                if ts.external_id is not None:
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

    arg_list = [
        (
            client_src,
            client_dst,
            1,
            shared_external_ids,
            limit,
            mock_run,
            partition_size,
            src_datapoint_transform,
            timerange_transform,
            start,
            end,
            value_manipulation_lambda_fnc,
        )
    ]

    replicate_datapoints_several_ts(*arg_list[0])
