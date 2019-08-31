#!/usr/bin/env python3

"""
This script serves the purpose of replicating new datapoints from a source tenant to a destination tenant that holds corresponding time series.

REQUIREMENTS: SAME external_id IN SRC AND DST TENANT, client_src, client_dst, SOURCE_API_KEY, DESTINATION_API_KEY
OPTIONAL: keep_asset_connection (DEFAULT=True), change limit

"""
import logging
import multiprocessing as mp
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries


def retrieve_insert(
    thread_id: int,
    src_ext_id_list_parts: List[str],
    dst_ext_id_list: List[str],
    client_src: CogniteClient,
    client_dst: CogniteClient,
    dp_limit: int,
):

    for src_ext_id in src_ext_id_list_parts:
        if src_ext_id in dst_ext_id_list:
            # SOURCE
            latest_src_dp = client_src.datapoints.retrieve_latest(external_id=src_ext_id)
            if not latest_src_dp:
                logging.info(
                    f"Thread {thread_id}: No datapoints found in source -- skipping time series associated with: {src_ext_id}"
                )
                continue

            logging.debug(
                f"Thread {thread_id}: Latest timestamp source with ext_id {src_ext_id}: {latest_src_dp[0].timestamp}"
            )
            latest_src_time = latest_src_dp[0].timestamp

            # DESTINATION
            latest_destination_dp = client_dst.datapoints.retrieve_latest(external_id=src_ext_id)
            if not latest_destination_dp:
                latest_dst_time = 0
                logging.info(
                    f"Thread {thread_id}: No datapoints in destination, starting copying from time(epoch): {latest_dst_time}"
                )
            elif latest_destination_dp:
                latest_dst_time = latest_destination_dp[0].timestamp

            # Retrieve and insert missing datapoints
            logging.info(f"Thread {thread_id} is retrieving datapoints between {latest_dst_time} and {latest_src_time}")

            datapoints = client_src.datapoints.retrieve(
                external_id=src_ext_id, start=latest_dst_time, end=latest_src_time, limit=dp_limit
            )
            logging.info(f"Thread {thread_id}: Number of datapoints: {len(datapoints)}")
            new_objects = [(o.timestamp, o.value) for o in datapoints]
            client_dst.datapoints.insert(new_objects, external_id=src_ext_id)
            # Update latest datapoint in destination for initializing next replication
            latest_destination_dp = client_dst.datapoints.retrieve_latest(external_id=src_ext_id)


def replicate(client_src, client_dst, keep_asset_connection=True, num_threads=10, limit=100000):

    logging.info(f"Asset_connection is set to :{str(keep_asset_connection)}")
    ts_src = client_src.time_series.list(limit=None)
    logging.info(f"Number of time series in source: {len(ts_src)}")
    ts_dst = client_dst.time_series.list(limit=None)
    logging.info(f"Number of time series in destination: {len(ts_dst)}")

    src_ext_id_list = [ts_id.external_id for ts_id in ts_src]
    logging.debug(f"{src_ext_id_list}")
    dst_ext_id_list = [ts_id.external_id for ts_id in ts_dst]
    logging.debug(f"List of external id's in destination: {dst_ext_id_list}")
    step = len(ts_src) // num_threads

    jobs = []
    for thread_id in range(num_threads):
        parts = src_ext_id_list[thread_id * step : (thread_id + 1) * step]
        p = mp.Process(target=retrieve_insert, args=(thread_id, parts, dst_ext_id_list, client_src, client_dst, limit))
        jobs.append(p)
        p.start()
    for p in jobs:
        p.join()
