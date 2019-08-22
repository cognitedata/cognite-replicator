#!/usr/bin/env python3

"""
This script serves the purpose of replicating new datapoints from a source tenant to a destination tenant that holds corresponding time series.

REQUIREMENTS: SAME external_id IN SRC AND DST TENANT, CLIENT_SRC, CLIENT DST, SOURCE_API_KEY, DESTINATION_API_KEY
OPTIONAL: DATAPOINT_LIMIT (DEFAULT=10 000 000), keep_asset_connection (DEFAULT=True)

"""
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.assets import Asset
import multiprocessing as mp

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

def retrieve_insert(i,src_ext_id_list_parts,dst_ext_id_list,CLIENT_SRC, CLIENT_DST, keep_asset_connection, num_threads):
    
    for src_ext_id in src_ext_id_list_parts[i]:
        if src_ext_id in dst_ext_id_list:
            # SOURCE
            latest_src_dp = CLIENT_SRC.datapoints.retrieve_latest(
                external_id=src_ext_id
            )
            if not latest_src_dp:
                logging.debug(
                    f"No datapoints found in source -- skipping time series associated with: {src_ext_id}"
                )
                continue

            logging.debug(f"Latest timestamp source with ext_id {src_ext_id}: {latest_src_dp[0].timestamp}")
            latest_src_time = latest_src_dp[0].timestamp

            # DESTINATION
            latest_destination_dp = CLIENT_DST.datapoints.retrieve_latest(
                external_id=src_ext_id
            )
            if not latest_destination_dp:
                latest_dst_time = 0
                logging.debug(
                    f"No datapoints in destination, starting copying from time(epoch): {latest_dst_time}"
                )
            elif latest_destination_dp:
                latest_dst_time = latest_destination_dp[0].timestamp

            # Retrieve and insert missing datapoints
            logging.info(
                f"Thread {i} is retrieving datapoints between {latest_dst_time} and {latest_src_time}\n-----------------------"
            )
            
            datapoints = CLIENT_SRC.datapoints.retrieve(
                external_id=src_ext_id,
                start=latest_dst_time,
                end=latest_src_time,
                limit=10000000
            )
            logging.info(f"Number of datapoints: {len(datapoints)}")
            new_objects = [(o.timestamp, o.value) for o in datapoints]
            CLIENT_DST.datapoints.insert(new_objects, external_id=src_ext_id)
            # Update latest datapoint in destination for initializing next replication
            latest_destination_dp = CLIENT_DST.datapoints.retrieve_latest(
                external_id=src_ext_id
            )
    


def replicate(
    CLIENT_SRC, CLIENT_DST, keep_asset_connection=True, num_threads=10
):

    logging.info(f"Asset_connection is set to :{str(keep_asset_connection)}")
    ts_src = CLIENT_SRC.time_series.list(limit=None)
    logging.info(f"Number of time series in source: {len(ts_src)}")
    ts_dst = CLIENT_DST.time_series.list(limit=None)
    logging.info(f"Number of time series in destination: {len(ts_dst)}")

    src_ext_id_list = [ts_id.external_id for ts_id in ts_src]
    logging.debug(f"{src_ext_id_list}")
    dst_ext_id_list = [ts_id.external_id for ts_id in ts_dst]
    logging.debug(f"List of external id's in destination: {dst_ext_id_list}")
    step = len(ts_src) // num_threads
    src_ext_id_list_parts = [src_ext_id_list[x:x+step] for x in range(0, len(src_ext_id_list), step)]
    jobs = []
    for i in range(num_threads):
        p = mp.Process(target=retrieve_insert, args=(i,src_ext_id_list_parts,dst_ext_id_list,CLIENT_SRC, CLIENT_DST, keep_asset_connection, num_threads))
        jobs.append(p)
        p.start()
        p.join
