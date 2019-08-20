#!/usr/bin/env python3

"""
This script serves the purpose of replicating new datapoints from a source tenant to a destination tenant that has the same asset.

REQUIREMENTS: SAME external_id IN SRC AND DST TENANT, PROJECT_SRC, PROJECT_DST, CLIENT_NAME, SOURCE_API_KEY, DESTINATION_API_KEY
OPTIONAL: DATAPOINT_LIMIT (DEFAULT=10 000), keep_asset_connection (DEFAULT=True), timing (DEFAULT=False)

Todo: add logging to stackdriver, add multiprocessing
"""

# SECRET_SCOPE: Name your Databricks secret scope here.
SECRET_SCOPE = "tao_default"
# PROJECT_SRC: Name the source project here, this should also match the corresponding source key given the Databricks secret scope.
PROJECT_SRC = "publicdata"
# PROJECT_DST: Name the destination project here, this should also match the corresponding destination key given the Databricks secret scope.
PROJECT_DST = "gcproject"
# CLIENT_NAME: This is a user-defined string intended to give the client a unique identifier.
CLIENT_NAME = "cognite-databricks-replicator-assets"

source_key = open("publicdata.key", "r")
destination_key = open("gcproject.key", "r")

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.assets import Asset

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)


def datapoint_replication(
    CLIENT_SRC, CLIENT_DST, keep_asset_connection=True, dp_limit=10000, timing=False
):

    logging.info(f"Asset_connection is set to :{str(keep_asset_connection)}")
    ts_src = CLIENT_SRC.time_series.list(limit=0)
    logging.info(f"Number of time series in source: {len(ts_src)}")
    ts_dst = CLIENT_DST.time_series.list(limit=0)
    logging.info(f"Number of time series in destination: {len(ts_dst)}")

    src_ext_id_list = [ts_id.external_id for ts_id in ts_src]
    logging.debug(f"{src_ext_id_list}")
    dst_ext_id_list = [ts_id.external_id for ts_id in ts_dst]
    logging.debug(f"List of external id's in destination: {dst_ext_id_list}")

    for src_ext_id in src_ext_id_list:
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

            logging.debug(f"Latest timestamp source : {latest_src_dp[0].timestamp}")
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
                logging.debug(f"Latest timestamp dst : {latest_dst_time}")

            # Retrieve and insert missing datapoints
            logging.info(
                f"Retrieving datapoints between {latest_dst_time} and {latest_src_time}\n-----------------------"
            )
            datapoints = CLIENT_SRC.datapoints.retrieve(
                external_id=src_ext_id,
                start=latest_dst_time,
                end=latest_src_time,
                limit=dp_limit,
            )
            logfile = open("timinglog.txt", "w+")
            while len(datapoints) == dp_limit:
                start = time.time()
                datapoints = CLIENT_SRC.datapoints.retrieve(
                    external_id=src_ext_id,
                    start=latest_dst_time,
                    end=latest_src_time,
                    limit=dp_limit,
                )
                logging.debug(f"Number of datapoints: {len(datapoints)}")
                new_objects = [(o.timestamp, o.value) for o in datapoints]
                CLIENT_DST.datapoints.insert(new_objects, external_id=src_ext_id)
                # Update latest datapoint in destination for initializing next replication
                latest_destination_dp = CLIENT_DST.datapoints.retrieve_latest(
                    external_id=src_ext_id
                )
                logging.debug(
                    f"Latest datapoint (destination): {latest_destination_dp}"
                )
                if timing:
                    end = time.time()
                    diff = end - start
                    logfile.write(
                        f"Transfering {str(dp_limit)} datapoints took {str(diff)} time\n"
                    )
            if len(datapoints) > dp_limit:
                raise Exception(
                    f"The number of datapoints is {len(datapoints)}, this exceeds the limit of: {dp_limit}"
                )


if __name__ == "__main__":

    CLIENT_SRC = CogniteClient(
        api_key=str(source_key.read()), project=PROJECT_SRC, client_name=CLIENT_NAME
    )
    CLIENT_DST = CogniteClient(
        api_key=str(destination_key.read()),
        project=PROJECT_DST,
        client_name=CLIENT_NAME,
        timeout=90,
    )
    datapoint_replication(CLIENT_SRC, CLIENT_DST, keep_asset_connection=True)

    source_key.close()
    destination_key.close()
