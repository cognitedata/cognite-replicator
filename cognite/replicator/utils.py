from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoint
from datetime import timedelta

from typing import Tuple


def remove_recent_datapoints(client: CogniteClient):
    time_series = client.time_series.list(limit=None)
    for ts in time_series:
        if ts.external_id:
            client.datapoints.delete_range(start="7d-ago", end="now", external_id=ts.external_id)


def one_week_offset(dp: Datapoint) -> Datapoint:
    dp.timestamp = dp.timestamp + 1000 * int(timedelta(days=7).total_seconds())
    return dp


def one_week_timerange_shift(start: int, end: int) -> Tuple[int, int]:
    return start - 1000 * int(timedelta(days=7).total_seconds()), end - 1000 * int(timedelta(days=7).total_seconds())
