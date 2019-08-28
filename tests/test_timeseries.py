from cognite.client.data_classes import TimeSeries
from cognite.replicator.time_series import filter_away_service_account_ts


def test_filter_away_service_account_ts():
    ts_src = [
        TimeSeries(name="holy_timeseries_service_account_metrics", metadata={}),
        TimeSeries(name="not holy timeseries service_account_metrics", metadata={}),
        TimeSeries(name="in-holy timeseries", metadata={}),
    ]
    ts_list = filter_away_service_account_ts(ts_src)
    assert len(ts_list) == 1
    assert ts_list[0].name == "in-holy timeseries"
