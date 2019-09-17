from cognite.client.data_classes import TimeSeries
from cognite.replicator.time_series import filter_non_copyable_ts


def test_filter_away_service_account_ts():
    ts_src = [
        TimeSeries(name="holy_timeseries_service_account_metrics", metadata={}),
        TimeSeries(name="not holy timeseries service_account_metrics", metadata={}),
        TimeSeries(name="in-holy timeseries", metadata={}),
        TimeSeries(name="secure timeseries", metadata={}, security_categories=[2]),
        TimeSeries(name="insecure timeseries 1", metadata={}, security_categories=[]),
        TimeSeries(name="insecure timeseries 2", metadata={}),
    ]
    ts_list = filter_non_copyable_ts(ts_src)
    assert len(ts_list) == 3
    assert ts_list[0].name == "in-holy timeseries"
    assert ts_list[1].name == "insecure timeseries 1"
    assert ts_list[2].name == "insecure timeseries 2"
