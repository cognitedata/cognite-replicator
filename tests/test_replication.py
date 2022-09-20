from cognite.client.data_classes import Asset, Event, TimeSeries
from cognite.client.testing import monkeypatch_cognite_client

from cognite.replicator.replication import (
    existing_mapping,
    filter_objects,
    find_objects_to_delete_if_not_in_src,
    find_objects_to_delete_not_replicated_in_dst,
    make_id_object_map,
    make_objects_batch,
    remove_replication_metadata,
)


def test_make_id_object_map():
    assets = [Asset(id=3, metadata={"_replicatedInternalId": 55}), Asset(id=2)]
    mapping = make_id_object_map(assets)
    assert 1 == len(mapping)
    assert 3 == mapping[55].id


def test_existing_mapping():
    assets = [
        Asset(id=3, name="holy grenade", metadata={"_replicatedInternalId": 33}),
        Asset(id=7, name="not holy grenade", parent_id=3, metadata={"_replicatedInternalId": 77}),
        Asset(id=5, name="in-holy grenade", parent_id=7, metadata={"_replicatedInternalId": 55}),
    ]
    ids = existing_mapping(*assets)
    assert ids[assets[0].metadata["_replicatedInternalId"]] == assets[0].id
    assert ids[assets[1].metadata["_replicatedInternalId"]] == assets[1].id
    assert ids[assets[2].metadata["_replicatedInternalId"]] == assets[2].id


def test_find_objects_to_delete_not_replicated_in_dst():
    assets = [
        Asset(id=3, name="holy grenade", metadata={"_replicatedSource": "source_tenant", "_replicatedInternalId": 123}),
        Asset(id=7, name="not holy grenade", metadata={}),
        Asset(id=5, name="in-holy grenade", metadata={"source": "None"}),
    ]
    to_delete = find_objects_to_delete_not_replicated_in_dst(assets)
    assert len(to_delete) == 2
    assert set(to_delete) == {5, 7}
    assert find_objects_to_delete_not_replicated_in_dst([]) == []


def test_find_objects_to_delete_if_not_in_src():
    assets_dst = [
        Asset(id=3, name="holy grenade", metadata={"_replicatedSource": "source_tenant", "_replicatedInternalId": 3}),
        Asset(id=13, name="unlucky holy grenade", metadata={"_replicatedInternalId": 123}),
        Asset(id=7, name="not holy grenade", metadata={}),
        Asset(id=5, name="in-holy grenade", metadata={"_replicatedInternalId": 5}),
    ]
    assets_src = [Asset(id=3, name="holy grenade", metadata={}), Asset(id=5, name="in-holy grenade", metadata={})]
    to_delete = find_objects_to_delete_if_not_in_src(assets_src, assets_dst)
    assert len(to_delete) == 1
    assert to_delete[0] == 13
    assert find_objects_to_delete_if_not_in_src([], []) == []


def test_filter_objects():
    time_series = [TimeSeries(id=1, asset_id=100), TimeSeries(id=2), TimeSeries(id=3, asset_id=101)]
    events = [Event(id=10, asset_ids=[100, 101]), Event(id=11), Event(id=12, asset_ids=[101])]
    src_dst_asset_id_map = {100: 1000}

    dummy_filtered_events = filter_objects(events, src_dst_asset_id_map)
    dummy_filtered_ts = filter_objects(time_series, src_dst_asset_id_map)
    assert dummy_filtered_events == events
    assert dummy_filtered_ts == time_series

    asset_events = filter_objects(events, src_dst_asset_id_map, skip_nonasset=True)
    asset_ts = filter_objects(time_series, src_dst_asset_id_map, skip_nonasset=True)
    assert len(asset_events) == 2
    assert len(asset_ts) == 2
    for i in range(len(asset_ts)):
        assert asset_ts[i].asset_id is not None
        assert asset_events[i].asset_ids is not None

    linkable_events = filter_objects(events, src_dst_asset_id_map, skip_nonasset=True, skip_unlinkable=True)
    linkable_ts = filter_objects(time_series, src_dst_asset_id_map, skip_nonasset=True, skip_unlinkable=True)
    assert len(linkable_events) == 1
    assert len(linkable_ts) == 1
    assert linkable_events[0] == events[0]
    assert linkable_ts[0] == time_series[0]

    odd_id_events = filter_objects(events, src_dst_asset_id_map, filter_fn=lambda x: x.id % 2 == 1)
    assert len(odd_id_events) == 1
    for event in odd_id_events:
        assert event.id % 2 == 1


def test_make_objects_batch():
    client = monkeypatch_cognite_client()
    src_objects = [
        Event(id=1, last_updated_time=1000),
        Event(id=2, last_updated_time=1000),
        Event(id=3, last_updated_time=1000),
    ]
    src_id_to_dst = {
        1: Event(id=11, metadata={"_replicatedTime": 100}),
        2: Event(id=12, metadata={"_replicatedTime": 10000}),
    }

    def dummy_update(
        src_obj,
        dst_obj,
        src_dst_ids_assets,
        project_src,
        replicated_runtime,
        src_client,
        dst_client,
        src_dst_dataset_mapping,
        config,
    ):
        return dst_obj

    def dummy_create(
        src_obj,
        src_dst_ids_assets,
        project_src,
        replicated_runtime,
        src_client,
        dst_client,
        src_dst_dataset_mapping,
        config,
    ):
        return src_obj

    created, updated, unchanged = make_objects_batch(
        src_objects, src_id_to_dst, {}, dummy_create, dummy_update, "test project", 10000, client, client, {}, {}
    )
    assert len(created) == len(updated) == len(unchanged) == 1
    assert updated[0].id == 11
    assert unchanged[0].id == 12
    assert created[0].id == 3


def test_remove_replication_metadata():
    events = [
        Event(metadata={"_replicatedInternalId": 10, "_replicatedSource": "src_project", "_replicatedTime": 10000000}),
        Event(metadata={}),
        Event(id=3, metadata={"_replicatedInternalId": 10, "misc1": 16, "misc2": "text"}),
    ]
    remove_replication_metadata(events)

    for event in events:
        assert "_replicatedInternalId" not in event.metadata
        assert "_replicatedSource" not in event.metadata
        assert "_replicatedTime" not in event.metadata
    assert len(events[2].metadata.keys()) == 2
