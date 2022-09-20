from cognite.client.data_classes import Event
from cognite.client.testing import monkeypatch_cognite_client

from cognite.replicator.events import copy_events, create_event, update_event


def test_create_event():
    client = monkeypatch_cognite_client()
    events_src = [
        Event(metadata={}, id=1007, asset_ids=[3, 9], start_time=0, end_time=1),
        Event(metadata={}, id=2007, asset_ids=[7], start_time=1, end_time=2),
        Event(start_time=2, asset_ids=[5], end_time=4, metadata={}),
        Event(start_time=6, end_time=8, metadata={}),
    ]
    id_mapping = {3: 333, 7: 777, 5: 555, 9: 999}
    for i, event in enumerate(events_src):
        created_event = create_event(
            event, id_mapping, "src-project-name {}".format(i), 10000000, client, client, {}, {}
        )
        assert created_event.metadata["_replicatedInternalId"] == events_src[i].id
        assert created_event.metadata["_replicatedSource"] == "src-project-name {}".format(i)
        assert (created_event.asset_ids is None) == (events_src[i].asset_ids is None)
        if created_event.asset_ids is not None:
            assert len(created_event.asset_ids) == len(events_src[i].asset_ids)
            for j, asset_id in enumerate(created_event.asset_ids):
                assert asset_id == id_mapping[events_src[i].asset_ids[j]]


def test_update_event():
    client = monkeypatch_cognite_client()
    events_src = [
        Event(metadata={}, id=1007, asset_ids=[3, 9], start_time=0, end_time=1),
        Event(metadata={}, id=2007, asset_ids=[7], start_time=1, end_time=2),
        Event(start_time=2, asset_ids=[5], end_time=4, metadata={}),
    ]
    events_dst = [
        Event(asset_ids=[333], metadata={}),
        Event(asset_ids=[777], metadata={"_replicatedInternalId": 7}),
        Event(asset_ids=[555], metadata={"source": "None"}),
    ]
    id_mapping = {3: 333, 7: 777, 5: 555, 9: 999}

    for i in range(len(events_src)):
        updated_event = update_event(
            events_src[i], events_dst[i], id_mapping, "src-project-name", 1000000, client, client, {}, {}
        )
        assert updated_event.metadata["_replicatedInternalId"] == events_src[i].id
        assert updated_event.metadata["_replicatedSource"] == "src-project-name"
        assert len(updated_event.asset_ids) == len(events_src[i].asset_ids)
        for j, asset_id in enumerate(updated_event.asset_ids):
            assert asset_id == id_mapping[events_src[i].asset_ids[j]]


def test_copy_events():
    events_src = [
        Event(metadata={}, id=1007, asset_ids=[3, 9], start_time=0, end_time=1),
        Event(metadata={}, id=2007, asset_ids=[7], start_time=1, end_time=2),
        Event(start_time=2, asset_ids=[5], end_time=4, metadata={}),
    ]
    id_mapping = {i: i * 111 for i in range(1, 10)}
    with monkeypatch_cognite_client() as client_dst:
        copy_events(events_src, {}, id_mapping, "src-project-name", 1000000, client_dst, client_dst, {}, {}, None)
