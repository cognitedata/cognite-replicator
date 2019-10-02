import time

from cognite.client.data_classes import Event
from cognite.replicator.events import create_event, update_event


def test_create_event():
    events_src = [
        Event(metadata={}, id=1007, asset_ids=[3], start_time=0, end_time=1),
        Event(metadata={}, id=2007, asset_ids=[7], start_time=1, end_time=2),
        Event(start_time=2, asset_ids=[5], end_time=4, metadata={}),
        Event(start_time=6, end_time=8, metadata={}),
    ]
    runtime = time.time() * 1000
    id_mapping = {3: 333, 7: 777, 5: 555}
    created_events = []
    for i, event in enumerate(events_src):
        created_events.append(create_event(event, id_mapping, "Road to the Holy Grail {}".format(i), runtime))

    assert len(created_events) == len(events_src)

    for i, event_obj in enumerate(created_events):
        assert event_obj.metadata["_replicatedInternalId"] == events_src[i].id
        assert event_obj.metadata["_replicatedSource"] == "Road to the Holy Grail {}".format(i)
        if event_obj.asset_ids is not None:
            assert event_obj.asset_ids[0] == id_mapping[events_src[i].asset_ids[0]]


def test_update_event():
    events_src = [
        Event(metadata={}, id=1007, asset_ids=[3], start_time=0, end_time=1),
        Event(metadata={}, id=2007, asset_ids=[7], start_time=1, end_time=2),
        Event(start_time=2, asset_ids=[5], end_time=4, metadata={}),
    ]
    events_dst = [
        Event(asset_ids=[333], metadata={}),
        Event(asset_ids=[777], metadata={"_replicatedInternalId": 7}),
        Event(asset_ids=[555], metadata={"source": "None"}),
    ]
    runtime = time.time() * 1000
    id_mapping = {3: 333, 7: 777, 5: 555}
    updated_event0 = update_event(events_src[0], events_dst[0], id_mapping, "Road to the Holy Grail", runtime)
    updated_event1 = update_event(events_src[1], events_dst[1], id_mapping, "Road to the Holy Grail", runtime)
    updated_event2 = update_event(events_src[2], events_dst[2], id_mapping, "Road to the Holy Grail", runtime)
    assert updated_event0.metadata["_replicatedInternalId"] == events_src[0].id
    assert updated_event1.metadata["_replicatedInternalId"] == events_src[1].id
    assert updated_event2.metadata["_replicatedInternalId"] == events_src[2].id
    assert updated_event0.asset_ids[0] == id_mapping[events_src[0].asset_ids[0]]
    assert updated_event1.asset_ids[0] == id_mapping[events_src[1].asset_ids[0]]
    assert updated_event2.asset_ids[0] == id_mapping[events_src[2].asset_ids[0]]
    assert updated_event0.metadata["_replicatedSource"] == "Road to the Holy Grail"
    assert updated_event1.metadata["_replicatedSource"] == "Road to the Holy Grail"
    assert updated_event2.metadata["_replicatedSource"] == "Road to the Holy Grail"
