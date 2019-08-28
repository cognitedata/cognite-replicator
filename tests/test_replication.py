from cognite.client.data_classes.assets import Asset
from cognite.replicator.replication import (
    existing_mapping,
    find_objects_to_delete_if_not_in_src,
    find_objects_to_delete_not_replicated_in_dst,
    make_id_object_map,
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
