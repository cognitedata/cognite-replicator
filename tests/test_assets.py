import time

from cognite.client.data_classes.assets import Asset
from cognite.replicator.assets import build_asset_create
from cognite.replicator.assets import find_children


def test_build_asset_create():
    asset = Asset(id=3, name="holy grenade", metadata={})
    runtime = time.time() * 1000
    created = build_asset_create(asset, {}, "source_tenant", runtime, 0)
    assert "holy grenade" == created.name
    assert created.parent_id is None
    assert created.metadata
    assert "source_tenant" == created.metadata["_replicatedSource"]
    assert 3 == created.metadata["_replicatedInternalId"]

    asset = Asset(id=4, parent_id=2, name="holy grail", metadata={"_replicatedInternalId": 55})
    src_id_dst_map = {2: 5}
    second = build_asset_create(asset, src_id_dst_map, "source_tenant", runtime, 2)
    assert 5 == second.parent_id


def test_find_children():
    assets = [
        Asset(id=3, name="holy grenade", metadata={}),
        Asset(id=7, name="not holy grenade", parent_id=3, metadata={}),
        Asset(id=5, name="in-holy grenade", parent_id=7, metadata={"source": "None"}),
    ]
    parents = find_children(assets, [None])
    children1 = find_children(assets, parents)
    children2 = find_children(assets, children1)
    assert parents[0].id == 3
    assert children1[0].id == 7
    assert children2[0].id == 5
    assert children1[0].parent_id == 3
    assert children2[0].parent_id == 7
