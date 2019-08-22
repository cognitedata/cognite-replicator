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
