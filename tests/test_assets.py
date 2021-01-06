import time

from cognite.client import CogniteClient
from cognite.client.data_classes.assets import Asset
from cognite.client.testing import monkeypatch_cognite_client
from cognite.replicator.assets import (
    build_asset_create,
    build_asset_update,
    create_hierarchy,
    find_children,
    unlink_subtree_parents,
)


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


def test_build_asset_update():
    assets_src = [
        Asset(id=3, name="Dog", external_id="Woff Woff", metadata={}, description="Humans best friend"),
        Asset(id=7, name="Cat", parent_id=3, external_id="Miau Miau", metadata={}),
        Asset(id=5, name="Cow", parent_id=7, metadata={}),
    ]
    assets_dst = [
        Asset(id=333, name="Copy-Dog", metadata={"_replicatedInternalId": 3}),
        Asset(id=777, name="Copy-Cat", parent_id=333, metadata={"_replicatedInternalId": 7}),
        Asset(id=555, name="Copy-Cow", parent_id=777, metadata={"_replicatedInternalId": 5, "source": "None"}),
    ]
    runtime = time.time() * 1000
    id_mapping = {3: 333, 7: 777, 5: 555}
    dst_asset_0 = build_asset_update(assets_src[0], assets_dst[0], id_mapping, "Flying Circus", runtime, depth=0)
    dst_asset_1 = build_asset_update(assets_src[1], assets_dst[1], id_mapping, "Flying Circus", runtime, depth=1)
    dst_asset_2 = build_asset_update(assets_src[2], assets_dst[2], id_mapping, "Flying Circus", runtime, depth=1)
    assert dst_asset_0.metadata["_replicatedSource"] == "Flying Circus"
    assert dst_asset_1.metadata["_replicatedSource"] == "Flying Circus"
    assert dst_asset_2.metadata["_replicatedSource"] == "Flying Circus"
    assert dst_asset_0.metadata["_replicatedInternalId"] == assets_src[0].id
    assert dst_asset_1.metadata["_replicatedInternalId"] == assets_src[1].id
    assert dst_asset_2.metadata["_replicatedInternalId"] == assets_src[2].id
    assert dst_asset_0.description == assets_src[0].description
    assert dst_asset_1.parent_id == 333
    assert dst_asset_2.parent_id == 777

    assets_src[2].parent_id = 3
    dst_asset_changed_2 = build_asset_update(
        assets_src[2], assets_dst[2], id_mapping, "Flying Circus", runtime, depth=1
    )
    assert dst_asset_changed_2.parent_id == 333


def test_create_hierarchy_without_dst_list():
    with monkeypatch_cognite_client() as client:
        runtime = time.time() * 1000
        assets_src = [
            Asset(
                id=3, name="Queen", external_id="Queen in the Kingdom", metadata={}, description="Married to the King"
            ),
            Asset(id=7, name="Prince", parent_id=3, external_id="Future King", metadata={}),
            Asset(id=5, name="Princess", parent_id=3, metadata={}),
        ]
        # src_empty_dst_ids = create_hierarchy(assets_src, [], "Evens Kingdom", runtime, client)


def test_create_hierarchy_with_dst_list():
    assets_src = [
        Asset(id=3, name="Queen", external_id="Queen in the Kingdom", metadata={}, description="Married to the King"),
        Asset(id=7, name="Prince", parent_id=3, external_id="Future King", metadata={}),
        Asset(id=5, name="Princess", parent_id=3, metadata={}),
    ]

    assets_dst = [
        Asset(
            id=333,
            name="Copy-Queen",
            external_id="Queen in the Kingdom",
            metadata={"_replicatedInternalId": 3, "_replicatedTime": 1},
            description="Married to the King",
        ),
        Asset(
            id=777,
            name="Copy-Prince",
            external_id="Future King",
            metadata={"_replicatedInternalId": 7, "_replicatedTime": 1},
        ),
        Asset(id=555, name="Copy-Princess", metadata={"_replicatedInternalId": 5, "_replicatedTime": 1}),
        Asset(id=101, name="Adopted", metadata={}),
    ]


def test_unlink_subtree_parents():
    assets = [
        Asset(id=1),
        Asset(id=2, parent_id=1),
        Asset(external_id="ext_1"),
        Asset(external_id="ext_2", parent_external_id="ext_1", metadata={}),
    ]
    original_parent_ids = [asset.parent_id for asset in assets]
    original_parent_external_ids = [asset.parent_external_id for asset in assets]
    subtree_root_ids = [2]
    subtree_root_external_ids = ["ext_2"]
    unlink_subtree_parents(assets, subtree_ids=subtree_root_ids, subtree_external_ids=subtree_root_external_ids)
    for i, asset in enumerate(assets):
        if asset.id in subtree_root_ids or asset.external_id in subtree_root_external_ids:
            assert asset.parent_id is None
            assert asset.parent_external_id is None
            assert asset.metadata.get("_replicatedOriginalParentId") == original_parent_ids[i]
            assert asset.metadata.get("_replicatedOriginalParentExternalId") == original_parent_external_ids[i]
        else:
            assert asset.parent_id == original_parent_ids[i]
            assert asset.parent_external_id == original_parent_external_ids[i]
