import pytest

from cognite.client.data_classes.assets import Asset
from cognite.replicator.replication import make_id_object_map


def test_make_id_object_map():
    assets = [Asset(id=3, metadata={"_replicatedInternalId": 55}), Asset(id=2)]
    map = make_id_object_map(assets)
    assert 1 == len(map)
    assert 3 == map[55].id


if __name__ == "__main__":
    pytest.main()
