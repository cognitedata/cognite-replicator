from cognite.client.data_classes.raw import Database, Row, Table

from cognite.replicator.raw import get_not_created_names


def test_get_not_created_names():
    db_src = [Database("new creative name"), Database("another creative name"), Database("a boring name")]
    db_dst = [Database("new creative name"), Database("another creative name")]
    db_src_name, db_not_created_in_dst_name = get_not_created_names(db_src, db_dst)
    assert len(db_src) == len(db_src_name)
    assert len(db_not_created_in_dst_name) == 1
    assert db_not_created_in_dst_name[0] == "a boring name"
