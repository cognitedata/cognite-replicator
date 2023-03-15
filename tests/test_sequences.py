from cognite.client.data_classes import Sequence
from cognite.client.testing import monkeypatch_cognite_client
from cognite.replicator.sequences import create_sequence, update_sequence


def test_create_sequence():
    client = monkeypatch_cognite_client()
    src_sequences = [
        Sequence(metadata={}, id=1007, asset_id=3),
        Sequence(metadata={"arbitrary_1": 12, "arbitrary_2": "some text"}, id=2007, asset_id=7),
        Sequence(
            id=3007,
            columns=[
                {"valueType": "STRING", "externalId": "user", "description": "some description"},
                {"valueType": "DOUBLE", "externalId": "amount"},
            ],
            metadata={},
            asset_id=5,
        ),
    ]
    runtime = 10000000
    id_mapping = {3: 333, 7: 777, 5: 555}
    for i, src_sequence in enumerate(src_sequences):
        created_sequence = create_sequence(
            src_sequence, id_mapping, f"source project name {i}", runtime, client, client, {}, {}
        )

        assert created_sequence.metadata["_replicatedInternalId"] == src_sequences[i].id
        assert created_sequence.metadata["_replicatedSource"] == f"source project name {i}"
        if src_sequences[i].metadata:
            assert len(created_sequence.metadata.keys()) == len(src_sequences[i].metadata.keys()) + 3
            for key in src_sequences[i].metadata.keys():
                assert key in created_sequence.metadata.keys()
                assert src_sequences[i].metadata[key] == created_sequence.metadata[key]
        assert created_sequence.columns == src_sequences[i].columns
        assert created_sequence.asset_id == id_mapping[src_sequences[i].asset_id]


def test_update_sequence():
    client = monkeypatch_cognite_client()
    src_sequences = [
        Sequence(metadata={}, id=1007, asset_id=3),
        Sequence(metadata={}, id=2007, asset_id=7),
        Sequence(asset_id=5),
    ]
    dst_sequences = [
        Sequence(asset_id=333, metadata={}),
        Sequence(asset_id=777, metadata={"_replicatedInternalId": 7}),
        Sequence(asset_id=555, metadata={"source": "None"}),
    ]
    runtime = 10000000
    id_mapping = {3: 333, 7: 777, 5: 555}

    for i in range(len(src_sequences)):
        updated_sequence = update_sequence(
            src_sequences[i], dst_sequences[i], id_mapping, "source project name", runtime, client, client, {}, {}
        )

        assert updated_sequence.metadata["_replicatedInternalId"] == src_sequences[i].id
        assert updated_sequence.metadata["_replicatedSource"] == "source project name"
        if src_sequences[i].metadata:
            assert len(updated_sequence.metadata.keys()) == len(src_sequences[i].metadata.keys()) + 3
            for key in src_sequences[i].metadata.keys():
                assert key in updated_sequence.metadata.keys()
                assert src_sequences[i].metadata[key] == updated_sequence.metadata[key]
        assert updated_sequence.asset_id == id_mapping[src_sequences[i].asset_id]
