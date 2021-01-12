import time

from cognite.client.data_classes import FileMetadata

from cognite.replicator.files import create_file, update_file


def test_create_file():
    files_src = [
        FileMetadata(metadata={}, id=1007, asset_ids=[3], mime_type="pdf"),
        FileMetadata(metadata={}, id=2007, asset_ids=[7], mime_type="application/pdf"),
        FileMetadata(metadata={}, asset_ids=[5]),
        FileMetadata(),
    ]
    runtime = int(time.time() * 1000)
    id_mapping = {3: 333, 7: 777, 5: 555}
    created_files = []

    for i, event in enumerate(files_src):
        created_files.append(create_file(event, id_mapping, "source project name", runtime))

    assert len(created_files) == len(files_src)
    for i, file_obj in enumerate(created_files):
        assert file_obj.metadata["_replicatedInternalId"] == files_src[i].id
        assert file_obj.metadata["_replicatedSource"] == "source project name"
        assert (file_obj.asset_ids is None) == (files_src[i].asset_ids is None)
        if file_obj.asset_ids is not None:
            assert len(file_obj.asset_ids) == len(files_src[i].asset_ids)
            for asset_id in files_src[i].asset_ids:
                assert id_mapping[asset_id] in file_obj.asset_ids

    assert created_files[0].mime_type == "application/pdf"
    assert created_files[1].mime_type == "application/pdf"
    assert created_files[2].mime_type is None


def test_update_file():
    files_src = [
        FileMetadata(metadata={}, id=1007, asset_ids=[3], mime_type="pdf"),
        FileMetadata(metadata={}, id=2007, asset_ids=[7], mime_type="application/pdf"),
        FileMetadata(metadata={}, asset_ids=[5]),
    ]
    files_dst = [
        FileMetadata(metadata={}, id=1007, asset_ids=[3], mime_type="tiff"),
        FileMetadata(metadata={}, id=2007, mime_type="jpg"),
        FileMetadata(metadata={"dummy": True}, asset_ids=[3]),
    ]
    runtime = time.time() * 1000
    id_mapping = {3: 333, 7: 777, 5: 555}

    updated_files = [
        update_file(files_src[i], files_dst[i], id_mapping, "source project name", int(runtime))
        for i in range(len(files_src))
    ]

    assert len(updated_files) == len(files_src)

    for i, file_obj in enumerate(updated_files):
        assert file_obj.metadata["_replicatedInternalId"] == files_src[i].id
        assert file_obj.metadata["_replicatedSource"] == "source project name"
        assert (file_obj.asset_ids is None) == (files_src[i].asset_ids is None)
        if file_obj.asset_ids is not None:
            assert len(file_obj.asset_ids) == len(files_src[i].asset_ids)
            for asset_id in files_src[i].asset_ids:
                assert id_mapping[asset_id] in file_obj.asset_ids
