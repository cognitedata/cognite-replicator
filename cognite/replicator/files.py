import logging
import mimetypes
import time
from typing import Dict, List

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.exceptions import CogniteAPIError

from . import replication


def create_file(
    src_file: FileMetadata, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> FileMetadata:
    """
    Makes a new copy of the file to be replicated based on a source file.

    Args:
        src_file: The file from the source to be replicated to destination.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The replicated file to be created in the destination.
    """
    logging.debug(f"Creating a new file based on source file id {src_file.id}")
    mime_type = mimetypes.types_map.get(f".{src_file.mime_type}", src_file.mime_type)

    return FileMetadata(
        external_id=src_file.external_id,
        name=src_file.name,
        source=src_file.source,
        mime_type=mime_type,
        metadata=replication.new_metadata(src_file, project_src, runtime),
        asset_ids=replication.get_asset_ids(src_file.asset_ids, src_dst_ids_assets),
    )


def update_file(
    src_file: FileMetadata, dst_file: FileMetadata, src_dst_ids_assets: Dict[int, int], project_src: str, runtime: int
) -> FileMetadata:
    """
    Makes an updated version of the destination file based on the corresponding source file.

    Args:
        src_file: The file from the source to be replicated.
        dst_file: The file from the destination that needs to be updated to reflect changes made to its source file.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.

    Returns:
        The updated file object for the replication destination.
    """
    logging.debug(f"Updating existing file {dst_file.id} based on source file id {src_file.id}")

    dst_file.external_id = src_file.external_id
    dst_file.source = src_file.source
    dst_file.metadata = replication.new_metadata(src_file, project_src, runtime)
    dst_file.asset_ids = replication.get_asset_ids(src_file.asset_ids, src_dst_ids_assets)
    dst_file.source_created_time = src_file.source_created_time
    dst_file.source_modified_time = src_file.source_modified_time
    return dst_file


def copy_files(
    src_files: List[FileMetadata],
    src_id_dst_file: Dict[int, FileMetadata],
    src_dst_ids_assets: Dict[int, int],
    project_src: str,
    runtime: int,
    client: CogniteClient,
):
    """
    Creates/updates file objects and then attempts to create and update these objects in the destination.

    Args:
        src_files: A list of the files that are in the source.
        src_id_dst_file:  A dictionary of a files source id to it's matching destination object.
        src_dst_ids_assets: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        client: The client corresponding to the destination project.

    """
    logging.debug(f"Starting to replicate {len(src_files)} files.")

    create_files, update_files, unchanged_files = replication.make_objects_batch(
        src_files, src_id_dst_file, src_dst_ids_assets, create_file, update_file, project_src, runtime
    )

    logging.info(f"Creating {len(create_files)} new files and updating {len(update_files)} existing files.")

    create_urls = []
    if create_files:
        logging.debug(f"Attempting to create {len(create_files)} files.")
        for file in create_files:
            response = None
            try:
                response = replication.retry(client.files.create, file)
            except CogniteAPIError as exc:
                logging.error(f"Failed to create file {file.name}. {exc}")
                if "Invalid MIME type" in exc.message:
                    file.mime_type = None
                    response = replication.retry(client.files.create, file)

            if response:
                create_urls.append(response)
        logging.debug(f"Successfully created {len(create_urls)} files.")

    if update_files:
        logging.debug(f"Attempting to update {len(update_files)} files.")
        update_files = replication.retry(client.files.update, update_files)
        logging.debug(f"Successfully updated {len(update_files)} files.")

    logging.info(f"Created {len(create_urls)} new files and updated {len(update_files)} existing files.")


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    batch_size: int = 10000,
    num_threads: int = 1,
    delete_replicated_if_not_in_src: bool = False,
    delete_not_replicated_in_dst: bool = False,
    skip_unlinkable: bool = False,
    skip_nonasset: bool = False,
):
    """
    Replicates all the files from the source project into the destination project.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        batch_size: The biggest batch size to post chunks in.
        num_threads: The number of threads to be used.
        delete_replicated_if_not_in_src: If True, will delete replicated files that are in the destination,
        but no longer in the source project (Default=False).
        delete_not_replicated_in_dst: If True, will delete files from the destination if they were not replicated
        from the source (Default=False).
        skip_unlinkable: If no assets exist in the destination for a file, do not replicate it
        skip_nonasset: If a file has no associated assets, do not replicate it
    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    files_src = client_src.files.list(limit=None)
    files_dst = client_dst.files.list(limit=None)
    logging.info(f"There are {len(files_src)} existing files in source ({project_src}).")
    logging.info(f"There are {len(files_dst)} existing files in destination ({project_dst}).")

    src_id_dst_file = replication.make_id_object_map(files_dst)

    assets_dst = client_dst.assets.list(limit=None)
    src_dst_ids_assets = replication.existing_mapping(*assets_dst)
    logging.info(
        f"If a files asset ids is one of the {len(src_dst_ids_assets)} assets "
        f"that have been replicated then it will be linked."
    )

    if skip_unlinkable or skip_nonasset:
        pre_filter_length = len(files_src)
        files_src = replication.filter_objects(files_src, src_dst_ids_assets, skip_unlinkable, skip_nonasset)
        logging.info(f"Filtered out {pre_filter_length - len(files_src)} files. {len(files_src)} files remain.")

    replicated_runtime = int(time.time()) * 1000
    logging.info(f"These copied/updated files will have a replicated run time of: {replicated_runtime}.")

    logging.info(
        f"Starting to copy and update {len(files_src)} files from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if len(files_src) > batch_size:
        replication.thread(
            num_threads=num_threads,
            copy=copy_files,
            src_objects=files_src,
            src_id_dst_obj=src_id_dst_file,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            replicated_runtime=replicated_runtime,
            client=client_dst,
        )
    else:
        copy_files(
            src_files=files_src,
            src_id_dst_file=src_id_dst_file,
            src_dst_ids_assets=src_dst_ids_assets,
            project_src=project_src,
            runtime=replicated_runtime,
            client=client_dst,
        )

    logging.info(
        f"Finished copying and updating {len(files_src)} files from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if delete_replicated_if_not_in_src:
        ids_to_delete = replication.find_objects_to_delete_if_not_in_src(files_src, files_dst)
        client_dst.files.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} files in destination ({project_dst})"
            f" because they were no longer in source ({project_src})   "
        )
    if delete_not_replicated_in_dst:
        ids_to_delete = replication.find_objects_to_delete_not_replicated_in_dst(files_dst)
        client_dst.files.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} files in destination ({project_dst}) because"
            f"they were not replicated from source ({project_src})   "
        )
