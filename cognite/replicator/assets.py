import logging
import time
from typing import Dict, List, Optional, Union, cast

from cognite.client import CogniteClient
from cognite.client.data_classes.assets import Asset, AssetList

from . import replication


def build_asset_create(
    src_asset: Asset, src_id_dst_map: Dict[int, int], project_src: str, runtime: int, depth: int
) -> Asset:
    """
    Makes a new copy of the asset to be replicated based on the source asset.

    Args:
        src_asset: The asset from the source to be replicated to destination.
        src_id_dst_map: A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        depth: The depth of the asset within the asset hierarchy.

    Returns:
        The replicated asset to be created in the destination.

    """
    logging.debug(f"Creating a new asset based on source event id {src_asset.id}")

    return Asset(
        external_id=src_asset.external_id,
        name=src_asset.name,
        description=src_asset.description,
        metadata=replication.new_metadata(src_asset, project_src, runtime),
        source=src_asset.source,
        parent_id=src_id_dst_map[src_asset.parent_id] if depth > 0 else None,
    )


def build_asset_update(
    src_asset: Asset, dst_asset: Asset, src_id_dst_map: Dict[int, int], project_src: str, runtime: int, depth: int
) -> Asset:
    """
    Makes an updated version of the destination asset based on the corresponding source asset.

    Args:
        src_asset: The asset from the source to be replicated to destination.
        dst_asset: The asset from the destination that needs to be updated to reflect changes made to its source asset.
        src_id_dst_map: \\*\\*A dictionary of all the mappings of source asset id to destination asset id.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        depth: \\*\\*The depth of the asset within the asset hierarchy.
    \\*\\* only needed when hierarchy becomes mutable

    Returns:
        The updated asset object for the replication destination.

    """
    logging.debug(f"Updating existing event {dst_asset.id} based on source event id {src_asset.id}")

    dst_asset.external_id = src_asset.external_id
    dst_asset.name = src_asset.name
    dst_asset.description = src_asset.description
    dst_asset.metadata = replication.new_metadata(src_asset, project_src, runtime)
    dst_asset.source = src_asset.source
    dst_asset.parent_id = src_id_dst_map[src_asset.parent_id] if depth > 0 else None  # when asset hierarchy is mutable
    return dst_asset


def find_children(assets: List[Asset], parents: Union[List[None], List[Asset]]) -> List[Asset]:
    """
    Creates a list of all the assets that are children of the parent assets.

    Args:
        assets: A list of all the assets to search for children from.
        parents: A list of all the assets to find children for.

    Returns:
        A list of all the assets that are children to the parents.

    """
    parent_ids = {parent.id for parent in parents} if parents != [None] else parents
    return [asset for asset in assets if asset.parent_id in parent_ids]


def create_assets_replicated_id_validation(assets: List[Asset], function_create, function_list) -> List[Asset]:
    """
    Create assets and validate that was not already created.
    It takes more time to execute.
    Args:
        assets: A list of the assets to create.
        function_create: Instance of CogniteClient.assets.create.
        function_list: Instance of CogniteClient.assets.list
    """
    ret: List[Asset] = []
    ids_assets_already_created: List[str] = []
    assets_missing = []

    for asset in assets:
        assets_already_created = []

        if asset.metadata:
            assets_already_created = function_list(
                limit=1, metadata={"_replicatedInternalId": asset.metadata["_replicatedInternalId"]}
            )

        if len(assets_already_created) > 0:
            for item in assets_already_created:
                ret.append(item)
        else:
            assets_missing.append(asset)

    if len(assets_missing) > 0:
        asset_created = cast(AssetList, function_create(assets_missing))
        if len(asset_created) > 0:
            for item in asset_created:
                ret.append(item)

    return ret


def create_hierarchy(
    src_assets: List[Asset],
    dst_assets: List[Asset],
    project_src: str,
    runtime: int,
    client: CogniteClient,
    subtree_ids: Optional[List[int]] = None,
    subtree_external_ids: Optional[List[str]] = None,
    subtree_max_depth: Optional[int] = None,
):
    """
    Creates/updates the asset hierarchy in batches by depth, starting with the root assets and then moving on to the
    children of those roots, etc.

    Args:
        src_assets: A list of the assets that are in the source.
        dst_assets: A list of the assets that are in the destination.
        project_src: The name of the project the object is being replicated from.
        runtime: The timestamp to be used in the new replicated metadata.
        client: The client corresponding to the destination project.
        subtree_ids: The id of the subtree root to replicate,
        subtree_external_ids: The external id of the subtree root to replicate,
        subtree_max_depth: The maximum tree depth to replicate,
    """
    depth = 0
    parents = [None]  # root nodes parent id is None
    if subtree_ids is not None or subtree_external_ids is not None:
        unlink_subtree_parents(src_assets, subtree_ids, subtree_external_ids)
    children = find_children(src_assets, parents)

    src_dst_ids: Dict[int, int] = {}
    src_id_dst_asset = replication.make_id_object_map(dst_assets)

    while children:
        logging.info(f"Starting depth {depth}, with {len(children)} assets.")
        create_assets, update_assets, unchanged_assets = replication.make_objects_batch(
            children,
            src_id_dst_asset,
            src_dst_ids,
            build_asset_create,
            build_asset_update,
            project_src,
            runtime,
            depth=depth,
        )

        logging.info(f"Attempting to create {len(create_assets)} assets.")
        created_assets = replication.retry(
            create_assets_replicated_id_validation,
            create_assets,
            function_create=client.assets.create,
            function_list=client.assets.list,
        )

        logging.info(f"Attempting to update {len(update_assets)} assets.")
        updated_assets = replication.retry(client.assets.update, update_assets)

        src_dst_ids = replication.existing_mapping(*created_assets, *updated_assets, *unchanged_assets, ids=src_dst_ids)
        logging.debug(f"Dictionary of current asset mappings: {src_dst_ids}")

        num_assets = len(created_assets) + len(updated_assets)
        logging.info(
            f"Finished depth {depth}, updated {len(updated_assets)} and "
            f"posted {len(created_assets)} assets (total of {num_assets} assets)."
        )

        depth += 1
        if subtree_max_depth is not None and depth > subtree_max_depth:
            logging.info("Reached max depth")
            break
        children = find_children(src_assets, children)

    return src_dst_ids


def unlink_subtree_parents(
    src_assets: List[Asset], subtree_ids: Optional[List[int]] = None, subtree_external_ids: Optional[List[str]] = None
):
    """Sets the parentId of subtree roots to be None

    Args:
        src_assets: A list of the assets that are in the source.
        subtree_ids: The id(s) of the subtree root(s).
        subtree_external_ids: The external id(s) of the subtree root(s).
    """
    logging.info("Searching for subtree parent...")
    for asset in src_assets:
        if (subtree_ids is not None and asset.id in subtree_ids) or (
            subtree_external_ids is not None and asset.external_id in subtree_external_ids
        ):
            logging.info(f"Found the subtree root: {asset.id} with parent id: {asset.parent_id}")
            if asset.metadata is None:
                asset.metadata = {}

            if asset.parent_id:
                asset.metadata["_replicatedOriginalParentId"] = asset.parent_id
            if asset.parent_external_id:
                asset.metadata["_replicatedOriginalParentExternalId"] = asset.parent_external_id
            asset.parent_id = None
            asset.parent_external_id = None


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    delete_replicated_if_not_in_src: bool = False,
    delete_not_replicated_in_dst: bool = False,
    subtree_ids: Optional[Union[int, List[int]]] = None,
    subtree_external_ids: Optional[Union[str, List[str]]] = None,
    subtree_max_depth: Optional[int] = None,
):
    """
    Replicates all the assets from the source project into the destination project.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        delete_replicated_if_not_in_src: If True, will delete replicated assets that are in the destination,
        but no longer in the source project (Default=False).
        delete_not_replicated_in_dst: If True, will delete assets from the destination if they were not replicated
        from the source (Default=False).
        subtree_ids: The id or list of ids of subtree root to replicate,
        subtree_external_ids: The external id or list of external ids of the subtree root to replicate,
        subtree_max_depth: The maximum tree depth to replicate,
    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    if not subtree_ids and not subtree_external_ids:
        assets_src = client_src.assets.list(limit=None)
    else:
        logging.info(f"Loading subtree(s) with id(s) {subtree_ids} or external id(s) {subtree_external_ids}")
        assets_src = AssetList(resources=[])
        if subtree_ids:
            if not isinstance(subtree_ids, list):
                subtree_ids = [subtree_ids]
            for subtree_id in subtree_ids:
                assets_src += client_src.assets.retrieve_subtree(id=subtree_id, depth=subtree_max_depth)
        if subtree_external_ids:
            if not isinstance(subtree_external_ids, list):
                subtree_external_ids = [subtree_external_ids]
            for subtree_id in subtree_external_ids:
                assets_src += client_src.assets.retrieve_subtree(external_id=subtree_id, depth=subtree_max_depth)
    assets_dst = client_dst.assets.list(limit=None)

    logging.info(f"There are {len(assets_src)} existing assets in source ({project_src}).")
    logging.info(f"There are {len(assets_dst)} existing assets in destination ({project_dst}).")

    replicated_runtime = int(time.time()) * 1000
    logging.info(f"These copied/updated assets will have a replicated run time of: {replicated_runtime}.")

    logging.info(
        f"Starting to copy and update {len(assets_src)} assets from "
        f"source ({project_src}) to destination ({project_dst})."
    )
    src_dst_ids_assets = create_hierarchy(
        assets_src,
        assets_dst,
        project_src,
        replicated_runtime,
        client_dst,
        subtree_ids,
        subtree_external_ids,
        subtree_max_depth,
    )

    logging.info(
        f"Finished copying and updating {len(src_dst_ids_assets)} assets from "
        f"source ({project_src}) to destination ({project_dst})."
    )

    if delete_replicated_if_not_in_src:
        ids_to_delete = replication.find_objects_to_delete_if_not_in_src(assets_src, assets_dst)
        client_dst.assets.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} assets in destination ({project_dst})"
            f" because they were no longer in source ({project_src})   "
        )
    if delete_not_replicated_in_dst:
        ids_to_delete = replication.find_objects_to_delete_not_replicated_in_dst(assets_dst)
        client_dst.assets.delete(id=ids_to_delete)
        logging.info(
            f"Deleted {len(ids_to_delete)} assets in destination ({project_dst}) because"
            f"they were not replicated from source ({project_src})   "
        )
