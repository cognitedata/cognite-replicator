import logging
from typing import Dict, List, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes.raw import Database, Row, Table

from . import replication


def get_not_created_names(
    src_objects: List[Union[Database, Table]], dst_objects: List[Union[Database, Table]]
) -> Tuple[List[str], List[str]]:
    """
    Creates a list of all the source object names and a list of source object names that do not exist in destination.
    Args:
        src_objects: A list of all the source objects, either databases or tables.
        dst_objects: A list of all the destination objects, either databases or tables.

    Returns:
        src_names: A list of all the source object (database or table) names.
        not_created: A list of all the source object (database or table) names that do not exist in destination.

    """

    src_names = [obj.name for obj in src_objects]
    dst_names = [obj.name for obj in dst_objects]

    not_created = [obj_name for obj_name in src_names if obj_name not in dst_names]

    return src_names, not_created


def create_databases_tables(client_src: CogniteClient, client_dst: CogniteClient) -> Dict[str, List[str]]:
    """
    Creates the databases and tables in the destination and makes a dictionary with the database name as the key and
    a list of the table names as the value.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.

    Returns:
        A dictionary mapping database names to a list of it's table names.

    """
    db_list_src = client_src.raw.databases.list(limit=None)
    db_list_dst = client_dst.raw.databases.list(limit=None)

    db_names_src, db_not_created = get_not_created_names(db_list_src, db_list_dst)

    db_create = client_dst.raw.databases.create(name=db_not_created)

    assert len(db_create) == len(db_not_created)

    db_tb = {}
    for db_name in db_names_src:
        tb_list_src = client_src.raw.tables.list(db_name=db_name, limit=None)
        tb_list_dst = client_dst.raw.tables.list(db_name=db_name, limit=None)

        tb_names_src, tb_not_created = get_not_created_names(tb_list_src, tb_list_dst)

        tb_create = client_dst.raw.tables.create(db_name=db_name, name=tb_not_created)

        assert len(tb_create) == len(tb_not_created)

        db_tb[db_name] = tb_names_src

    return db_tb


def insert_rows(rows: List[Row], db_name: str, tb_name: str, client: CogniteClient):
    """
    Inserts the input rows from database and table from the source to the destination.

    Args:
        rows:  A list of all the rows that need to be copied over.
        db_name: The name of the database that rows are copied from.
        tb_name: The name of the table that rows are copied from.
        client: The client corresponding to the destination project.

    Returns:
        The output of insert.

    """
    return client.raw.rows.insert(db_name=db_name, table_name=tb_name, row=rows)


def copy_rows(client_src: CogniteClient, client_dst: CogniteClient, db_tb_dict: Dict[str, List[str]], chunk_size: int):
    """
    Goes through the databases and their tables in order to fetch the rows from source and post them to
    destination in chunks.

    Args:
      client_src: The client corresponding to the source project.
      client_dst: The client corresponding to the destination project.
      db_tb_dict: A dictionary over the databases and tables within the databases.
      chunk_size: The size of the chunks to fetch and post rows in.

    """
    for db_name, tb_list in db_tb_dict.items():
        for tb_name in tb_list:
            for row_list in client_src.raw.rows(db_name=db_name, table_name=tb_name, chunk_size=chunk_size):
                new_rows = {row.key: row.columns for row in row_list}
                kwargs = {"db_name": db_name, "tb_name": tb_name, "client": client_dst}
                replication.retry(insert_rows, new_rows, **kwargs)
            logging.info(f"Rows are added from database {db_name} and table {tb_name}")


def replicate(client_src: CogniteClient, client_dst: CogniteClient, chunk_size: int):
    """
    Replicates all the raw from the source project into the destination project.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        chunk_size: The biggest chunk size to fetch and post in.

    """
    project_src = client_src.config.project
    project_dst = client_dst.config.project

    logging.info(f"Starting to copy raw from source ({project_src}) to destination ({project_dst}).")

    db_tb = create_databases_tables(client_src, client_dst)
    logging.info(
        f"There are {len(db_tb)} databases with a total of {sum([len(db_tb[x]) for x in db_tb])} "
        f"tables to be copied to the destination"
    )

    copy_rows(client_src, client_dst, db_tb, chunk_size)

    logging.info(f"Finished copying raw from source ({project_src}) to destination ({project_dst}).")
