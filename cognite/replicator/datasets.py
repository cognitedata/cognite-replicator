from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet


def replicate(
    src_client: CogniteClient, dst_client: CogniteClient, src_dataset_id: int, src_dst_dataset_mapping: dict[int, int]
):
    def get_dst_dataset_by_name_or_create(src_dataset: DataSet):
        try:
            dst_datasets = dst_client.data_sets.list(limit=None)
            dst_dataset = list(filter(lambda x: x.name == src_dataset, dst_datasets))[0]
            dst_dataset_id = dst_dataset.id
        except IndexError:
            dst_dataset = dst_client.data_sets.create(
                DataSet(
                    external_id=src_dataset.external_id,
                    name=src_dataset.name,
                    description=src_dataset.description,
                    metadata=src_dataset.metadata,
                    write_protected=src_dataset.write_protected,
                )
            )
            dst_dataset_id = dst_dataset.id
        return dst_dataset_id

    if src_dataset_id:
        try:
            dst_dataset_id = src_dst_dataset_mapping[src_dataset_id]
        except KeyError:
            src_dataset = src_client.data_sets.retrieve(id=src_dataset_id)
            if src_dataset.external_id:
                dst_dataset = dst_client.data_sets.retrieve(external_id=src_dataset.external_id)
                if dst_dataset:
                    dst_dataset_id = dst_dataset.id
                else:
                    dst_dataset_id = get_dst_dataset_by_name_or_create(src_dataset)
            else:
                dst_dataset_id = get_dst_dataset_by_name_or_create(src_dataset)

            src_dst_dataset_mapping[src_dataset_id] = dst_dataset_id
    else:
        dst_dataset_id = None

    return dst_dataset_id
