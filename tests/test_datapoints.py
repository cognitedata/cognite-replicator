from cognite.client.data_classes import Datapoints

from cognite.replicator import datapoints


def test_get_chunk():
    full_list = list(range(18))
    num_batches = 10
    sample_arg_list = [datapoints._get_chunk(full_list, num_batches, i) for i in range(num_batches)]
    assert sample_arg_list == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10, 11], [12, 13], [14, 15], [16], [17]]

    full_list = list(range(10))
    sample_arg_list = [datapoints._get_chunk(full_list, num_batches, i) for i in range(num_batches)]
    assert sample_arg_list == [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]]

    num_batches = 5
    sample_arg_list = [datapoints._get_chunk(full_list, num_batches, i) for i in range(num_batches)]
    assert sample_arg_list == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]

    full_list = [1]
    sample_arg_list = [datapoints._get_chunk(full_list, num_batches, i) for i in range(num_batches)]
    assert sample_arg_list == [[1], [], [], [], []]
