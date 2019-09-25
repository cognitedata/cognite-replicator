from cognite.replicator import datapoints


def test_get_chunk():
    """Should split the array [0..17] as follows:

    [(0, [0, 1]),
     (1, [2, 3]),
     (2, [4, 5]),
     (3, [6, 7]),
     (4, [8, 9]),
     (5, [10, 11]),
     (6, [12, 13]),
     (7, [14, 15]),
     (8, [16]),
     (9, [17])
     ]

     """
    full_list = list(range(18))
    num_batches = 10
    sample_arg_list = [(i, datapoints._get_chunk(full_list, num_batches, i)) for i in range(num_batches)]
    last_val = -1
    assert len(sample_arg_list) == num_batches
    for i, arg in enumerate(sample_arg_list):
        if i < 8:
            assert len(arg[1]) == 2
        else:
            assert len(arg[1]) == 1

        for val in arg[1]:
            assert val == last_val + 1
            last_val = val
