import logging
import multiprocessing as mp
import re
from datetime import datetime
from math import ceil, floor
from typing import Any, List, Optional, Tuple

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


def _get_chunk(lst: List[Any], num_chunks: int, chunk_number: int) -> List[Any]:
    """Returns a slice of the given list such that all slices are as even in size as possible.

    Args:
        lst: The list to slice
        num_chunks: The amount of chunks that the list should be split into
        chunk_number: Which chunk of the lst to return (0-indexed)

    Returns:
        The chunk_number-th chunk of lst such that the concat of all num_chunks chunks is equivalent to the full lst,
        and each chunk has equal size +-1
    """
    chunk_size = len(lst) // num_chunks
    num_excess_elements = len(lst) % num_chunks

    start_index = chunk_number * chunk_size
    start_index += min(chunk_number, num_excess_elements)  # offset by amount of excess elements used in previous chunks

    end_index = start_index + chunk_size
    if chunk_number < num_excess_elements:  # if we need to include an extra element
        end_index += 1

    return lst[start_index:end_index]


def replicate_sequence_rows(
    client_src: CogniteClient, client_dst: CogniteClient, seq_external_id: str, mock_run: bool = False, job_id: int = 1
) -> Tuple[bool, int]:
    """
    Copies sequence rows from the source tenant into the destination project, for the given sequences.

    If sequence rows already exist in the destination for the sequence, ???

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        seq_external_id: The external id of the sequences to replicate sequence rows for
        mock_run: If true, only retrieves sequence rows from source and does not insert into destination
        job_id: The batch number being processed

    Returns:
        A tuple of the success status (True if no failures) and the number of sequence rows successfully replicated
    """
    try:
        latest_src_seq_rows = client_src.sequences.data.retrieve(
            external_id=seq_external_id, start=0, end=None, limit=None
        )
    except CogniteAPIError as exc:
        logging.error(f"Job {job_id}: Failed for external id {seq_external_id}. {exc}")
        return False, 0

    if not latest_src_seq_rows:
        return True, 0

    seq_len = len(latest_src_seq_rows)
    logging.debug(f"Job {job_id}: Ext_id: {seq_external_id} Replicating {seq_len} sequence rows.")
    if not mock_run:
        client_dst.sequences.data.insert(latest_src_seq_rows, external_id=seq_external_id, column_external_ids=None)

    logging.debug(f"Job {job_id}: Ext_id: {seq_external_id} Number of sequence rows: {seq_len}")
    return True, seq_len


def batch_replicate(
    client_src: CogniteClient, client_dst: CogniteClient, job_id: int, ext_ids: List[str], mock_run: bool = False
):
    """
    Replicates sequence rows for each sequences specified by the external id list.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        job_id: The batch number being processed
        ext_ids: The list of external ids for sequences to copy over
        mock_run: If true, only retrieves sequences from source and does not insert into destination
    """

    def log_status(total_seq_count):
        logging.info(
            f"Job {job_id}: Current results: {updated_sequences_count} sequences updated, "
            f"{total_seq_count - updated_sequences_count - len(failed_external_ids)} "
            f"sequences up-to-date. {total_sequence_rows_copied} sequence rows copied. "
            f"{len(failed_external_ids)} failure(s)."
        )

    logging.info(f"Job {job_id}: Starting sequence row replication for {len(ext_ids)} sequences...")
    updated_sequences_count = 0
    total_sequence_rows_copied = 0
    failed_external_ids = []
    start_time = datetime.now()

    for i, ext_id in enumerate(ext_ids):
        if i % ceil(len(ext_ids) / 10) == 0:
            logging.info(
                f"Job {job_id}: Progress: On sequences {i+1}/{len(ext_ids)} "
                f"({floor(100 * i / len(ext_ids))}% complete) in {datetime.now()-start_time}"
            )
            log_status(i)

        success_status, sequence_rows_copied_count = replicate_sequence_rows(
            client_src, client_dst, ext_id, mock_run=mock_run, job_id=job_id
        )

        if not success_status:
            failed_external_ids.append(ext_id)
        else:
            updated_sequences_count += 1
        total_sequence_rows_copied += sequence_rows_copied_count

    log_status(len(ext_ids))

    logging.info(f"Total elapsed time: {datetime.now() - start_time}")
    logging.info(f"Job {job_id}: Sample of failed ids: {failed_external_ids[:10]}")


def replicate(
    client_src: CogniteClient,
    client_dst: CogniteClient,
    batch_size: Optional[int] = None,
    num_threads: int = 10,
    external_ids: Optional[List[str]] = None,
    mock_run: bool = False,
    exclude_pattern: str = None,
):
    """
    Replicates sequence rows from the source project into the destination project for all sequences that
    exist in both environments.

    Args:
        client_src: The client corresponding to the source project.
        client_dst: The client corresponding to the destination project.
        batch_size: The size of batches to split the external id list into. Defaults to num_threads.
        num_threads: The number of threads to be used.
        external_ids: A list of sequence external ids to replicate sequences for
        mock_run: If true, runs the replication without insert, printing what would happen
        exclude_pattern: Regex pattern; sequences whose names match will not be replicated from
    """

    if external_ids and exclude_pattern:
        raise ValueError(
            f"List of sequence external ids AND a regex exclusion rule was given! Either remove the filter {exclude_pattern} or the list of sequences {external_ids}"
        )
    elif external_ids is not None:  # Specified list of sequences is given
        seq_src = client_src.sequences.retrieve_multiple(external_ids=external_ids)
        seq_dst = client_dst.sequences.retrieve_multiple(external_ids=external_ids)
        src_ext_id_list = [seq_obj.external_id for seq_obj in seq_src]
    else:
        seq_src = client_src.sequences.list(limit=None)
        seq_dst = client_dst.sequences.list(limit=None)
        filtered_seq_src = []
        skipped_seq = []
        if exclude_pattern:  # Filtering based on regex rule given
            compiled_re = re.compile(exclude_pattern)
            for seq in seq_src:
                if seq.external_id is not None:
                    if compiled_re.search(seq.external_id):
                        skipped_seq.append(seq.external_id)
                    else:
                        filtered_seq_src.append(seq.external_id)
            src_ext_id_list = filtered_seq_src
            logging.info(
                f"Excluding sequence rows from {len(skipped_seq)} sequences, due to regex rule: {exclude_pattern}. Sample: {skipped_seq[:5]}"
            )
            # Should probably change to logging.debug after a while
        else:  # Expects to replicate all shared sequences
            src_ext_id_list = [seq_obj.external_id for seq_obj in seq_src]
    logging.info(f"Number of sequences in source: {len(seq_src)}")
    logging.info(f"Number of sequences in destination: {len(seq_dst)}")

    dst_ext_id_list = set([seq_obj.external_id for seq_obj in seq_dst])
    shared_external_ids = [ext_id for ext_id in src_ext_id_list if ext_id in dst_ext_id_list and ext_id]
    logging.info(f"Number of common sequences external ids between destination and source: {len(shared_external_ids)}")

    if batch_size is None:
        batch_size = ceil(len(shared_external_ids) / num_threads)
    num_batches = ceil(len(shared_external_ids) / batch_size)

    logging.info(f"{num_batches} batches of size {batch_size}")
    arg_list = [
        (client_src, client_dst, job_id, _get_chunk(shared_external_ids, num_batches, job_id), mock_run)
        for job_id in range(num_batches)
    ]

    if num_threads > 1:
        with mp.Pool(num_threads) as pool:
            pool.starmap(batch_replicate, arg_list)
    else:
        batch_replicate(*arg_list[0])
