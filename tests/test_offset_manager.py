import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "asr-worker"))

from offset_manager import ProcessingStatus, RecordResult, compute_safe_commit_offsets  # noqa: E402


def test_empty_batch_returns_no_commit():
    assert compute_safe_commit_offsets([]) == {}


def test_all_success_commits_last_offset_plus_one():
    results = [
        RecordResult(partition=0, offset=10, status=ProcessingStatus.SUCCESS),
        RecordResult(partition=0, offset=11, status=ProcessingStatus.SUCCESS),
        RecordResult(partition=0, offset=12, status=ProcessingStatus.SUCCESS),
    ]
    assert compute_safe_commit_offsets(results) == {0: 13}


def test_all_failed_dlq_still_commits_since_messages_are_handled():
    # Tous en echec mais route vers la DLQ -> "traites", donc l'offset avance quand meme
    results = [
        RecordResult(partition=0, offset=10, status=ProcessingStatus.FAILED_DLQ),
        RecordResult(partition=0, offset=11, status=ProcessingStatus.FAILED_DLQ),
    ]
    assert compute_safe_commit_offsets(results) == {0: 12}


def test_pending_message_blocks_commit_of_later_offsets():
    # offset 11 est PENDING -> on ne peut committer que jusqu'a offset 10 (exclu le pending)
    results = [
        RecordResult(partition=0, offset=10, status=ProcessingStatus.SUCCESS),
        RecordResult(partition=0, offset=11, status=ProcessingStatus.PENDING),
        RecordResult(partition=0, offset=12, status=ProcessingStatus.SUCCESS),
    ]
    assert compute_safe_commit_offsets(results) == {0: 11}


def test_pending_as_first_message_blocks_all_commits_for_partition():
    results = [
        RecordResult(partition=0, offset=10, status=ProcessingStatus.PENDING),
        RecordResult(partition=0, offset=11, status=ProcessingStatus.SUCCESS),
    ]
    assert compute_safe_commit_offsets(results) == {}


def test_multiple_partitions_are_independent():
    results = [
        RecordResult(partition=0, offset=5, status=ProcessingStatus.SUCCESS),
        RecordResult(partition=1, offset=20, status=ProcessingStatus.PENDING),
        RecordResult(partition=1, offset=21, status=ProcessingStatus.SUCCESS),
    ]
    assert compute_safe_commit_offsets(results) == {0: 6}


def test_unordered_input_is_sorted_by_offset():
    results = [
        RecordResult(partition=0, offset=12, status=ProcessingStatus.SUCCESS),
        RecordResult(partition=0, offset=10, status=ProcessingStatus.SUCCESS),
        RecordResult(partition=0, offset=11, status=ProcessingStatus.SUCCESS),
    ]
    assert compute_safe_commit_offsets(results) == {0: 13}
