"""Crash-recovery tests: kill -9 a worker mid-operation, then reopen the pool.

After every simulated crash the pool must satisfy:
  - fsck passes (no block owned by two volumes, no out-of-range maps);
  - pool usage equals the sum of per-volume usages and the real block count;
  - the interrupted operation is either fully absent or fully applied;
  - operations that returned success before the crash are preserved.
"""

import os
import subprocess
import sys

import pytest

from thinpool import BLOCK_SIZE as B, StoragePool, VolumeNotFoundError

from crash_worker import pattern

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_worker.py")


def run_worker(args: list) -> None:
    result = subprocess.run(
        [sys.executable, WORKER, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert result.returncode == 9, (
        f"worker should have been killed, rc={result.returncode}: "
        f"{result.stderr.decode()}"
    )


def reopen_and_check(paths, volumes):
    """Reopen the pool and verify global invariants plus per-volume content.

    volumes maps a volume name to the list of acceptable full-volume
    contents ("before" and "after" images).
    """
    pool = StoragePool.open(paths[0], paths[1])
    pool.fsck()
    total = 0
    for name, candidates in volumes.items():
        info = pool.volume_info(name)
        content = pool.read(name, 0, info.nominal_bytes)
        assert content in candidates, f"volume {name}: neither before nor after"
        assert info.used_bytes % B == 0
        total += info.used_bytes
    assert pool.pool_info().used_bytes == total
    return pool


# -- crash during an unaligned multi-block overwrite (pool full) -----------

OVERWRITE_LABELS = ["undo_committed", "inplace_written", "committed"]


@pytest.mark.parametrize("kill_label", OVERWRITE_LABELS)
def test_crash_during_inplace_overwrite(tmp_path, kill_label):
    paths = (str(tmp_path / "d"), str(tmp_path / "m"))
    pool = StoragePool.create(paths[0], paths[1], 2 * B, oversub_ratio=2.0)
    pool.create_volume("a", 2 * B)
    old = pattern(1, 2 * B)
    pool.write("a", 0, old)  # pool now physically full
    pool.close()

    offset, length = 100, 2 * B - 200  # unaligned, spans both blocks
    run_worker(["write", *paths, kill_label, "a", str(offset), "2", str(length)])

    after = bytearray(old)
    after[offset : offset + length] = pattern(2, length)
    pool = reopen_and_check(paths, {"a": [old, bytes(after)]})
    assert pool.pool_info().used_bytes == 2 * B  # still exactly full, no leak
    pool.close()


# -- crash during a fresh unaligned multi-block write (CoW path) -----------

COW_LABELS = ["cow_data_written", "cow_data_synced", "committed"]


@pytest.mark.parametrize("kill_label", COW_LABELS)
def test_crash_during_fresh_write(tmp_path, kill_label):
    paths = (str(tmp_path / "d"), str(tmp_path / "m"))
    pool = StoragePool.create(paths[0], paths[1], 8 * B)
    pool.create_volume("a", 4 * B)
    pool.close()

    offset, length = 100, 3 * B - 200  # unaligned, spans 3 blocks
    run_worker(["write", *paths, kill_label, "a", str(offset), "3", str(length)])

    before = bytes(4 * B)
    after = bytearray(before)
    after[offset : offset + length] = pattern(3, length)
    pool = reopen_and_check(paths, {"a": [before, bytes(after)]})
    assert pool.pool_info().used_bytes in (0, 3 * B)
    pool.close()


# -- crash during a mixed write (overwrite + new blocks) -------------------

MIXED_LABELS = [
    "cow_data_written",
    "cow_data_synced",
    "undo_committed",
    "inplace_written",
    "committed",
]


@pytest.mark.parametrize("kill_label", MIXED_LABELS)
def test_crash_during_mixed_write(tmp_path, kill_label):
    paths = (str(tmp_path / "d"), str(tmp_path / "m"))
    pool = StoragePool.create(paths[0], paths[1], 8 * B)
    pool.create_volume("a", 4 * B)
    pool.write("a", 0, pattern(4, B))  # block 0 occupied, blocks 1-3 virgin
    pool.close()

    offset, length = B // 2, 2 * B  # covers tail of block 0 + blocks 1-2
    run_worker(["write", *paths, kill_label, "a", str(offset), "5", str(length)])

    before = bytearray(4 * B)
    before[:B] = pattern(4, B)
    after = bytearray(before)
    after[offset : offset + length] = pattern(5, length)
    pool = reopen_and_check(paths, {"a": [bytes(before), bytes(after)]})
    assert pool.pool_info().used_bytes in (1 * B, 3 * B)
    pool.close()


# -- crash during delete ----------------------------------------------------

DELETE_LABELS = ["delete_map_removed", "delete_committed"]


@pytest.mark.parametrize("kill_label", DELETE_LABELS)
def test_crash_during_delete(tmp_path, kill_label):
    paths = (str(tmp_path / "d"), str(tmp_path / "m"))
    pool = StoragePool.create(paths[0], paths[1], 4 * B)
    pool.create_volume("a", 2 * B)
    pool.create_volume("b", 2 * B)
    pool.write("a", 0, pattern(6, 2 * B))
    pool.write("b", 0, pattern(7, B))
    pool.close()

    run_worker(["delete", *paths, kill_label, "a"])

    pool = StoragePool.open(paths[0], paths[1])
    pool.fsck()
    try:
        info = pool.volume_info("a")
    except VolumeNotFoundError:
        info = None
    if info is None:
        # delete fully applied: blocks reclaimed, other volume untouched
        assert pool.pool_info().used_bytes == B
    else:
        # delete rolled back: volume fully intact
        assert pool.read("a", 0, 2 * B) == pattern(6, 2 * B)
        assert pool.pool_info().used_bytes == 3 * B
    assert pool.read("b", 0, B) == pattern(7, B)
    pool.close()


# -- after a crashed write, the pool keeps working --------------------------

def test_pool_usable_after_crash(tmp_path):
    paths = (str(tmp_path / "d"), str(tmp_path / "m"))
    pool = StoragePool.create(paths[0], paths[1], 4 * B)
    pool.create_volume("a", 4 * B)
    pool.write("a", 0, pattern(8, 2 * B))
    pool.close()

    run_worker(["write", *paths, "undo_committed", "a", "0", "9", str(2 * B)])

    pool = StoragePool.open(paths[0], paths[1])
    pool.fsck()
    pool.write("a", 0, pattern(10, 4 * B))
    assert pool.read("a", 0, 4 * B) == pattern(10, 4 * B)
    pool.fsck()
    pool.close()
