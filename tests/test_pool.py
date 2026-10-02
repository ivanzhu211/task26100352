"""Functional tests for the thinpool storage pool library."""

import os

import pytest

from thinpool import (
    BLOCK_SIZE as B,
    InvalidPoolConfigError,
    OutOfBoundsError,
    PoolExistsError,
    PoolFullError,
    PoolNotFoundError,
    QuotaExceededError,
    StoragePool,
    VolumeExistsError,
    VolumeNotFoundError,
)


def pattern(seed: int, length: int) -> bytes:
    return bytes((seed + i * 7 + i // 251) % 256 for i in range(length))


@pytest.fixture()
def paths(tmp_path):
    return str(tmp_path / "pool.data"), str(tmp_path / "pool.meta")


@pytest.fixture()
def pool(paths):
    p = StoragePool.create(paths[0], paths[1], 8 * B, oversub_ratio=2.0)
    yield p
    p.close()


# -- pool creation / opening ----------------------------------------------

def test_create_requires_block_multiple(paths):
    with pytest.raises(InvalidPoolConfigError):
        StoragePool.create(paths[0], paths[1], 8 * B + 1)


def test_create_requires_positive_capacity(paths):
    with pytest.raises(InvalidPoolConfigError):
        StoragePool.create(paths[0], paths[1], 0)


def test_create_requires_ratio_at_least_one(paths):
    with pytest.raises(InvalidPoolConfigError):
        StoragePool.create(paths[0], paths[1], 8 * B, oversub_ratio=0.5)


def test_create_sizes_data_file_to_capacity(paths):
    p = StoragePool.create(paths[0], paths[1], 8 * B)
    assert os.path.getsize(paths[0]) == 8 * B
    p.close()


def test_create_twice_rejected(paths):
    StoragePool.create(paths[0], paths[1], 8 * B).close()
    with pytest.raises(PoolExistsError):
        StoragePool.create(paths[0], paths[1], 8 * B)


def test_open_missing_pool(paths):
    with pytest.raises(PoolNotFoundError):
        StoragePool.open(paths[0], paths[1])


# -- volume management -----------------------------------------------------

def test_duplicate_volume_name_rejected(pool):
    pool.create_volume("a", B)
    with pytest.raises(VolumeExistsError):
        pool.create_volume("a", B)


def test_oversubscription_quota_enforced(pool):
    # capacity 8 blocks, ratio 2.0 -> 16 blocks of nominal space
    pool.create_volume("a", 10 * B)
    pool.create_volume("b", 6 * B)
    with pytest.raises(QuotaExceededError):
        pool.create_volume("c", 1)


def test_delete_unknown_volume(pool):
    with pytest.raises(VolumeNotFoundError):
        pool.delete_volume("nope")


def test_fresh_volume_has_zero_usage(pool):
    pool.create_volume("a", 4 * B)
    assert pool.volume_info("a").used_bytes == 0
    assert pool.pool_info().used_bytes == 0


# -- reads / writes --------------------------------------------------------

def test_unwritten_regions_read_as_zero(pool):
    pool.create_volume("a", 4 * B)
    assert pool.read("a", 0, 4 * B) == b"\x00" * (4 * B)


def test_unaligned_multiblock_write_roundtrip(pool):
    pool.create_volume("a", 4 * B)
    data = pattern(1, 3 * B - 200)
    pool.write("a", 100, data)
    assert pool.read("a", 100, len(data)) == data
    assert pool.read("a", 0, 100) == b"\x00" * 100
    tail = 4 * B - (100 + len(data))
    assert pool.read("a", 100 + len(data), tail) == b"\x00" * tail


def test_first_write_allocates_one_block_per_touched_region(pool):
    pool.create_volume("a", 4 * B)
    pool.write("a", 0, b"x")
    assert pool.volume_info("a").used_bytes == B
    pool.write("a", 2 * B, b"x")
    assert pool.volume_info("a").used_bytes == 2 * B
    assert pool.pool_info().used_bytes == 2 * B


def test_rewrite_of_same_region_allocates_nothing(pool):
    pool.create_volume("a", 4 * B)
    pool.write("a", 10, pattern(2, 1000))
    pool.write("a", 10, pattern(3, 1000))
    assert pool.volume_info("a").used_bytes == B
    assert pool.read("a", 10, 1000) == pattern(3, 1000)


def test_write_persists_across_reopen(paths, pool):
    pool.create_volume("a", 4 * B)
    data = pattern(4, 2 * B + 12345)
    pool.write("a", 7, data)
    pool.close()
    p2 = StoragePool.open(paths[0], paths[1])
    assert p2.read("a", 7, len(data)) == data
    assert p2.volume_info("a").used_bytes == 3 * B
    p2.fsck()
    p2.close()


def test_read_write_out_of_bounds(pool):
    pool.create_volume("a", B)
    with pytest.raises(OutOfBoundsError):
        pool.write("a", B - 10, b"x" * 11)
    with pytest.raises(OutOfBoundsError):
        pool.read("a", B - 10, 11)
    with pytest.raises(OutOfBoundsError):
        pool.write("a", -1, b"x")


# -- pool full behaviour ---------------------------------------------------

def test_pool_full_rejects_write_needing_new_blocks(paths):
    p = StoragePool.create(paths[0], paths[1], 2 * B, oversub_ratio=2.0)
    p.create_volume("a", 2 * B)
    p.create_volume("b", 2 * B)
    p.write("a", 0, pattern(5, 2 * B))  # pool now full
    before = p.read("b", 0, 2 * B)
    with pytest.raises(PoolFullError):
        p.write("b", 0, b"x")
    # rejected write changed nothing
    assert p.read("b", 0, 2 * B) == before
    assert p.volume_info("b").used_bytes == 0
    p.close()


def test_pool_full_still_allows_overwrite_of_occupied_ranges(paths):
    p = StoragePool.create(paths[0], paths[1], 2 * B)
    p.create_volume("a", 2 * B)
    p.write("a", 0, pattern(6, 2 * B))  # pool full
    new = pattern(7, 100)
    p.write("a", 500, new)  # unaligned, in-place, must succeed
    assert p.read("a", 500, 100) == new
    assert p.read("a", 0, 500) == pattern(6, 2 * B)[:500]
    p.fsck()
    p.close()


# -- delete / reuse --------------------------------------------------------

def test_delete_frees_blocks_for_other_volumes(paths):
    p = StoragePool.create(paths[0], paths[1], 2 * B, oversub_ratio=2.0)
    p.create_volume("a", 2 * B)
    p.create_volume("b", 2 * B)
    p.write("a", 0, pattern(8, 2 * B))  # pool full
    p.delete_volume("a")
    assert p.pool_info().used_bytes == 0
    p.write("b", 0, pattern(9, 2 * B))  # reuses the freed blocks
    assert p.read("b", 0, 2 * B) == pattern(9, 2 * B)
    p.close()


def test_new_volume_never_sees_deleted_volume_data(paths):
    p = StoragePool.create(paths[0], paths[1], 4 * B)
    p.create_volume("old", 4 * B)
    p.write("old", 0, b"\xff" * (4 * B))
    p.delete_volume("old")
    p.create_volume("new", 4 * B)
    # scattered writes, then everything unwritten must read as zero
    p.write("new", 10, b"hello")
    p.write("new", 3 * B + 5, b"world")
    expected = bytearray(4 * B)
    expected[10:15] = b"hello"
    expected[3 * B + 5 : 3 * B + 10] = b"world"
    assert p.read("new", 0, 4 * B) == bytes(expected)
    p.fsck()
    p.close()


def test_usage_consistent_after_mixed_operations(paths):
    p = StoragePool.create(paths[0], paths[1], 8 * B)
    p.create_volume("a", 4 * B)
    p.create_volume("b", 4 * B)
    p.write("a", 0, pattern(10, 3 * B))
    p.write("b", B, pattern(11, B))
    assert p.volume_info("a").used_bytes == 3 * B
    assert p.volume_info("b").used_bytes == 1 * B
    assert p.pool_info().used_bytes == 4 * B
    p.delete_volume("a")
    assert p.pool_info().used_bytes == 1 * B
    p.fsck()
    p.close()
