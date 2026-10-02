"""thinpool 自动化测试，含进程强杀后的崩溃恢复验收场景。"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from thinpool import (  # noqa: E402
    BLOCK_SIZE,
    InvalidConfigError,
    OutOfBoundsError,
    OversubscriptionError,
    PoolExistsError,
    PoolFullError,
    PoolNotFoundError,
    StoragePool,
    VolumeExistsError,
    VolumeNotFoundError,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHILD = os.path.join(REPO_ROOT, "tests", "_crash_child.py")

HOOK_POINTS = ["after_journal_tx", "after_journal", "after_data", "before_commit"]


def pattern(seed: int, n: int) -> bytes:
    """与 tests/_crash_child.py 中一致的确定性数据模式。"""
    return bytes(((i * 131 + seed * 17) & 0xFF) for i in range(n))


def apply_write(image: bytearray, offset: int, data: bytes) -> None:
    image[offset : offset + len(data)] = data


class ThinPoolTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="thinpool-test-")
        self.pool_path = os.path.join(self.dir, "pool")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def make_pool(self, blocks: int = 8, ratio: float = 2.0) -> StoragePool:
        return StoragePool.create(self.pool_path, BLOCK_SIZE * blocks, ratio)

    def run_child(self, *args: str) -> int:
        proc = subprocess.run(
            [sys.executable, CHILD, REPO_ROOT, self.pool_path, *args],
            capture_output=True,
        )
        return proc.returncode


class TestPoolConfig(ThinPoolTestCase):
    def test_capacity_must_be_block_multiple(self):
        with self.assertRaises(InvalidConfigError):
            StoragePool.create(self.pool_path, BLOCK_SIZE + 1)

    def test_data_file_size_equals_capacity(self):
        pool = self.make_pool(blocks=4)
        size = os.path.getsize(os.path.join(self.pool_path, "pool.data"))
        self.assertEqual(size, BLOCK_SIZE * 4)
        self.assertEqual(pool.capacity_bytes, BLOCK_SIZE * 4)
        pool.close()

    def test_double_create_rejected(self):
        self.make_pool().close()
        with self.assertRaises(PoolExistsError):
            self.make_pool()

    def test_open_missing_pool(self):
        with self.assertRaises(PoolNotFoundError):
            StoragePool.open(self.pool_path)

    def test_oversubscription_limit_enforced(self):
        pool = self.make_pool(blocks=4, ratio=2.0)  # 上限 8 块标称
        pool.create_volume("a", BLOCK_SIZE * 5)
        pool.create_volume("b", BLOCK_SIZE * 3)
        with self.assertRaises(OversubscriptionError):
            pool.create_volume("c", 1)
        pool.close()

    def test_volume_errors(self):
        pool = self.make_pool()
        pool.create_volume("a", BLOCK_SIZE)
        with self.assertRaises(VolumeExistsError):
            pool.create_volume("a", BLOCK_SIZE)
        with self.assertRaises(VolumeNotFoundError):
            pool.read("ghost", 0, 1)
        with self.assertRaises(VolumeNotFoundError):
            pool.delete_volume("ghost")
        with self.assertRaises(OutOfBoundsError):
            pool.write("a", BLOCK_SIZE - 1, b"xy")
        with self.assertRaises(OutOfBoundsError):
            pool.read("a", 0, BLOCK_SIZE + 1)
        pool.close()


class TestReadWrite(ThinPoolTestCase):
    def test_unaligned_multiblock_write(self):
        pool = self.make_pool()
        pool.create_volume("v", BLOCK_SIZE * 6)
        data = pattern(1, BLOCK_SIZE * 2 + 12345)  # 跨 3 块、不对齐
        pool.write("v", 999, data)
        self.assertEqual(pool.read("v", 999, len(data)), data)
        self.assertEqual(pool.read("v", 0, 999), b"\x00" * 999)
        tail = 999 + len(data)
        self.assertEqual(
            pool.read("v", tail, BLOCK_SIZE * 6 - tail),
            b"\x00" * (BLOCK_SIZE * 6 - tail),
        )
        self.assertEqual(pool.volume_used_bytes("v"), BLOCK_SIZE * 3)
        self.assertEqual(pool.pool_used_bytes(), BLOCK_SIZE * 3)
        pool.check_consistency()
        pool.close()

    def test_overwrite_and_persistence(self):
        pool = self.make_pool()
        pool.create_volume("v", BLOCK_SIZE * 2)
        pool.write("v", 0, pattern(1, BLOCK_SIZE * 2))
        pool.write("v", 100, pattern(2, 500))
        pool.close()
        pool = StoragePool.open(self.pool_path)
        expected = bytearray(pattern(1, BLOCK_SIZE * 2))
        apply_write(expected, 100, pattern(2, 500))
        self.assertEqual(pool.read("v", 0, BLOCK_SIZE * 2), bytes(expected))
        pool.check_consistency()
        pool.close()

    def test_empty_write_is_noop(self):
        pool = self.make_pool()
        pool.create_volume("v", BLOCK_SIZE)
        pool.write("v", 0, b"")
        self.assertEqual(pool.pool_used_bytes(), 0)
        pool.close()


class TestPoolFull(ThinPoolTestCase):
    def test_full_pool_rejects_new_block_but_allows_overwrite(self):
        pool = self.make_pool(blocks=2, ratio=4.0)
        pool.create_volume("v", BLOCK_SIZE * 4)
        pool.write("v", 0, pattern(1, BLOCK_SIZE * 2))  # 占满 2 块
        self.assertEqual(pool.pool_used_bytes(), BLOCK_SIZE * 2)

        # 需要新块的写入整笔拒绝，卷内容不变
        with self.assertRaises(PoolFullError):
            pool.write("v", BLOCK_SIZE * 2, pattern(2, BLOCK_SIZE))
        self.assertEqual(pool.read("v", 0, BLOCK_SIZE * 2), pattern(1, BLOCK_SIZE * 2))
        self.assertEqual(
            pool.read("v", BLOCK_SIZE * 2, BLOCK_SIZE), b"\x00" * BLOCK_SIZE
        )

        # 只覆盖已占用区间的写入，池满也必须成功
        pool.write("v", 7, pattern(3, 100))
        expected = bytearray(pattern(1, BLOCK_SIZE * 2))
        apply_write(expected, 7, pattern(3, 100))
        self.assertEqual(pool.read("v", 0, BLOCK_SIZE * 2), bytes(expected))
        pool.check_consistency()
        pool.close()


class TestDeleteAndReuse(ThinPoolTestCase):
    def test_delete_frees_blocks_and_new_volume_reads_zero(self):
        """验收场景：删掉写满非零数据的卷，新建卷零散写入，未写部分全为零。"""
        pool = self.make_pool(blocks=4, ratio=4.0)
        pool.create_volume("old", BLOCK_SIZE * 4)
        pool.write("old", 0, pattern(9, BLOCK_SIZE * 4))  # 写满非零数据
        self.assertEqual(pool.pool_used_bytes(), BLOCK_SIZE * 4)
        pool.delete_volume("old")
        self.assertEqual(pool.pool_used_bytes(), 0)

        pool.create_volume("new", BLOCK_SIZE * 4)
        # 零散写入：不对齐、跨块、落在不同块上
        writes = [(13, pattern(1, 100)), (BLOCK_SIZE + 7, pattern(2, 70000))]
        for offset, data in writes:
            pool.write("new", offset, data)

        expected = bytearray(BLOCK_SIZE * 4)
        for offset, data in writes:
            apply_write(expected, offset, data)
        actual = pool.read("new", 0, BLOCK_SIZE * 4)
        self.assertEqual(actual, bytes(expected))
        # 显式验证：未写过的部分全是零（而不是旧卷残留的非零数据）
        unwritten = bytearray(actual)
        for offset, data in writes:
            unwritten[offset : offset + len(data)] = b"\x00" * len(data)
        self.assertEqual(bytes(unwritten), b"\x00" * (BLOCK_SIZE * 4))
        pool.check_consistency()
        pool.close()

        # 重开后依然成立
        pool = StoragePool.open(self.pool_path)
        self.assertEqual(pool.read("new", 0, BLOCK_SIZE * 4), bytes(expected))
        pool.check_consistency()
        pool.close()

    def test_deleted_volume_blocks_reusable(self):
        pool = self.make_pool(blocks=2, ratio=4.0)
        pool.create_volume("a", BLOCK_SIZE * 2)
        pool.write("a", 0, pattern(1, BLOCK_SIZE * 2))  # 占满
        pool.delete_volume("a")
        pool.create_volume("b", BLOCK_SIZE * 2)
        pool.write("b", 0, pattern(2, BLOCK_SIZE * 2))  # 复用释放的块
        self.assertEqual(pool.read("b", 0, BLOCK_SIZE * 2), pattern(2, BLOCK_SIZE * 2))
        pool.check_consistency()
        pool.close()


class TestCrashRecovery(ThinPoolTestCase):
    """验收场景：跨多块不对齐写入/删卷中途强杀，重开后池一致。"""

    def _seed_volume(self, blocks: int = 8) -> None:
        shutil.rmtree(self.pool_path, ignore_errors=True)
        pool = self.make_pool(blocks=blocks, ratio=2.0)
        pool.create_volume("v", BLOCK_SIZE * 6)
        # 已提交的数据：强杀后必须保留
        pool.write("v", 0, pattern(1, BLOCK_SIZE * 2))
        pool.close()

    def _expected_images(self) -> tuple[bytes, bytes]:
        before = bytearray(BLOCK_SIZE * 6)
        apply_write(before, 0, pattern(1, BLOCK_SIZE * 2))
        after = bytearray(before)
        # 跨 4 块的不对齐写入，覆盖新旧区间
        apply_write(after, BLOCK_SIZE - 50, pattern(2, BLOCK_SIZE * 3 + 100))
        return bytes(before), bytes(after)

    def _killed_write(self, hook_point: str) -> None:
        rc = self.run_child(
            hook_point, "write", "v",
            str(BLOCK_SIZE - 50), "2", str(BLOCK_SIZE * 3 + 100),
        )
        self.assertEqual(rc, 99, "子进程应在钩子点被强杀")

    def test_crash_mid_write(self):
        for hook_point in HOOK_POINTS:
            with self.subTest(hook=hook_point):
                self._seed_volume()
                before, after = self._expected_images()
                self._killed_write(hook_point)

                pool = StoragePool.open(self.pool_path)
                content = pool.read("v", 0, BLOCK_SIZE * 6)
                # 卷内容只能是写入前或写入后的样子，不能是一半新一半旧
                self.assertIn(content, (before, after))
                # 用量与真实占用一致
                used_chunks = 2 if content == before else 5
                self.assertEqual(pool.volume_used_bytes("v"), BLOCK_SIZE * used_chunks)
                self.assertEqual(pool.pool_used_bytes(), BLOCK_SIZE * used_chunks)
                # 无泄漏块、无一块多主
                pool.check_consistency()
                pool.close()

                # 恢复后池仍可正常读写
                pool = StoragePool.open(self.pool_path)
                pool.write("v", 0, pattern(3, BLOCK_SIZE))
                self.assertEqual(pool.read("v", 0, BLOCK_SIZE), pattern(3, BLOCK_SIZE))
                pool.check_consistency()
                pool.close()

    def test_stale_journal_file_not_reused(self):
        """上一次成功写入残留的日志文件，不能被恢复流程误当成本次的旧镜像。"""
        pool = self.make_pool()
        pool.create_volume("v", BLOCK_SIZE * 4)
        pool.write("v", 0, pattern(1, BLOCK_SIZE * 2))
        # 一次成功的原地覆盖：磁盘上留下 gen=N 的日志文件
        pool.write("v", 10, pattern(2, 100))
        before = pool.read("v", 0, BLOCK_SIZE * 4)
        pool.close()

        # 下一次写入在日志事务提交后、日志文件重写前被强杀：
        # journal 表是 gen=N+1，日志文件还是 gen=N
        rc = self.run_child(
            "after_journal_tx", "write", "v", "20", "3", str(BLOCK_SIZE)
        )
        self.assertEqual(rc, 99)

        pool = StoragePool.open(self.pool_path)
        # 数据未被修改，恢复后必须仍是写入前的样子
        self.assertEqual(pool.read("v", 0, BLOCK_SIZE * 4), before)
        pool.check_consistency()
        pool.close()

    def test_crash_mid_delete(self):
        for hook_point in ["before_commit"]:
            with self.subTest(hook=hook_point):
                pool = self.make_pool()
                pool.create_volume("v", BLOCK_SIZE * 2)
                pool.write("v", 0, pattern(1, BLOCK_SIZE * 2))
                pool.close()

                rc = self.run_child(hook_point, "delete", "v")
                self.assertEqual(rc, 99)

                pool = StoragePool.open(self.pool_path)
                # 删卷事务未提交 -> 卷必须完整保留
                self.assertEqual(pool.list_volumes(), ["v"])
                self.assertEqual(
                    pool.read("v", 0, BLOCK_SIZE * 2), pattern(1, BLOCK_SIZE * 2)
                )
                self.assertEqual(pool.pool_used_bytes(), BLOCK_SIZE * 2)
                pool.check_consistency()
                pool.close()

    def test_committed_ops_survive_kill(self):
        """已经返回成功的操作，在之后发生强杀时一定保留。"""
        pool = self.make_pool()
        pool.create_volume("v", BLOCK_SIZE * 6)
        pool.write("v", 0, pattern(1, BLOCK_SIZE))       # 已提交
        pool.write("v", BLOCK_SIZE, pattern(2, BLOCK_SIZE))  # 已提交
        pool.close()

        self._killed_write("after_data")

        pool = StoragePool.open(self.pool_path)
        self.assertEqual(pool.read("v", 0, BLOCK_SIZE), pattern(1, BLOCK_SIZE))
        self.assertEqual(
            pool.read("v", BLOCK_SIZE, BLOCK_SIZE), pattern(2, BLOCK_SIZE)
        )
        pool.check_consistency()
        pool.close()

    def test_repeated_crash_cycles(self):
        """多轮 写入->强杀->恢复，池始终一致且不泄漏块。"""
        pool = self.make_pool(blocks=8, ratio=2.0)
        pool.create_volume("v", BLOCK_SIZE * 8)
        pool.close()
        for i, hook_point in enumerate(HOOK_POINTS * 2):
            rc = self.run_child(
                hook_point, "write", "v",
                str(i * 1000), str(i + 1), str(BLOCK_SIZE * 2 + 7),
            )
            self.assertEqual(rc, 99)
            pool = StoragePool.open(self.pool_path)
            pool.check_consistency()
            self.assertLessEqual(pool.pool_used_bytes(), BLOCK_SIZE * 8)
            pool.close()


if __name__ == "__main__":
    unittest.main()
