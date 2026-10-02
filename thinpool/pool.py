"""存储池主体：数据文件 + SQLite 元数据 + 崩溃恢复。

磁盘布局（path 为池目录）：
  pool.data     固定大小的数据文件，大小即池物理容量，按 64 KiB 分块
  pool.meta     SQLite 元数据（卷表、块映射、块状态、写入日志）
  pool.journal  原地写时保存被覆盖块的旧镜像，用于崩溃回滚
"""

from __future__ import annotations

import os
import sqlite3
import struct
from typing import Callable, Optional

from .errors import (
    CorruptPoolError,
    InvalidConfigError,
    OutOfBoundsError,
    OversubscriptionError,
    PoolClosedError,
    PoolExistsError,
    PoolFullError,
    PoolNotFoundError,
    VolumeExistsError,
    VolumeNotFoundError,
)

BLOCK_SIZE = 64 * 1024  # 物理分配块大小：64 KiB

DATA_FILE = "pool.data"
META_FILE = "pool.meta"
JOURNAL_FILE = "pool.journal"

_JOURNAL_MAGIC = b"TPJRNAL1"
_JOURNAL_HEADER = struct.Struct("<8sQI")  # magic + 代次号 + 旧镜像块数

# 崩溃测试钩子挂载点（仅测试使用）
HOOK_AFTER_JOURNAL_TX = "after_journal_tx"  # 日志事务提交之后、日志文件写入之前
HOOK_AFTER_JOURNAL = "after_journal"      # 日志落盘之后、写数据之前
HOOK_AFTER_DATA = "after_data"            # 数据落盘之后、提交映射之前
HOOK_BEFORE_COMMIT = "before_commit"      # 提交映射事务之前

_SCHEMA = """
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE volumes (
    name       TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL
);
CREATE TABLE blocks (
    idx   INTEGER PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('free', 'used', 'pending'))
);
CREATE TABLE blockmap (
    volume TEXT NOT NULL REFERENCES volumes(name) ON DELETE CASCADE,
    chunk  INTEGER NOT NULL,
    block  INTEGER NOT NULL,
    PRIMARY KEY (volume, chunk)
);
CREATE TABLE journal (
    id     INTEGER PRIMARY KEY,
    gen    INTEGER NOT NULL,
    volume TEXT NOT NULL,
    chunk  INTEGER NOT NULL,
    kind   TEXT NOT NULL CHECK (kind IN ('cow', 'inplace')),
    block  INTEGER NOT NULL,
    slot   INTEGER NOT NULL
);
"""


class StoragePool:
    """一个存储池对应一个固定大小的本地数据文件。"""

    def __init__(
        self,
        path: str,
        crash_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._path = path
        self._crash_hook = crash_hook
        self._data_fd: int = -1
        self._db: Optional[sqlite3.Connection] = None
        self._closed = True

    # ---- 建池 / 开池 ----

    @classmethod
    def create(
        cls,
        path: str,
        capacity_bytes: int,
        oversub_ratio: float = 1.0,
        crash_hook: Optional[Callable[[str], None]] = None,
    ) -> "StoragePool":
        """在指定目录创建新池：容量必须是 64 KiB 的整数倍，建池后不变。"""
        if capacity_bytes <= 0 or capacity_bytes % BLOCK_SIZE != 0:
            raise InvalidConfigError(
                f"capacity {capacity_bytes} 不是 {BLOCK_SIZE} 的正整数倍"
            )
        if oversub_ratio < 1.0:
            raise InvalidConfigError("oversub_ratio 必须 >= 1.0")
        if os.path.exists(path):
            raise PoolExistsError(f"池目录已存在: {path}")

        os.makedirs(path)
        data_path = os.path.join(path, DATA_FILE)
        try:
            # 数据文件：一次性扩展到物理容量，之后大小不变
            fd = os.open(data_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            try:
                os.ftruncate(fd, capacity_bytes)
                os.fsync(fd)
            finally:
                os.close(fd)

            pool = cls(path, crash_hook=crash_hook)
            pool._open_files()
            pool._init_schema(capacity_bytes, oversub_ratio)
            pool._closed = False
            return pool
        except BaseException:
            # 建池失败则清理半成品目录
            for name in (DATA_FILE, META_FILE, JOURNAL_FILE):
                try:
                    os.unlink(os.path.join(path, name))
                except OSError:
                    pass
            try:
                os.rmdir(path)
            except OSError:
                pass
            raise

    @classmethod
    def open(
        cls,
        path: str,
        crash_hook: Optional[Callable[[str], None]] = None,
    ) -> "StoragePool":
        """打开已有池并执行崩溃恢复。"""
        if not os.path.isdir(path) or not os.path.exists(
            os.path.join(path, META_FILE)
        ):
            raise PoolNotFoundError(f"池不存在: {path}")
        pool = cls(path, crash_hook=crash_hook)
        pool._open_files()
        pool._recover()
        pool._closed = False
        return pool

    def _open_files(self) -> None:
        self._data_fd = os.open(
            os.path.join(self._path, DATA_FILE), os.O_RDWR
        )
        self._db = sqlite3.connect(os.path.join(self._path, META_FILE))
        self._db.execute("PRAGMA synchronous = FULL")
        self._db.execute("PRAGMA foreign_keys = ON")

    def _init_schema(self, capacity_bytes: int, oversub_ratio: float) -> None:
        assert self._db is not None
        total_blocks = capacity_bytes // BLOCK_SIZE
        with self._db:
            self._db.executescript(_SCHEMA)
            self._db.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?)",
                ("capacity_bytes", str(capacity_bytes)),
            )
            self._db.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?)",
                ("oversub_ratio", repr(oversub_ratio)),
            )
            self._db.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?)",
                ("journal_gen", "0"),
            )
            self._db.executemany(
                "INSERT INTO blocks(idx, state) VALUES (?, 'free')",
                ((i,) for i in range(total_blocks)),
            )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._data_fd >= 0:
            os.close(self._data_fd)
            self._data_fd = -1

    def __enter__(self) -> "StoragePool":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- 内部工具 ----

    def _check_open(self) -> sqlite3.Connection:
        if self._closed or self._db is None:
            raise PoolClosedError("池已关闭")
        return self._db

    def _hook(self, point: str) -> None:
        if self._crash_hook is not None:
            self._crash_hook(point)

    def _recover(self) -> None:
        """崩溃恢复：回滚未提交的写入。

        journal 表非空说明上次写入没走到提交点：
          - 原地覆盖的块：若日志文件完整，把旧镜像写回数据文件；
            日志文件不完整说明数据尚未被修改，无需回滚。
          - pending 的新块：未被任何映射引用，直接归还空闲列表。
        """
        assert self._db is not None
        db = self._db
        rows = db.execute(
            "SELECT gen, volume, chunk, kind, block, slot FROM journal "
            "ORDER BY id"
        ).fetchall()
        if not rows:
            return

        journal_data = self._read_journal_file()
        for gen, volume, chunk, kind, block, slot in rows:
            # 只有当代次号匹配时，日志文件里的旧镜像才属于这次写入；
            # 否则说明数据尚未被修改（文件是更早一次成功写入的残留）。
            if (
                kind == "inplace"
                and journal_data is not None
                and journal_data[0] == gen
                and slot < len(journal_data[1])
            ):
                os.pwrite(
                    self._data_fd, journal_data[1][slot], block * BLOCK_SIZE
                )
        os.fsync(self._data_fd)

        with db:
            db.execute(
                "UPDATE blocks SET state = 'free' WHERE state = 'pending'"
            )
            db.execute("DELETE FROM journal")

    def _read_journal_file(self) -> Optional[tuple[int, list[bytes]]]:
        """读取日志文件中的 (代次号, 旧镜像列表)；文件缺失或不完整返回 None。"""
        path = os.path.join(self._path, JOURNAL_FILE)
        try:
            with open(path, "rb") as f:
                header = f.read(_JOURNAL_HEADER.size)
                if len(header) != _JOURNAL_HEADER.size:
                    return None
                magic, gen, count = _JOURNAL_HEADER.unpack(header)
                if magic != _JOURNAL_MAGIC:
                    return None
                images = []
                for _ in range(count):
                    image = f.read(BLOCK_SIZE)
                    if len(image) != BLOCK_SIZE:
                        return None
                    images.append(image)
                return (gen, images)
        except OSError:
            return None

    # ---- 卷管理 ----

    def create_volume(self, name: str, size_bytes: int) -> None:
        """建卷：只登记标称大小，不占物理空间。

        所有卷的标称大小之和不得超过 物理容量 * 超分比。
        """
        db = self._check_open()
        if size_bytes <= 0:
            raise InvalidConfigError("卷的标称大小必须为正数")
        if db.execute(
            "SELECT 1 FROM volumes WHERE name = ?", (name,)
        ).fetchone():
            raise VolumeExistsError(f"卷已存在: {name}")
        row = db.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM volumes"
        ).fetchone()
        limit = int(self.capacity_bytes * self.oversub_ratio)
        if row[0] + size_bytes > limit:
            raise OversubscriptionError(
                f"标称总量 {row[0] + size_bytes} 超出超分上限 {limit}"
            )
        with db:
            db.execute(
                "INSERT INTO volumes(name, size_bytes) VALUES (?, ?)",
                (name, size_bytes),
            )

    def delete_volume(self, name: str) -> None:
        """删卷：单个事务内删除映射并释放块，崩溃安全。"""
        db = self._check_open()
        size = self._volume_size(name)
        del size
        self._hook(HOOK_BEFORE_COMMIT)
        with db:
            db.execute(
                "UPDATE blocks SET state = 'free' WHERE idx IN "
                "(SELECT block FROM blockmap WHERE volume = ?)",
                (name,),
            )
            db.execute("DELETE FROM blockmap WHERE volume = ?", (name,))
            db.execute("DELETE FROM volumes WHERE name = ?", (name,))

    def list_volumes(self) -> list[str]:
        db = self._check_open()
        return [
            r[0] for r in db.execute("SELECT name FROM volumes ORDER BY name")
        ]

    def _volume_size(self, name: str) -> int:
        db = self._check_open()
        row = db.execute(
            "SELECT size_bytes FROM volumes WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            raise VolumeNotFoundError(f"卷不存在: {name}")
        return int(row[0])

    # ---- 读写 ----

    def write(self, volume: str, offset: int, data: bytes) -> None:
        """任意偏移、任意长度写入；整笔原子生效。

        崩溃安全协议：
          1. 事务一：把新块标记为 pending 并记录写入日志（journal 表）。
          2. 原地覆盖的块先把旧镜像写入日志文件并 fsync。
          3. 写数据（新块先整塊填零再覆盖，保证读不到别的卷的旧数据），fsync。
          4. 事务二：应用块映射、清除日志。SQLite 事务提交是原子的。
        在第 4 步之前被强杀，恢复时回滚到写入前；之后则保留写入后。
        """
        db = self._check_open()
        size = self._volume_size(volume)
        if offset < 0 or offset + len(data) > size:
            raise OutOfBoundsError(
                f"写入范围 [{offset}, {offset + len(data)}) 超出卷大小 {size}"
            )
        if not data:
            return

        first_chunk = offset // BLOCK_SIZE
        last_chunk = (offset + len(data) - 1) // BLOCK_SIZE

        # 规划每个块的写入方式：未分配 -> cow（需要新块）；已分配 -> 原地覆盖
        cow_chunks: list[int] = []
        inplace_chunks: list[tuple[int, int]] = []  # (chunk, block)
        for chunk in range(first_chunk, last_chunk + 1):
            row = db.execute(
                "SELECT block FROM blockmap WHERE volume = ? AND chunk = ?",
                (volume, chunk),
            ).fetchone()
            if row is None:
                cow_chunks.append(chunk)
            else:
                inplace_chunks.append((chunk, row[0]))

        free_blocks = [
            r[0]
            for r in db.execute(
                "SELECT idx FROM blocks WHERE state = 'free' ORDER BY idx"
            ).fetchall()
        ]
        if len(cow_chunks) > len(free_blocks):
            # 池满：需要新块的写入整笔拒绝，卷内容不变
            raise PoolFullError(
                f"需要 {len(cow_chunks)} 个新块，仅剩 {len(free_blocks)} 个"
            )

        # 事务一：分配新日志代次、预留新块、记录日志
        cow_plan = list(zip(cow_chunks, free_blocks))  # (chunk, new_block)
        with db:
            gen = int(
                db.execute(
                    "SELECT value FROM meta WHERE key = 'journal_gen'"
                ).fetchone()[0]
            ) + 1
            db.execute(
                "UPDATE meta SET value = ? WHERE key = 'journal_gen'",
                (str(gen),),
            )
            for chunk, block in cow_plan:
                db.execute(
                    "UPDATE blocks SET state = 'pending' WHERE idx = ?",
                    (block,),
                )
                db.execute(
                    "INSERT INTO journal(gen, volume, chunk, kind, block, slot) "
                    "VALUES (?, ?, ?, 'cow', ?, -1)",
                    (gen, volume, chunk, block),
                )
            for slot, (chunk, block) in enumerate(inplace_chunks):
                db.execute(
                    "INSERT INTO journal(gen, volume, chunk, kind, block, slot) "
                    "VALUES (?, ?, ?, 'inplace', ?, ?)",
                    (gen, volume, chunk, block, slot),
                )

        # 日志文件：保存原地覆盖块的旧镜像并落盘（带代次号，防止误用旧文件）
        self._hook(HOOK_AFTER_JOURNAL_TX)
        self._write_journal_file(gen, [block for _, block in inplace_chunks])
        self._hook(HOOK_AFTER_JOURNAL)

        # 写数据并落盘
        for chunk, block in cow_plan:
            image = bytearray(BLOCK_SIZE)  # 整块填零，避免泄露旧卷数据
            seg = self._chunk_segment(offset, len(data), chunk)
            image[seg[2] : seg[2] + (seg[1] - seg[0])] = data[seg[0] : seg[1]]
            os.pwrite(self._data_fd, bytes(image), block * BLOCK_SIZE)
        for chunk, block in inplace_chunks:
            seg = self._chunk_segment(offset, len(data), chunk)
            os.pwrite(
                self._data_fd,
                data[seg[0] : seg[1]],
                block * BLOCK_SIZE + seg[2],
            )
        os.fsync(self._data_fd)
        self._hook(HOOK_AFTER_DATA)

        # 事务二：应用映射、清除日志（原子提交点）
        self._hook(HOOK_BEFORE_COMMIT)
        with db:
            for chunk, block in cow_plan:
                db.execute(
                    "INSERT INTO blockmap(volume, chunk, block) "
                    "VALUES (?, ?, ?)",
                    (volume, chunk, block),
                )
                db.execute(
                    "UPDATE blocks SET state = 'used' WHERE idx = ?", (block,)
                )
            db.execute("DELETE FROM journal")

    @staticmethod
    def _chunk_segment(
        offset: int, length: int, chunk: int
    ) -> tuple[int, int, int]:
        """返回 (data起点, data终点, 块内偏移)。"""
        chunk_start = chunk * BLOCK_SIZE
        start = max(offset, chunk_start)
        end = min(offset + length, chunk_start + BLOCK_SIZE)
        return (start - offset, end - offset, start - chunk_start)

    def _write_journal_file(self, gen: int, blocks: list[int]) -> None:
        path = os.path.join(self._path, JOURNAL_FILE)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            header = _JOURNAL_HEADER.pack(_JOURNAL_MAGIC, gen, len(blocks))
            os.write(fd, header)
            for block in blocks:
                old = os.pread(self._data_fd, BLOCK_SIZE, block * BLOCK_SIZE)
                os.write(fd, old)
            os.fsync(fd)
        finally:
            os.close(fd)

    def read(self, volume: str, offset: int, length: int) -> bytes:
        """读卷；从未写过的区域返回零，不触碰数据文件。"""
        db = self._check_open()
        size = self._volume_size(volume)
        if offset < 0 or length < 0 or offset + length > size:
            raise OutOfBoundsError(
                f"读取范围 [{offset}, {offset + length}) 超出卷大小 {size}"
            )
        if length == 0:
            return b""

        first_chunk = offset // BLOCK_SIZE
        last_chunk = (offset + length - 1) // BLOCK_SIZE
        rows = db.execute(
            "SELECT chunk, block FROM blockmap "
            "WHERE volume = ? AND chunk BETWEEN ? AND ?",
            (volume, first_chunk, last_chunk),
        ).fetchall()
        mapping = {chunk: block for chunk, block in rows}

        buf = bytearray(length)
        for chunk in range(first_chunk, last_chunk + 1):
            block = mapping.get(chunk)
            if block is None:
                continue  # 未写过的区域保持为零
            seg = self._chunk_segment(offset, length, chunk)
            chunk_data = os.pread(
                self._data_fd, seg[1] - seg[0], block * BLOCK_SIZE + seg[2]
            )
            buf[seg[0] : seg[1]] = chunk_data
        return bytes(buf)

    # ---- 用量 ----

    @property
    def capacity_bytes(self) -> int:
        db = self._check_open()
        row = db.execute(
            "SELECT value FROM meta WHERE key = 'capacity_bytes'"
        ).fetchone()
        return int(row[0])

    @property
    def oversub_ratio(self) -> float:
        db = self._check_open()
        row = db.execute(
            "SELECT value FROM meta WHERE key = 'oversub_ratio'"
        ).fetchone()
        return float(row[0])

    def pool_used_bytes(self) -> int:
        """池实际用量 = 被卷占用的块数 * 64 KiB。"""
        db = self._check_open()
        row = db.execute("SELECT COUNT(*) FROM blockmap").fetchone()
        return row[0] * BLOCK_SIZE

    def volume_used_bytes(self, name: str) -> int:
        db = self._check_open()
        self._volume_size(name)
        row = db.execute(
            "SELECT COUNT(*) FROM blockmap WHERE volume = ?", (name,)
        ).fetchone()
        return row[0] * BLOCK_SIZE

    # ---- 一致性检查（供测试与运维） ----

    def check_consistency(self) -> None:
        """校验：无泄漏块、无一块多主、用量与映射一致；不一致则抛 CorruptPoolError。"""
        db = self._check_open()
        problems: list[str] = []

        total_blocks = self.capacity_bytes // BLOCK_SIZE

        # 没有处于 pending 状态的块（打开时恢复应已清理干净）
        pending = db.execute(
            "SELECT COUNT(*) FROM blocks WHERE state = 'pending'"
        ).fetchone()[0]
        if pending:
            problems.append(f"{pending} 个块停留在 pending 状态")

        # 没有残留日志
        journalled = db.execute("SELECT COUNT(*) FROM journal").fetchone()[0]
        if journalled:
            problems.append(f"{journalled} 条未清理的写入日志")

        # 块状态总数守恒
        states = dict(
            db.execute("SELECT state, COUNT(*) FROM blocks GROUP BY state")
        )
        if sum(states.values()) != total_blocks:
            problems.append("块总数与容量不符")

        # 映射引用的块必须合法且为 used，且不能一块多主
        mapped = db.execute("SELECT block FROM blockmap").fetchall()
        mapped_idx = [r[0] for r in mapped]
        if len(set(mapped_idx)) != len(mapped_idx):
            problems.append("存在被两个卷/区间同时占用的块")
        for idx in mapped_idx:
            if not 0 <= idx < total_blocks:
                problems.append(f"映射引用了越界块 {idx}")
                break
        used = states.get("used", 0)
        if used != len(mapped_idx):
            problems.append(
                f"used 块数 {used} 与映射条数 {len(mapped_idx)} 不一致（泄漏或丢失）"
            )

        # 映射必须挂在存在的卷上，且区间不越界
        orphans = db.execute(
            "SELECT COUNT(*) FROM blockmap WHERE volume NOT IN "
            "(SELECT name FROM volumes)"
        ).fetchone()[0]
        if orphans:
            problems.append(f"{orphans} 条映射属于不存在的卷")
        for name, size in db.execute(
            "SELECT name, size_bytes FROM volumes"
        ):
            max_chunk = (size - 1) // BLOCK_SIZE
            row = db.execute(
                "SELECT MAX(chunk) FROM blockmap WHERE volume = ?", (name,)
            ).fetchone()
            if row[0] is not None and row[0] > max_chunk:
                problems.append(f"卷 {name} 存在超出标称大小的映射")

        # 数据文件大小必须等于物理容量
        actual = os.fstat(self._data_fd).st_size
        if actual != self.capacity_bytes:
            problems.append(
                f"数据文件大小 {actual} 与容量 {self.capacity_bytes} 不符"
            )

        if problems:
            raise CorruptPoolError("; ".join(problems))
