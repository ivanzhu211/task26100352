"""Thin-provisioned storage pool backed by a local data file and SQLite metadata.

Layout:
  - data file: fixed-size file, one 64 KiB slot per physical block.
  - meta file: SQLite database holding pool config, volumes, block map and
    the undo journal used for crash-safe in-place overwrites.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Union

from .errors import (
    InvalidPoolConfigError,
    OutOfBoundsError,
    PoolError,
    PoolExistsError,
    PoolFullError,
    PoolNotFoundError,
    QuotaExceededError,
    VolumeExistsError,
    VolumeNotFoundError,
)

BLOCK_SIZE = 64 * 1024  # 64 KiB physical allocation unit

PathLike = Union[str, os.PathLike]

# Optional hook invoked at labelled points inside write(); used by tests to
# simulate a kill -9 at an exact step. Signature: hook(label: str) -> None.
CrashHook = Optional[Callable[[str], None]]


@dataclass(frozen=True)
class VolumeInfo:
    name: str
    nominal_bytes: int
    used_bytes: int  # physical bytes actually occupied by this volume


@dataclass(frozen=True)
class PoolInfo:
    capacity_bytes: int
    used_bytes: int  # physical bytes actually occupied by all volumes
    oversub_ratio: float


class StoragePool:
    """A thin-provisioned pool of 64 KiB blocks over one local data file."""

    _SCHEMA = """
    CREATE TABLE meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE volumes (
        id            INTEGER PRIMARY KEY,
        name          TEXT NOT NULL UNIQUE,
        nominal_bytes INTEGER NOT NULL
    );
    CREATE TABLE block_map (
        volume_id      INTEGER NOT NULL REFERENCES volumes(id),
        logical_index  INTEGER NOT NULL,
        physical_block INTEGER NOT NULL,
        PRIMARY KEY (volume_id, logical_index),
        UNIQUE (physical_block)
    );
    CREATE TABLE undo (
        physical_block INTEGER PRIMARY KEY,
        data           BLOB NOT NULL
    );
    """

    def __init__(self) -> None:
        self._data_path: Path = Path()
        self._meta_path: Path = Path()
        self._db: Optional[sqlite3.Connection] = None
        self._fd: int = -1
        self._capacity_blocks: int = 0
        self._oversub_ratio: float = 1.0
        self._crash_hook: CrashHook = None

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def create(
        cls,
        data_path: PathLike,
        meta_path: PathLike,
        capacity_bytes: int,
        oversub_ratio: float = 1.0,
    ) -> "StoragePool":
        """Create a new pool. The data file is sized to capacity_bytes."""
        data_path = Path(data_path)
        meta_path = Path(meta_path)
        if capacity_bytes <= 0 or capacity_bytes % BLOCK_SIZE != 0:
            raise InvalidPoolConfigError(
                "capacity_bytes must be a positive multiple of 64 KiB"
            )
        if oversub_ratio < 1.0:
            raise InvalidPoolConfigError("oversub_ratio must be >= 1.0")
        if data_path.exists() or meta_path.exists():
            raise PoolExistsError(f"pool already exists at {data_path}")

        pool = cls()
        pool._data_path = data_path
        pool._meta_path = meta_path
        pool._capacity_blocks = capacity_bytes // BLOCK_SIZE
        pool._oversub_ratio = float(oversub_ratio)

        # Size the data file once; it never changes afterwards.
        pool._fd = os.open(data_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.ftruncate(pool._fd, capacity_bytes)
            os.fsync(pool._fd)
            pool._db = pool._connect_meta(meta_path, fresh=True)
            pool._db.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?)",
                ("capacity_blocks", str(pool._capacity_blocks)),
            )
            pool._db.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?)",
                ("oversub_ratio", repr(pool._oversub_ratio)),
            )
            pool._db.commit()
        except BaseException:
            pool._cleanup_files()
            raise
        return pool

    @classmethod
    def open(cls, data_path: PathLike, meta_path: PathLike) -> "StoragePool":
        """Open an existing pool, running crash recovery first."""
        data_path = Path(data_path)
        meta_path = Path(meta_path)
        if not data_path.exists() or not meta_path.exists():
            raise PoolNotFoundError(f"no pool at {data_path}")

        pool = cls()
        pool._data_path = data_path
        pool._meta_path = meta_path
        pool._db = pool._connect_meta(meta_path, fresh=False)
        try:
            row = pool._db.execute(
                "SELECT value FROM meta WHERE key = 'capacity_blocks'"
            ).fetchone()
            ratio_row = pool._db.execute(
                "SELECT value FROM meta WHERE key = 'oversub_ratio'"
            ).fetchone()
            if row is None or ratio_row is None:
                raise PoolNotFoundError(f"invalid pool metadata at {meta_path}")
            pool._capacity_blocks = int(row[0])
            pool._oversub_ratio = float(ratio_row[0])
            expected = pool._capacity_blocks * BLOCK_SIZE
            actual = data_path.stat().st_size
            if actual != expected:
                raise PoolNotFoundError(
                    f"data file size {actual} != pool capacity {expected}"
                )
            pool._fd = os.open(data_path, os.O_RDWR)
            pool._recover()
        except BaseException:
            pool._cleanup_files()
            raise
        return pool

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    # -- internal helpers --------------------------------------------------

    @staticmethod
    def _connect_meta(meta_path: Path, fresh: bool) -> sqlite3.Connection:
        if fresh:
            fd = os.open(meta_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
        db = sqlite3.connect(meta_path, isolation_level=None)
        db.execute("PRAGMA journal_mode = WAL")
        db.execute("PRAGMA synchronous = FULL")
        db.execute("PRAGMA foreign_keys = ON")
        if fresh:
            db.executescript(StoragePool._SCHEMA)
        return db

    def _cleanup_files(self) -> None:
        self.close()
        for path in (self._data_path, self._meta_path):
            try:
                if str(path):
                    path.unlink(missing_ok=True)
            except OSError:
                pass

    def _require_open(self) -> sqlite3.Connection:
        if self._db is None or self._fd < 0:
            raise PoolError("pool is closed")
        return self._db

    def _recover(self) -> None:
        """Roll back any in-place overwrite interrupted by a crash.

        A committed undo row means the write's final metadata commit never
        completed, so the operation must appear as "before": restore the old
        block contents into the data file, then drop the journal.
        """
        assert self._db is not None
        rows = self._db.execute(
            "SELECT physical_block, data FROM undo"
        ).fetchall()
        if rows:
            for physical_block, data in rows:
                os.pwrite(self._fd, data, physical_block * BLOCK_SIZE)
            os.fsync(self._fd)
            self._db.execute("DELETE FROM undo")
            self._db.commit()

    def __enter__(self) -> "StoragePool":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- volume management -------------------------------------------------

    def create_volume(self, name: str, nominal_bytes: int) -> None:
        db = self._require_open()
        if nominal_bytes <= 0:
            raise InvalidPoolConfigError("nominal_bytes must be positive")
        exists = db.execute(
            "SELECT 1 FROM volumes WHERE name = ?", (name,)
        ).fetchone()
        if exists is not None:
            raise VolumeExistsError(f"volume {name!r} already exists")
        row = db.execute(
            "SELECT COALESCE(SUM(nominal_bytes), 0) FROM volumes"
        ).fetchone()
        limit = int(self._capacity_blocks * BLOCK_SIZE * self._oversub_ratio)
        if row[0] + nominal_bytes > limit:
            raise QuotaExceededError(
                "sum of nominal sizes would exceed capacity * oversub ratio"
            )
        try:
            db.execute(
                "INSERT INTO volumes(name, nominal_bytes) VALUES (?, ?)",
                (name, nominal_bytes),
            )
            db.commit()
        except sqlite3.IntegrityError:
            db.rollback()
            raise VolumeExistsError(f"volume {name!r} already exists") from None

    def delete_volume(self, name: str) -> None:
        db = self._require_open()
        vid = self._volume_id(name)
        # One transaction: map rows vanish and their blocks become free again.
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM block_map WHERE volume_id = ?", (vid,))
        self._hook("delete_map_removed")
        db.execute("DELETE FROM volumes WHERE id = ?", (vid,))
        db.commit()
        self._hook("delete_committed")

    def list_volumes(self) -> list[VolumeInfo]:
        db = self._require_open()
        rows = db.execute(
            "SELECT v.name, v.nominal_bytes, COUNT(m.physical_block) "
            "FROM volumes v LEFT JOIN block_map m ON m.volume_id = v.id "
            "GROUP BY v.id ORDER BY v.name"
        ).fetchall()
        return [
            VolumeInfo(name=r[0], nominal_bytes=r[1], used_bytes=r[2] * BLOCK_SIZE)
            for r in rows
        ]

    def volume_info(self, name: str) -> VolumeInfo:
        db = self._require_open()
        row = db.execute(
            "SELECT v.nominal_bytes, COUNT(m.physical_block) "
            "FROM volumes v LEFT JOIN block_map m ON m.volume_id = v.id "
            "WHERE v.name = ? GROUP BY v.id",
            (name,),
        ).fetchone()
        if row is None:
            raise VolumeNotFoundError(f"no volume named {name!r}")
        return VolumeInfo(
            name=name, nominal_bytes=row[0], used_bytes=row[1] * BLOCK_SIZE
        )

    def _volume_id(self, name: str) -> int:
        db = self._require_open()
        row = db.execute("SELECT id FROM volumes WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise VolumeNotFoundError(f"no volume named {name!r}")
        return int(row[0])

    # -- data path ---------------------------------------------------------

    def write(self, name: str, offset: int, data: bytes) -> None:
        """Write data at an arbitrary offset; the whole write is atomic.

        Strategy: unmapped regions are copy-on-write into free blocks;
        already-mapped regions are overwritten in place under an undo
        journal. A single SQLite transaction at the end commits the new
        block map and drops the journal, so a crash at any point leaves
        the volume either fully "before" or fully "after".
        """
        db = self._require_open()
        vid = self._volume_id(name)
        nominal = self._nominal_bytes(vid)
        if offset < 0 or offset + len(data) > nominal:
            raise OutOfBoundsError("write range exceeds the volume's nominal size")
        if not data:
            return

        first = offset // BLOCK_SIZE
        last = (offset + len(data) - 1) // BLOCK_SIZE
        mapped = self._mapped_blocks(vid, first, last)

        cow: list[int] = []       # logical indices needing a fresh block
        inplace: list[int] = []   # logical indices overwritten in place
        for li in range(first, last + 1):
            (inplace if li in mapped else cow).append(li)

        used = self._used_blocks()
        free = [b for b in range(self._capacity_blocks) if b not in used]
        if len(free) < len(cow):
            # Reject before touching anything: the volume is unchanged.
            raise PoolFullError("not enough free physical blocks for this write")
        cow_targets = {li: free[i] for i, li in enumerate(cow)}

        def segment(li: int) -> tuple[int, bytes]:
            start = max(offset, li * BLOCK_SIZE)
            end = min(offset + len(data), (li + 1) * BLOCK_SIZE)
            return start - li * BLOCK_SIZE, data[start - offset : end - offset]

        # Step 1: write CoW blocks to free slots (never visible until commit).
        for li in cow:
            buf = bytearray(BLOCK_SIZE)
            pos, chunk = segment(li)
            buf[pos : pos + len(chunk)] = chunk
            os.pwrite(self._fd, bytes(buf), cow_targets[li] * BLOCK_SIZE)
        self._hook("cow_data_written")
        os.fsync(self._fd)
        self._hook("cow_data_synced")

        # Step 2: journal old contents of in-place blocks, then overwrite.
        if inplace:
            db.execute("BEGIN IMMEDIATE")
            for li in inplace:
                old = os.pread(self._fd, BLOCK_SIZE, mapped[li] * BLOCK_SIZE)
                db.execute(
                    "INSERT INTO undo(physical_block, data) VALUES (?, ?)",
                    (mapped[li], old),
                )
            db.commit()
            self._hook("undo_committed")
            for li in inplace:
                buf = bytearray(
                    os.pread(self._fd, BLOCK_SIZE, mapped[li] * BLOCK_SIZE)
                )
                pos, chunk = segment(li)
                buf[pos : pos + len(chunk)] = chunk
                os.pwrite(self._fd, bytes(buf), mapped[li] * BLOCK_SIZE)
            os.fsync(self._fd)
            self._hook("inplace_written")

        # Step 3: one atomic commit: publish CoW blocks, drop the journal.
        db.execute("BEGIN IMMEDIATE")
        for li in cow:
            db.execute(
                "INSERT INTO block_map(volume_id, logical_index, physical_block)"
                " VALUES (?, ?, ?)",
                (vid, li, cow_targets[li]),
            )
        if inplace:
            db.execute("DELETE FROM undo")
        db.commit()
        self._hook("committed")

    def read(self, name: str, offset: int, length: int) -> bytes:
        db = self._require_open()
        vid = self._volume_id(name)
        nominal = self._nominal_bytes(vid)
        if offset < 0 or length < 0 or offset + length > nominal:
            raise OutOfBoundsError("read range exceeds the volume's nominal size")
        if length == 0:
            return b""
        first = offset // BLOCK_SIZE
        last = (offset + length - 1) // BLOCK_SIZE
        mapped = self._mapped_blocks(vid, first, last)
        out = bytearray()
        for li in range(first, last + 1):
            start = max(offset, li * BLOCK_SIZE)
            end = min(offset + length, (li + 1) * BLOCK_SIZE)
            if li in mapped:
                block = os.pread(self._fd, BLOCK_SIZE, mapped[li] * BLOCK_SIZE)
                out += block[start - li * BLOCK_SIZE : end - li * BLOCK_SIZE]
            else:
                out += b"\x00" * (end - start)
        return bytes(out)

    def _nominal_bytes(self, vid: int) -> int:
        db = self._require_open()
        row = db.execute(
            "SELECT nominal_bytes FROM volumes WHERE id = ?", (vid,)
        ).fetchone()
        assert row is not None
        return int(row[0])

    def _mapped_blocks(self, vid: int, first: int, last: int) -> dict[int, int]:
        db = self._require_open()
        rows = db.execute(
            "SELECT logical_index, physical_block FROM block_map "
            "WHERE volume_id = ? AND logical_index BETWEEN ? AND ?",
            (vid, first, last),
        ).fetchall()
        return {int(li): int(pb) for li, pb in rows}

    def _used_blocks(self) -> set[int]:
        db = self._require_open()
        rows = db.execute("SELECT physical_block FROM block_map").fetchall()
        return {int(r[0]) for r in rows}

    def _hook(self, label: str) -> None:
        if self._crash_hook is not None:
            self._crash_hook(label)

    # -- introspection -----------------------------------------------------

    def pool_info(self) -> PoolInfo:
        self._require_open()
        return PoolInfo(
            capacity_bytes=self._capacity_blocks * BLOCK_SIZE,
            used_bytes=len(self._used_blocks()) * BLOCK_SIZE,
            oversub_ratio=self._oversub_ratio,
        )

    def fsck(self) -> None:
        """Verify pool invariants; raise PoolError on any inconsistency."""
        db = self._require_open()
        problems: list[str] = []

        rows = db.execute(
            "SELECT physical_block, COUNT(*) c FROM block_map "
            "GROUP BY physical_block HAVING c > 1"
        ).fetchall()
        if rows:
            problems.append(f"blocks mapped more than once: {[r[0] for r in rows]}")

        row = db.execute(
            "SELECT COUNT(*), COUNT(DISTINCT physical_block) FROM block_map"
        ).fetchone()
        if row[0] != row[1]:
            problems.append("block_map contains duplicate physical blocks")

        rows = db.execute(
            "SELECT physical_block FROM block_map "
            "WHERE physical_block < 0 OR physical_block >= ?",
            (self._capacity_blocks,),
        ).fetchall()
        if rows:
            problems.append(f"blocks out of range: {[r[0] for r in rows]}")

        rows = db.execute(
            "SELECT m.volume_id, m.logical_index FROM block_map m "
            "JOIN volumes v ON v.id = m.volume_id "
            "WHERE m.logical_index < 0 OR m.logical_index >= (v.nominal_bytes + ? - 1) / ?",
            (BLOCK_SIZE, BLOCK_SIZE),
        ).fetchall()
        if rows:
            problems.append(f"map entries beyond nominal size: {rows}")

        pending = db.execute("SELECT COUNT(*) FROM undo").fetchone()[0]
        if pending:
            problems.append(f"{pending} unrecovered undo journal rows")

        if problems:
            raise PoolError("fsck failed: " + "; ".join(problems))
