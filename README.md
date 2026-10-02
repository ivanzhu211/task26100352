# thinpool — 精简配置存储池库

供平台存储管理服务调用的 Python 库：一个存储池对应一个固定大小的本地数据文件
（模拟一块盘），元数据存 SQLite。卷按标称大小超分承诺，物理空间按 64 KiB
块在首次写入时才分配。

## 布局

- 数据文件：建池时 `ftruncate` 到物理容量（64 KiB 的整数倍），之后大小不变；
  每个 64 KiB 槽位是一个物理块。
- 元数据文件（SQLite，WAL + `synchronous=FULL`）：
  - `volumes`：卷名、标称大小；
  - `block_map`：`(卷, 逻辑块号) -> 物理块号`，物理块号有 UNIQUE 约束；
  - `undo`：原地覆盖的撤销日志（旧块内容）；
  - 空闲块不单独持久化，由 `block_map` 推导，因此已用量永远等于
    `count(block_map) * 64 KiB`，不可能和实际占用对不上，也不存在
    “分配出去却没人用”的泄漏块。

## 写入的崩溃一致性

一笔写入（可跨多个块、不对齐）按以下顺序执行：

1. 未占用区间走写时复制：新内容写进空闲块，`fsync`。
   提交前崩溃只会在空闲块里留下垃圾，元数据未变，卷保持写入前的样子。
2. 已占用区间走原地覆盖：先把这些块的旧内容写入 `undo` 表并提交，
   再覆盖数据文件并 `fsync`。
3. 最后一个 SQLite 事务原子地完成：发布新块映射 + 清空 `undo` 表。

重新打开池时，若 `undo` 表非空，说明第 3 步没完成，恢复流程把旧内容
写回数据文件——整笔写入表现为“写入前”；否则表现为“写入后”。
不会出
现一半新一半旧。已返回成功的操作（第 3 步已提交）一定保留。

物理块在提交映射前不会被任何卷引用；新卷 / 新区间只有整块写好后才映射，
未写过的区间读出来一律是零，因此永远读不到已删除卷的旧数据。

## 池满行为

- 需要新块的写入在空闲块不足时整笔拒绝（`PoolFullError`），卷内容不变；
- 只覆盖已占用区间的写入不需要新块（原地 + undo 日志），池满时照样成功。

## API 概览

```python
from thinpool import StoragePool, BLOCK_SIZE

pool = StoragePool.create("pool.data", "pool.meta",
                          capacity_bytes=64 * BLOCK_SIZE, oversub_ratio=2.0)
pool.create_volume("tenant-a", 32 * BLOCK_SIZE)
pool.write("tenant-a", 100, b"...")      # 任意偏移、任意长度
data = pool.read("tenant-a", 0, BLOCK_SIZE)
pool.volume_info("tenant-a").used_bytes  # 该卷实际占用
pool.pool_info().used_bytes              # 池实际占用
pool.delete_volume("tenant-a")           # 块立即回收
pool.fsck()                              # 校验一致性（无双重占用、无越界映射等）
pool.close()

pool = StoragePool.open("pool.data", "pool.meta")  # 打开时自动做崩溃恢复
```

## 测试

```bash
python3 -m pytest tests/ -q
```

- `tests/test_pool.py`：功能测试（配额、超分、不对齐读写、池满行为、
  删卷后新卷读不到旧数据等）。
- `tests/test_crash.py` + `tests/crash_worker.py`：在写入 / 删卷的每个
  关键步骤用 `os._exit(9)` 模拟强杀，重新打开后校验：卷内容只能是
  写入前或写入后、池与卷用量和实际占用块数一致、`fsck` 通过。
