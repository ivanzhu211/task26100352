# thinpool — 精简单配置持久卷存储池

供平台存储管理服务调用的 Python 库。卷数据存放在一个固定大小的本地数据文件中，
元数据存放在 SQLite 中，写入与删卷在进程被强杀后仍保证池一致。

## 快速开始

```python
from thinpool import StoragePool, BLOCK_SIZE

# 建池：容量 = 物理盘大小，必须是 64 KiB 的整数倍，之后不变
pool = StoragePool.create("/var/lib/mypool", BLOCK_SIZE * 1024, oversub_ratio=2.0)

pool.create_volume("tenant-a", BLOCK_SIZE * 512)   # 只登记标称大小，不占物理空间
pool.write("tenant-a", 100, b"hello")              # 任意偏移、任意长度，不要求对齐
data = pool.read("tenant-a", 0, 4096)              # 从未写过的部分读出来全是零

print(pool.pool_used_bytes())                      # 池实际用量 = 占用块数 * 64 KiB
print(pool.volume_used_bytes("tenant-a"))          # 单卷实际用量
pool.delete_volume("tenant-a")                     # 释放的块立即可分给别的卷
pool.close()

pool = StoragePool.open("/var/lib/mypool")         # 重开时自动崩溃恢复
pool.check_consistency()                           # 校验无泄漏块、无一块多主
```

## 磁盘布局（池目录内）

| 文件 | 作用 |
| --- | --- |
| `pool.data` | 固定大小的数据文件，大小即物理容量，按 64 KiB 分块 |
| `pool.meta` | SQLite 元数据：卷表、块映射、块状态、写入日志 |
| `pool.journal` | 原地覆盖前保存被覆盖块的旧镜像，用于崩溃回滚 |

## 崩溃安全协议

写入一笔数据（可跨多个 64 KiB 块）按以下顺序执行：

1. **日志事务**（SQLite 原子提交）：新块标记为 `pending`，写入日志行（含代次号）。
2. **保存旧镜像**：原地覆盖的块先把旧内容写入 `pool.journal` 并 fsync。
3. **写数据**：新块先整块填零再覆盖（保证读不到已删除卷的旧数据）；fsync 数据文件。
4. **提交事务**（SQLite 原子提交点）：应用块映射、新块转 `used`、清除日志。

在第 4 步之前被强杀，重开时恢复流程回滚到写入前（旧镜像写回、`pending` 块归还）；
之后被强杀，写入完整保留。删卷是单个 SQLite 事务，天然原子。
日志文件带代次号，恢复时只认当前日志事务的代次，不会误用上一次的残留文件。

## 保证

- 被打断的写入，卷内容只能是写入前或写入后的样子，不会一半新一半旧。
- 没有分配出去却没人用的块，也没有一块同时被两个卷占用。
- 池和每个卷的实际用量始终等于真实占用的块数乘 64 KiB。
- 已经返回成功的操作一定保留（SQLite `synchronous=FULL` + 数据文件 fsync）。
- 池满时，需要新块的写入整笔拒绝且卷内容不变；只覆盖已占用区间的写入仍成功。

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖：跨多块不对齐写入/删卷中途强杀后重开的一致性与原子性、
残留日志文件误用防护、删卷后新建卷未写区域全零、池满行为、超分上限等。
