"""崩溃测试子进程：在指定钩子点用 os._exit 模拟进程被强杀。

用法: python _crash_child.py <repo_root> <pool_path> <hook_point> <op> [op参数]
  op=write  : <volume> <offset> <seed> <length>
  op=delete : <volume>
"""

import os
import sys

sys.path.insert(0, sys.argv[1])

from thinpool import StoragePool  # noqa: E402


def pattern(seed: int, n: int) -> bytes:
    return bytes(((i * 131 + seed * 17) & 0xFF) for i in range(n))


def main() -> None:
    pool_path, hook_point, op = sys.argv[2], sys.argv[3], sys.argv[4]

    def hook(point: str) -> None:
        if point == hook_point:
            os._exit(99)  # 模拟 SIGKILL：不刷新、不清理

    pool = StoragePool.open(pool_path, crash_hook=hook)
    if op == "write":
        volume, offset, seed, length = (
            sys.argv[5],
            int(sys.argv[6]),
            int(sys.argv[7]),
            int(sys.argv[8]),
        )
        pool.write(volume, offset, pattern(seed, length))
    elif op == "delete":
        pool.delete_volume(sys.argv[5])
    else:
        raise AssertionError(f"unknown op {op}")
    pool.close()
    os._exit(0)


if __name__ == "__main__":
    main()
