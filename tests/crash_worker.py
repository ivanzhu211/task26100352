"""Child-process worker for crash-recovery tests.

Performs exactly one pool operation and dies via os._exit(9) when the
pool's crash hook fires with the label given on the command line,
simulating kill -9 at that precise step.

Usage:
    python crash_worker.py write <data> <meta> <label> <vol> <offset> <seed> <len>
    python crash_worker.py delete <data> <meta> <label> <vol>
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from thinpool import StoragePool  # noqa: E402


def pattern(seed: int, length: int) -> bytes:
    return bytes((seed + i * 7 + i // 251) % 256 for i in range(length))


def main() -> None:
    mode, data_path, meta_path, kill_label = sys.argv[1:5]
    pool = StoragePool.open(data_path, meta_path)

    def hook(label: str) -> None:
        if label == kill_label:
            os._exit(9)

    pool._crash_hook = hook
    if mode == "write":
        name, offset, seed, length = sys.argv[5:9]
        pool.write(name, int(offset), pattern(int(seed), int(length)))
    elif mode == "delete":
        pool.delete_volume(sys.argv[5])
    else:
        raise SystemExit(f"unknown mode {mode}")
    pool.close()


if __name__ == "__main__":
    main()
