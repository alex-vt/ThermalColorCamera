#!/usr/bin/env python3
import importlib.util
import os
import sys


def _load_common_module():
    common_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tc001-color-camera-common.py")
    spec = importlib.util.spec_from_file_location("tc001_color_camera_common", common_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load common module from {common_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_common = _load_common_module()


def main() -> int:
    _common._require_root()
    v4l2loopback_ctl = _common._which("v4l2loopback-ctl")
    lock_fd = _common._acquire_v4l2loopback_lock()
    try:
        connected_keys = _common._connected_tc001_identity_keys()
        removed = 0
        preserved = 0

        by_key = {}
        for record in _common._list_tc001_loopback_nodes():
            node_key = record[2]
            by_key.setdefault(node_key, []).append(record)

        for node_key, records in by_key.items():
            ordered = sorted(records, key=lambda item: item[0])

            keeper_idx, keeper_node, _ = ordered[0]
            for idx, node, _ in ordered:
                if _common._video_node_busy(node):
                    keeper_idx = idx
                    keeper_node = node
                    break

            for idx, node, _ in ordered:
                if idx == keeper_idx and node == keeper_node:
                    continue
                if _common._video_node_busy(node):
                    preserved += 1
                    continue
                if _common._delete_tc001_loopback_dst_unlocked(v4l2loopback_ctl, idx, expected_key=node_key):
                    removed += 1
                else:
                    preserved += 1

            if node_key in connected_keys:
                preserved += 1
                continue
            if _common._video_node_busy(keeper_node):
                preserved += 1
                continue
            if _common._delete_tc001_loopback_dst_unlocked(v4l2loopback_ctl, keeper_idx, expected_key=node_key):
                removed += 1
            else:
                preserved += 1

        print(f"Cleanup complete: removed={removed} preserved={preserved}", file=sys.stderr)
    finally:
        try:
            os.close(lock_fd)
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
