#!/usr/bin/env python3
import argparse
import importlib.util
import os
import re
import sys
import time
from typing import List, Optional, Sequence, Tuple


def _load_common_module():
    common_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tc001-color-camera-common.py")
    spec = importlib.util.spec_from_file_location("tc001_color_camera_common", common_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load common module from {common_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_common = _load_common_module()

_which = _common._which
_run = _common._run
_read_text = _common._read_text
_parse_nonnegative_int = _common._parse_nonnegative_int
_require_root = _common._require_root
_sysfs_video_dir = _common._sysfs_video_dir
_video_usb_vid_pid = _common._video_usb_vid_pid
_video_usb_bus_device_numbers = _common._video_usb_bus_device_numbers
_is_tc001 = _common._is_tc001
_tc001_identity_key = _common._tc001_identity_key
_tc001_loopback_name_for_key = _common._tc001_loopback_name_for_key
_lowest_free_video_nr = _common._lowest_free_video_nr
_list_video_nodes = _common._list_video_nodes
_list_tc001_loopback_nodes = _common._list_tc001_loopback_nodes
_connected_tc001_identity_keys = _common._connected_tc001_identity_keys
_video_node_busy = _common._video_node_busy
_tc001_loopback_node_key = _common._tc001_loopback_node_key
_is_tc001_loopback_node = _common._is_tc001_loopback_node
_acquire_v4l2loopback_lock = _common._acquire_v4l2loopback_lock
_delete_tc001_loopback_dst_unlocked = _common._delete_tc001_loopback_dst_unlocked


def _sysfs_sibling_devices(preferred: str) -> List[str]:
    video_basename = os.path.basename(preferred)
    sys_link = f"/sys/class/video4linux/{video_basename}/device"
    try:
        dev_real = os.path.realpath(sys_link)
    except OSError:
        return [preferred]

    base = "/sys/class/video4linux"
    try:
        entries = os.listdir(base)
    except OSError:
        return [preferred]

    candidates: List[str] = []
    for entry in entries:
        if not entry.startswith("video"):
            continue
        try:
            entry_real = os.path.realpath(os.path.join(base, entry, "device"))
        except OSError:
            continue
        if entry_real == dev_real:
            candidates.append(f"/dev/{entry}")

    def _video_num(video_path: str) -> int:
        try:
            return int(os.path.basename(video_path).replace("video", ""))
        except Exception:
            return 1_000_000

    candidates = sorted(set(candidates), key=_video_num)
    if preferred in candidates:
        candidates.remove(preferred)
        candidates.insert(0, preferred)
    return candidates or [preferred]


def _wait_for_tc001_device_nodes(video_node: str, timeout_s: float = 1.0) -> None:
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        nodes = _sysfs_sibling_devices(video_node)
        indices = set()
        for node in nodes:
            video_basename = os.path.basename(node)
            idx_path = f"/sys/class/video4linux/{video_basename}/index"
            try:
                indices.add(_read_text(idx_path))
            except Exception:
                continue
        if "0" in indices and "1" in indices:
            return
        time.sleep(0.05)


def _loopback_state(node_key: Optional[str], connected_keys: set[str], busy: bool) -> Optional[str]:
    if node_key is None:
        return None
    if node_key in connected_keys:
        return "active"
    return "orphan-busy" if busy else "orphan-idle"


def _create_loopback_device(
    v4l2loopback_ctl: str,
    dst_video_index: int,
    *,
    loopback_name: str,
    expected_key: Optional[str] = None,
    timeout_s: float = 2.5,
) -> str:
    dst = f"/dev/video{dst_video_index}"
    _run([v4l2loopback_ctl, "add", "--name", loopback_name, "--exclusive-caps", "1", str(dst_video_index)])
    deadline = time.monotonic() + timeout_s
    seen_path = False
    while time.monotonic() < deadline:
        if os.path.exists(dst):
            seen_path = True
            if _is_tc001_loopback_node(dst, expected_key=expected_key):
                return dst
        time.sleep(0.05)
    _run([v4l2loopback_ctl, "delete", str(dst_video_index)], check=False)
    if seen_path:
        raise RuntimeError(
            f"Created destination --dst-video-index {dst_video_index} ({dst}) is not a TC001 loopback node"
        )
    raise FileNotFoundError(dst)


def _write_runtime_dropin(
    *,
    src_video_index: int,
    dst_video_index: int,
    dst_name_key: str,
    allowed_video_nodes: Sequence[str],
    bus: int,
    dev: int,
) -> str:
    unit_name = f"tc001-color-camera@{src_video_index}.service"
    dropin_dir = f"/run/systemd/system/{unit_name}.d"
    os.makedirs(dropin_dir, mode=0o755, exist_ok=True)
    dropin_path = os.path.join(dropin_dir, "10-launcher.conf")

    dst = f"/dev/video{dst_video_index}"
    nodes = sorted(set(list(allowed_video_nodes) + [dst]))
    lines: List[str] = [
        "[Service]",
        (
            f'Environment="TC001_LAUNCH_ARGS=--dst-video-index {dst_video_index} '
            f'--dst-name-key {dst_name_key} --skip-modprobe --dst-precreated"'
        ),
        "DevicePolicy=closed",
        "DeviceAllow=",
    ]
    for node in nodes:
        lines.append(f"DeviceAllow={node} rw")
    lines.append(f"DeviceAllow=/dev/bus/usb/{bus:03d}/{dev:03d} rw")
    lines.extend(
        [
            "CapabilityBoundingSet=",
            "AmbientCapabilities=",
            "NoNewPrivileges=yes",
            "",
        ]
    )
    with open(dropin_path, "w", encoding="utf-8") as file_handle:
        file_handle.write("\n".join(lines))
    return dropin_path


def _runtime_dropin_path(src_video_index: int) -> str:
    unit_name = f"tc001-color-camera@{src_video_index}.service"
    return f"/run/systemd/system/{unit_name}.d/10-launcher.conf"


def _runtime_dst_video_index(src_video_index: int) -> Optional[int]:
    dropin_path = _runtime_dropin_path(src_video_index)
    try:
        content = _read_text(dropin_path)
    except Exception:
        return None
    match = re.search(r"--dst-video-index\s+(\d+)", content)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _runtime_dst_name_key(src_video_index: int) -> Optional[str]:
    dropin_path = _runtime_dropin_path(src_video_index)
    try:
        content = _read_text(dropin_path)
    except Exception:
        return None
    match = re.search(r"--dst-name-key\s+([0-9a-f]{10})", content)
    if not match:
        return None
    return str(match.group(1))


def _remove_runtime_dropin(src_video_index: int) -> bool:
    dropin_path = _runtime_dropin_path(src_video_index)
    dropin_dir = os.path.dirname(dropin_path)
    removed = False
    try:
        os.unlink(dropin_path)
        removed = True
    except FileNotFoundError:
        return False
    except Exception:
        return False
    try:
        os.rmdir(dropin_dir)
    except Exception:
        pass
    return removed


def _reconcile_dst_video_index(v4l2loopback_ctl: str, *, physical_key: str) -> int:
    loopbacks = _list_tc001_loopback_nodes()
    connected_keys = _connected_tc001_identity_keys()
    connected_keys.add(physical_key)

    by_key: dict[str, List[Tuple[int, str, Optional[str]]]] = {}
    for record in loopbacks:
        node_key = record[2]
        if node_key is None:
            continue
        by_key.setdefault(node_key, []).append(record)

    reuse_dst_video_index: Optional[int] = None
    for node_key, records in by_key.items():
        ordered = sorted(records, key=lambda item: item[0])
        keeper_idx, keeper_node, _ = ordered[0]
        for idx, node, _ in ordered:
            if _video_node_busy(node):
                keeper_idx = idx
                keeper_node = node
                break
        for idx, node, _ in ordered:
            if idx == keeper_idx and node == keeper_node:
                continue
            if _video_node_busy(node):
                continue
            _delete_tc001_loopback_dst_unlocked(v4l2loopback_ctl, idx, expected_key=node_key)
        if node_key == physical_key:
            reuse_dst_video_index = keeper_idx

    loopbacks = _list_tc001_loopback_nodes()
    for idx, node, node_key in loopbacks:
        state = _loopback_state(node_key, connected_keys, _video_node_busy(node))
        if state != "orphan-idle":
            continue
        _delete_tc001_loopback_dst_unlocked(v4l2loopback_ctl, idx, expected_key=node_key)

    if reuse_dst_video_index is not None:
        reuse_node = f"/dev/video{reuse_dst_video_index}"
        if _is_tc001_loopback_node(reuse_node, expected_key=physical_key):
            return reuse_dst_video_index

    dst_video_index = _lowest_free_video_nr()
    _create_loopback_device(
        v4l2loopback_ctl,
        dst_video_index,
        loopback_name=_tc001_loopback_name_for_key(physical_key),
        expected_key=physical_key,
    )
    return dst_video_index


def _launch_runtime_instance(src_video_index: int) -> int:
    systemctl = _which("systemctl")
    modprobe = _which("modprobe")
    v4l2loopback_ctl = _which("v4l2loopback-ctl")

    src = f"/dev/video{src_video_index}"
    if not _is_tc001(src):
        ids = _video_usb_vid_pid(src)
        got = f"{ids[0]:04x}:{ids[1]:04x}" if ids else "unknown"
        raise RuntimeError(
            f"--src-video-index {src_video_index} (/dev/video{src_video_index}) is not a TC001 camera "
            f"(expected 0bda:5830, got {got})"
        )

    _wait_for_tc001_device_nodes(src, timeout_s=1.0)
    sibling_nodes = [n for n in _sysfs_sibling_devices(src) if _is_tc001(n)]
    if src not in sibling_nodes:
        sibling_nodes.insert(0, src)
    physical_key = _tc001_identity_key(src)

    if not os.path.isdir("/sys/module/v4l2loopback"):
        _run([modprobe, "v4l2loopback", "devices=0"])

    v4l2_lock_fd = _acquire_v4l2loopback_lock()
    dst_video_index = -1
    try:
        dst_video_index = _reconcile_dst_video_index(v4l2loopback_ctl, physical_key=physical_key)
    finally:
        try:
            os.close(v4l2_lock_fd)
        except Exception:
            pass

    unit_name = f"tc001-color-camera@{src_video_index}.service"
    dropin_path = ""
    try:
        bus, dev = _video_usb_bus_device_numbers(src)
        _remove_runtime_dropin(src_video_index)
        dropin_path = _write_runtime_dropin(
            src_video_index=src_video_index,
            dst_video_index=dst_video_index,
            dst_name_key=physical_key,
            allowed_video_nodes=sibling_nodes,
            bus=bus,
            dev=dev,
        )
        _run([systemctl, "daemon-reload"])
        _run([systemctl, "start", unit_name])
    except Exception:
        _run([v4l2loopback_ctl, "delete", str(dst_video_index)], check=False)
        _remove_runtime_dropin(src_video_index)
        _run([systemctl, "daemon-reload"], check=False)
        raise

    print(
        f"Launch: {unit_name} src-index={src_video_index} (/dev/video{src_video_index}) "
        f"dst-index={dst_video_index} (/dev/video{dst_video_index}) key={physical_key}",
        file=sys.stderr,
    )
    print(f"Drop-in: {dropin_path}", file=sys.stderr)
    return 0


def _cleanup_runtime_instance(src_video_index: int) -> int:
    systemctl = _which("systemctl")
    v4l2loopback_ctl = _which("v4l2loopback-ctl")
    unit_name = f"tc001-color-camera@{src_video_index}.service"
    dst_video_index = _runtime_dst_video_index(src_video_index)
    dst_name_key = _runtime_dst_name_key(src_video_index)
    _run([systemctl, "stop", unit_name], check=False)
    if dst_video_index is not None:
        dst = f"/dev/video{dst_video_index}"
        v4l2_lock_fd = _acquire_v4l2loopback_lock()
        try:
            node_key = dst_name_key or _tc001_loopback_node_key(dst)
            connected_keys = _connected_tc001_identity_keys()
            if node_key is not None and node_key in connected_keys:
                print(
                    (
                        f"Cleanup: preserving {dst} because key={node_key} "
                        "is still mapped to a connected TC001 camera"
                    ),
                    file=sys.stderr,
                )
            elif _delete_tc001_loopback_dst_unlocked(v4l2loopback_ctl, dst_video_index, expected_key=node_key):
                print(
                    f"Cleanup: removed TC001 loopback dst-index={dst_video_index} ({dst})",
                    file=sys.stderr,
                )
            else:
                print(
                    f"Cleanup: preserving {dst} because it is not a matching removable TC001 loopback node",
                    file=sys.stderr,
                )
        finally:
            try:
                os.close(v4l2_lock_fd)
            except Exception:
                pass
    if _remove_runtime_dropin(src_video_index):
        _run([systemctl, "daemon-reload"], check=False)
        print(f"Cleanup: removed runtime drop-in for tc001-color-camera@{src_video_index}.service", file=sys.stderr)
    return 0


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="TC001 hotplug launcher")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--src-video-index",
        help="TC001 source video index (number only, e.g. 2)",
    )
    mode.add_argument(
        "--cleanup-src-video-index",
        help="Cleanup source instance by index: stop runtime service, remove mapped TC001 loopback node, and remove drop-in",
    )
    args = parser.parse_args(list(argv))
    _require_root()
    if args.src_video_index is not None:
        src_video_index = _parse_nonnegative_int(str(args.src_video_index), "--src-video-index")
        return _launch_runtime_instance(src_video_index)

    cleanup_src_video_index = _parse_nonnegative_int(
        str(args.cleanup_src_video_index), "--cleanup-src-video-index"
    )
    return _cleanup_runtime_instance(cleanup_src_video_index)


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
