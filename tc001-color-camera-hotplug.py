#!/usr/bin/env python3
import argparse
import fcntl
import os
import re
import subprocess
import sys
import time
from typing import List, Optional, Sequence, Tuple


def _which(cmd: str) -> str:
    trusted_dirs = ("/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin")
    for path_dir in trusted_dirs:
        candidate = os.path.join(path_dir, cmd)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    raise FileNotFoundError(f"{cmd} not found in trusted dirs {':'.join(trusted_dirs)}")


def _run(cmd: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, stdin=subprocess.DEVNULL)


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as file_handle:
        return file_handle.read().strip()


def _parse_nonnegative_int(raw: str, flag_name: str) -> int:
    text = raw.strip()
    if not text.isdigit():
        raise ValueError(f"{flag_name} must be a non-negative integer (e.g. 2)")
    return int(text)


def _require_root() -> None:
    if os.geteuid() != 0:
        raise PermissionError("Must run as root (needs access to device nodes and /run lock files).")


def _sysfs_video_dir(video_node: str) -> str:
    video_basename = os.path.basename(video_node)
    return os.path.realpath(f"/sys/class/video4linux/{video_basename}/device")


def _sysfs_find_up(start_dir: str, filenames: Sequence[str]) -> Optional[str]:
    current_dir = start_dir
    while current_dir.startswith("/sys/") and current_dir != "/sys":
        if all(os.path.exists(os.path.join(current_dir, filename)) for filename in filenames):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    return None


def _video_usb_vid_pid(video_node: str) -> Optional[Tuple[int, int]]:
    try:
        dev_dir = _sysfs_video_dir(video_node)
    except Exception:
        return None

    found = _sysfs_find_up(dev_dir, ("idVendor", "idProduct"))
    if not found:
        return None
    try:
        vid = int(_read_text(os.path.join(found, "idVendor")), 16)
        pid = int(_read_text(os.path.join(found, "idProduct")), 16)
        return vid, pid
    except Exception:
        return None


def _video_usb_bus_device_numbers(video_node: str) -> Tuple[int, int]:
    dev_dir = _sysfs_video_dir(video_node)
    found = _sysfs_find_up(dev_dir, ("busnum", "devnum"))
    if not found:
        raise RuntimeError(f"Unable to locate USB busnum/devnum for {video_node}")
    bus = int(_read_text(os.path.join(found, "busnum")))
    dev = int(_read_text(os.path.join(found, "devnum")))
    return bus, dev


def _is_tc001(video_node: str) -> bool:
    ids = _video_usb_vid_pid(video_node)
    return ids == (0x0BDA, 0x5830)


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


def _lowest_free_video_nr() -> int:
    used = set()
    try:
        entries = os.listdir("/dev")
    except OSError as exc:
        raise RuntimeError(f"Cannot list /dev: {exc}") from exc

    for name in entries:
        if not name.startswith("video"):
            continue
        suffix = name[5:]
        if suffix.isdigit():
            used.add(int(suffix))

    n = 0
    while n in used:
        n += 1
    return n


def _is_tc001_loopback_node(video_node: str) -> bool:
    video_basename = os.path.basename(video_node)
    class_dir = f"/sys/class/video4linux/{video_basename}"
    try:
        name = _read_text(os.path.join(class_dir, "name"))
        dev_real = os.path.realpath(os.path.join(class_dir, "device"))
    except Exception:
        return False
    if name != "TC001 Color Camera":
        return False
    return dev_real.startswith("/sys/devices/virtual/video4linux/")


def _acquire_v4l2loopback_lock() -> int:
    lock_path = "/run/tc001-color-camera-v4l2loopback.lock"
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    fd = os.open(lock_path, flags, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except Exception:
        os.close(fd)
        raise
    return fd


def _create_loopback_device(v4l2loopback_ctl: str, dst_video_index: int, *, timeout_s: float = 2.5) -> str:
    dst = f"/dev/video{dst_video_index}"
    _run([v4l2loopback_ctl, "add", "--name", "TC001 Color Camera", "--exclusive-caps", "1", str(dst_video_index)])
    deadline = time.monotonic() + timeout_s
    seen_path = False
    while time.monotonic() < deadline:
        if os.path.exists(dst):
            seen_path = True
            if _is_tc001_loopback_node(dst):
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
        f'Environment="TC001_LAUNCH_ARGS=--dst-video-index {dst_video_index} --skip-modprobe --dst-precreated"',
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


def _delete_tc001_loopback_dst(v4l2loopback_ctl: str, dst_video_index: int, *, timeout_s: float = 1.0) -> bool:
    dst = f"/dev/video{dst_video_index}"
    v4l2_lock_fd = _acquire_v4l2loopback_lock()
    try:
        if not os.path.exists(dst):
            return True
        if not _is_tc001_loopback_node(dst):
            return False
        _run([v4l2loopback_ctl, "delete", str(dst_video_index)], check=False)
        deadline = time.monotonic() + timeout_s
        while os.path.exists(dst) and time.monotonic() < deadline:
            time.sleep(0.05)
        return not os.path.exists(dst)
    finally:
        try:
            os.close(v4l2_lock_fd)
        except Exception:
            pass


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

    if not os.path.isdir("/sys/module/v4l2loopback"):
        _run([modprobe, "v4l2loopback", "devices=0"])

    v4l2_lock_fd = _acquire_v4l2loopback_lock()
    dst_video_index = -1
    try:
        dst_video_index = _lowest_free_video_nr()
        _create_loopback_device(v4l2loopback_ctl, dst_video_index)
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
        f"dst-index={dst_video_index} (/dev/video{dst_video_index})",
        file=sys.stderr,
    )
    print(f"Drop-in: {dropin_path}", file=sys.stderr)
    return 0


def _cleanup_runtime_instance(src_video_index: int) -> int:
    systemctl = _which("systemctl")
    v4l2loopback_ctl = _which("v4l2loopback-ctl")
    unit_name = f"tc001-color-camera@{src_video_index}.service"
    dst_video_index = _runtime_dst_video_index(src_video_index)
    _run([systemctl, "stop", unit_name], check=False)
    if dst_video_index is not None:
        dst = f"/dev/video{dst_video_index}"
        if _delete_tc001_loopback_dst(v4l2loopback_ctl, dst_video_index):
            print(
                f"Cleanup: removed TC001 loopback dst-index={dst_video_index} ({dst})",
                file=sys.stderr,
            )
        else:
            print(
                f"Cleanup: preserving {dst} because it is not a TC001 loopback node",
                file=sys.stderr,
            )
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
