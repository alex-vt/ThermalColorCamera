#!/usr/bin/env python3
import fcntl
import hashlib
import os
import re
import subprocess  # nosec B404: required; calls use argv lists without shell
import time
from typing import List, Optional, Sequence, Tuple

_TC001_LOOPBACK_NAME = "TC001 Color Camera"
_TC001_LOOPBACK_KEY_RE = re.compile(r"^TC001 Color Camera \[([0-9a-f]{10})\]$")


def _which(cmd: str) -> str:
    trusted_dirs = (
        "/usr/local/sbin",
        "/usr/local/bin",
        "/usr/sbin",
        "/usr/bin",
        "/sbin",
        "/bin",
    )
    for path_dir in trusted_dirs:
        candidate = os.path.join(path_dir, cmd)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    raise FileNotFoundError(f"{cmd} not found in trusted dirs {':'.join(trusted_dirs)}")


def _run(cmd: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(  # nosec B603: trusted executable/arg vectors only
        cmd, check=check, stdin=subprocess.DEVNULL
    )


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
        raise PermissionError(
            "Must run as root (needs access to device nodes and /run lock " "files)."
        )


def _sysfs_video_dir(video_node: str) -> str:
    video_basename = os.path.basename(video_node)
    return os.path.realpath(f"/sys/class/video4linux/{video_basename}/device")


def _sysfs_find_up(start_dir: str, filenames: Sequence[str]) -> Optional[str]:
    current_dir = start_dir
    while current_dir.startswith("/sys/") and current_dir != "/sys":
        if all(
            os.path.exists(os.path.join(current_dir, filename))
            for filename in filenames
        ):
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


def _tc001_usb_identity(video_node: str) -> str:
    ids = _video_usb_vid_pid(video_node)
    if ids is None:
        raise RuntimeError(f"Unable to identify USB VID:PID for {video_node}")
    vid, pid = ids
    dev_dir = _sysfs_video_dir(video_node)
    found = _sysfs_find_up(dev_dir, ("idVendor", "idProduct"))

    serial = ""
    path_key = ""
    if found:
        path_key = os.path.basename(found)
        serial_path = os.path.join(found, "serial")
        try:
            if os.path.exists(serial_path):
                serial = _read_text(serial_path)
        except Exception:
            serial = ""

    if serial:
        return f"vid={vid:04x};pid={pid:04x};serial={serial}"
    if path_key:
        return f"vid={vid:04x};pid={pid:04x};path={path_key}"

    bus, dev = _video_usb_bus_device_numbers(video_node)
    return f"vid={vid:04x};pid={pid:04x};bus={bus:03d};dev={dev:03d}"


def _tc001_identity_key(video_node: str) -> str:
    identity = _tc001_usb_identity(video_node)
    digest = hashlib.sha1(  # nosec B303: non-cryptographic stable key derivation
        identity.encode("utf-8")
    ).hexdigest()
    return digest[:10]


def _tc001_loopback_name_for_key(key: str) -> str:
    return f"{_TC001_LOOPBACK_NAME} [{key}]"


def _parse_tc001_loopback_name(name: str) -> Tuple[bool, Optional[str]]:
    match = _TC001_LOOPBACK_KEY_RE.fullmatch(name)
    if not match:
        return False, None
    return True, match.group(1)


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


def _list_video_nodes() -> List[str]:
    base = "/sys/class/video4linux"
    try:
        entries = os.listdir(base)
    except OSError:
        return []
    nodes: List[str] = []
    for entry in entries:
        if not entry.startswith("video"):
            continue
        suffix = entry[5:]
        if not suffix.isdigit():
            continue
        nodes.append(f"/dev/{entry}")

    def _video_num(video_path: str) -> int:
        try:
            return int(os.path.basename(video_path).replace("video", ""))
        except Exception:
            return 1_000_000

    return sorted(nodes, key=_video_num)


def _list_tc001_loopback_nodes() -> List[Tuple[int, str, Optional[str]]]:
    nodes: List[Tuple[int, str, Optional[str]]] = []
    base = "/sys/class/video4linux"
    try:
        entries = os.listdir(base)
    except OSError:
        return nodes
    for entry in entries:
        if not entry.startswith("video"):
            continue
        suffix = entry[5:]
        if not suffix.isdigit():
            continue
        class_dir = os.path.join(base, entry)
        name: Optional[str] = None
        dev_real: Optional[str] = None
        try:
            name = _read_text(os.path.join(class_dir, "name"))
            dev_real = os.path.realpath(os.path.join(class_dir, "device"))
        except Exception:
            name = None
            dev_real = None
        if name is None or dev_real is None:
            continue
        is_tc001_name, node_key = _parse_tc001_loopback_name(name)
        if not is_tc001_name:
            continue
        if not dev_real.startswith("/sys/devices/virtual/video4linux/"):
            continue
        nodes.append((int(suffix), f"/dev/{entry}", node_key))
    nodes.sort(key=lambda item: item[0])
    return nodes


def _connected_tc001_identity_keys() -> set[str]:
    keys: set[str] = set()
    for node in _list_video_nodes():
        if not _is_tc001(node):
            continue
        node_key: Optional[str] = None
        try:
            node_key = _tc001_identity_key(node)
        except Exception:
            node_key = None
        if node_key is None:
            continue
        keys.add(node_key)
    return keys


def _video_node_busy(video_node: str) -> bool:
    try:
        target = os.path.realpath(video_node)
    except Exception:
        return False
    if not target:
        return False
    try:
        pids = os.listdir("/proc")
    except OSError:
        return False
    for pid in pids:
        if not pid.isdigit():
            continue
        fd_dir = f"/proc/{pid}/fd"
        try:
            fds = os.listdir(fd_dir)
        except Exception:
            fds = []
        for fd_name in fds:
            fd_path = os.path.join(fd_dir, fd_name)
            same_target = False
            try:
                same_target = os.path.realpath(fd_path) == target
            except Exception:
                same_target = False
            if same_target:
                return True
    return False


def _tc001_loopback_node_key(video_node: str) -> Optional[str]:
    video_basename = os.path.basename(video_node)
    class_dir = f"/sys/class/video4linux/{video_basename}"
    try:
        name = _read_text(os.path.join(class_dir, "name"))
        dev_real = os.path.realpath(os.path.join(class_dir, "device"))
    except Exception:
        return None
    if not dev_real.startswith("/sys/devices/virtual/video4linux/"):
        return None
    is_tc001_name, node_key = _parse_tc001_loopback_name(name)
    if not is_tc001_name:
        return None
    return node_key


def _is_tc001_loopback_node(
    video_node: str, *, expected_key: Optional[str] = None
) -> bool:
    node_key = _tc001_loopback_node_key(video_node)
    if node_key is None:
        return False
    if expected_key is not None and node_key != expected_key:
        return False
    return True


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


def _delete_tc001_loopback_dst_unlocked(
    v4l2loopback_ctl: str,
    dst_video_index: int,
    *,
    expected_key: Optional[str] = None,
    timeout_s: float = 1.0,
) -> bool:
    dst = f"/dev/video{dst_video_index}"
    if not os.path.exists(dst):
        return True
    if not _is_tc001_loopback_node(dst, expected_key=expected_key):
        return False
    _run([v4l2loopback_ctl, "delete", str(dst_video_index)], check=False)
    deadline = time.monotonic() + timeout_s
    while os.path.exists(dst) and time.monotonic() < deadline:
        time.sleep(0.05)
    return not os.path.exists(dst)
