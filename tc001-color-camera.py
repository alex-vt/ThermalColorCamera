#!/usr/bin/env python3
import argparse
import ctypes
import ctypes.util
import errno
import fcntl
import hashlib
import os
import re
import select
import signal
import subprocess
import sys
import time
import threading
from array import array
from typing import Iterable, List, Optional, Sequence, Tuple


_THERMAL_SHM_DIR = "/dev/shm/sensors/camera/thermal"
_THERMAL_SHM_FILES = {
    "min": os.path.join(_THERMAL_SHM_DIR, "temperature_min"),
    "max": os.path.join(_THERMAL_SHM_DIR, "temperature_max"),
    "median": os.path.join(_THERMAL_SHM_DIR, "temperature_median"),
}
_THERMAL_SHM_ZONE_IDS = range(1, 10)
_THERMAL_SHM_ZONE_KEYS = ("min", "median", "max")
_THERMAL_TELEMETRY_DISABLED = False
_TC001_LOOPBACK_NAME = "TC001 Color Camera"
_TC001_LOOPBACK_KEY_RE = re.compile(r"^TC001 Color Camera \[([0-9a-f]{10})\]$")


def _thermal_zone_file(zone_id: int, key: str) -> str:
    if zone_id not in _THERMAL_SHM_ZONE_IDS:
        raise ValueError(f"zone_id must be 1..9, got {zone_id}")
    if key not in _THERMAL_SHM_ZONE_KEYS:
        raise ValueError(f"key must be one of {_THERMAL_SHM_ZONE_KEYS}, got {key}")
    return os.path.join(_THERMAL_SHM_DIR, f"temperature_zone{zone_id}_{key}")


def _split_3_bounds(size: int) -> Tuple[int, int, int, int]:
    if size <= 0:
        raise ValueError("size must be > 0")
    # Split into 3 zones with a 1/4, 1/2, 1/4 ratio (central zone largest).
    # Any remainder is added to the central zone to keep symmetry
    a = size // 4
    b = size - (2 * a)
    b0 = 0
    b1 = a
    b2 = a + b
    b3 = size
    return (b0, b1, b2, b3)


def _median_from_sorted(values: Sequence[int]) -> float:
    n = len(values)
    if n == 0:
        raise ValueError("median of empty sequence")
    if n % 2 == 0:
        return (values[(n // 2) - 1] + values[n // 2]) / 2.0
    return float(values[n // 2])


def _thermal_stats_k64(thermal_u16: memoryview, width: int, height: int) -> Tuple[Tuple[float, float, float], dict[int, Tuple[float, float, float]]]:
    temps_k = list(thermal_u16)
    temps_k.sort()
    overall = (float(temps_k[0]), float(temps_k[-1]), _median_from_sorted(temps_k))

    x0, x1, x2, x3 = _split_3_bounds(width)
    y0, y1, y2, y3 = _split_3_bounds(height)
    x_bounds = (x0, x1, x2, x3)
    y_bounds = (y0, y1, y2, y3)

    zones: dict[int, Tuple[float, float, float]] = {}
    for zone_id in _THERMAL_SHM_ZONE_IDS:
        row = (zone_id - 1) // 3
        col = (zone_id - 1) % 3
        xs = x_bounds[col]
        xe = x_bounds[col + 1]
        ys = y_bounds[row]
        ye = y_bounds[row + 1]

        zone_vals: List[int] = []
        for y in range(ys, ye):
            i0 = (y * width) + xs
            i1 = (y * width) + xe
            zone_vals.extend(thermal_u16[i0:i1])
        zone_vals.sort()
        zones[zone_id] = (float(zone_vals[0]), float(zone_vals[-1]), _median_from_sorted(zone_vals))

    return overall, zones


def _rotated_dimensions(width: int, height: int, rotate: str) -> Tuple[int, int]:
    if rotate == "0":
        return (width, height)
    if rotate == "90" or rotate == "270":
        return (height, width)
    if rotate == "180":
        return (width, height)
    raise ValueError(f"rotate must be one of 0/90/180/270, got {rotate}")


def _center_crop_bounds(in_w: int, in_h: int, out_w: int, out_h: int) -> Tuple[int, int, int, int]:
    if in_w <= 0 or in_h <= 0:
        raise ValueError("in_w and in_h must be > 0")
    if out_w <= 0 or out_h <= 0:
        raise ValueError("out_w and out_h must be > 0")

    in_ar_cmp = in_w * out_h - in_h * out_w
    if in_ar_cmp == 0:
        return (0, 0, in_w, in_h)

    if in_ar_cmp > 0:
        # Crop width, keep full height.
        crop_h = in_h
        crop_w = (in_h * out_w + (out_h // 2)) // out_h
        crop_w = min(in_w, max(1, crop_w))
    else:
        # Crop height, keep full width.
        crop_w = in_w
        crop_h = (in_w * out_h + (out_w // 2)) // out_w
        crop_h = min(in_h, max(1, crop_h))

    x0 = max(0, (in_w - crop_w) // 2)
    y0 = max(0, (in_h - crop_h) // 2)
    return (x0, y0, crop_w, crop_h)


def _thermal_stats_k64_visible(
    thermal_u16: memoryview,
    width: int,
    height: int,
    *,
    rotate: str,
    out_w: int,
    out_h: int,
) -> Tuple[Tuple[float, float, float], dict[int, Tuple[float, float, float]]]:
    # Computing stats on the portion of the thermal image visible in the output video.
    rot_w, rot_h = _rotated_dimensions(width, height, rotate)
    crop_x, crop_y, crop_w, crop_h = _center_crop_bounds(rot_w, rot_h, out_w, out_h)

    # Build a contiguous u16 buffer in the same orientation/crop as the output video.
    visible = array("H")
    for yr in range(crop_y, crop_y + crop_h):
        for xr in range(crop_x, crop_x + crop_w):
            if rotate == "0":
                x = xr
                y = yr
            elif rotate == "90":
                x = yr
                y = height - 1 - xr
            elif rotate == "180":
                x = width - 1 - xr
                y = height - 1 - yr
            elif rotate == "270":
                x = width - 1 - yr
                y = xr
            else:
                raise ValueError(f"rotate must be one of 0/90/180/270, got {rotate}")

            visible.append(int(thermal_u16[(y * width) + x]))

    return _thermal_stats_k64(memoryview(visible), crop_w, crop_h)


def _which(cmd: str) -> str:
    trusted_dirs = ("/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin")
    for path_dir in trusted_dirs:
        candidate = os.path.join(path_dir, cmd)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    raise FileNotFoundError(f"{cmd} not found in trusted dirs {':'.join(trusted_dirs)}")


def _clamp_int(name: str, value: int, low: int, high: int) -> int:
    if value < low:
        print(f"{name} clamped to {low} (requested {value})", file=sys.stderr)
        return low
    if value > high:
        print(f"{name} clamped to {high} (requested {value})", file=sys.stderr)
        return high
    return value


def _clamp_float(name: str, value: float, low: float, high: float) -> float:
    if value < low:
        print(f"{name} clamped to {low} (requested {value})", file=sys.stderr)
        return low
    if value > high:
        print(f"{name} clamped to {high} (requested {value})", file=sys.stderr)
        return high
    return value


def _parse_video_index(raw: str, flag_name: str) -> int:
    return _parse_nonnegative_int(raw, flag_name)


def _parse_dst_resolution(resolution: str) -> Tuple[int, int]:
    text = resolution.strip().lower()
    if "x" not in text:
        raise ValueError("--dst-resolution must be WxH, e.g. 640x480")
    w_text, h_text = text.split("x", 1)
    try:
        out_w = int(w_text)
        out_h = int(h_text)
    except ValueError as exc:
        raise ValueError("--dst-resolution must be WxH, e.g. 640x480") from exc
    out_w = _clamp_int("--dst-resolution width", out_w, 128, 2048)
    out_h = _clamp_int("--dst-resolution height", out_h, 128, 2048)
    return (out_w, out_h)


def _parse_nonnegative_int(raw: str, flag_name: str) -> int:
    text = raw.strip()
    if not text.isdigit():
        raise ValueError(f"{flag_name} must be a non-negative integer (e.g. 2)")
    return int(text)


def _parse_ffc_disable_after(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    text = raw.strip().lower()
    if text == "" or text == "none":
        return None
    try:
        delay = float(text)
    except ValueError as exc:
        raise ValueError("--ffc-disable-after must be a number of seconds, 'none', or passed without a value") from exc
    if delay < 0:
        print(f"--ffc-disable-after clamped to 0 (requested {delay})", file=sys.stderr)
        delay = 0.0
    return delay


def _require_root() -> None:
    if os.geteuid() != 0:
        raise PermissionError("Must run as root (needs access to device nodes and /run lock files).")


def _run(cmd: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, stdin=subprocess.DEVNULL)


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as file_handle:
        return file_handle.read().strip()


def _kelvin64_to_celsius(k: float) -> float:
    return (k / 64.0) - 273.15


def _thermal_stats_paths() -> List[str]:
    paths = list(_THERMAL_SHM_FILES.values())
    for zone_id in _THERMAL_SHM_ZONE_IDS:
        for key in _THERMAL_SHM_ZONE_KEYS:
            paths.append(_thermal_zone_file(zone_id, key))
    return paths


def _ensure_thermal_stats_dir() -> None:
    for directory in ("/dev/shm/sensors", "/dev/shm/sensors/camera", _THERMAL_SHM_DIR):
        os.makedirs(directory, mode=0o755, exist_ok=True)


def _write_thermal_text_file(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as file_handle:
        file_handle.write(text)


def _clear_thermal_stats() -> None:
    _ensure_thermal_stats_dir()
    for path in _thermal_stats_paths():
        _write_thermal_text_file(path, "")


def _write_thermal_stats(
    min_c: float,
    max_c: float,
    median_c: float,
    *,
    zone_stats: Optional[dict[int, Tuple[float, float, float]]] = None,
) -> None:
    _ensure_thermal_stats_dir()
    _write_thermal_text_file(_THERMAL_SHM_FILES["min"], f"{min_c:.1f}\n")
    _write_thermal_text_file(_THERMAL_SHM_FILES["max"], f"{max_c:.1f}\n")
    _write_thermal_text_file(_THERMAL_SHM_FILES["median"], f"{median_c:.1f}\n")
    if zone_stats:
        for zone_id in _THERMAL_SHM_ZONE_IDS:
            stats = zone_stats.get(zone_id)
            if stats is None:
                continue
            zone_min_c, zone_max_c, zone_median_c = stats
            _write_thermal_text_file(_thermal_zone_file(zone_id, "min"), f"{zone_min_c:.1f}\n")
            _write_thermal_text_file(_thermal_zone_file(zone_id, "max"), f"{zone_max_c:.1f}\n")
            _write_thermal_text_file(_thermal_zone_file(zone_id, "median"), f"{zone_median_c:.1f}\n")


def _update_thermal_stats_from_frame(
    frame_buf: bytes,
    *,
    picture_bytes: int,
    width: int,
    height: int,
    rotate: str,
    out_w: int,
    out_h: int,
) -> None:
    thermal_u16 = memoryview(frame_buf)[picture_bytes:].cast("H")
    overall_k, zones_k = _thermal_stats_k64_visible(thermal_u16, width, height, rotate=rotate, out_w=out_w, out_h=out_h)
    zones_c = {
        zone_id: (
            _kelvin64_to_celsius(zone_stats_k[0]),
            _kelvin64_to_celsius(zone_stats_k[1]),
            _kelvin64_to_celsius(zone_stats_k[2]),
        )
        for zone_id, zone_stats_k in zones_k.items()
    }
    _write_thermal_stats(
        _kelvin64_to_celsius(overall_k[0]),
        _kelvin64_to_celsius(overall_k[1]),
        _kelvin64_to_celsius(overall_k[2]),
        zone_stats=zones_c,
    )


def _try_thermal_telemetry(operation, *args, **kwargs) -> None:
    global _THERMAL_TELEMETRY_DISABLED
    if _THERMAL_TELEMETRY_DISABLED:
        return
    try:
        # best-effort
        operation(*args, **kwargs)
    except Exception as exc:
        _THERMAL_TELEMETRY_DISABLED = True
        print(f"Thermal telemetry disabled: {exc}", file=sys.stderr)


def _build_palette_lut(points: Sequence[Tuple[float, Tuple[int, int, int]]]) -> List[bytes]:
    if not points or points[0][0] != 0.0 or points[-1][0] != 1.0:
        raise ValueError("palette must start at 0.0 and end at 1.0")

    lut: List[bytes] = []
    j = 0
    for i in range(256):
        t = i / 255.0
        while j + 1 < len(points) and t > points[j + 1][0]:
            j += 1
        t0, c0 = points[j]
        t1, c1 = points[min(j + 1, len(points) - 1)]
        if t1 <= t0:
            r, g, b = c0
        else:
            f = (t - t0) / (t1 - t0)
            r = int(round(c0[0] + (c1[0] - c0[0]) * f))
            g = int(round(c0[1] + (c1[1] - c0[1]) * f))
            b = int(round(c0[2] + (c1[2] - c0[2]) * f))
        lut.append(bytes((r, g, b)))
    return lut


def _colorize_gray_frame(frame: bytes, lut: Sequence[bytes]) -> bytes:
    lo = min(frame)
    hi = max(frame)
    if hi <= lo:
        return b"\x00\x00\x00" * len(frame)

    span = hi - lo
    out = bytearray(len(frame) * 3)
    oi = 0
    for p in frame:
        idx = (p - lo) * 255 // span
        rgb = lut[idx]
        out[oi : oi + 3] = rgb
        oi += 3
    return bytes(out)


def _iter_exact_frames(stream, frame_size: int) -> Iterable[bytes]:
    pending = bytearray()
    while True:
        chunk = stream.read(frame_size - len(pending))
        if not chunk:
            return
        pending.extend(chunk)
        if len(pending) < frame_size:
            continue
        yield bytes(pending)
        pending.clear()


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
    return hashlib.sha1(identity.encode("utf-8")).hexdigest()[:10]


def _tc001_loopback_name_for_key(key: str) -> str:
    return f"{_TC001_LOOPBACK_NAME} [{key}]"


def _parse_tc001_loopback_name(name: str) -> Tuple[bool, Optional[str]]:
    match = _TC001_LOOPBACK_KEY_RE.fullmatch(name)
    if not match:
        return False, None
    return True, match.group(1)


def _parse_dst_name_key(raw: str) -> str:
    key = raw.strip().lower()
    if not re.fullmatch(r"[0-9a-f]{10}", key):
        raise ValueError("--dst-name-key must be 10 lowercase hex characters")
    return key


def _is_tc001_loopback_node(video_node: str, *, expected_key: Optional[str] = None) -> bool:
    video_basename = os.path.basename(video_node)
    class_dir = f"/sys/class/video4linux/{video_basename}"
    try:
        name = _read_text(os.path.join(class_dir, "name"))
        dev_real = os.path.realpath(os.path.join(class_dir, "device"))
    except Exception:
        return False

    is_tc001_name, node_key = _parse_tc001_loopback_name(name)
    if not is_tc001_name:
        return False
    if expected_key is not None and node_key != expected_key:
        return False
    return dev_real.startswith("/sys/devices/virtual/video4linux/")


def _find_tc001_video(prefer: str = "/dev/video0") -> str:
    if os.path.exists(prefer) and _is_tc001(prefer):
        return prefer

    candidates: List[str] = []
    for name in os.listdir("/dev"):
        if not name.startswith("video"):
            continue
        suffix = name[5:]
        if not suffix.isdigit():
            continue
        node = f"/dev/{name}"
        if _is_tc001(node):
            candidates.append(node)

    if not candidates:
        raise RuntimeError("TC001 camera not found (expected USB VID:PID 0bda:5830).")

    def _video_num(video_path: str) -> int:
        return int(os.path.basename(video_path).replace("video", ""))

    candidates = sorted(set(candidates), key=_video_num)
    # Prefer the interface with index 0 when present.
    for node in candidates:
        video_basename = os.path.basename(node)
        idx_path = f"/sys/class/video4linux/{video_basename}/index"
        try:
            if _read_text(idx_path) == "0":
                return node
        except Exception:
            continue
    return candidates[0]


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


def _acquire_single_instance_lock(video_node: str) -> Tuple[int, str]:
    bus, dev = _video_usb_bus_device_numbers(video_node)
    lock_path = f"/run/tc001-color-camera-{bus:03d}-{dev:03d}.lock"
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    fd = os.open(lock_path, flags, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
            raise RuntimeError(f"Already running for this TC001 camera (lock: {lock_path}).") from exc
        raise
    os.ftruncate(fd, 0)
    os.write(fd, f"{os.getpid()}\n".encode("utf-8"))
    return fd, lock_path


def _acquire_v4l2loopback_lock() -> Tuple[int, str]:
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
    return fd, lock_path


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


def _read_exact_with_timeout(stream, frame_size: int, timeout_s: float) -> bytes:
    if timeout_s <= 0:
        raise ValueError("timeout_s must be > 0")
    fd = stream.fileno()
    out = bytearray()
    deadline = time.monotonic() + timeout_s
    while len(out) < frame_size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Timed out after {timeout_s:.1f}s while probing stream")
        try:
            ready, _, _ = select.select([fd], [], [], remaining)
        except InterruptedError:
            continue
        if not ready:
            raise TimeoutError(f"Timed out after {timeout_s:.1f}s while probing stream")
        chunk = os.read(fd, frame_size - len(out))
        if not chunk:
            raise RuntimeError("short read")
        out.extend(chunk)
    return bytes(out)


# ---- TC001 FFC disable (auto-shutter off) via USBDEVFS_CONTROL ioctl ----

class _UsbdevfsCtrlTransfer(ctypes.Structure):
    _fields_ = [
        ("bRequestType", ctypes.c_ubyte),
        ("bRequest", ctypes.c_ubyte),
        ("wValue", ctypes.c_ushort),
        ("wIndex", ctypes.c_ushort),
        ("wLength", ctypes.c_ushort),
        ("timeout", ctypes.c_uint),
        ("data", ctypes.c_void_p),
    ]


def _usbdevfs_ioctl_iowr(size: int) -> int:
    IOC_NRBITS = 8
    IOC_TYPEBITS = 8
    IOC_SIZEBITS = 14
    IOC_DIRBITS = 2
    IOC_NRSHIFT = 0
    IOC_TYPESHIFT = IOC_NRSHIFT + IOC_NRBITS
    IOC_SIZESHIFT = IOC_TYPESHIFT + IOC_TYPEBITS
    IOC_DIRSHIFT = IOC_SIZESHIFT + IOC_SIZEBITS
    IOC_WRITE = 1
    IOC_READ = 2
    direction = IOC_READ | IOC_WRITE
    ioc_type = ord("U")
    nr = 0
    return (direction << IOC_DIRSHIFT) | (ioc_type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT) | (size << IOC_SIZESHIFT)


def _usbdevfs_control(fd: int, transfer: _UsbdevfsCtrlTransfer) -> None:
    libc_path = ctypes.util.find_library("c") or "libc.so.6"
    libc = ctypes.CDLL(libc_path, use_errno=True)
    req = _usbdevfs_ioctl_iowr(ctypes.sizeof(_UsbdevfsCtrlTransfer))
    res = libc.ioctl(fd, req, ctypes.byref(transfer))
    if res < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def _ctrl_out(fd: int, request_type: int, request: int, value: int, index: int, data: bytes) -> None:
    buf = ctypes.create_string_buffer(data)
    transfer = _UsbdevfsCtrlTransfer(
        bRequestType=request_type,
        bRequest=request,
        wValue=value,
        wIndex=index,
        wLength=len(data),
        timeout=1000,
        data=ctypes.cast(buf, ctypes.c_void_p).value,
    )
    _usbdevfs_control(fd, transfer)


def _ctrl_in(fd: int, request_type: int, request: int, value: int, index: int, length: int) -> bytes:
    buf = ctypes.create_string_buffer(length)
    transfer = _UsbdevfsCtrlTransfer(
        bRequestType=request_type,
        bRequest=request,
        wValue=value,
        wIndex=index,
        wLength=length,
        timeout=1000,
        data=ctypes.cast(buf, ctypes.c_void_p).value,
    )
    _usbdevfs_control(fd, transfer)
    return bytes(buf.raw[:length])


def _tc001_set_auto_shutter(video_node: str, *, enabled: bool) -> None:
    bus, dev = _video_usb_bus_device_numbers(video_node)
    bus_file = f"/dev/bus/usb/{bus:03d}/{dev:03d}"

    enabled_value = 1 if enabled else 0
    payload1 = bytes((0x14, 0xC2, 0x00, 0x00, 0x00, 0x00, 0x00, enabled_value))
    payload2 = b"\x00" * 8

    fd = os.open(bus_file, os.O_RDWR)
    try:
        _ctrl_out(fd, request_type=0x41, request=0x45, value=0x0078, index=0x9D00, data=payload1)
        _ctrl_out(fd, request_type=0x41, request=0x45, value=0x0078, index=0x1D08, data=payload2)

        time.sleep(0.05)
        status = _ctrl_in(fd, request_type=0xC1, request=0x44, value=0x0078, index=0x0200, length=1)[0]

        if status & 0b11111100:
            raise RuntimeError(f"FFC command error status=0x{status:02x}")
    finally:
        os.close(fd)


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="TC001 -> v4l2loopback color virtual camera")
    parser.add_argument(
        "--src-video-index",
        default=None,
        help="TC001 source video index (number only, e.g. 2). If omitted, auto-detects (prefers index 0 when available).",
    )
    parser.add_argument("--fps", type=int, default=25, help="Capture/output framerate")
    parser.add_argument(
        "--dst-resolution",
        default="640x480",
        help="Output resolution WxH (aspect-preserving, center-cropped; default 640x480)",
    )
    parser.add_argument(
        "--rotate",
        choices=("none", "0", "90", "180", "270"),
        default="90",
        help="Rotate output by 0/90/180/270 degrees (or 'none' for 0; default: 90)",
    )
    parser.add_argument(
        "--ffc-disable-after",
        nargs="?",
        default="30",
        const="none",
        help="Disable auto-shutter (FFC) after N seconds from start (default: 30). Use 'none' or no value to keep auto-shutter enabled.",
    )
    parser.add_argument(
        "--temps-every",
        type=float,
        default=1.0,
        help="Write temperature_min/median/max and temperature_zone{1..9}_{min,median,max} (°C) files every N seconds (default: 1).",
    )
    parser.add_argument(
        "--dst-video-index",
        default=None,
        help="Force destination video index N (number only, e.g. 2). If omitted, uses lowest free index.",
    )
    parser.add_argument("--dst-name-key", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--skip-modprobe", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--dst-precreated", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(list(argv))
    if args.rotate == "none":
        args.rotate = "0"

    _require_root()

    if args.dst_precreated and args.dst_video_index is None:
        raise ValueError("--dst-precreated requires --dst-video-index")

    ffmpeg = _which("ffmpeg")
    modprobe = _which("modprobe") if not args.skip_modprobe else ""
    v4l2loopback_ctl = _which("v4l2loopback-ctl")

    src = _find_tc001_video("/dev/video0")
    requested_src_video_index: Optional[int] = None
    if args.src_video_index is not None:
        requested_src_video_index = _parse_video_index(str(args.src_video_index), "--src-video-index")
        src = f"/dev/video{requested_src_video_index}"
    if not _is_tc001(src):
        ids = _video_usb_vid_pid(src)
        got = f"{ids[0]:04x}:{ids[1]:04x}" if ids else "unknown"
        if requested_src_video_index is not None:
            raise RuntimeError(
                f"--src-video-index {requested_src_video_index} (/dev/video{requested_src_video_index}) "
                f"is not a TC001 camera (expected 0bda:5830, got {got})"
            )
        raise RuntimeError(f"Auto-detected source {src} is not a TC001 camera (expected 0bda:5830, got {got})")

    _lock_fd, _lock_path = _acquire_single_instance_lock(src)
    _try_thermal_telemetry(_clear_thermal_stats)
    temps_every = _clamp_float("--temps-every", float(args.temps_every), 0.1, 3600.0)
    fps = _clamp_int("--fps", int(args.fps), 1, 60)
    out_w, out_h = _parse_dst_resolution(args.dst_resolution)
    ffc_disable_after = _parse_ffc_disable_after(args.ffc_disable_after)

    if (not args.skip_modprobe) and (not os.path.isdir("/sys/module/v4l2loopback")):
        _run([modprobe, "v4l2loopback", "devices=0"])

    _wait_for_tc001_device_nodes(src, timeout_s=1.0)

    dst_name_key: Optional[str] = None
    if args.dst_name_key is not None:
        dst_name_key = _parse_dst_name_key(str(args.dst_name_key))
    elif not args.dst_precreated:
        dst_name_key = _tc001_identity_key(src)
    loopback_name = _TC001_LOOPBACK_NAME if dst_name_key is None else _tc001_loopback_name_for_key(dst_name_key)

    dst_video_index = args.dst_video_index
    if dst_video_index is None:
        dst_video_index = _lowest_free_video_nr()
    else:
        dst_video_index = _parse_nonnegative_int(str(dst_video_index), "--dst-video-index")
    dst = f"/dev/video{dst_video_index}"

    if args.dst_precreated:
        if not os.path.exists(dst):
            # Recovery path for restarts where a prior run already deleted the
            # pre-created node during cleanup.
            v4l2_lock_fd, _v4l2_lock_path = _acquire_v4l2loopback_lock()
            try:
                _create_loopback_device(
                    v4l2loopback_ctl,
                    int(dst_video_index),
                    loopback_name=loopback_name,
                    expected_key=dst_name_key,
                )
            finally:
                try:
                    os.close(v4l2_lock_fd)
                except Exception:
                    pass
        if not _is_tc001_loopback_node(dst, expected_key=dst_name_key):
            raise RuntimeError(
                f"--dst-precreated expected a TC001 loopback node at --dst-video-index {dst_video_index} "
                f"({dst}), but found a different device"
            )
    else:
        # Serialize destination index selection/device creation across all instances.
        v4l2_lock_fd, _v4l2_lock_path = _acquire_v4l2loopback_lock()
        try:
            _create_loopback_device(
                v4l2loopback_ctl,
                int(dst_video_index),
                loopback_name=loopback_name,
                expected_key=dst_name_key,
            )
        finally:
            try:
                os.close(v4l2_lock_fd)
            except Exception:
                pass

    # False-color palette with black->blue->green->yellow->orange->white.
    lut = _build_palette_lut(
        (
            (0.00, (0, 0, 0)),
            (0.20, (0, 0, 255)),
            (0.50, (0, 255, 0)),
            (0.75, (255, 255, 0)),
            (0.90, (255, 165, 0)),
            (1.00, (255, 255, 255)),
        )
    )

    width = 256
    height = 192
    combined_h = 384
    picture_bytes = width * height * 2
    frame_size = picture_bytes * 2

    def _reader_cmd(device: str) -> List[str]:
        return [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "v4l2",
            "-input_format",
            "yuyv422",
            "-video_size",
            f"{width}x{combined_h}",
            "-framerate",
            str(fps),
            "-i",
            device,
            "-f",
            "rawvideo",
            "-pix_fmt",
            "yuyv422",
            "-",
        ]

    vf_parts = []
    if args.rotate == "90":
        vf_parts.append("transpose=clock")
    elif args.rotate == "180":
        vf_parts.append("transpose=clock,transpose=clock")
    elif args.rotate == "270":
        vf_parts.append("transpose=cclock")
    vf_parts.append(f"scale=w={out_w}:h={out_h}:flags=bilinear:force_original_aspect_ratio=increase")
    vf_parts.append(f"crop={out_w}:{out_h}")
    vf_parts.append("format=yuyv422")

    writer_cmd = [
        ffmpeg,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "-video_size",
        f"{width}x{height}",
        "-framerate",
        str(fps),
        "-i",
        "-",
        "-vf",
        ",".join(vf_parts),
        "-f",
        "v4l2",
        "-pix_fmt",
        "yuyv422",
        dst,
    ]

    stopping = False
    stream_failed = False
    reader = None
    writer = None

    def _handle_signal(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    first_buf = b""
    probe_errors: List[str] = []
    for candidate in _sysfs_sibling_devices(src):
        proc = subprocess.Popen(
            _reader_cmd(candidate),
            stdout=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            assert proc.stdout is not None
            probe = _read_exact_with_timeout(proc.stdout, frame_size, timeout_s=2.0)
            if len(probe) != frame_size:
                raise RuntimeError("short read")
            reader = proc
            first_buf = probe
            src = candidate
            break
        except Exception as exc:
            probe_errors.append(f"{candidate}: {exc}")
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    if reader is None:
        _run([v4l2loopback_ctl, "delete", str(dst_video_index)], check=False)
        details = "; ".join(probe_errors) if probe_errors else "no sibling video nodes found"
        raise RuntimeError(f"Failed to open TC001 camera stream on any sibling video node. Details: {details}")

    ffc_stop = threading.Event()

    def _ffc_worker() -> None:
        delay = ffc_disable_after
        if delay is None:
            return
        if not ffc_stop.wait(delay):
            try:
                _tc001_set_auto_shutter(src, enabled=False)
                print("FFC: auto-shutter disabled", file=sys.stderr)
            except Exception as exc:
                print(f"FFC: failed to disable auto-shutter: {exc}", file=sys.stderr)

    try:
        writer = subprocess.Popen(
            writer_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            start_new_session=True,
        )
        print(f"Source: {src}", file=sys.stderr)
        print(f"Output: {dst} (TC001 Color Camera)", file=sys.stderr)

        ffc_thread = threading.Thread(target=_ffc_worker, name="tc001-ffc", daemon=True)
        ffc_thread.start()

        assert reader.stdout is not None
        assert writer.stdin is not None
        next_temp_write = time.monotonic()
        if first_buf:
            first_gray = first_buf[:picture_bytes:2]
            try:
                writer.stdin.write(_colorize_gray_frame(first_gray, lut))
            except (BrokenPipeError, OSError) as exc:
                if not (stopping and isinstance(exc, OSError) and exc.errno == errno.EPIPE):
                    raise
                stopping = True

            _try_thermal_telemetry(
                _update_thermal_stats_from_frame,
                first_buf,
                picture_bytes=picture_bytes,
                width=width,
                height=height,
                rotate=args.rotate,
                out_w=out_w,
                out_h=out_h,
            )
            next_temp_write = time.monotonic() + temps_every

        for buf in _iter_exact_frames(reader.stdout, frame_size):
            if stopping:
                break
            gray = buf[:picture_bytes:2]
            try:
                writer.stdin.write(_colorize_gray_frame(gray, lut))
            except (BrokenPipeError, OSError) as exc:
                if not (stopping and isinstance(exc, OSError) and exc.errno == errno.EPIPE):
                    raise
                break

            now = time.monotonic()
            if now >= next_temp_write:
                _try_thermal_telemetry(
                    _update_thermal_stats_from_frame,
                    buf,
                    picture_bytes=picture_bytes,
                    width=width,
                    height=height,
                    rotate=args.rotate,
                    out_w=out_w,
                    out_h=out_h,
                )
                next_temp_write = now + temps_every
        else:
            if not stopping:
                stream_failed = True
    finally:
        ffc_stop.set()
        try:
            if writer and writer.stdin:
                writer.stdin.close()
        except Exception:
            pass
        if stopping:
            for proc in (reader, writer):
                if proc is None:
                    continue
                try:
                    proc.send_signal(signal.SIGINT)
                except Exception:
                    pass
        else:
            for proc in (reader, writer):
                if proc is None:
                    continue
                try:
                    proc.terminate()
                except Exception:
                    pass
        for proc in (reader, writer):
            if proc is None:
                continue
            try:
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        _run([v4l2loopback_ctl, "delete", str(dst_video_index)], check=False)
        _try_thermal_telemetry(_clear_thermal_stats)

    if stream_failed:
        raise RuntimeError("TC001 stream ended unexpectedly.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
