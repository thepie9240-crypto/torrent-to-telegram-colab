# ============================================================
# Telegram Torrent -> Colab Bot (GPU 720p & 480p RE-ENCODE)
# FLOOD_WAIT-SAFE + HIGH-SPEED ASYNC MTProto UPLOAD + BATCH MODE & SUB EXTRACT
# ONE-CELL GOOGLE COLAB SCRIPT
# ============================================================

import os
import sys
import subprocess
import time
import asyncio
import contextlib
import glob
import hashlib
import json
import math
import mimetypes
import random
import shutil
import html
import re
import logging
from pathlib import Path

# ------------------------------------------------------------
# 1. INSTALL & GPU FFMPEG SETUP
# ------------------------------------------------------------
print("⏳ Installing required libraries & NVENC FFmpeg...")

subprocess.run(
    [
        sys.executable, "-m", "pip", "install", "-q",
        "pyrogram==2.0.106",
        "tgcrypto",
        "aria2p",
        "nest_asyncio",
    ],
    check=True,
)

subprocess.run(["apt-get", "update", "-qq"], check=True)
subprocess.run(["apt-get", "install", "-y", "-qq", "aria2"], check=True)

# Install FFmpeg with NVENC support (if not already installed)
try:
    ffmpeg_check = subprocess.run(["ffmpeg", "-encoders"], capture_output=True, text=True)
    ffmpeg_has_nvenc = "h264_nvenc" in ffmpeg_check.stdout
except FileNotFoundError:
    ffmpeg_has_nvenc = False
if not ffmpeg_has_nvenc:
    print("⏳ Downloading FFmpeg with GPU (NVENC) support...")
    os.system("wget -q -O ffmpeg.tar.xz https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz")
    os.system("tar -xf ffmpeg.tar.xz")
    os.system("cp -f ffmpeg-master-latest-linux64-gpl/bin/ffmpeg /usr/bin/ffmpeg")
    os.system("cp -f ffmpeg-master-latest-linux64-gpl/bin/ffprobe /usr/bin/ffprobe")
    os.system("rm -rf ffmpeg.tar.xz ffmpeg-master-latest-linux64-gpl")

print("✅ Installation complete.\n")

# ------------------------------------------------------------
# 2. IMPORTS & LOGGING SETUP (FIXING PYROGRAM WARNINGS)
# ------------------------------------------------------------
import nest_asyncio
import aria2p

from pyrogram import Client, filters, enums, idle, raw
from pyrogram.errors import FloodWait, RPCError
from pyrogram.session import Session
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

nest_asyncio.apply()

# TgCrypto (C AES-IGE) is mandatory for high upload speed; pure-Python crypto caps at a few MB/s.
try:
    import tgcrypto  # noqa: F401
    print("✅ TgCrypto active (fast encryption)")
except ImportError:
    print("⚠️ TgCrypto missing - upload speed will be very slow!")

# Bigger thread pool so os.pread calls for many in-flight parts never queue up.
from concurrent.futures import ThreadPoolExecutor
asyncio.get_event_loop().set_default_executor(ThreadPoolExecutor(max_workers=64))

logging.getLogger("pyrogram.session.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.session.internals").setLevel(logging.ERROR)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

# ------------------------------------------------------------
# 3. TELEGRAM CONFIG
# ------------------------------------------------------------
def _get_secret(name):
    """Read a credential from the environment, then from Colab Secrets (the key icon in the left bar)."""
    value = os.environ.get(name)
    if not value:
        try:
            from google.colab import userdata
            value = userdata.get(name)
        except Exception:
            value = None
    if not value:
        raise SystemExit(f"❌ Missing secret '{name}'. Add it in Colab: left bar -> 🔑 Secrets (enable Notebook access).")
    return value.strip()

API_ID = int(_get_secret("API_ID"))
API_HASH = _get_secret("API_HASH")
BOT_TOKEN = _get_secret("BOT_TOKEN")

# ------------------------------------------------------------
# 4. SETTINGS
# ------------------------------------------------------------
PART_SIZE = 512 * 1024                 # Telegram max part size
UPLOAD_CONNECTIONS = 16                # Separate TCP connections to Telegram (keep <= 20, more triggers FLOOD_WAIT)
PARTS_PER_CONNECTION = 5               # Parts in flight per connection (hides network latency) -> 80 parts in flight
UPLOAD_WORKERS = 8                     # Used only by Pyrogram's own save_file (thumbnails)
PART_RETRIES = 8
UI_INTERVAL = 1.5
MAX_FILE_SIZE = 2000 * 1024 * 1024     # 2 GiB
BIG_FILE_THRESHOLD = 10 * 1024 * 1024

SESSION_DIR = "/content/telegram_sessions"
os.makedirs(SESSION_DIR, exist_ok=True)
MAIN_SESSION_NAME = os.path.join(SESSION_DIR, "colab_torrent_bot")

THUMB_DIR = "/content/bot_thumbnail"
os.makedirs(THUMB_DIR, exist_ok=True)
CUSTOM_THUMB_PATH = os.path.join(THUMB_DIR, "custom_thumb.jpg")

app = Client(
    MAIN_SESSION_NAME,
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=32,
    sleep_threshold=60, # Increased native wait limit
    max_concurrent_transmissions=UPLOAD_WORKERS,
    in_memory=False,
)

upload_client = None

# ------------------------------------------------------------
# 5. RUNTIME STATE
# ------------------------------------------------------------
current_tasks = {}
cancel_flags = {}
upload_runtime = {}
startup_flood_wait = None
batch_states = {}
thumbnail_states = {}
user_settings = {}
nvenc_cache = None      # None = not checked yet, True/False = NVENC usable on this runtime
quality_settings = {}   # user_id -> "orig" | "720" | "480" | "all"

QUALITY_LABELS = {
    "orig": "Original Only",
    "720": "Original + 720p",
    "480": "Original + 480p",
    "all": "All (Original + 720p + 480p)",
}

# ------------------------------------------------------------
# 6. HELPERS
# ------------------------------------------------------------
def format_bytes(size):
    try: size = float(size)
    except Exception: size = 0.0
    if size <= 0: return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = min(int(math.floor(math.log(size, 1024))), len(units) - 1)
    value = size / (1024 ** i)
    if value >= 100: return f"{value:.0f} {units[i]}"
    if value >= 10: return f"{value:.1f} {units[i]}"
    return f"{value:.2f} {units[i]}"

def format_mbps(bytes_per_second):
    return f"{(bytes_per_second * 8) / 1_000_000:.2f} Mbps"

def format_time(seconds):
    try: seconds = max(0, int(seconds))
    except Exception: seconds = 0
    if seconds == 0: return "0s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

def parse_selection(selection_str, max_idx):
    if selection_str.lower() == "all":
        return list(range(1, max_idx + 1))
    indices = []
    for part in selection_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = part.split("-", 1)
                start, end = int(start.strip()), int(end.strip())
                step = 1 if start <= end else -1
                for idx in range(start, end + step, step):
                    if 1 <= idx <= max_idx and idx not in indices:
                        indices.append(idx)
            except ValueError: pass
        elif part.isdigit():
            idx = int(part)
            if 1 <= idx <= max_idx and idx not in indices:
                indices.append(idx)
    return indices

def md5_file_sync(path):
    h = hashlib.md5()
    with open(path, "rb", buffering=1024 * 1024) as f:
        while True:
            chunk = f.read(4 * 1024 * 1024)
            if not chunk: break
            h.update(chunk)
    return h.hexdigest()

def get_mime_type(path):
    return mimetypes.guess_type(path)[0] or "application/octet-stream"

def safe_html(text):
    return html.escape(str(text), quote=False)

def build_file_tree(files, is_html=True):
    tree = {}
    for i, f in enumerate(files):
        parts = Path(f.path).parts
        current = tree
        for i_part, part in enumerate(parts):
            if i_part == len(parts) - 1:
                current[part] = {'index': i + 1, 'size': f.length}
            else:
                if part not in current: current[part] = {}
                current = current[part]

    def render_tree(node, prefix=""):
        lines = []
        keys = list(node.keys())
        for idx, key in enumerate(keys):
            is_last_item = (idx == len(keys) - 1)
            connector = "└── " if is_last_item else "├── "
            child_prefix = "    " if is_last_item else "│   "
            val = node[key]
            if isinstance(val, dict) and 'index' not in val:
                folder_name = html.escape(key) if is_html else key
                lines.append(f"{prefix}{connector}📁 {folder_name}")
                lines.extend(render_tree(val, prefix + child_prefix))
            else:
                file_idx = val['index']
                size_mb = val['size'] / (1024 * 1024)
                idx_str = f"[{file_idx}]"
                if is_html: idx_str = f"<code>{idx_str}</code>"
                file_name = html.escape(key) if is_html else key
                lines.append(f"{prefix}{connector}📄 {idx_str} {file_name} ({size_mb:.2f} MB)")
        return lines
    return "\n".join(render_tree(tree))

def generate_res_name(original_name, target_res):
    name_without_ext = os.path.splitext(original_name)[0]
    def res_repl(m):
        return f"{target_res}P" if 'P' in m.group(0) else f"{target_res}p"
    enc_base_name = re.sub(r'(?i)(1080p|720p|480p|2160p|4k|1080)', res_repl, name_without_ext)
    if enc_base_name == name_without_ext:
        enc_base_name += f"_{target_res}p"
    return enc_base_name


# ------------------------------------------------------------
# 7. VIDEO PROBE & EXTRACTION
# ------------------------------------------------------------
async def get_video_meta(filepath):
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", filepath,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
        info = json.loads(stdout.decode("utf-8", errors="ignore"))
        duration = int(float(info.get("format", {}).get("duration", 1) or 1))
        width, height = 1280, 720

        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                width = int(stream.get("width", 1280) or 1280)
                height = int(stream.get("height", 720) or 720)
                break
        return max(duration, 1), max(width, 1), max(height, 1)
    except Exception:
        return 1, 1280, 720

async def probe_media_streams(filepath):
    cmd = [
        "ffprobe", "-v", "error", "-print_format", "json",
        "-show_streams", "-show_format", filepath,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(stderr.decode("utf-8", errors="ignore")[:2000])
    return json.loads(stdout.decode("utf-8", errors="ignore"))

def subtitle_is_text_codec(codec):
    return codec.lower() in {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text"}

async def prepare_streamable_video(filepath):
    info = await probe_media_streams(filepath)
    streams = info.get("streams", [])

    videos = [s for s in streams if s.get("codec_type") == "video"]
    subtitles = [s for s in streams if s.get("codec_type") == "subtitle"]
    if not videos:
        return filepath, False, "No video stream"

    video_codec = (videos[0].get("codec_name") or "").lower()
    mp4_video_codecs = {"h264", "hevc", "av1", "mpeg4", "vp9"}
    if video_codec not in mp4_video_codecs:
        return filepath, False, f"Video codec {video_codec} not suitable for direct MP4 remux"

    for s in subtitles:
        if not subtitle_is_text_codec((s.get("codec_name") or "unknown").lower()):
            return filepath, False, "Image subtitle track detected; original container kept"

    out = os.path.splitext(filepath)[0] + "_stream.mp4"
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", filepath,
        "-threads", "0", # Maximize CPU for Muxing
        "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?",
        "-c:v", "copy", "-c:a", "copy", "-c:s", "mov_text",
        "-movflags", "+faststart", out,
    ]
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.communicate()

    if proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
        return out, True, "Faststart MP4 created"
    with contextlib.suppress(Exception): os.remove(out)
    return filepath, False, "MP4 remux failed"

async def extract_and_send_subtitles(client, filepath, chat_id, reply_to_message_id, temp_dir, custom_filename=None):
    try:
        info = await probe_media_streams(filepath)
        subtitles = [s for s in info.get("streams", []) if s.get("codec_type") == "subtitle"]
        if not subtitles:
            await safe_send_message(client, chat_id, "No Soft subtitle found.", reply_to_message_id=reply_to_message_id)
            return

        ext_map = {"subrip": ".srt", "ass": ".ass", "ssa": ".ssa", "webvtt": ".vtt", "mov_text": ".srt"}
        sub_files = []
        base_name = os.path.splitext(custom_filename or os.path.basename(filepath))[0]

        for sub_index, sub in enumerate(subtitles, start=1):
            codec = (sub.get("codec_name") or "unknown").lower()
            ext = ext_map.get(codec, f".{codec}" if codec != "unknown" else ".srt")
            file_name = f"{base_name} - Subtitle {sub_index}{ext}" if len(subtitles) > 1 else f"{base_name}{ext}"
            out_path = os.path.join(temp_dir, file_name)
            
            cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", filepath, "-map", f"0:{sub.get('index')}", "-c:s", "copy", out_path]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.communicate()
            if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                sub_files.append(out_path)

        for sub_file in sub_files:
            try:
                await client.send_document(chat_id=chat_id, document=sub_file, reply_to_message_id=reply_to_message_id)
            except FloodWait as e:
                wait_time = int(getattr(e, "value", 0) or 0) + 1
                await asyncio.sleep(wait_time)
                await client.send_document(chat_id=chat_id, document=sub_file, reply_to_message_id=reply_to_message_id)
    except Exception as e:
        print(f"[ERROR] Subtitle extraction error: {e}")

async def get_thumbnail(filepath):
    thumb_path = filepath + "_thumb.jpg"
    try:
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", "00:00:02", "-i", filepath, "-vf", "scale='min(320,iw)':-2", "-vframes", "1", "-q:v", "5", thumb_path]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await asyncio.wait_for(proc.communicate(), timeout=60)
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
    except Exception: pass
    return None

# ------------------------------------------------------------
# 8. PROGRESS UI
# ------------------------------------------------------------
async def update_ui(
    message, action, filename, current, total, start_time,
    last_edit_time, task_id=None, force=False, extra_lines=None, is_encoding=False, interval=UI_INTERVAL
):
    now = time.time()
    if not force and (now - last_edit_time[0] < interval) and current < total:
        return last_edit_time[0]

    last_edit_time[0] = now
    percentage = (current / total) * 100 if total > 0 else 0
    bar_length = 18
    filled = min(bar_length, int(bar_length * percentage / 100))
    bar = "█" * filled + "░" * (bar_length - filled)
    elapsed = max(now - start_time, 0.001)
    
    speed = current / elapsed
    remaining = max(total - current, 0)
    eta = remaining / speed if speed > 0 else 0

    msg = f"<b>🎬 {safe_html(filename)}</b>\n\n"
    msg += f"<b>ක්‍රියාවලිය:</b> {safe_html(action)}\n"
    msg += f"<code>[{bar}] {percentage:.1f}%</code>\n"

    if is_encoding:
        msg += f"<i>{format_time(current)} / {format_time(total)}</i>\n"
        msg += f"<b>Speed:</b> {speed:.2f}x\n"
        msg += f"<b>ETA:</b> {format_time(eta)}\n"
    else:
        msg += f"<i>{format_bytes(current)} / {format_bytes(total)}</i>\n"
        msg += f"<b>වේගය:</b> {format_bytes(speed)}/s ({format_mbps(speed)})\n"
        msg += f"<b>ETA:</b> {format_time(eta)}\n"

    if extra_lines:
        msg += "\n" + "\n".join(extra_lines) + "\n"

    markup = None
    if task_id:
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Task", callback_data=f"stop_{task_id}")]])

    try: await message.edit_text(msg, parse_mode=enums.ParseMode.HTML, reply_markup=markup)
    except Exception: pass
    return last_edit_time[0]

# ------------------------------------------------------------
# 9. GPU + CPU ENCODING LOGIC (DYNAMIC MULTI-RES)
# ------------------------------------------------------------
async def _run_capture(*cmd, timeout=30):
    """Run a command, return (returncode, stdout, stderr) as text. (-1, '', error) if it can't run."""
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, out.decode("utf-8", errors="ignore"), err.decode("utf-8", errors="ignore")
    except Exception as e:
        return -1, "", str(e)

async def check_gpu():
    """Inspect the Colab GPU. Never raises; a missing GPU simply yields active=False."""
    global nvenc_cache
    info = {"active": False, "name": None, "memory": None, "driver": None, "util": None, "mem_used": None, "nvenc": False}

    rc, out, _ = await _run_capture(
        "nvidia-smi", "--query-gpu=name,memory.total,driver_version,utilization.gpu,memory.used",
        "--format=csv,noheader,nounits",
    )
    if rc == 0 and out.strip():
        parts = [p.strip() for p in out.strip().splitlines()[0].split(",")]
        if len(parts) >= 5:
            info.update(active=True, name=parts[0], memory=parts[1], driver=parts[2], util=parts[3], mem_used=parts[4])

    if info["active"]:
        rc, out, _ = await _run_capture("ffmpeg", "-hide_banner", "-encoders")
        if rc == 0 and "h264_nvenc" in out:
            # Real test: the encoder must actually initialise on this GPU, not just exist in the build.
            rc, _, _ = await _run_capture(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=black:s=256x256:d=0.2",
                "-c:v", "h264_nvenc", "-f", "null", "-",
            )
            info["nvenc"] = (rc == 0)

    nvenc_cache = info["nvenc"]
    return info

async def nvenc_usable():
    if nvenc_cache is None:
        await check_gpu()
    return bool(nvenc_cache)

# Output size targets (fraction of the ORIGINAL file size) and safety floors.
TARGET_SIZE_RATIO = {720: 0.45, 480: 0.20}      # 720p >50% smaller, 480p ~80% smaller
MIN_VIDEO_KBPS = {720: 350, 480: 180}           # never go below this (quality floor)
AAC_KBPS = {720: 128, 480: 80}                  # used only when the original audio is too big to copy
AUDIO_COPY_MAX_SHARE = 0.25                     # copy audio only if it uses <= 25% of the size budget
COMPRESS_SAFETY = 0.95                          # aim at 95% of the 2 GiB limit when shrinking oversized files
COMPRESS_ASK_TIMEOUT = 600                      # seconds to wait for the user's Yes/No answer
compress_prompts = {}                           # task_id -> asyncio.Future (True = compress, False = skip)

def estimate_audio_bps(audio_streams):
    total = 0
    for s in audio_streams:
        bps = 0
        with contextlib.suppress(Exception): bps = int(s.get("bit_rate") or 0)
        if not bps:
            for k, v in (s.get("tags") or {}).items():
                if k.upper().startswith("BPS"):
                    with contextlib.suppress(Exception): bps = int(v)
                    break
        if not bps:
            channels = 2
            with contextlib.suppress(Exception): channels = int(s.get("channels") or 2)
            bps = 448_000 if channels >= 6 else 192_000
        total += bps
    return total

async def plan_encode(input_path, duration, resolution):
    """Work out the video bitrate that lands the output at the target size.
    Returns (video_bps, copy_audio, audio_track_count)."""
    duration = max(float(duration or 1), 1.0)
    size_bytes = os.path.getsize(input_path)
    # Never plan an output above the Telegram limit, even for huge sources
    target_bytes = min(size_bytes * TARGET_SIZE_RATIO[resolution], MAX_FILE_SIZE * COMPRESS_SAFETY)
    target_bps = target_bytes * 8 / duration

    audio_streams = []
    with contextlib.suppress(Exception):
        info = await probe_media_streams(input_path)
        audio_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]

    audio_bps = estimate_audio_bps(audio_streams)
    copy_audio = audio_bps <= target_bps * AUDIO_COPY_MAX_SHARE
    if not copy_audio:
        audio_bps = len(audio_streams) * AAC_KBPS[resolution] * 1000

    video_bps = (target_bps - audio_bps) * 0.94      # ~6% margin for container/subtitle overhead
    video_bps = max(video_bps, MIN_VIDEO_KBPS[resolution] * 1000)
    return int(video_bps), copy_audio, len(audio_streams)

def build_multi_encode_cmd(mode, input_path, outputs, plans):
    """One ffmpeg run: decode the source ONCE, then scale/encode every requested resolution.
    outputs = [(res, out_path), ...]   plans = {res: (video_bps, copy_audio, audio_tracks)}"""
    def scale_filter(res):
        if mode == "gpu_full":
            return f"scale_cuda=w=-2:h='min({res},ih)':format=nv12"
        return f"scale=-2:'min({res},ih)',format=yuv420p"

    if len(outputs) == 1:
        fc = f"[0:v:0]{scale_filter(outputs[0][0])}[o0]"
    else:
        splits = "".join(f"[s{n}]" for n in range(len(outputs)))
        chains = ";".join(f"[s{n}]{scale_filter(res)}[o{n}]" for n, (res, _) in enumerate(outputs))
        fc = f"[0:v:0]split={len(outputs)}{splits};{chains}"

    if mode == "gpu_full":
        pre = ["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"]
    elif mode == "gpu_sw":
        pre = ["-hwaccel", "cuda"]
    else:
        pre = []

    cmd = ["ffmpeg", "-y", "-hide_banner", "-threads", "0", *pre, "-i", input_path, "-filter_complex", fc]

    for n, (res, out_path) in enumerate(outputs):
        video_bps, copy_audio, _ = plans[res]
        maxrate, bufsize = int(video_bps * 1.15), int(video_bps * 2)
        if mode.startswith("gpu"):
            vcodec = [
                "-c:v", "h264_nvenc", "-preset", "p4", "-tune", "hq", "-profile:v", "high",
                "-rc", "vbr", "-b:v", str(video_bps), "-maxrate", str(maxrate), "-bufsize", str(bufsize),
                "-spatial-aq", "1", "-temporal-aq", "1",
            ]
        else:
            vcodec = [
                "-c:v", "libx264", "-preset", "veryfast", "-profile:v", "high",
                "-b:v", str(video_bps), "-maxrate", str(maxrate), "-bufsize", str(bufsize),
            ]
        audio = ["-c:a", "copy"] if copy_audio else ["-c:a", "aac", "-b:a", f"{AAC_KBPS[res]}k", "-ac", "2"]
        cmd += [
            "-map", f"[o{n}]", "-map", "0:a?", "-map", "0:s?",
            *vcodec, *audio, "-c:s", "copy", "-max_muxing_queue_size", "8192",
            out_path,
        ]

    cmd += ["-progress", "pipe:1", "-nostats", "-loglevel", "error"]
    return cmd

async def encode_multi(client, chat_id, input_path, resolutions, total_duration, task_id, filename):
    """Encode every resolution in `resolutions` with a single decode. Runs in the background
    while the original is uploading and reports progress in its OWN message.
    Returns {resolution: output_path} for the outputs that were produced."""
    has_nvenc = await nvenc_usable()
    src_size = os.path.getsize(input_path)
    stem = os.path.splitext(input_path)[0]

    plans = {res: await plan_encode(input_path, total_duration, res) for res in resolutions}
    outputs = [(res, f"{stem}_{res}p_temp.mkv") for res in resolutions]
    targets = {res: src_size * TARGET_SIZE_RATIO[res] for res in resolutions}
    res_label = " + ".join(f"{res}p" for res in resolutions)
    print(f"[ENCODE] {res_label} | " + " | ".join(f"{res}p ≈{targets[res] / 1048576:.0f} MB @ {plans[res][0] // 1000} kbps" for res in resolutions))

    enc_msg = await safe_send_message(
        client, chat_id, f"⚙️ <b>Encode ආරම්භ කරමින්... ({res_label})</b>", parse_mode=enums.ParseMode.HTML,
    )

    modes = (["gpu_full", "gpu_sw"] if has_nvenc else []) + ["cpu"]
    labels = {"gpu_full": "Full GPU", "gpu_sw": "GPU", "cpu": "CPU"}
    last_err = ""
    process = None

    def detail_lines(mode):
        lines = [f"<b>Mode:</b> {labels[mode]} • Decode එක වතාවක් පමණි", f"<b>Source:</b> {format_bytes(src_size)}"]
        for res, path in outputs:
            cur = os.path.getsize(path) if os.path.exists(path) else 0
            video_bps, copy_audio, _ = plans[res]
            lines.append(
                f"<b>{res}p:</b> {format_bytes(cur)} / ≈{format_bytes(targets[res])} "
                f"({video_bps // 1000} kbps, audio {'copy' if copy_audio else 'AAC'})"
            )
        return lines

    try:
        for mode in modes:
            cmd = build_multi_encode_cmd(mode, input_path, outputs, plans)
            action_name = f"⚙️ Re-Encoding {res_label} ({labels[mode]})"
            process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            start_time = time.time()
            last_edit_time = [0]

            while True:
                if cancel_flags.get(task_id):
                    raise asyncio.CancelledError("Encoding cancelled by user.")
                line = await process.stdout.readline()
                if not line:
                    break
                line = line.decode("utf-8", errors="ignore").strip()
                if line.startswith("out_time_us=") and enc_msg:
                    try:
                        current_sec = max(0, int(line.split("=")[1]) / 1_000_000)
                        await update_ui(
                            message=enc_msg, action=action_name, filename=filename,
                            current=current_sec, total=max(total_duration, 1), start_time=start_time,
                            last_edit_time=last_edit_time, task_id=task_id, force=False, is_encoding=True,
                            extra_lines=detail_lines(mode), interval=3.0,
                        )
                    except ValueError: pass

            await process.wait()
            if process.returncode == 0 and all(os.path.exists(p) and os.path.getsize(p) > 0 for _, p in outputs):
                done = [f"<b>{res}p:</b> {format_bytes(os.path.getsize(p))} ({os.path.getsize(p) * 100 / max(src_size, 1):.0f}% of original)" for res, p in outputs]
                if enc_msg:
                    with contextlib.suppress(Exception):
                        await enc_msg.edit_text(
                            f"<b>✅ Encode අවසන් ({res_label} • {labels[mode]})</b>\n\n<b>🎬 {safe_html(filename)}</b>\n" + "\n".join(done),
                            parse_mode=enums.ParseMode.HTML,
                        )
                return {res: p for res, p in outputs}

            err = await process.stderr.read()
            last_err = err.decode("utf-8", errors="ignore")[-1000:]
            print(f"[ENCODE] mode '{mode}' failed (code {process.returncode}); trying next mode...\n{last_err}")
            for _, p in outputs:
                with contextlib.suppress(Exception): os.remove(p)

        if enc_msg:
            with contextlib.suppress(Exception):
                await enc_msg.edit_text(f"⚠️ <b>Encode අසාර්ථකයි ({res_label})</b>\n<code>{safe_html(last_err[-800:])}</code>", parse_mode=enums.ParseMode.HTML)
        return {}

    except asyncio.CancelledError:
        if enc_msg:
            with contextlib.suppress(Exception):
                await enc_msg.edit_text("🚫 <b>Encode අවලංගු කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)
        raise
    finally:
        if process and process.returncode is None:
            with contextlib.suppress(Exception): process.kill()
            with contextlib.suppress(Exception): await process.wait()


# ------------------------------------------------------------
# 9b. OVERSIZED FILE (> 2 GiB) -> ASK -> GPU COMPRESS TO FIT
# ------------------------------------------------------------
async def ask_compress(client, chat_id, task_id, filename, size_bytes):
    """Show a Yes/No inline menu and wait for the answer. Returns True / False (False on timeout or cancel)."""
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    compress_prompts[task_id] = fut
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ OK, Compress", callback_data=f"cmp_yes_{task_id}"),
        InlineKeyboardButton("❌ No", callback_data=f"cmp_no_{task_id}"),
    ]])
    prompt = await safe_send_message(
        client, chat_id,
        f"⚠️ <b>File එක Telegram Bot සීමාව (2GB) ඉක්මවයි</b>\n\n"
        f"<code>{safe_html(filename)}</code>\n"
        f"<b>Size:</b> {format_bytes(size_bytes)} → <b>Limit:</b> {format_bytes(MAX_FILE_SIZE)}\n\n"
        f"GPU (T4) එකෙන් 2GB දක්වා Size එක අඩු කරලා Download/Upload කරන්නද?",
        parse_mode=enums.ParseMode.HTML, reply_markup=markup,
    )
    answer = False
    try:
        waited = 0
        while waited < COMPRESS_ASK_TIMEOUT and not fut.done():
            if cancel_flags.get(task_id): break
            await asyncio.sleep(1)
            waited += 1
        if fut.done(): answer = fut.result()
    finally:
        compress_prompts.pop(task_id, None)
        if prompt:
            with contextlib.suppress(Exception):
                await prompt.edit_text(
                    "✅ <b>2GB වලට Compress කරමින්...</b>" if answer else "⏭️ <b>Compress නොකර ඉදිරියට යයි.</b>",
                    parse_mode=enums.ParseMode.HTML,
                )
    return answer

def build_compress_cmd(mode, input_path, out_path, video_bps, copy_audio):
    maxrate, bufsize = int(video_bps * 1.1), int(video_bps * 2)
    if mode == "gpu_full":
        cmd = ["ffmpeg", "-y", "-hide_banner", "-hwaccel", "cuda", "-hwaccel_output_format", "cuda", "-i", input_path,
               "-vf", "scale_cuda=format=nv12"]
    elif mode == "gpu_sw":
        cmd = ["ffmpeg", "-y", "-hide_banner", "-hwaccel", "cuda", "-i", input_path, "-vf", "format=yuv420p"]
    else:
        cmd = ["ffmpeg", "-y", "-hide_banner", "-threads", "0", "-i", input_path, "-vf", "format=yuv420p"]

    if mode.startswith("gpu"):
        vcodec = ["-c:v", "h264_nvenc", "-preset", "p3", "-profile:v", "high", "-rc", "vbr",
                  "-b:v", str(video_bps), "-maxrate", str(maxrate), "-bufsize", str(bufsize)]
    else:
        vcodec = ["-c:v", "libx264", "-preset", "veryfast", "-profile:v", "high",
                  "-b:v", str(video_bps), "-maxrate", str(maxrate), "-bufsize", str(bufsize)]
    audio = ["-c:a", "copy"] if copy_audio else ["-c:a", "aac", "-b:a", "128k", "-ac", "2"]
    cmd += ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", *vcodec, *audio, "-c:s", "copy",
            "-max_muxing_queue_size", "8192", "-progress", "pipe:1", "-nostats", "-loglevel", "error", out_path]
    return cmd

async def compress_to_limit(client, chat_id, input_path, task_id, filename, ui_msg):
    """Shrink `input_path` (keeping resolution) until it fits under Telegram's 2 GiB limit.
    Live progress (%, speed, ETA) is shown in ui_msg. Returns the output path or None on failure."""
    duration, _, _ = await get_video_meta(input_path)
    duration = max(duration, 1)
    src_size = os.path.getsize(input_path)
    out_path = os.path.splitext(input_path)[0] + "_2GB.mkv"

    audio_streams = []
    with contextlib.suppress(Exception):
        info = await probe_media_streams(input_path)
        audio_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    target_bytes = MAX_FILE_SIZE * COMPRESS_SAFETY
    target_bps = target_bytes * 8 / duration
    audio_bps = estimate_audio_bps(audio_streams)
    copy_audio = audio_bps <= target_bps * AUDIO_COPY_MAX_SHARE
    if not copy_audio: audio_bps = len(audio_streams) * 128_000
    video_bps = int(max((target_bps - audio_bps) * 0.94, 150_000))

    has_nvenc = await nvenc_usable()
    modes = (["gpu_full", "gpu_sw"] if has_nvenc else []) + ["cpu"]
    labels = {"gpu_full": "Full GPU", "gpu_sw": "GPU", "cpu": "CPU"}
    process = None
    try:
        for attempt in range(3):                       # extra passes only if the result still overshoots
            for mode in modes:
                with contextlib.suppress(Exception): os.remove(out_path)
                cmd = build_compress_cmd(mode, input_path, out_path, video_bps, copy_audio)
                process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                start_time, last_edit_time = time.time(), [0]
                while True:
                    if cancel_flags.get(task_id): raise asyncio.CancelledError("Compression cancelled by user.")
                    line = await process.stdout.readline()
                    if not line: break
                    line = line.decode("utf-8", errors="ignore").strip()
                    if line.startswith("out_time_us="):
                        try: cur = max(0, int(line.split("=")[1]) / 1_000_000)
                        except ValueError: continue
                        await update_ui(
                            ui_msg, f"🗜️ Compressing to 2GB ({labels[mode]}" + (f", pass {attempt + 1}" if attempt else "") + ")",
                            filename, min(cur, duration), duration, start_time, last_edit_time, task_id, is_encoding=True,
                            extra_lines=[f"<b>Source:</b> {format_bytes(src_size)}",
                                         f"<b>Output:</b> {format_bytes(os.path.getsize(out_path) if os.path.exists(out_path) else 0)} / ≈{format_bytes(target_bytes)}"],
                            interval=2.0,
                        )
                await process.wait()
                if process.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                    break
                err = (await process.stderr.read()).decode("utf-8", errors="ignore")[-600:]
                print(f"[COMPRESS] mode '{mode}' failed (code {process.returncode})\n{err}")
                with contextlib.suppress(Exception): os.remove(out_path)
            else:
                return None                            # every mode failed

            out_size = os.path.getsize(out_path)
            if out_size <= MAX_FILE_SIZE:
                return out_path
            print(f"[COMPRESS] output {out_size} still above limit, retrying with lower bitrate")
            video_bps = int(video_bps * (target_bytes / out_size) * 0.95)
        with contextlib.suppress(Exception): os.remove(out_path)
        return None
    except asyncio.CancelledError:
        with contextlib.suppress(Exception): os.remove(out_path)
        raise
    finally:
        if process and process.returncode is None:
            with contextlib.suppress(Exception): process.kill()
            with contextlib.suppress(Exception): await process.wait()

# ------------------------------------------------------------
# 10. CLEANUP / ARIA2 SETUP
# ------------------------------------------------------------
print("[INFO] Cleaning old temporary files...")
for pattern in ["/content/meta_*", "/content/downloads_*"]:
    for path in glob.glob(pattern): shutil.rmtree(path, ignore_errors=True)
for pattern in ["/content/*.jpg", "/content/*.mkv", "/content/*.mp4", "/content/*.torrent"]:
    for path in glob.glob(pattern):
        with contextlib.suppress(Exception): os.remove(path)

os.system("pkill -9 aria2c >/dev/null 2>&1 || true")
time.sleep(1)

print("[INFO] Starting optimized aria2c...")
aria2_process = subprocess.Popen(
    ["aria2c", "--enable-rpc=true", "--rpc-listen-all=false", "--rpc-listen-port=6800", "--rpc-max-request-size=32M", "--enable-dht=true", "--bt-enable-lpd=true", "--enable-peer-exchange=true", "--seed-time=0", "--max-concurrent-downloads=8", "--split=16", "--max-connection-per-server=16", "--bt-max-peers=100", "--min-split-size=4M", "--disk-cache=64M", "--file-allocation=none", "--allow-overwrite=true", "--continue=true", "--connect-timeout=10", "--timeout=30", "--retry-wait=2", "--max-tries=5", "--console-log-level=warn", "--daemon=true"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
time.sleep(2)
aria2 = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))

# ------------------------------------------------------------
# 11. PARALLEL MTProto UPLOAD
# ------------------------------------------------------------
async def open_upload_sessions(count):
    """Open `count` dedicated media connections (same DC + auth key) so uploads are not
    throttled by Telegram's per-connection limit. Returns whatever opened successfully."""
    dc_id = await app.storage.dc_id()
    auth_key = await app.storage.auth_key()
    test_mode = await app.storage.test_mode()

    async def _open():
        session = Session(app, dc_id, auth_key, test_mode, is_media=True)
        await asyncio.wait_for(session.start(), timeout=30)
        return session

    results = await asyncio.gather(*[_open() for _ in range(count)], return_exceptions=True)
    return [r for r in results if isinstance(r, Session)]

async def close_upload_sessions(sessions):
    await asyncio.gather(*[s.stop() for s in sessions], return_exceptions=True)

async def upload_part(session, fd, file_id, part_no, total_parts, file_size, is_big, cancel_event):
    offset = part_no * PART_SIZE
    size_to_read = min(PART_SIZE, file_size - offset)
    if size_to_read <= 0: return 0
    data = await asyncio.to_thread(os.pread, fd, size_to_read, offset)
    if cancel_event.is_set(): raise asyncio.CancelledError
    if not data: raise IOError(f"Empty data for part {part_no}")

    last_error = None
    for attempt in range(1, PART_RETRIES + 1):
        if cancel_event.is_set(): raise asyncio.CancelledError
        try:
            if is_big: result = await session.invoke(raw.functions.upload.SaveBigFilePart(file_id=file_id, file_part=part_no, file_total_parts=total_parts, bytes=data), timeout=30)
            else: result = await session.invoke(raw.functions.upload.SaveFilePart(file_id=file_id, file_part=part_no, bytes=data), timeout=30)
            if result: return len(data)
            raise RuntimeError("Telegram returned False for upload part")
        except FloodWait as e:
            wait_seconds = int(getattr(e, "value", 0) or 0)
            await asyncio.sleep(min(wait_seconds, 300) if wait_seconds > 0 else 5)
            last_error = e
        except asyncio.CancelledError: raise
        except Exception as e:
            last_error = e
            if attempt >= PART_RETRIES: break
            await asyncio.sleep(min(2.0, 0.15 * (2 ** (attempt - 1))))
    raise RuntimeError(f"Part {part_no} failed after {PART_RETRIES} attempts: {last_error}")

async def parallel_upload_file(path, filename, task_id, ui_msg):
    global upload_client
    file_size = os.path.getsize(path)
    if file_size <= 0: raise ValueError("Cannot upload an empty file.")
    if file_size > MAX_FILE_SIZE: raise ValueError(f"File is larger than {format_bytes(MAX_FILE_SIZE)}.")

    is_big = file_size > BIG_FILE_THRESHOLD
    total_parts = math.ceil(file_size / PART_SIZE)
    md5_checksum = await asyncio.to_thread(md5_file_sync, path) if not is_big else None
    
    cancel_event = asyncio.Event()

    runtime = {"cancel_event": cancel_event, "upload_tasks": set(), "progress_task": None}
    upload_runtime[task_id] = runtime

    # One TCP connection is throttled by Telegram, so spread parts over several connections.
    sessions = await open_upload_sessions(min(UPLOAD_CONNECTIONS, max(1, total_parts)))
    if not sessions:
        # Fallback: share the main bot connection (slower, but the upload still works).
        sessions = [upload_client]
        owned_sessions = []
    else:
        owned_sessions = sessions
    worker_count = min(len(sessions) * PARTS_PER_CONNECTION, total_parts)
    file_id = random.randint(-2**63, 2**63 - 1)
    queue = asyncio.Queue()
    for part_no in range(total_parts): queue.put_nowait(part_no)

    state = {"uploaded": 0, "lock": asyncio.Lock(), "last_bytes": 0, "last_time": time.time(), "instant": 0.0}
    start_time = time.time()
    last_edit_time = [0]

    async def progress_pump():
        while not cancel_event.is_set():
            await asyncio.sleep(0.5)
            current = state["uploaded"]
            now = time.time()
            dt = max(now - state["last_time"], 0.001)
            state["instant"] = max(0.0, (current - state["last_bytes"]) / dt)
            state["last_bytes"] = current
            state["last_time"] = now
            if current >= file_size: break
            avg = current / max(now - start_time, 0.001)
            await update_ui(
                ui_msg, "📤 Telegram Upload", filename, current, file_size, start_time, last_edit_time, task_id,
                extra_lines=[f"<b>⚡ Instant:</b> {format_bytes(state['instant'])}/s ({format_mbps(state['instant'])})", f"<b>AVG:</b> {format_bytes(avg)}/s ({format_mbps(avg)})", f"<b>🔗 Connections:</b> {len(sessions)}"],
            )

    runtime["progress_task"] = asyncio.create_task(progress_pump())
    fd = os.open(path, os.O_RDONLY)

    async def worker(session):
        while not cancel_event.is_set():
            try: part_no = queue.get_nowait()
            except asyncio.QueueEmpty: return
            try:
                sent_bytes = await upload_part(session, fd, file_id, part_no, total_parts, file_size, is_big, cancel_event)
                async with state["lock"]: state["uploaded"] += sent_bytes
            except Exception:
                cancel_event.set()
                raise
            finally: queue.task_done()

    try:
        tasks = [asyncio.create_task(worker(sessions[n % len(sessions)])) for n in range(worker_count)]
        runtime["upload_tasks"] = set(tasks)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if cancel_event.is_set() and cancel_flags.get(task_id): raise asyncio.CancelledError
        if state["uploaded"] != file_size: raise RuntimeError(f"Upload incomplete: {state['uploaded']} / {file_size}")

        if is_big: input_file = raw.types.InputFileBig(id=file_id, parts=total_parts, name=filename)
        else: input_file = raw.types.InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum=md5_checksum)
        
        avg = file_size / max(time.time() - start_time, 0.001)
        await update_ui(
            ui_msg, f"✅ Upload complete | AVG {format_bytes(avg)}/s", filename, file_size, file_size, start_time, last_edit_time, task_id, force=True,
            extra_lines=["<b>⏳ Telegram file එක process කරමින් සිටී...</b> (විශාල files වලට විනාඩි කිහිපයක් යා හැක)"],
        )
        return input_file
    finally:
        cancel_event.set()
        with contextlib.suppress(Exception): os.close(fd)
        if runtime.get("progress_task"): runtime["progress_task"].cancel()
        for task in list(runtime.get("upload_tasks", set())):
            if not task.done(): task.cancel()
        upload_runtime.pop(task_id, None)
        if owned_sessions:
            # Close the 16 connections in the background so SendMedia is not delayed by slow socket shutdowns.
            asyncio.create_task(asyncio.wait_for(close_upload_sessions(owned_sessions), timeout=20))

# ------------------------------------------------------------
# 12. SEND RAW UPLOADED MEDIA & SAFE MESSAGES
# ------------------------------------------------------------
async def safe_send_message(client, chat_id, text, reply_to_message_id=None, **kwargs):
    """Helper to catch FloodWait and prevent crashing when sending messages."""
    for attempt in range(3):
        try:
            return await client.send_message(chat_id, text, reply_to_message_id=reply_to_message_id, **kwargs)
        except FloodWait as e:
            wait_time = int(getattr(e, "value", 0) or 0) + 1
            print(f"⚠️ [FLOOD_WAIT] Telegram requires a wait of {wait_time}s to send message. Sleeping...")
            await asyncio.sleep(wait_time)
        except Exception as e:
            print(f"❌ [ERROR] Could not send message: {e}")
            return None
    return None

async def send_uploaded_media(chat_id, input_file, filename, is_video, duration, width, height, thumb_path, caption):
    sender = upload_client
    thumb = None
    if thumb_path and os.path.exists(thumb_path):
        with contextlib.suppress(Exception): thumb = await sender.save_file(thumb_path)

    if is_video:
        mime_type = "video/mp4" if filename.lower().endswith(".mp4") else "video/x-matroska"
        attributes = [
            raw.types.DocumentAttributeVideo(duration=int(duration or 1), w=int(width or 1280), h=int(height or 720), supports_streaming=True),
            raw.types.DocumentAttributeFilename(file_name=filename),
        ]
        media = raw.types.InputMediaUploadedDocument(file=input_file, thumb=thumb, mime_type=mime_type, attributes=attributes, force_file=None)
    else:
        media = raw.types.InputMediaUploadedDocument(file=input_file, thumb=thumb, mime_type=get_mime_type(filename), attributes=[raw.types.DocumentAttributeFilename(file_name=filename)], force_file=True)

    peer = await sender.resolve_peer(chat_id)
    parsed_data = await sender.parser.parse(caption or "", enums.ParseMode.HTML)
    
    # Retry loop to catch FloodWait when sending the media file
    for attempt in range(3):
        try:
            return await sender.invoke(raw.functions.messages.SendMedia(
                peer=peer, 
                media=media, 
                message=parsed_data.get("message", ""), 
                entities=parsed_data.get("entities", None), 
                random_id=sender.rnd_id()
            ), timeout=900)  # Telegram assembles/processes big files server-side; default 15s timeout just re-sends the request
        except FloodWait as e:
            wait_time = int(getattr(e, "value", 0) or 0) + 2
            print(f"⚠️ [FLOOD_WAIT] Video uploading hit rate limit. Sleeping for {wait_time} seconds before retrying...")
            await asyncio.sleep(wait_time)
        except Exception as e:
            raise e
            
    raise RuntimeError("Failed to send media after multiple FloodWaits.")

# ------------------------------------------------------------
# 13. COMMANDS
# ------------------------------------------------------------
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await message.reply_text("ඔයාට ඕන Magnet Link එකක් මට එවන්න. මම ඒකේ Files ලිස්ට් එක අරන් දෙන්නම්!\n\nBatch (එක දිගට) දාන්න ඕනි නම් /batch යවන්න.\nSubtitle එක්කම Batch දාන්න ඕනි නම් /batch_sub යවන්න.\nවීඩියෝ සඳහා Custom Thumbnail එකක් යොදන්න ඕනි නම් /thumbnail යවන්න.\nUpload Mode සහ Quality (Original / 720p / 480p) වෙනස් කිරීමට /settings යවන්න.\nColab GPU (T4) එක සක්‍රියද කියලා බලන්න /gpu යවන්න.")

@app.on_message(filters.command("gpu"))
async def gpu_cmd(client, message):
    status_msg = await message.reply_text("🔍 GPU එක පරීක්ෂා කරමින්...")
    info = await check_gpu()

    if not info["active"]:
        text = (
            "❌ <b>GPU එක සක්‍රිය නැහැ</b>\n\n"
            "Colab එකේ GPU runtime එකක් තෝරලා නැහැ.\n"
            "සක්‍රිය කිරීමට: <b>Runtime → Change runtime type → T4 GPU</b> තෝරලා Cell එක නැවත run කරන්න.\n\n"
            "ℹ️ GPU නැතත් <b>Original Download / Upload සාමාන්‍ය පරිදි වැඩ කරනවා</b>. "
            "720p / 480p Encode කරන්නේ CPU එකෙන් (මන්දගාමී)."
        )
    else:
        is_t4 = "T4" in (info["name"] or "")
        nvenc_line = "✅ NVENC (GPU Encode) වැඩ කරනවා" if info["nvenc"] else "⚠️ NVENC වැඩ කරන්නේ නැහැ, Encode CPU එකෙන් වේ"
        text = (
            f"{'✅' if is_t4 else '⚠️'} <b>GPU සක්‍රියයි{'' if is_t4 else ' (T4 නොවේ)'}</b>\n\n"
            f"<b>GPU:</b> {safe_html(info['name'])}\n"
            f"<b>VRAM:</b> {safe_html(info['mem_used'])} / {safe_html(info['memory'])} MiB\n"
            f"<b>Utilization:</b> {safe_html(info['util'])}%\n"
            f"<b>Driver:</b> {safe_html(info['driver'])}\n\n"
            f"{nvenc_line}"
        )
    with contextlib.suppress(Exception):
        await status_msg.edit_text(text, parse_mode=enums.ParseMode.HTML)

def build_settings_ui(user_id):
    is_doc_mode = user_settings.get(user_id, True)
    quality = quality_settings.get(user_id, "orig")
    current_mode = "Send the original MKV as a document. (Default)" if is_doc_mode else "Set it up so it can be streamed and watched directly via Telegram."
    mode_btn = "🔄 Change to: Streamable MP4" if is_doc_mode else "🔄 Change to: Original Document"

    def q_btn(key, label):
        return InlineKeyboardButton(("✅ " if quality == key else "") + label, callback_data=f"set_q_{key}")

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(mode_btn, callback_data="toggle_mode")],
        [q_btn("orig", "Original Only")],
        [q_btn("720", "+720p"), q_btn("480", "+480p")],
        [q_btn("all", "All (720p + 480p)")],
    ])
    text = (
        f"⚙️ <b>Settings / සැකසුම්</b>\n\n"
        f"<b>Current Upload Mode:</b>\n<i>{current_mode}</i>\n\n"
        f"<b>Upload Quality:</b>\n<i>{QUALITY_LABELS[quality]}</i>\n\n"
        f"<i>වෙනස් කිරීමට පහත buttons ඔබන්න.</i>"
    )
    return text, markup

@app.on_message(filters.command("settings"))
async def settings_cmd(client, message):
    text, markup = build_settings_ui(message.from_user.id)
    await message.reply_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex(r"^toggle_mode$"))
async def toggle_mode_cb(client, callback_query):
    user_id = callback_query.from_user.id
    user_settings[user_id] = not user_settings.get(user_id, True)
    text, markup = build_settings_ui(user_id)
    await callback_query.message.edit_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
    await callback_query.answer("Upload Setting Updated!", show_alert=False)

@app.on_callback_query(filters.regex(r"^set_q_(orig|720|480|all)$"))
async def set_quality_cb(client, callback_query):
    user_id = callback_query.from_user.id
    choice = callback_query.data.split("set_q_", 1)[1]
    if quality_settings.get(user_id, "orig") == choice:
        return await callback_query.answer("දැනටමත් තෝරා ඇත.", show_alert=False)
    quality_settings[user_id] = choice
    text, markup = build_settings_ui(user_id)
    await callback_query.message.edit_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
    await callback_query.answer(f"Quality: {QUALITY_LABELS[choice]}", show_alert=False)

@app.on_message(filters.command("batch"))
async def batch_cmd(client, message):
    user_id = message.from_user.id
    batch_states[user_id] = {"is_active": True, "mode": "batch", "queue": [], "last_msg": await message.reply_text("📦 <b>Batch Mode ආරම්භ කළා!</b>\n\nඔයාට batch වශයෙන් download කරන්න ඕනි ටොරන්ට් ෆයිල් වල Link එකින් එක එවන්න...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="batch_cancel")]]), parse_mode=enums.ParseMode.HTML)}

@app.on_message(filters.command("batch_sub"))
async def batch_sub_cmd(client, message):
    user_id = message.from_user.id
    batch_states[user_id] = {"is_active": True, "mode": "batch_sub", "queue": [], "last_msg": await message.reply_text("📦 <b>Batch Sub Mode ආරම්භ කළා!</b>\n\nමෙමගින් වීඩියෝ Upload වූ පසු එහි ඇති Soft Subtitles වෙනමම ලබා දෙනු ඇත.\n\nඔයාට download කරන්න ඕනි ටොරන්ට් ෆයිල් වල Link එකින් එක එවන්න...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="batch_cancel")]]), parse_mode=enums.ParseMode.HTML)}

@app.on_message(filters.command("thumbnail"))
async def thumb_cmd(client, message):
    thumbnail_states[message.from_user.id] = True
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🖼️ View Thumbnail", callback_data="view_thumb"), InlineKeyboardButton("🗑️ Delete Thumbnail", callback_data="del_thumb")], [InlineKeyboardButton("⬅️ Cancel", callback_data="cancel_thumb")]])
    await message.reply_text("🖼 <b>Custom Thumbnail මෙනුව</b>\n\nඔබට අලුත් Thumbnail එකක් ලබා දීමට අවශ්‍ය නම්, <b>දැන්ම අදාළ ඡායාරූපය (Photo) එකක් ලෙස මෙතැනට එවන්න.</b>", reply_markup=markup, parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex(r"^view_thumb$"))
async def view_thumb_cb(c, cb):
    if os.path.exists(CUSTOM_THUMB_PATH): await cb.message.reply_photo(photo=CUSTOM_THUMB_PATH, caption="🖼️ දැනට Save වී ඇති Custom Thumbnail එක.")
    else: await cb.answer("❌ Custom Thumbnail එකක් සොයාගත නොහැකි විය.", show_alert=True)

@app.on_callback_query(filters.regex(r"^del_thumb$"))
async def del_thumb_cb(c, cb):
    if os.path.exists(CUSTOM_THUMB_PATH):
        os.remove(CUSTOM_THUMB_PATH)
        await cb.answer("🗑️ Custom Thumbnail මකා දමන ලදී!", show_alert=True)
    else: await cb.answer("❌ Delete කිරීමට Thumbnail එකක් නැත.", show_alert=True)

@app.on_callback_query(filters.regex(r"^cancel_thumb$"))
async def cancel_thumb_cb(c, cb):
    thumbnail_states[cb.from_user.id] = False
    with contextlib.suppress(Exception): await cb.message.edit_text("🚫 <b>Thumbnail සැකසීම අවලංගු කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)

@app.on_message(filters.photo & filters.private)
async def save_thumbnail(client, message):
    user_id = message.from_user.id
    if thumbnail_states.get(user_id, False):
        msg = await message.reply_text("📥 Thumbnail එක Save කරමින් පවතී...")
        await message.download(CUSTOM_THUMB_PATH)
        thumbnail_states[user_id] = False
        await msg.edit_text("✅ <b>Custom Thumbnail එක සාර්ථකව Save කරගත්තා!</b>\n\nමීළඟට Upload කරන සියලුම වීඩියෝ සඳහා මෙම Thumbnail එක භාවිතා වනු ඇත.", parse_mode=enums.ParseMode.HTML)

# ------------------------------------------------------------
# 14. MAGNET HANDLER
# ------------------------------------------------------------
@app.on_message(filters.regex(r"^magnet:\?xt="))
async def handle_magnet(client, message):
    user_id = message.from_user.id
    is_batch = user_id in batch_states and batch_states[user_id]["is_active"]
    batch_mode = batch_states[user_id].get("mode", "batch") if is_batch else "single"

    if is_batch and batch_states[user_id].get("last_msg"):
        with contextlib.suppress(Exception): await batch_states[user_id]["last_msg"].delete()

    status_msg = await message.reply_text("🔍 ටොරන්ට් දත්ත ලබාගනිමින් පවතී...")
    temp_dir = f"/content/meta_{message.id}"
    os.makedirs(temp_dir, exist_ok=True)

    try:
        cmd = ["aria2c", "--bt-metadata-only=true", "--bt-save-metadata=true", "--console-log-level=error", "--dir", temp_dir, message.text.strip()]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try: await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            return await status_msg.edit_text("❌ Metadata ලබාගැනීම timeout විය.")

        torrent_file_path = next((os.path.join(temp_dir, name) for name in os.listdir(temp_dir) if name.endswith(".torrent")), None)
        if not torrent_file_path: return await status_msg.edit_text("❌ Torrent metadata ලබාගැනීම අසාර්ථකයි.")

        meta_download = aria2.add_torrent(torrent_file_path, options={"pause": "true"})
        files, torrent_name = meta_download.files, meta_download.name
        file_list_html, file_list_text = build_file_tree(files, True), build_file_tree(files, False)

        with contextlib.suppress(Exception): aria2.remove([meta_download], force=True, files=False)
        await status_msg.delete()

        prompt_caption = "✅ <b>ගොනුව හඳුනාගත්තා!</b>\n\n👉 <b>මෙම පණිවිඩයට Reply කරමින්</b> අවශ්‍ය file අංක දෙන්න.\nඋදා: <code>1,3,5-7</code> (සියල්ලට: <code>all</code>)\n\n✏️ <b>Rename කිරීමට:</b> <code>[1] /rename අලුත් නම</code>"
        
        if len(file_list_html) > 3500:
            txt_path = os.path.join(temp_dir, "file_list.txt")
            with open(txt_path, "w", encoding="utf-8") as f: f.write(file_list_text)
            prompt_msg = await message.reply_document(document=txt_path, caption=f"Torrent එකේ file list එක දිගු වැඩියි.\n\n{prompt_caption}", parse_mode=enums.ParseMode.HTML)
        else:
            prompt_msg = await message.reply_text(f"<b>Files:</b>\n{file_list_html}\n\n{prompt_caption}", parse_mode=enums.ParseMode.HTML)

        task_id = str(prompt_msg.id)
        await prompt_msg.edit_reply_markup(InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{task_id}")]]))
        current_tasks[task_id] = {"torrent_file_path": torrent_file_path, "temp_dir": temp_dir, "torrent_name": torrent_name, "files": files, "user_id": user_id, "is_batch": is_batch, "mode": batch_mode}

    except Exception as e:
        with contextlib.suppress(Exception): await status_msg.edit_text(f"❌ දෝෂයක්:\n<code>{safe_html(str(e)[:3500])}</code>", parse_mode=enums.ParseMode.HTML)

# ------------------------------------------------------------
# 15. FILE SELECTION
# ------------------------------------------------------------
@app.on_message(filters.reply & filters.text)
async def handle_selection(client, message):
    if not message.reply_to_message: return
    replied_id = str(message.reply_to_message.id)
    if replied_id not in current_tasks: return

    task_data = current_tasks.pop(replied_id)
    selection_str = message.text.strip()
    
    if selection_str.lower() in ["cancel", "stop"]:
        await message.reply_text("🚫 <b>ක්‍රියාවලිය අවලංගු කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)
        if os.path.exists(task_data.get("temp_dir", "")): shutil.rmtree(task_data["temp_dir"], ignore_errors=True)
        return

    files, max_idx, renames, selected_indices = task_data["files"], len(task_data["files"]), {}, []

    if "/rename" in selection_str.lower():
        for line in selection_str.split('\n'):
            match = re.search(r'^\[?(\d+)\]?.*?/rename\s+(.+)$', line.strip(), re.IGNORECASE)
            if match and 1 <= int(match.group(1)) <= max_idx:
                idx = int(match.group(1))
                if idx not in selected_indices: selected_indices.append(idx)
                renames[idx] = match.group(2).strip()
        if not selected_indices: return await message.reply_text("❌ Rename format එක වැරදියි.")
    else:
        selected_indices = parse_selection(selection_str, max_idx)
        if not selected_indices: return await message.reply_text("❌ වලංගු file number එකක් නැහැ.")

    task_data.update({"renames": renames, "selected_indices": selected_indices, "original_msg_id": int(replied_id)})
    new_task_id = str(message.id)
    with contextlib.suppress(Exception): await message.reply_to_message.edit_reply_markup(None)

    if task_data.get("is_batch"):
        user_id = message.from_user.id
        task_data["task_id"] = new_task_id
        batch_states[user_id]["queue"].append(task_data)
        batch_states[user_id]["queue"] = sorted(batch_states[user_id]["queue"], key=lambda x: x["original_msg_id"])
        
        q_text = f"📦 <b>Batch Queue:</b>\n\n" + "".join([f"{idx}. {html.escape(q['torrent_name'])} (Files: {','.join(map(str, q['selected_indices']))})\n" for idx, q in enumerate(batch_states[user_id]["queue"], start=1)]) + "\n👉 <b>ඊළඟ Link එක දෙන්න නැත්නම් Start කරන්න</b>."
        batch_states[user_id]["last_msg"] = await message.reply_text(q_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Start", callback_data="batch_start")], [InlineKeyboardButton("❌ Cancel", callback_data="batch_cancel")]]), parse_mode=enums.ParseMode.HTML)
    else:
        current_tasks[new_task_id] = task_data
        cancel_flags[new_task_id] = False
        await message.reply_text(f"✅ තෝරාගත් ගොනු: <b><code>{','.join(map(str, selected_indices))}</code></b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Download Start", callback_data=f"dl_{new_task_id}")], [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{new_task_id}")]]), parse_mode=enums.ParseMode.HTML)

# ------------------------------------------------------------
# 16. BATCH ACTIONS & CALLBACKS
# ------------------------------------------------------------
@app.on_callback_query(filters.regex(r"^batch_cancel"))
async def cancel_batch_cb(c, cb):
    user_id = cb.from_user.id
    if user_id in batch_states:
        for q in batch_states[user_id].get("queue", []): shutil.rmtree(q.get("temp_dir", ""), ignore_errors=True)
        del batch_states[user_id]
    await cb.message.edit_text("🚫 <b>Batch Mode අක්‍රීය කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex(r"^batch_start"))
async def start_batch_cb(c, cb):
    user_id = cb.from_user.id
    if user_id not in batch_states or not batch_states[user_id]["queue"]: return await cb.answer("Queue එක හිස්!", show_alert=True)
    queue = sorted(batch_states[user_id]["queue"], key=lambda x: x["original_msg_id"])
    del batch_states[user_id]
    await cb.message.edit_text("⏳ <b>Batch Processing ආරම්භ කරමින්...</b>", parse_mode=enums.ParseMode.HTML)
    
    for idx, task_data in enumerate(queue, start=1):
        task_id = task_data["task_id"]
        current_tasks[task_id] = task_data
        cancel_flags[task_id] = False
        ui_msg = await cb.message.reply_text(f"⏳ <b>Batch File {idx}/{len(queue)} ආරම්භ වේ...</b>\n\n<code>{html.escape(task_data['torrent_name'])}</code>", parse_mode=enums.ParseMode.HTML)
        await execute_download_upload(c, cb.message.chat.id, task_id, task_data, ui_msg)
    await cb.message.reply_text("✅ <b>Batch Processing සම්පූර්ණයෙන්ම අවසන්!</b>", parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex(r"^(cancel|stop)_"))
async def cancel_callback(client, callback_query):
    task_id = callback_query.data.split("_", 1)[1]
    cancel_flags[task_id] = True
    if runtime := upload_runtime.get(task_id):
        runtime["cancel_event"].set()
        for task in runtime.get("upload_tasks", set()): task.cancel()
    
    if task := current_tasks.pop(task_id, None): shutil.rmtree(task.get("temp_dir", ""), ignore_errors=True)
    with contextlib.suppress(Exception): await callback_query.message.edit_text("🚫 <b>ක්‍රියාවලිය අවලංගු කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex(r"^cmp_(yes|no)_"))
async def compress_answer_cb(client, callback_query):
    _, choice, task_id = callback_query.data.split("_", 2)
    fut = compress_prompts.get(task_id)
    if not fut or fut.done():
        return await callback_query.answer("මෙම ප්‍රශ්නය කල් ඉකුත් වී ඇත.", show_alert=True)
    fut.set_result(choice == "yes")
    await callback_query.answer("OK!" if choice == "yes" else "Compress නොකරයි.", show_alert=False)

@app.on_callback_query(filters.regex(r"^dl_"))
async def download_callback(client, callback_query):
    task_id = callback_query.data.split("_", 1)[1]
    if task_id not in current_tasks: return await callback_query.answer("මෙම කාර්යය කල් ඉකුත් වී ඇත.", show_alert=True)
    cancel_flags[task_id] = False
    ui_msg = await callback_query.message.edit_text("⏳ ක්‍රියාවලිය ආරම්භ කරමින්...")
    await execute_download_upload(client, callback_query.message.chat.id, task_id, current_tasks[task_id], ui_msg)

async def upload_encoded_version(client, chat_id, task_id, ui_msg, encoded_path, res, filename, is_doc_mode, is_renamed, files_to_cleanup):
    base_name = generate_res_name(filename, res)
    final_path, final_name = encoded_path, base_name + ".mkv"

    if not is_doc_mode:
        await update_ui(ui_msg, f"⚙️ Player preparation ({res}p)", final_name, 0, 1, time.time(), [0], task_id, force=True)
        prepared_path, made_mp4, _ = await prepare_streamable_video(encoded_path)
        if made_mp4:
            final_path, final_name = prepared_path, base_name + ".mp4"
            files_to_cleanup.add(prepared_path)

    enc_duration, enc_w, enc_h = await get_video_meta(final_path)
    enc_input_file = await parallel_upload_file(final_path, final_name, task_id, ui_msg)
    if cancel_flags.get(task_id): return

    enc_thumb_path = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else await get_thumbnail(final_path)
    if enc_thumb_path and enc_thumb_path != CUSTOM_THUMB_PATH: files_to_cleanup.add(enc_thumb_path)
    caption = f"<code>{final_name}</code>" if is_renamed else f"✅ {final_name}"
    await send_uploaded_media(chat_id, enc_input_file, final_name, not is_doc_mode, enc_duration, enc_w, enc_h, enc_thumb_path, caption)

# ------------------------------------------------------------
# 18. EXECUTE DOWNLOAD -> (UPLOAD ORIGINAL || ENCODE 720P+480P) -> UPLOAD ENCODES
# ------------------------------------------------------------
async def execute_download_upload(client, chat_id, task_id, task_data, ui_msg):
    user_id = task_data.get("user_id")
    is_doc_mode = user_settings.get(user_id, True)
    quality_choice = quality_settings.get(user_id, "orig")
    want_720 = quality_choice in ("720", "all")
    want_480 = quality_choice in ("480", "all")
    torrent_file_path, temp_dir, files = task_data["torrent_file_path"], task_data["temp_dir"], task_data["files"]
    custom_renames = task_data.get("renames", {})
    download_dir = f"/content/downloads_{task_id}"
    os.makedirs(download_dir, exist_ok=True)

    try:
        for i in task_data["selected_indices"]:
            if cancel_flags.get(task_id): break

            # Collection of file paths to delete ONLY at the end of loop
            files_to_cleanup = set()

            file_obj = files[i - 1]
            original_filename = os.path.basename(str(file_obj.path))
            orig_ext = os.path.splitext(original_filename)[1]
            
            if i in custom_renames:
                u_name = custom_renames[i].strip()
                _, u_ext = os.path.splitext(u_name)
                # Ensure we only append original extension if user didn't provide a valid one
                if u_ext.lower() in [".mkv", ".mp4", ".avi", ".webm", ".flv", ".mov", ".m4v", ".ts", orig_ext.lower()]:
                    filename = u_name
                else:
                    filename = u_name + orig_ext
            else:
                filename = original_filename

            # --- 1. DOWNLOAD ---
            dl_opts = {"select-file": str(i), "seed-time": "0", "dir": download_dir, "allow-overwrite": "true", "max-connection-per-server": "16", "split": "16", "min-split-size": "4M", "file-allocation": "none"}
            active_dl, downloaded_file_path = None, None

            try:
                active_dl = aria2.add_torrent(torrent_file_path, options=dl_opts)
                start_time, last_edit_time = time.time(), [0]
                while active_dl.status not in ["complete", "error", "removed"]:
                    if cancel_flags.get(task_id):
                        with contextlib.suppress(Exception): aria2.remove([active_dl], force=True, files=False)
                        break
                    await asyncio.sleep(1.0)
                    with contextlib.suppress(Exception): active_dl.update()
                    if active_dl.total_length > 0:
                        await update_ui(ui_msg, "📥 Torrent Download", filename, active_dl.completed_length, active_dl.total_length, start_time, last_edit_time, task_id, extra_lines=[f"<b>Download:</b> {format_bytes(active_dl.download_speed)}/s", f"<b>Connections:</b> {active_dl.connections}"])

                if cancel_flags.get(task_id): break
                if active_dl.status == "error":
                    await ui_msg.edit_text(f"❌ Download error:\n<code>{safe_html(filename)}</code>", parse_mode=enums.ParseMode.HTML)
                    continue

                for f in active_dl.files:
                    if getattr(f, "selected", False) and os.path.exists(str(f.path)): downloaded_file_path = str(f.path); break
            finally:
                if active_dl:
                    with contextlib.suppress(Exception): aria2.remove([active_dl], force=True, files=False)
            if not downloaded_file_path: continue

            original_path = downloaded_file_path
            files_to_cleanup.add(original_path)
            encode_task = None
            wanted_res = [r for r, w in ((720, want_720), (480, want_480)) if w]

            try:
                # --- 2. UPLOAD ORIGINAL QUALITY ---
                is_actual_video = original_path.lower().endswith((".mp4", ".mkv", ".avi", ".webm", ".flv", ".mov", ".m4v", ".ts"))
                upload_src, compressed = original_path, False

                # --- 2a. FILE > 2 GiB: ask, then shrink with the GPU ---
                original_size = os.path.getsize(original_path)
                if is_actual_video and original_size > MAX_FILE_SIZE:
                    if await ask_compress(client, chat_id, task_id, filename, original_size):
                        try:
                            small = await compress_to_limit(client, chat_id, original_path, task_id, filename, ui_msg)
                        except asyncio.CancelledError: break
                        if small:
                            upload_src, compressed = small, True
                            files_to_cleanup.add(small)
                            filename = os.path.splitext(filename)[0] + ".mkv"
                        else:
                            await safe_send_message(client, chat_id, "⚠️ <b>Compress අසාර්ථකයි.</b> Original file එක Upload කිරීමට උත්සාහ කරයි.", parse_mode=enums.ParseMode.HTML)
                    if cancel_flags.get(task_id): break
                downloaded_file_path = upload_src

                if is_actual_video and not is_doc_mode:
                    prepared_path, made_mp4, _ = await prepare_streamable_video(upload_src)
                    if made_mp4: filename = os.path.splitext(filename)[0] + ".mp4"
                    downloaded_file_path = prepared_path
                    files_to_cleanup.add(prepared_path)

                thumb_path = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else None
                try:
                    duration, width, height = (await get_video_meta(downloaded_file_path)) if is_actual_video else (0, 0, 0)

                    # Start Re-Encoding in the background NOW (decode once for all resolutions),
                    # so it runs while the original file is uploading. It has its own progress message.
                    if is_actual_video and wanted_res:
                        for r in wanted_res: files_to_cleanup.add(f"{os.path.splitext(original_path)[0]}_{r}p_temp.mkv")
                        encode_task = asyncio.create_task(
                            encode_multi(client, chat_id, original_path, wanted_res, duration, task_id, filename)
                        )
                    if is_actual_video and not thumb_path: 
                        thumb_path = await get_thumbnail(downloaded_file_path)
                        if thumb_path: files_to_cleanup.add(thumb_path)

                    input_file = await parallel_upload_file(downloaded_file_path, filename, task_id, ui_msg)
                    if cancel_flags.get(task_id): raise asyncio.CancelledError

                    caption_text = f"<code>{filename}</code>" if i in custom_renames else f"✅ {filename}"
                    updates = await send_uploaded_media(
                        chat_id, input_file, filename, 
                        is_actual_video and not is_doc_mode, 
                        duration, width, height, thumb_path, 
                        caption_text
                    )

                    # Extract Subs
                    if task_data.get("mode") == "batch_sub" and is_actual_video:
                        msg_id = None
                        for u in getattr(updates, "updates", None) or []:
                            m = getattr(u, "message", None)  # UpdateNewMessage / UpdateNewChannelMessage (UpdateMessageID has none)
                            if m is not None and getattr(m, "id", None):
                                msg_id = m.id
                                break
                        try:
                            await extract_and_send_subtitles(client, original_path, chat_id, msg_id, temp_dir, custom_filename=filename)
                        except Exception as sub_err:
                            print(f"[ERROR] Subtitle step failed: {sub_err}")

                    # Compressed copy is on Telegram now -> free the Colab SSD by deleting the big original
                    # (kept only if 720p/480p encodes still need it as their source).
                    if compressed and not encode_task:
                        with contextlib.suppress(Exception): os.remove(original_path)

                except asyncio.CancelledError: break
                except Exception as e:
                    await safe_send_message(client, chat_id, f"❌ <b>Original Upload error:</b>\n<code>{safe_html(str(e)[:3500])}</code>", parse_mode=enums.ParseMode.HTML)


                # --- 3. UPLOAD THE RE-ENCODED VERSIONS (encoding ran in parallel with the original upload) ---
                if encode_task and not cancel_flags.get(task_id):
                    if not encode_task.done():
                        with contextlib.suppress(Exception):
                            await ui_msg.edit_text(
                                "⏳ <b>Original Upload අවසන්. Encode එක ඉවර වෙනකම් බලා සිටිමින්...</b>\n<i>Encode progress වෙනම message එකේ පෙන්වයි.</i>",
                                parse_mode=enums.ParseMode.HTML,
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Task", callback_data=f"stop_{task_id}")]]),
                            )
                    try:
                        encoded_files = await encode_task
                    except asyncio.CancelledError: break
                    except Exception as e:
                        encoded_files = {}
                        await safe_send_message(client, chat_id, f"⚠️ <b>Encode අසාර්ථකයි:</b>\n<code>{safe_html(str(e)[:1000])}</code>", parse_mode=enums.ParseMode.HTML)
                    if compressed:
                        with contextlib.suppress(Exception): os.remove(original_path)

                    for res in wanted_res:
                        if cancel_flags.get(task_id): break
                        encoded_path = encoded_files.get(res)
                        if not encoded_path: continue
                        try:
                            await upload_encoded_version(
                                client, chat_id, task_id, ui_msg, encoded_path, res, filename,
                                is_doc_mode, i in custom_renames, files_to_cleanup,
                            )
                        except asyncio.CancelledError: break
                        except Exception as e:
                            await safe_send_message(client, chat_id, f"⚠️ <b>{res}p Upload අසාර්ථකයි:</b>\n<code>{safe_html(str(e)[:1000])}</code>", parse_mode=enums.ParseMode.HTML)

            finally:
                if encode_task and not encode_task.done():
                    encode_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception): await encode_task
                # --- 5. FINAL CLEANUP (Delete ALL at the very end of processing the file) ---
                for p in files_to_cleanup:
                    if p and os.path.exists(p) and p != CUSTOM_THUMB_PATH:
                        with contextlib.suppress(Exception): os.remove(p)

        # MESSAGE_NOT_MODIFIED (cancel handler may have already set this text) is harmless
        with contextlib.suppress(Exception):
            if cancel_flags.get(task_id): await ui_msg.edit_text("🚫 <b>ක්‍රියාවලිය අවලංගු කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)
            else: await ui_msg.edit_text("<b>✅ සියලුම තෝරාගත් files Upload කර අවසන්!</b>", parse_mode=enums.ParseMode.HTML)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        shutil.rmtree(download_dir, ignore_errors=True)
        current_tasks.pop(task_id, None)
        cancel_flags.pop(task_id, None)


# ------------------------------------------------------------
# 19. START BOT SAFELY
# ------------------------------------------------------------
async def run_bot():
    global upload_client, startup_flood_wait
    print("\n" + "=" * 64)
    print("🚀 Starting ONE persistent Telegram bot session...")
    print("=" * 64)
    try: await app.start()
    except FloodWait as e:
        print(f"\n❌ TELEGRAM FLOOD_WAIT DETECTED. Wait {e.value} seconds.")
        return
    except RPCError as e:
        print(f"\n❌ Telegram RPC error: {e}")
        return

    upload_client = app
    print("✅ Main Telegram session authorized.\n✅ TELEGRAM TORRENT BOT IS RUNNING")
    print("👉 Telegram වෙත ගොස් Bot එකට /start යවන්න.\n")
    try: await idle()
    finally:
        upload_client = None
        with contextlib.suppress(Exception): await app.stop()
        with contextlib.suppress(Exception): aria2_process.terminate()

# ------------------------------------------------------------
# 20. START
# ------------------------------------------------------------
await run_bot()