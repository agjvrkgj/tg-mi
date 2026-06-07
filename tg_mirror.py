#!/usr/bin/env python3
"""Monitor Telegram channels and mirror new videos to another channel."""

import asyncio
import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlparse
from pathlib import Path
from typing import Iterable

from telethon import TelegramClient, events, functions, types, utils
from telethon.tl.custom.message import Message
from telethon.tl.types import DocumentAttributeVideo

ROOT_DIR = Path(__file__).resolve().parent


def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE pairs from a .env file without overriding existing env vars."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("\'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file(ROOT_DIR / ".env")
SESSION_NAME = str(ROOT_DIR / "channel_mirror")


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_channels(raw: str) -> list[int | str]:
    channels: list[int | str] = []
    for item in raw.split(","):
        value = item.strip()
        if not value:
            continue
        if re.fullmatch(r"-?\d+", value):
            channels.append(int(value))
        else:
            channels.append(value)
    return channels


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    source_channels: list[int | str]
    target_channel: int | str
    download_dir: Path
    album_wait: float
    upload_timeout: int
    trim_video_seconds: int
    thumbnail_second: int
    include_photos: bool
    strip_links: bool
    admin_enabled: bool
    admin_host: str
    admin_port: int
    admin_token: str

    @classmethod
    def from_env(cls) -> "Settings":
        api_id_raw = os.getenv("TG_API_ID", "").strip()
        api_hash = os.getenv("TG_API_HASH", "").strip()
        source_channels = parse_channels(os.getenv("TG_SOURCE_CHANNELS", ""))
        target_channel_raw = os.getenv("TG_TARGET_CHANNEL", "").strip()

        errors = []
        if not api_id_raw:
            errors.append("TG_API_ID")
        if not api_hash:
            errors.append("TG_API_HASH")
        if not source_channels:
            errors.append("TG_SOURCE_CHANNELS")
        if not target_channel_raw:
            errors.append("TG_TARGET_CHANNEL")
        if errors:
            joined = ", ".join(errors)
            raise RuntimeError(f"缺少必要环境变量: {joined}。请复制 .env.example 为 .env 后填写。")

        target_channel: int | str
        if re.fullmatch(r"-?\d+", target_channel_raw):
            target_channel = int(target_channel_raw)
        else:
            target_channel = target_channel_raw

        download_dir = Path(os.getenv("TG_DOWNLOAD_DIR", str(ROOT_DIR / "tg_mirror_tmp"))).expanduser()
        if not download_dir.is_absolute():
            download_dir = ROOT_DIR / download_dir

        return cls(
            api_id=int(api_id_raw),
            api_hash=api_hash,
            source_channels=source_channels,
            target_channel=target_channel,
            download_dir=download_dir,
            album_wait=float(os.getenv("TG_ALBUM_WAIT", "3")),
            upload_timeout=int(os.getenv("TG_UPLOAD_TIMEOUT", "1800")),
            trim_video_seconds=int(os.getenv("TG_TRIM_VIDEO_SECONDS", "10")),
            thumbnail_second=int(os.getenv("TG_THUMBNAIL_SECOND", "5")),
            include_photos=env_bool("TG_INCLUDE_PHOTOS", False),
            strip_links=env_bool("TG_STRIP_LINKS", True),
            admin_enabled=env_bool("TG_ADMIN_ENABLED", True),
            admin_host=os.getenv("TG_ADMIN_HOST", "127.0.0.1").strip() or "127.0.0.1",
            admin_port=int(os.getenv("TG_ADMIN_PORT", "8080")),
            admin_token=os.getenv("TG_ADMIN_TOKEN", "").strip(),
        )


settings = Settings.from_env()
settings.download_dir.mkdir(parents=True, exist_ok=True)

client = TelegramClient(SESSION_NAME, settings.api_id, settings.api_hash)
client.flood_sleep_threshold = 60

task_queue: asyncio.Queue[tuple[str, Message | list[Message]]] = asyncio.Queue()
album_buffer: dict[int, dict[str, object]] = {}
recent_logs: deque[str] = deque(maxlen=300)
started_at = time.time()
mirror_paused = False
stats = {
    "seen": 0,
    "queued_single": 0,
    "queued_album": 0,
    "downloaded": 0,
    "trimmed": 0,
    "sent_single": 0,
    "sent_album": 0,
    "failed": 0,
    "skipped": 0,
}


@dataclass
class PreparedMedia:
    path: Path
    is_video: bool
    thumb_path: Path | None = None
    attributes: list[DocumentAttributeVideo] | None = None


def log(message: str) -> None:
    line = f"{time.strftime('[%Y-%m-%d %H:%M:%S]')} {message}"
    recent_logs.append(line)
    print(line, flush=True)


def inc_stat(name: str, value: int = 1) -> None:
    stats[name] = stats.get(name, 0) + value


def clean_caption(text: str | None) -> str:
    if not text:
        return ""
    if not settings.strip_links:
        return text.strip()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\bt\.me/\S+", "", text)
    text = re.sub(r"@\S+", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def is_video_message(message: Message) -> bool:
    if message.video or message.gif:
        return True
    if message.document and message.document.mime_type:
        return message.document.mime_type.startswith("video/")
    return False


def is_photo_message(message: Message) -> bool:
    if message.photo:
        return True
    if message.document and message.document.mime_type:
        return message.document.mime_type.startswith("image/")
    return False


def is_album_media(message: Message) -> bool:
    return is_video_message(message) or is_photo_message(message)


def is_wanted_media(message: Message) -> bool:
    return is_video_message(message) or (settings.include_photos and is_photo_message(message))


def remove_file(path: str | Path | None) -> None:
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except OSError as exc:
        log(f"清理文件失败: {path}: {exc}")


def ffmpeg_exists() -> bool:
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None



def video_duration(path: Path) -> int:
    if not ffmpeg_exists():
        return 0
    try:
        probe = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_format",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if probe.returncode != 0:
            return 0
        info = json.loads(probe.stdout)
        return int(float(info.get("format", {}).get("duration", 0) or 0))
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, ValueError):
        return 0

def video_metadata(path: Path, thumbnail_second: int = 5) -> tuple[int, int, int, Path | None]:
    """Return duration, width, height and thumbnail path when ffmpeg is available."""
    if not ffmpeg_exists():
        return 0, 0, 0, None

    duration = 0
    width = 0
    height = 0
    thumb_path = path.with_suffix(path.suffix + ".thumb.jpg")

    try:
        probe = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_streams",
                "-show_format",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if probe.returncode == 0:
            info = json.loads(probe.stdout)
            duration = int(float(info.get("format", {}).get("duration", 0) or 0))
            for stream in info.get("streams", []):
                if stream.get("codec_type") == "video":
                    width = int(stream.get("width", 0) or 0)
                    height = int(stream.get("height", 0) or 0)
                    break

        if duration > 1:
            seek_second = max(0, min(thumbnail_second, duration - 1))
        else:
            seek_second = max(0, thumbnail_second)
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                str(seek_second),
                "-i",
                str(path),
                "-vframes",
                "1",
                "-an",
                "-vf",
                "scale=320:-2",
                "-q:v",
                "2",
                str(thumb_path),
            ],
            capture_output=True,
            timeout=45,
            check=False,
        )
        if not thumb_path.exists() or thumb_path.stat().st_size == 0:
            remove_file(thumb_path)
            thumb_path = None
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, ValueError) as exc:
        log(f"读取视频元数据失败: {path.name}: {exc}")
        remove_file(thumb_path)
        thumb_path = None

    return duration, width, height, thumb_path


def ensure_video_suffix(path: Path) -> Path:
    if path.suffix.lower() in {".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v"}:
        return path
    new_path = path.with_suffix(path.suffix + ".mp4")
    path.rename(new_path)
    return new_path


def trim_video_start(path: Path, seconds: int) -> bool:
    """Remove the first N seconds from a video in-place before uploading it."""
    if seconds <= 0:
        return True
    if not ffmpeg_exists():
        log("视频裁剪失败: 未检测到 ffmpeg/ffprobe，无法按要求删除视频开头")
        return False

    duration = video_duration(path)
    if duration and duration <= seconds:
        log(f"视频裁剪失败: {path.name} 时长 {duration}s，不足以删除开头 {seconds}s")
        return False

    trimmed_path = path.with_suffix(path.suffix + ".trimmed.mp4")
    try:
        result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                str(seconds),
                "-i",
                str(path),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "20",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-movflags",
                "+faststart",
                "-force_key_frames",
                "expr:eq(t,0)",
                str(trimmed_path),
            ],
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        if result.returncode != 0 or not trimmed_path.exists() or trimmed_path.stat().st_size == 0:
            stderr = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown ffmpeg error"
            log(f"视频裁剪失败: {path.name}: {stderr}")
            remove_file(trimmed_path)
            return False

        trimmed_path.replace(path)
        inc_stat("trimmed")
        log(f"已删除视频开头 {seconds}s: {path.name}")
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        log(f"视频裁剪异常: {path.name}: {exc}")
        remove_file(trimmed_path)
        return False


def prepare_video(path: Path) -> PreparedMedia | None:
    path = ensure_video_suffix(path)
    if not trim_video_start(path, settings.trim_video_seconds):
        remove_file(path)
        return None

    duration, width, height, thumb_path = video_metadata(path, settings.thumbnail_second)
    attributes = [
        DocumentAttributeVideo(
            duration=duration,
            w=width or 1280,
            h=height or 720,
            supports_streaming=True,
        )
    ]
    if thumb_path:
        log(f"已选取裁剪后视频第 {settings.thumbnail_second}s 画面作为封面: {path.name}")
    return PreparedMedia(path=path, is_video=True, thumb_path=thumb_path, attributes=attributes)


async def refresh_message(message: Message) -> Message:
    try:
        chat = await message.get_input_chat()
        refreshed = await client.get_messages(chat, ids=message.id)
        return refreshed or message
    except Exception as exc:
        log(f"刷新消息失败 msg_id={message.id}: {exc}")
        return message


async def download_message(message: Message) -> Path | None:
    message = await refresh_message(message)
    downloaded = await message.download_media(file=str(settings.download_dir))
    if not downloaded:
        return None
    inc_stat("downloaded")
    path = Path(downloaded)
    if not path.exists() or path.stat().st_size == 0:
        remove_file(path)
        return None
    return path


async def send_single(message: Message) -> None:
    path = await download_message(message)
    if path is None:
        log(f"跳过 msg_id={message.id}: 下载失败或空文件")
        return

    thumb_path: Path | None = None
    try:
        if is_video_message(message):
            prepared = prepare_video(path)
            if not prepared:
                log(f"跳过 msg_id={message.id}: 视频裁剪失败，避免上传未裁剪视频")
                return
            path = prepared.path
            thumb_path = prepared.thumb_path
            await asyncio.wait_for(
                client.send_file(
                    settings.target_channel,
                    file=str(prepared.path),
                    caption=clean_caption(message.text),
                    thumb=str(prepared.thumb_path) if prepared.thumb_path else None,
                    attributes=prepared.attributes,
                    force_document=False,
                    supports_streaming=True,
                ),
                timeout=settings.upload_timeout,
            )
        else:
            await asyncio.wait_for(
                client.send_file(
                    settings.target_channel,
                    file=str(path),
                    caption=clean_caption(message.text),
                    force_document=False,
                ),
                timeout=settings.upload_timeout,
            )
        inc_stat("sent_single")
        log(f"搬运成功 msg_id={message.id}")
    except asyncio.TimeoutError:
        inc_stat("failed")
        log(f"搬运超时 msg_id={message.id}: 超过 {settings.upload_timeout}s")
    except Exception as exc:
        inc_stat("failed")
        log(f"搬运失败 msg_id={message.id}: {exc}")
    finally:
        remove_file(path)
        remove_file(thumb_path)


async def send_prepared_album(media_items: list[PreparedMedia], caption: str) -> None:
    """Send an album with per-video thumbnails while keeping video/photo/text grouped."""
    entity = await client.get_input_entity(settings.target_channel)
    multi_media = []
    cleaned_caption = clean_caption(caption)

    for index, item in enumerate(media_items):
        file_to_media_kwargs = {
            "force_document": False,
            "supports_streaming": True,
        }
        if item.is_video:
            file_to_media_kwargs.update(
                {
                    "attributes": item.attributes,
                    "thumb": str(item.thumb_path) if item.thumb_path else None,
                    "nosound_video": True,
                }
            )

        _, input_media, _ = await client._file_to_media(str(item.path), **file_to_media_kwargs)
        if isinstance(input_media, (types.InputMediaUploadedPhoto, types.InputMediaPhotoExternal)):
            uploaded = await client(functions.messages.UploadMediaRequest(entity, media=input_media))
            input_media = utils.get_input_media(uploaded.photo)
        elif isinstance(input_media, (types.InputMediaUploadedDocument, types.InputMediaDocumentExternal)):
            uploaded = await client(functions.messages.UploadMediaRequest(entity, media=input_media))
            input_media = utils.get_input_media(uploaded.document, supports_streaming=True)

        multi_media.append(
            types.InputSingleMedia(
                input_media,
                message=cleaned_caption if index == 0 else "",
            )
        )

    await client(functions.messages.SendMultiMediaRequest(entity, multi_media=multi_media))


async def send_album(messages: Iterable[Message]) -> None:
    album_messages = [message for message in messages if is_album_media(message)]
    has_video = any(is_video_message(message) for message in album_messages)
    if not album_messages or (not has_video and not settings.include_photos):
        return

    # 相册里只要有视频，就把同组图片也一起发送，保持“视频+图片+文字”一条消息。
    selected = album_messages if has_video else [message for message in album_messages if is_photo_message(message)]

    prepared_files: list[PreparedMedia] = []
    caption = next((message.text for message in selected if message.text), "")
    try:
        for message in selected:
            path = await download_message(message)
            if path is None:
                log(f"相册跳过 msg_id={message.id}: 下载失败或空文件")
                continue

            if is_video_message(message):
                prepared = prepare_video(path)
                if not prepared:
                    log(f"相册跳过: msg_id={message.id} 视频裁剪失败，避免发送不完整或未裁剪相册")
                    return
                prepared_files.append(prepared)
            else:
                prepared_files.append(PreparedMedia(path=path, is_video=False))

        if not prepared_files:
            log("相册跳过: 没有成功下载的媒体")
            return

        await asyncio.wait_for(
            send_prepared_album(prepared_files, caption),
            timeout=settings.upload_timeout,
        )
        inc_stat("sent_album")
        log(f"搬运成功 album count={len(prepared_files)}")
    except asyncio.TimeoutError:
        inc_stat("failed")
        log(f"相册搬运超时: 超过 {settings.upload_timeout}s")
    except Exception as exc:
        inc_stat("failed")
        log(f"相册搬运失败: {exc}")
    finally:
        for prepared in prepared_files:
            remove_file(prepared.path)
            remove_file(prepared.thumb_path)


async def worker() -> None:
    while True:
        task_type, payload = await task_queue.get()
        try:
            if task_type == "single":
                await send_single(payload)  # type: ignore[arg-type]
            if task_type == "album":
                await send_album(payload)  # type: ignore[arg-type]
        except Exception as exc:
            log(f"任务处理异常: {exc}")
        finally:
            task_queue.task_done()


async def cleanup_worker() -> None:
    while True:
        await asyncio.sleep(600)
        cutoff = time.time() - 7200
        cleaned = 0
        for path in settings.download_dir.iterdir():
            if path.is_file() and path.stat().st_mtime < cutoff:
                remove_file(path)
                cleaned += 1
        if cleaned:
            log(f"已清理过期临时文件 {cleaned} 个")


async def collect_album(grouped_id: int) -> None:
    await asyncio.sleep(settings.album_wait)
    data = album_buffer.pop(grouped_id, None)
    if not data:
        return
    messages = data.get("messages", [])
    await task_queue.put(("album", messages))
    inc_stat("queued_album")


@client.on(events.NewMessage(chats=settings.source_channels))
async def on_new_message(event: events.NewMessage.Event) -> None:
    message = event.message
    inc_stat("seen")
    if mirror_paused:
        inc_stat("skipped")
        return

    if message.grouped_id:
        if not is_album_media(message):
            return
        grouped_id = int(message.grouped_id)
        if grouped_id not in album_buffer:
            album_buffer[grouped_id] = {"messages": [], "task": None}
        album_buffer[grouped_id]["messages"].append(message)  # type: ignore[union-attr]
        task = album_buffer[grouped_id].get("task")
        if task:
            task.cancel()  # type: ignore[attr-defined]
        album_buffer[grouped_id]["task"] = asyncio.create_task(collect_album(grouped_id))
        return

    if not is_wanted_media(message):
        return
    await task_queue.put(("single", message))
    inc_stat("queued_single")


def disk_usage_snapshot() -> dict[str, object]:
    usage = shutil.disk_usage(settings.download_dir)
    temp_files = [path for path in settings.download_dir.iterdir() if path.is_file()]
    return {
        "download_dir": str(settings.download_dir),
        "total_gb": round(usage.total / 1024 / 1024 / 1024, 2),
        "used_gb": round(usage.used / 1024 / 1024 / 1024, 2),
        "free_gb": round(usage.free / 1024 / 1024 / 1024, 2),
        "temp_file_count": len(temp_files),
        "temp_file_mb": round(sum(path.stat().st_size for path in temp_files) / 1024 / 1024, 2),
    }


def admin_status() -> dict[str, object]:
    return {
        "running": True,
        "paused": mirror_paused,
        "uptime_seconds": int(time.time() - started_at),
        "queue_size": task_queue.qsize(),
        "album_buffer_size": len(album_buffer),
        "ffmpeg_available": ffmpeg_exists(),
        "sources": [str(channel) for channel in settings.source_channels],
        "target": str(settings.target_channel),
        "include_photos": settings.include_photos,
        "strip_links": settings.strip_links,
        "trim_video_seconds": settings.trim_video_seconds,
        "thumbnail_second": settings.thumbnail_second,
        "stats": dict(stats),
        "disk": disk_usage_snapshot(),
        "logs": list(recent_logs)[-80:],
    }


def render_admin_page() -> str:
    status = admin_status()
    disk = status["disk"]
    rows = "".join(
        f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in status["stats"].items()
    )
    logs = "\n".join(html.escape(line) for line in status["logs"])
    token_input = ""
    if settings.admin_token:
        token_input = f'<input type="hidden" name="token" value="{html.escape(settings.admin_token)}">'
    control_action = "resume" if status["paused"] else "pause"
    control_text = "继续搬运" if status["paused"] else "暂停搬运"
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TG Mirror 管理后台</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f6f7fb; color: #172033; }}
    header {{ background: #182235; color: white; padding: 20px 28px; }}
    main {{ padding: 24px; display: grid; gap: 18px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }}
    section {{ background: white; border-radius: 14px; padding: 18px; box-shadow: 0 8px 24px rgba(20, 31, 55, .08); }}
    h1, h2 {{ margin: 0 0 12px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; border-bottom: 1px solid #edf0f5; padding: 8px; }}
    .badge {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: {'#ffd8d8' if status['paused'] else '#d8f7df'}; }}
    pre {{ white-space: pre-wrap; max-height: 420px; overflow: auto; background: #101827; color: #d6e2ff; padding: 12px; border-radius: 10px; }}
    button {{ border: 0; border-radius: 10px; background: #2563eb; color: white; padding: 10px 14px; cursor: pointer; }}
  </style>
</head>
<body>
<header><h1>TG Mirror 管理后台</h1><div>状态：<span class="badge">{'已暂停' if status['paused'] else '运行中'}</span></div></header>
<main>
  <section>
    <h2>控制</h2>
    <form method="post" action="/control">
      {token_input}
      <input type="hidden" name="action" value="{control_action}">
      <button type="submit">{control_text}</button>
    </form>
    <p>队列：{status['queue_size']}；相册缓冲：{status['album_buffer_size']}；运行：{status['uptime_seconds']} 秒</p>
  </section>
  <section>
    <h2>配置</h2>
    <p><b>源频道：</b>{html.escape(', '.join(status['sources']))}</p>
    <p><b>目标频道：</b>{html.escape(str(status['target']))}</p>
    <p><b>视频裁剪：</b>{status['trim_video_seconds']} 秒；<b>封面：</b>裁剪后第 {status['thumbnail_second']} 秒</p>
    <p><b>ffmpeg：</b>{'可用' if status['ffmpeg_available'] else '不可用'}</p>
  </section>
  <section>
    <h2>磁盘</h2>
    <p><b>下载目录：</b>{html.escape(str(disk['download_dir']))}</p>
    <p>总计 {disk['total_gb']} GB，已用 {disk['used_gb']} GB，可用 {disk['free_gb']} GB</p>
    <p>临时文件 {disk['temp_file_count']} 个，共 {disk['temp_file_mb']} MB</p>
  </section>
  <section>
    <h2>统计</h2>
    <table>{rows}</table>
  </section>
  <section style="grid-column: 1 / -1;">
    <h2>最近日志</h2>
    <pre>{logs}</pre>
  </section>
</main>
</body>
</html>"""


class AdminHandler(BaseHTTPRequestHandler):
    server_version = "TGMirrorAdmin/1.0"

    def log_message(self, format: str, *args: object) -> None:
        return

    def is_authorized(self) -> bool:
        if not settings.admin_token:
            return True
        parsed = urlparse(self.path)
        token = parse_qs(parsed.query).get("token", [""])[0]
        return token == settings.admin_token or self.headers.get("X-Admin-Token") == settings.admin_token

    def send_bytes(self, body: bytes, content_type: str = "text/html; charset=utf-8", status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def require_auth(self) -> bool:
        if self.is_authorized():
            return True
        self.send_bytes(b"Unauthorized", "text/plain; charset=utf-8", 401)
        return False

    def do_GET(self) -> None:
        if not self.require_auth():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            self.send_bytes(json.dumps(admin_status(), ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        if parsed.path == "/api/logs":
            self.send_bytes(json.dumps(list(recent_logs), ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        self.send_bytes(render_admin_page().encode())

    def do_POST(self) -> None:
        global mirror_paused
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length).decode("utf-8") if length else ""
        form = parse_qs(body)
        token = form.get("token", [""])[0]
        if settings.admin_token and token != settings.admin_token and self.headers.get("X-Admin-Token") != settings.admin_token:
            self.send_bytes(b"Unauthorized", "text/plain; charset=utf-8", 401)
            return
        parsed = urlparse(self.path)
        if parsed.path not in {"/control", "/api/control"}:
            self.send_bytes(b"Not found", "text/plain; charset=utf-8", 404)
            return
        action = form.get("action", [""])[0]
        if action == "pause":
            mirror_paused = True
            log("管理后台：已暂停接收新搬运任务")
        elif action == "resume":
            mirror_paused = False
            log("管理后台：已恢复接收新搬运任务")
        else:
            self.send_bytes(b"Bad request", "text/plain; charset=utf-8", 400)
            return
        if parsed.path == "/api/control":
            self.send_bytes(json.dumps(admin_status(), ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        self.send_response(303)
        location = "/"
        if settings.admin_token:
            location = f"/?token={settings.admin_token}"
        self.send_header("Location", location)
        self.end_headers()


def start_admin_server() -> None:
    if not settings.admin_enabled:
        return
    server = ThreadingHTTPServer((settings.admin_host, settings.admin_port), AdminHandler)
    thread = Thread(target=server.serve_forever, name="tg-mirror-admin", daemon=True)
    thread.start()
    auth_hint = "（已启用 token 保护）" if settings.admin_token else "（未设置 token，仅建议本机访问）"
    log(f"管理后台已启动: http://{settings.admin_host}:{settings.admin_port}/ {auth_hint}")


async def main() -> None:
    await client.start()
    me = await client.get_me()
    log(f"已登录: {getattr(me, 'first_name', '')} (@{getattr(me, 'username', '')})")
    log(f"监控源频道: {settings.source_channels}")
    log(f"目标频道: {settings.target_channel}")
    log(f"下载目录: {settings.download_dir}")
    log(f"搬运图片: {'是' if settings.include_photos else '否；但含视频的相册会保留同组图片'}")
    log(f"视频上传前删除开头: {settings.trim_video_seconds}s")
    log(f"视频封面截取时间: 裁剪后第 {settings.thumbnail_second}s")
    if not ffmpeg_exists():
        log("提示: 未检测到 ffmpeg/ffprobe，将不生成视频缩略图和精确元数据")
    start_admin_server()

    asyncio.create_task(worker())
    asyncio.create_task(cleanup_worker())
    log("运行中，按 Ctrl+C 停止")
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("已停止")
    except RuntimeError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        sys.exit(1)
