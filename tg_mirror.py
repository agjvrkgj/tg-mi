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
from threading import Lock, Thread
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
    admin_config_file: Path
    rotate_after_videos: int

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

        admin_config_file = Path(os.getenv("TG_ADMIN_CONFIG_FILE", str(ROOT_DIR / "tg_mirror_admin.json"))).expanduser()
        if not admin_config_file.is_absolute():
            admin_config_file = ROOT_DIR / admin_config_file

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
            admin_config_file=admin_config_file,
            rotate_after_videos=max(1, int(os.getenv("TG_ROTATE_AFTER_VIDEOS", "3"))),
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
    "account_switches": 0,
}
config_lock = Lock()


def channel_to_str(channel: int | str) -> str:
    return str(channel).strip()


def default_runtime_config() -> dict[str, object]:
    return {
        "accounts": [
            {
                "name": "default",
                "api_id": settings.api_id,
                "api_hash": "",
                "session": Path(SESSION_NAME).name,
                "phone": "",
                "note": "当前运行账号；新增/切换账号资料后需要按对应配置重启进程",
            }
        ],
        "sources": [channel_to_str(channel) for channel in settings.source_channels],
        "targets": [channel_to_str(settings.target_channel)],
    }


def load_runtime_config() -> dict[str, object]:
    defaults = default_runtime_config()
    if not settings.admin_config_file.exists():
        return defaults
    try:
        loaded = json.loads(settings.admin_config_file.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            return defaults
    except (OSError, json.JSONDecodeError):
        return defaults

    for key, value in defaults.items():
        if key not in loaded or not isinstance(loaded[key], list):
            loaded[key] = value
    return loaded


runtime_config = load_runtime_config()


def save_runtime_config() -> None:
    settings.admin_config_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = settings.admin_config_file.with_suffix(settings.admin_config_file.suffix + ".tmp")
    tmp_path.write_text(json.dumps(runtime_config, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(settings.admin_config_file)


def parse_channel_value(value: str) -> int | str:
    value = value.strip()
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    return value


def managed_accounts() -> list[dict[str, object]]:
    with config_lock:
        return [dict(item) for item in runtime_config.get("accounts", []) if isinstance(item, dict)]


def managed_sources() -> list[str]:
    with config_lock:
        return [str(item) for item in runtime_config.get("sources", []) if str(item).strip()]


def managed_targets() -> list[str]:
    with config_lock:
        targets = [str(item) for item in runtime_config.get("targets", []) if str(item).strip()]
    return targets or [channel_to_str(settings.target_channel)]


def add_unique_config_value(key: str, value: str) -> bool:
    value = value.strip()
    if not value:
        return False
    with config_lock:
        values = [str(item) for item in runtime_config.get(key, [])]
        if value in values:
            return False
        values.append(value)
        runtime_config[key] = values
        save_runtime_config()
    return True


def delete_config_value(key: str, value: str) -> bool:
    value = value.strip()
    with config_lock:
        values = [str(item) for item in runtime_config.get(key, [])]
        new_values = [item for item in values if item != value]
        if len(new_values) == len(values):
            return False
        runtime_config[key] = new_values
        save_runtime_config()
    return True


def add_account(data: dict[str, str]) -> bool:
    name = data.get("name", "").strip()
    if not name:
        return False
    account = {
        "name": name,
        "api_id": data.get("api_id", "").strip(),
        "api_hash": data.get("api_hash", "").strip(),
        "session": data.get("session", "").strip() or name,
        "phone": data.get("phone", "").strip(),
        "note": data.get("note", "").strip(),
    }
    with config_lock:
        accounts = [item for item in runtime_config.get("accounts", []) if isinstance(item, dict)]
        accounts = [item for item in accounts if str(item.get("name", "")) != name]
        accounts.append(account)
        runtime_config["accounts"] = accounts
        save_runtime_config()
    return True


def delete_account(name: str) -> bool:
    name = name.strip()
    if not name:
        return False
    with config_lock:
        accounts = [item for item in runtime_config.get("accounts", []) if isinstance(item, dict)]
        new_accounts = [item for item in accounts if str(item.get("name", "")) != name]
        if len(new_accounts) == len(accounts):
            return False
        runtime_config["accounts"] = new_accounts
        save_runtime_config()
    return True


def mask_secret(value: object) -> str:
    text = str(value or "")
    if len(text) <= 8:
        return "***" if text else ""
    return f"{text[:4]}...{text[-4:]}"


@dataclass
class PreparedMedia:
    path: Path
    is_video: bool
    thumb_path: Path | None = None
    attributes: list[DocumentAttributeVideo] | None = None


@dataclass
class AccountRuntime:
    name: str
    client: TelegramClient
    session: str
    videos_uploaded: int = 0


account_clients: list[AccountRuntime] = []
active_account_index = 0


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


def account_session_path(session: str) -> Path:
    session = (session or Path(SESSION_NAME).name).strip()
    path = Path(session).expanduser()
    if not path.is_absolute():
        path = ROOT_DIR / path
    return path


def build_account_clients() -> None:
    account_clients.clear()
    seen_sessions: set[str] = set()
    for account in managed_accounts():
        name = str(account.get("name", "") or "default").strip() or "default"
        api_id_raw = account.get("api_id", settings.api_id)
        api_hash = str(account.get("api_hash", "") or "").strip() or settings.api_hash
        session_path = account_session_path(str(account.get("session", "") or name))
        session_key = str(session_path)
        if session_key in seen_sessions:
            continue
        seen_sessions.add(session_key)
        try:
            api_id = int(api_id_raw)
        except (TypeError, ValueError):
            log(f"账号 {name} 的 API ID 无效，已跳过")
            continue

        if session_key == SESSION_NAME:
            account_client = client
        else:
            account_client = TelegramClient(str(session_path), api_id, api_hash)
        account_client.flood_sleep_threshold = 60
        account_clients.append(AccountRuntime(name=name, client=account_client, session=session_path.name))

    if not account_clients:
        account_clients.append(AccountRuntime(name="default", client=client, session=Path(SESSION_NAME).name))


async def start_account_clients() -> None:
    build_account_clients()
    for index, account in enumerate(account_clients):
        await account.client.start()
        me = await account.client.get_me()
        log(f"账号已登录[{index + 1}/{len(account_clients)}]: {account.name} - {getattr(me, 'first_name', '')} (@{getattr(me, 'username', '')})")


def active_account() -> AccountRuntime:
    if not account_clients:
        build_account_clients()
    return account_clients[active_account_index % len(account_clients)]


def account_rotation_status() -> dict[str, object]:
    account = active_account()
    return {
        "active": account.name,
        "active_index": active_account_index % len(account_clients),
        "total": len(account_clients),
        "rotate_after_videos": settings.rotate_after_videos,
        "videos_uploaded_on_active": account.videos_uploaded,
        "accounts": [
            {"name": item.name, "session": item.session, "videos_uploaded": item.videos_uploaded}
            for item in account_clients
        ],
    }


def record_uploaded_videos(video_count: int) -> None:
    global active_account_index
    if video_count <= 0 or not account_clients:
        return
    account = active_account()
    account.videos_uploaded += video_count
    while account.videos_uploaded >= settings.rotate_after_videos and len(account_clients) > 1:
        account.videos_uploaded -= settings.rotate_after_videos
        active_account_index = (active_account_index + 1) % len(account_clients)
        inc_stat("account_switches")
        next_account = active_account()
        log(f"已上传 {settings.rotate_after_videos} 个视频，切换到 TG 账号: {next_account.name}")
        account = next_account



async def refresh_message(message: Message, tg_client: TelegramClient) -> Message | None:
    try:
        refreshed = await tg_client.get_messages(message.peer_id, ids=message.id)
        return refreshed or message
    except Exception as exc:
        log(f"刷新消息失败 msg_id={message.id}: {exc}")
        return None


async def download_message(message: Message, tg_client: TelegramClient) -> Path | None:
    message = await refresh_message(message, tg_client)
    if message is None:
        return None
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
    account = active_account()
    log(f"使用 TG 账号处理单条消息: {account.name}")
    path = await download_message(message, account.client)
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
            for target in managed_targets():
                await asyncio.wait_for(
                    account.client.send_file(
                        parse_channel_value(target),
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
            for target in managed_targets():
                await asyncio.wait_for(
                    account.client.send_file(
                        parse_channel_value(target),
                        file=str(path),
                        caption=clean_caption(message.text),
                        force_document=False,
                    ),
                    timeout=settings.upload_timeout,
                )
        inc_stat("sent_single")
        if is_video_message(message):
            record_uploaded_videos(1)
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


async def send_prepared_album(tg_client: TelegramClient, target: int | str, media_items: list[PreparedMedia], caption: str) -> None:
    """Send an album with per-video thumbnails while keeping video/photo/text grouped."""
    entity = await tg_client.get_input_entity(target)
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

        _, input_media, _ = await tg_client._file_to_media(str(item.path), **file_to_media_kwargs)
        if isinstance(input_media, (types.InputMediaUploadedPhoto, types.InputMediaPhotoExternal)):
            uploaded = await tg_client(functions.messages.UploadMediaRequest(entity, media=input_media))
            input_media = utils.get_input_media(uploaded.photo)
        elif isinstance(input_media, (types.InputMediaUploadedDocument, types.InputMediaDocumentExternal)):
            uploaded = await tg_client(functions.messages.UploadMediaRequest(entity, media=input_media))
            input_media = utils.get_input_media(uploaded.document, supports_streaming=True)

        multi_media.append(
            types.InputSingleMedia(
                input_media,
                message=cleaned_caption if index == 0 else "",
            )
        )

    await tg_client(functions.messages.SendMultiMediaRequest(entity, multi_media=multi_media))


async def send_album(messages: Iterable[Message]) -> None:
    account = active_account()
    log(f"使用 TG 账号处理相册: {account.name}")
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
            path = await download_message(message, account.client)
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

        for target in managed_targets():
            await asyncio.wait_for(
                send_prepared_album(account.client, parse_channel_value(target), prepared_files, caption),
                timeout=settings.upload_timeout,
            )
        inc_stat("sent_album")
        record_uploaded_videos(sum(1 for item in prepared_files if item.is_video))
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


def normalize_channel_name(value: str) -> str:
    return value.strip().lstrip("@").lower()


async def is_managed_source_event(event: events.NewMessage.Event) -> bool:
    sources = managed_sources()
    numeric_sources = {int(item) for item in sources if re.fullmatch(r"-?\d+", item)}
    chat_id = getattr(event, "chat_id", None)
    if chat_id in numeric_sources:
        return True
    if chat_id and chat_id < 0 and int(str(chat_id).replace("-100", "", 1)) in numeric_sources:
        return True

    named_sources = {normalize_channel_name(item) for item in sources if not re.fullmatch(r"-?\d+", item)}
    if not named_sources:
        return False
    try:
        chat = await event.get_chat()
    except Exception:
        return False
    username = normalize_channel_name(getattr(chat, "username", "") or "")
    return bool(username and username in named_sources)


async def on_new_message(event: events.NewMessage.Event) -> None:
    if not await is_managed_source_event(event):
        return
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
        "accounts": managed_accounts(),
        "sources": managed_sources(),
        "targets": managed_targets(),
        "config_file": str(settings.admin_config_file),
        "rotation": account_rotation_status(),
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
    token_input = ""
    if settings.admin_token:
        token_input = f'<input type="hidden" name="token" value="{html.escape(settings.admin_token)}">'
    rows = "".join(
        f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in status["stats"].items()
    )
    rotation = status["rotation"]
    rotation_accounts = "".join(
        f"<li>{html.escape(str(item['name']))} / session={html.escape(str(item['session']))} / 当前轮已上传视频 {item['videos_uploaded']} 个</li>"
        for item in rotation["accounts"]
    )
    account_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(account.get('name', '')))}</td>"
        f"<td>{html.escape(str(account.get('api_id', '')))}</td>"
        f"<td>{html.escape(mask_secret(account.get('api_hash', '')))}</td>"
        f"<td>{html.escape(str(account.get('session', '')))}</td>"
        f"<td>{html.escape(str(account.get('phone', '')))}</td>"
        f"<td>{html.escape(str(account.get('note', '')))}</td>"
        f"<td><form method='post' action='/manage'>{token_input}<input type='hidden' name='type' value='account'><input type='hidden' name='action' value='delete'><input type='hidden' name='name' value='{html.escape(str(account.get('name', '')))}'><button class='danger' type='submit'>删除</button></form></td>"
        "</tr>"
        for account in status["accounts"]
    )
    source_items = "".join(
        f"<li>{html.escape(source)} <form class='inline' method='post' action='/manage'>{token_input}<input type='hidden' name='type' value='source'><input type='hidden' name='action' value='delete'><input type='hidden' name='value' value='{html.escape(source)}'><button class='danger' type='submit'>删除</button></form></li>"
        for source in status["sources"]
    )
    target_items = "".join(
        f"<li>{html.escape(target)} <form class='inline' method='post' action='/manage'>{token_input}<input type='hidden' name='type' value='target'><input type='hidden' name='action' value='delete'><input type='hidden' name='value' value='{html.escape(target)}'><button class='danger' type='submit'>删除</button></form></li>"
        for target in status["targets"]
    )
    logs = "\n".join(html.escape(line) for line in status["logs"])
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
    button.danger {{ background: #dc2626; }}
    input {{ box-sizing: border-box; width: 100%; margin: 5px 0 10px; padding: 8px; border: 1px solid #d7dce5; border-radius: 8px; }}
    form.inline {{ display: inline; }}
    form.inline button {{ padding: 4px 8px; margin-left: 8px; }}
    ul {{ padding-left: 18px; }}
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
    <p><b>目标频道：</b>{html.escape(', '.join(status['targets']))}</p>
    <p><b>配置文件：</b>{html.escape(str(status['config_file']))}</p>
    <p><b>视频裁剪：</b>{status['trim_video_seconds']} 秒；<b>封面：</b>裁剪后第 {status['thumbnail_second']} 秒</p>
    <p><b>当前上传账号：</b>{html.escape(str(rotation['active']))}（{rotation['videos_uploaded_on_active']}/{rotation['rotate_after_videos']} 个视频后切换）</p>
    <p><b>ffmpeg：</b>{'可用' if status['ffmpeg_available'] else '不可用'}</p>
  </section>
  <section>
    <h2>账号轮换</h2>
    <p>当前账号：{html.escape(str(rotation['active']))}；账号数：{rotation['total']}；每 {rotation['rotate_after_videos']} 个视频切换。</p>
    <ul>{rotation_accounts}</ul>
  </section>
  <section style="grid-column: 1 / -1;">
    <h2>TG 账号管理</h2>
    <table><tr><th>名称</th><th>API ID</th><th>API Hash</th><th>Session</th><th>手机号</th><th>备注</th><th>操作</th></tr>{account_rows}</table>
    <form method="post" action="/manage">
      {token_input}
      <input type="hidden" name="type" value="account"><input type="hidden" name="action" value="add">
      <input name="name" placeholder="账号名称，例如 main">
      <input name="api_id" placeholder="API ID">
      <input name="api_hash" placeholder="API Hash（仅保存到本地管理配置）">
      <input name="session" placeholder="Session 名称，例如 channel_mirror">
      <input name="phone" placeholder="手机号/备注用，不会自动登录">
      <input name="note" placeholder="备注">
      <button type="submit">添加/更新账号资料</button>
    </form>
    <p>说明：运行中的 Telethon 客户端仍使用当前进程启动时的账号；新增或切换账号资料后，请按对应环境变量/Session 重启服务。</p>
  </section>
  <section>
    <h2>源频道管理</h2>
    <ul>{source_items}</ul>
    <form method="post" action="/manage">
      {token_input}
      <input type="hidden" name="type" value="source"><input type="hidden" name="action" value="add">
      <input name="value" placeholder="@source_channel 或 -100xxxx">
      <button type="submit">添加源频道</button>
    </form>
  </section>
  <section>
    <h2>目标频道管理</h2>
    <ul>{target_items}</ul>
    <form method="post" action="/manage">
      {token_input}
      <input type="hidden" name="type" value="target"><input type="hidden" name="action" value="add">
      <input name="value" placeholder="@target_channel 或 -100xxxx">
      <button type="submit">添加目标频道</button>
    </form>
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
        if parsed.path == "/api/config":
            self.send_bytes(json.dumps({"accounts": managed_accounts(), "sources": managed_sources(), "targets": managed_targets()}, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        self.send_bytes(render_admin_page().encode())

    def apply_manage_form(self, form: dict[str, list[str]]) -> bool:
        item_type = form.get("type", [""])[0]
        action = form.get("action", [""])[0]
        if item_type == "account":
            if action == "add":
                ok = add_account({key: values[0] for key, values in form.items()})
                if ok:
                    log(f"管理后台：已添加/更新 TG 账号资料 {form.get('name', [''])[0]}")
                return ok
            if action == "delete":
                name = form.get("name", [""])[0]
                ok = delete_account(name)
                if ok:
                    log(f"管理后台：已删除 TG 账号资料 {name}")
                return ok
        if item_type in {"source", "target"}:
            key = "sources" if item_type == "source" else "targets"
            label = "源频道" if item_type == "source" else "目标频道"
            value = form.get("value", [""])[0]
            if action == "add":
                ok = add_unique_config_value(key, value)
                if ok:
                    log(f"管理后台：已添加{label} {value}")
                return ok
            if action == "delete":
                ok = delete_config_value(key, value)
                if ok:
                    log(f"管理后台：已删除{label} {value}")
                return ok
        return False

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
        if parsed.path in {"/control", "/api/control"}:
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
        elif parsed.path in {"/manage", "/api/manage"}:
            if not self.apply_manage_form(form):
                self.send_bytes(b"Bad request", "text/plain; charset=utf-8", 400)
                return
            if parsed.path == "/api/manage":
                self.send_bytes(json.dumps({"accounts": managed_accounts(), "sources": managed_sources(), "targets": managed_targets()}, ensure_ascii=False).encode(), "application/json; charset=utf-8")
                return
        else:
            self.send_bytes(b"Not found", "text/plain; charset=utf-8", 404)
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
    await start_account_clients()
    listener = account_clients[0].client
    listener.add_event_handler(on_new_message, events.NewMessage())
    log(f"监听账号: {account_clients[0].name}")
    log(f"监控源频道: {managed_sources()}")
    log(f"目标频道: {managed_targets()}")
    log(f"账号轮换: 每上传 {settings.rotate_after_videos} 个视频切换到下一个账号")
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
    await listener.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("已停止")
    except RuntimeError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        sys.exit(1)
