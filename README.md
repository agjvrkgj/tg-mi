# TG MI - Telegram 频道视频搬运

一个用 **Telethon 用户会话** 监控 Telegram 源频道，并把新发布的视频下载后重新上传到目标频道的搬运项目。下载再上传的方式可用于普通转发受限的频道内容；请确保你拥有相关内容和频道的合法使用/转载权限。

## 功能

- 监听一个或多个源频道的新消息。
- 自动识别视频消息与视频文件，并搬运到目标频道。
- 支持相册/媒体组：视频+图片+文字同组消息会等待收齐后合并成一条消息发送。
- 视频上传前默认删除开头 10 秒，并自动截取裁剪后视频第 5 秒画面作为封面，然后再和同组图片一起上传。
- 默认只搬运视频；如果相册中含视频，会保留同组图片；也可通过 `TG_INCLUDE_PHOTOS=true` 搬运纯图片消息。
- 自动清理标题中的链接和 `@用户名`，目标频道文字里不会带链接或 `@` 提及。
- 本地下载到磁盘目录后重新上传，绕过 Telegram 的“禁止转发”限制。
- 强制视频以视频格式发送（`supports_streaming=True`、`force_document=False`），并为单条视频和相册中的视频生成视频元数据与封面。
- 内置管理后台，可查看运行状态、队列、统计、磁盘占用和最近日志，并支持暂停/恢复接收新任务。

## 准备

1. 登录 <https://my.telegram.org> 创建应用，拿到 `api_id` 和 `api_hash`。
2. 运行本项目的 Telegram 账号需要：
   - 能看到源频道内容；
   - 是目标频道管理员，并有发消息权限。
3. 建议安装 `ffmpeg`，用于生成视频缩略图与读取时长/分辨率：

```bash
sudo apt-get update && sudo apt-get install -y ffmpeg
```

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

编辑 `.env`：

```env
TG_API_ID=123456
TG_API_HASH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TG_SOURCE_CHANNELS=@source_channel_1,@source_channel_2
TG_TARGET_CHANNEL=@target_channel
TG_INCLUDE_PHOTOS=false
```

## 首次登录

首次运行会要求输入手机号、验证码；如果账号开启两步验证，还会要求输入密码。登录成功后会在项目目录生成 `channel_mirror.session`，之后可长期复用。

```bash
python tg_mirror.py
```

## 常用配置

| 变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `TG_API_ID` | 是 | - | Telegram API ID |
| `TG_API_HASH` | 是 | - | Telegram API Hash |
| `TG_SOURCE_CHANNELS` | 是 | - | 源频道列表，逗号分隔，支持 `@username`、用户名、`-100...` ID |
| `TG_TARGET_CHANNEL` | 是 | - | 目标频道 |
| `TG_INCLUDE_PHOTOS` | 否 | `false` | 是否搬运图片；默认只搬运视频 |
| `TG_DOWNLOAD_DIR` | 否 | `./tg_mirror_tmp` | 临时下载目录；建议使用磁盘目录，不要放到 `/dev/shm` 等 tmpfs 内存盘 |
| `TG_ALBUM_WAIT` | 否 | `3` | 相册消息收集等待秒数 |
| `TG_UPLOAD_TIMEOUT` | 否 | `1800` | 单次上传超时秒数 |
| `TG_TRIM_VIDEO_SECONDS` | 否 | `10` | 视频上传前删除开头多少秒，设为 `0` 可关闭 |
| `TG_THUMBNAIL_SECOND` | 否 | `5` | 删除开头后，从裁剪后视频第几秒截图作为封面 |
| `TG_STRIP_LINKS` | 否 | `true` | 是否清理 caption 中链接和 `@用户名` |
| `TG_ADMIN_ENABLED` | 否 | `true` | 是否启动管理后台 |
| `TG_ADMIN_HOST` | 否 | `127.0.0.1` | 管理后台监听地址；默认仅本机访问 |
| `TG_ADMIN_PORT` | 否 | `8080` | 管理后台端口 |
| `TG_ADMIN_TOKEN` | 否 | 空 | 管理后台访问令牌；建议生产环境设置 |

## 管理后台

脚本启动后默认会在本机启动管理后台：

```bash
http://127.0.0.1:8080/
```

后台功能：

- 查看是否暂停、运行时长、任务队列长度、相册缓冲数量。
- 查看源频道、目标频道、裁剪秒数、封面秒数、`ffmpeg` 可用性。
- 查看下载目录磁盘占用、临时文件数量和大小。
- 查看搬运统计和最近日志。
- 点击按钮暂停/恢复接收新的搬运任务；已进入队列的任务会继续处理。

建议在 `.env` 里设置 `TG_ADMIN_TOKEN`：

```env
TG_ADMIN_TOKEN=change_me_to_a_long_random_string
```

设置后访问：

```bash
http://127.0.0.1:8080/?token=change_me_to_a_long_random_string
```

脚本启动日志不会打印 token 明文；如果把后台暴露到公网，请务必设置强随机 `TG_ADMIN_TOKEN`，并优先放在反向代理或防火墙之后。

如果需要关闭后台：

```env
TG_ADMIN_ENABLED=false
```

## 后台运行示例

```bash
nohup python tg_mirror.py > tg-mirror.log 2>&1 &
tail -f tg-mirror.log
```

## 注意事项

- 不要把 `.env`、`.session` 文件提交到仓库或发给别人。
- 搬运大视频会消耗大量磁盘、带宽和 Telegram API 上传时间；`TG_DOWNLOAD_DIR` 请使用真实磁盘路径。
- Telegram 对账号行为、上传频率和文件大小有限制；脚本会遵守 Telethon 的 flood wait 休眠。
- 请遵守当地法律、Telegram 规则以及源内容版权要求。
