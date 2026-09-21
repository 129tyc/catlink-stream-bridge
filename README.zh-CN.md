# CATLINK Stream Bridge

[English](README.md)

CATLINK Stream Bridge 可以从 CATLINK 的可视设备拉取视频和音频，并通过
RTSP 提供给 Frigate、go2rtc 或其他 RTSP 下游使用。

## 功能

- **Cloud 模式**：通过 CATLINK/EZOpen 视频云拉取流；
- **Local 模式**：设备和 bridge 位于同一个 LAN 时，直接连接猫砂盆的本地媒体服务；
- **Auto 模式**：满足条件时优先使用 local，不可用时回退到 cloud；
- 通过 RTSP 输出 H.265 视频和 G.711A 音频，不进行音视频重编码；
- 通过标准 RTSP backchannel 为 Frigate/go2rtc 等客户端提供麦克风通话；
- 可以使用 CATLINK App 中显示的设备名称选择设备；单设备时可以自动选择；
- 复用已有 CATLINK 登录态，不接收手机号/密码，也不自行登录。

## 当前支持和限制

- 目前仅支持 CATLINK Visual C07 / 大白 Pro，后续会增加其他 CATLINK 可视设备的支持；
- 目前只测试过中国大陆 CATLINK 服务节点，其他区域节点的拉流效果尚未验证；
- 经过实测，C07 的 local 并发最多支持三路，第四路 local 会连接失败；
- 部分 Frigate/go2rtc WebRTC 组合可能无法同时保持视频播放和麦克风上行。开启麦克风后，协商音频上行时视频可能一直 loading 或中断。这属于客户端协商兼容性限制。
- local 拉流仍需要 CATLINK 云端认证和 bootstrap，不是完全离线的摄像头服务；
- route 在收到完整的 H.265 配置和关键帧后才会发布。上游暂时停止发送媒体时，RTSP route 仍保持可用，bridge 会继续恢复上游。

## 登录态和部署方式

CATLINK Stream Bridge 只读取已有的 CATLINK 登录态，不接受 CATLINK 手机号/密码，也不会自行登录或刷新 CATLINK 会话。这样可以避免多个组件互相顶掉登录态。

支持两种登录态来源。

### 和 Home Assistant 一起部署

先在 Home Assistant 中安装并配置 CATLINK 集成。bridge 以只读方式挂载 Home Assistant 的 CATLINK auth 目录，并读取其中已有的登录态。

配置示例：

```json
{
  "accounts": [
    {
      "id": "account-main",
      "sessionFile": "/ha-config/.storage/catlink/auth-*.json"
    }
  ]
}
```

通配符必须只匹配一个 auth 文件。如果 HA 中有多个 CATLINK 账号，请改用具体 auth 文件路径。

### 独立部署 session-reader

单独运行 CATLINK session-reader，由它维护 CATLINK 登录态并通过 API 发布。bridge 使用 API key 调用 session-reader 获取登录态。

配置示例：

```json
{
  "accounts": [
    {
      "id": "account-main",
      "sessionProviderUrl": "http://catlink-session-reader:8090/v1/session",
      "apiKey": "replace-with-session-reader-api-key"
    }
  ]
}
```

## 配置

进程通过 `CATLINK_GO_CONFIG_FILE` 指定配置文件：

```sh
CATLINK_GO_CONFIG_FILE=/config/streams.json /catlink-go-bridge
```

如果没有设置环境变量，也可以把配置文件路径作为第一个命令行参数传入。

完整配置示例：

- [`config/streams.go.example.json`](config/streams.go.example.json)：session-reader 模式；
- [`config/streams.session-file.example.json`](config/streams.session-file.example.json)：Home Assistant auth 文件模式。

顶层配置结构：

```json
{
  "listen": ":8554",
  "healthListen": ":8080",
  "openDomain": "https://open.ys7.com",
  "accounts": [],
  "devices": [],
  "routes": []
}
```

每个 route 的结构：

```json
{
  "name": "camera-ch1-hd",
  "deviceId": "device-main",
  "channel": 1,
  "quality": "hd",
  "source": "auto",
  "path": "/catlink_ch1_hd"
}
```

其中：

- 对 C07，`channel` `1` 是外部摄像头，`channel` `2` 是内部摄像头；
- `quality`：支持 `hd` 或 `sd`；
- `source`：支持 `cloud`、`local`、`auto`，省略时使用现有 cloud 路径；
- `path`：下游客户端访问的 RTSP 路径。

单设备时 bridge 可以自动选择设备。多设备时，可以使用 CATLINK App 中显示的设备名称：

```json
{
  "id": "device-main",
  "accountId": "account-main",
  "selector": {
    "deviceName": "大白 Pro"
  }
}
```

### C07 推荐配置

日常推荐配置是两路 local HD，每个摄像头/通道一路：

```json
{
  "routes": [
    {
      "name": "camera-ch1-hd",
      "deviceId": "device-main",
      "channel": 1,
      "quality": "hd",
      "source": "local",
      "path": "/catlink_ch1_hd"
    },
    {
      "name": "camera-ch2-hd",
      "deviceId": "device-main",
      "channel": 2,
      "quality": "hd",
      "source": "local",
      "path": "/catlink_ch2_hd"
    }
  ]
}
```

这样可以给下游录像提供高清视频源，同时让 CATLINK App 继续使用自己的标清 live 预览。

RTSP 监听端口是 `8554`，健康检查端口是 `8080`，健康检查地址是 `/healthz`。

## Docker 部署

镜像包含使用 vendored 依赖构建的静态链接 Go bridge。运行时不使用 Node.js、Chromium、FFmpeg 或 MediaMTX。

session-reader 模式示例：

```sh
docker run -d \
  --name catlink-stream-bridge \
  --restart unless-stopped \
  --network host \
  -v "$PWD/streams.json:/config/streams.json:ro" \
  -e CATLINK_GO_CONFIG_FILE=/config/streams.json \
  ghcr.io/<owner>/catlink-stream-bridge:<tag>
```

Home Assistant auth 文件模式需要挂载 CATLINK storage 目录：

```sh
docker run -d \
  --name catlink-stream-bridge \
  --restart unless-stopped \
  --network host \
  -v "$PWD/streams.session-file.json:/config/streams.json:ro" \
  -v /path/to/home-assistant/.storage/catlink:/ha-config/.storage/catlink:ro \
  -e CATLINK_GO_CONFIG_FILE=/config/streams.json \
  ghcr.io/<owner>/catlink-stream-bridge:<tag>
```

下游客户端可以使用：

```text
rtsp://<bridge-host>:8554/catlink_ch1_hd
rtsp://<bridge-host>:8554/catlink_ch2_hd
```

## 贡献

目前维护精力有限，暂不接受外部贡献和 PR。欢迎提交 issue，我们会尽力处理。

## 许可证

项目自有代码使用 MIT License。vendored 依赖保留各自许可证，详见 [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md)。

这是独立项目，与 CATLINK 或 EZOpen 没有官方隶属或背书关系。
