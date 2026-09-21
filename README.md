# CATLINK Stream Bridge

[中文文档](README.zh-CN.md)

CATLINK Stream Bridge pulls video and audio from CATLINK camera-enabled
devices and exposes them as RTSP streams for downstream consumers such as
Frigate, go2rtc, or other RTSP clients.

## Features

- Cloud streaming: pulls the stream through CATLINK/EZOpen cloud services.
- Local streaming: connects directly to the litter box on the same LAN when
  the device provides the required local media session.
- Auto mode: prefers local streaming when eligible and falls back to cloud
  streaming when local streaming is unavailable.
- H.265 video and G.711A audio are published through RTSP without video or
  audio re-encoding.
- Standard RTSP microphone backchannel for go2rtc/Frigate-compatible clients.
- Device selection by the device name shown in the CATLINK App. A single
  device can be selected automatically.
- Login state is shared with an existing CATLINK integration or session
  service; the bridge does not perform phone/password login.

## Current support and limitations

- The bridge currently supports only the CATLINK Visual C07 / 大白 Pro.
  Support for additional CATLINK camera-enabled devices will be added later.
- Stream behavior has only been tested with CATLINK's mainland China service
  endpoint. Other CATLINK service regions have not been tested.
- For the tested C07, local streaming supports at most three simultaneous
  routes; the fourth local route fails to connect.
- Some Frigate/go2rtc WebRTC combinations may not keep video playback and
  microphone uplink working at the same time. When the microphone is enabled,
  video may remain loading or stop while the audio uplink is negotiated. This
  is a client negotiation compatibility limitation.
- Local streaming still uses CATLINK cloud authentication/bootstrap. It is
  not an offline camera service.
- A route is published after a complete H.265 configuration and keyframe are
  received. If the upstream temporarily stops sending media, the RTSP route
  remains available while the bridge restores the source.

## Login state and deployment modes

CATLINK Stream Bridge only consumes an existing CATLINK login session. It does
not accept a CATLINK phone/password and does not log in or refresh the CATLINK
session itself. This avoids multiple components replacing each other's login
state.

There are two supported session sources.

### Home Assistant deployment

Install and configure the CATLINK integration in Home Assistant. The bridge
reads the existing Home Assistant CATLINK auth file from a read-only mounted
directory.

Use:

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

The pattern must match exactly one auth file. If Home Assistant contains more
than one CATLINK account, use the exact auth file path instead.

### Separate session-reader deployment

Run the CATLINK session-reader service separately. It owns the CATLINK login
state and exposes it through its API; the bridge calls that API with an API
key.

Use:

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

## Configuration

The process reads the configuration file from `CATLINK_GO_CONFIG_FILE`:

```sh
CATLINK_GO_CONFIG_FILE=/config/streams.json /catlink-go-bridge
```

If the environment variable is not set, the first command-line argument is
used as the configuration path.

The complete examples are:

- [`config/streams.go.example.json`](config/streams.go.example.json):
  session-reader mode;
- [`config/streams.session-file.example.json`](config/streams.session-file.example.json):
  Home Assistant auth-file mode.

Top-level configuration fields:

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

Each route contains:

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

For C07, `channel` `1` is the outer camera and `channel` `2` is the inner
camera. `quality` can be `hd` or `sd`; `source` can be `cloud`, `local`, or
`auto`. If `source` is omitted, the existing cloud path is used.

For one device, the bridge can select the device automatically. For multiple
devices, add a selector using the device name shown in the CATLINK App:

```json
{
  "id": "device-main",
  "accountId": "account-main",
  "selector": {
    "deviceName": "大白 Pro"
  }
}
```

### Recommended C07 route configuration

The recommended daily configuration is two local HD routes, one for each
camera/channel:

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

This provides high-definition sources for downstream recording while leaving
the CATLINK App's own standard-definition live preview available.

The bridge serves RTSP paths on port `8554` and health information on port
`8080` at `/healthz`.

## Docker

The image contains the statically linked Go bridge, built using vendored
dependencies. It does not run Node.js, Chromium, FFmpeg, or MediaMTX.

Example using session-reader mode:

```sh
docker run -d \
  --name catlink-stream-bridge \
  --restart unless-stopped \
  --network host \
  -v "$PWD/streams.json:/config/streams.json:ro" \
  -e CATLINK_GO_CONFIG_FILE=/config/streams.json \
  ghcr.io/<owner>/catlink-stream-bridge:<tag>
```

For Home Assistant auth-file mode, mount the CATLINK storage directory:

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

Use the session-file configuration example in that deployment mode.

Downstream RTSP consumers use paths such as:

```text
rtsp://<bridge-host>:8554/catlink_ch1_hd
rtsp://<bridge-host>:8554/catlink_ch2_hd
```

## Contributions

Maintenance capacity is currently limited, so external contributions and pull
requests are not accepted for now. Issue reports are welcome and will be
addressed when possible.

## License

Project-authored code is released under the MIT License. Vendored dependencies
retain their own licenses; see [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

This is an independent project and is not affiliated with or endorsed by
CATLINK or EZOpen.
