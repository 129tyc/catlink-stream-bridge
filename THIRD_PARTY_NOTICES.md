# Third-party notices

The bridge vendors the dependencies listed below under `vendor/`. Each
dependency retains its own license, copyright notice, and any accompanying
PATENTS/NOTICE files in its vendored directory. This file is an index; the
materials shipped with each dependency are authoritative. In the container,
the same files are available under `/licenses/<vendor-relative-path>`.

The project also carries a narrow local compatibility change in
`vendor/github.com/bluenviron/gortsplib/v5/server_session.go`, based on
gortsplib v5.6.2. The change tolerates an identical repeated ONVIF backchannel
SETUP from the same RTSP session and preserves the existing transport/SSRC.

The root MIT license applies only to bridge code authored for this project.
Compatibility constants and protocol behavior observed from CATLINK/EZOpen
clients are included for interoperability; this notice does not claim CATLINK
or EZOpen authorization, affiliation, or rights to proprietary SDKs and
services.

## Direct and transitive Go modules

- github.com/bluenviron/gortsplib/v5 v5.6.2 — `vendor/github.com/bluenviron/gortsplib/v5/LICENSE`
- github.com/bluenviron/mediacommon/v2 v2.9.2 — `vendor/github.com/bluenviron/mediacommon/v2/LICENSE`
- github.com/google/uuid v1.6.0 — `vendor/github.com/google/uuid/LICENSE`
- github.com/gorilla/websocket v1.5.3 — `vendor/github.com/gorilla/websocket/LICENSE`
- github.com/pion/datachannel v1.6.2 — `vendor/github.com/pion/datachannel/LICENSE`
- github.com/pion/dtls/v3 v3.1.8 — `vendor/github.com/pion/dtls/v3/LICENSE`
- github.com/pion/ice/v4 v4.4.2 — `vendor/github.com/pion/ice/v4/LICENSE`
- github.com/pion/interceptor v0.1.48 — `vendor/github.com/pion/interceptor/LICENSE`
- github.com/pion/logging v0.2.4 — `vendor/github.com/pion/logging/LICENSE`
- github.com/pion/mdns/v2 v2.2.0 — `vendor/github.com/pion/mdns/v2/LICENSE`
- github.com/pion/randutil v0.1.0 — `vendor/github.com/pion/randutil/LICENSE`
- github.com/pion/rtcp v1.2.17 — `vendor/github.com/pion/rtcp/LICENSE`
- github.com/pion/rtp v1.10.5 — `vendor/github.com/pion/rtp/LICENSE`
- github.com/pion/sctp v1.11.1 — `vendor/github.com/pion/sctp/LICENSE`
- github.com/pion/sdp/v3 v3.0.19 — `vendor/github.com/pion/sdp/v3/LICENSE`
- github.com/pion/srtp/v3 v3.0.13 — `vendor/github.com/pion/srtp/v3/LICENSE`
- github.com/pion/stun/v4 v4.0.0 — `vendor/github.com/pion/stun/v4/LICENSE`
- github.com/pion/transport/v4 v4.1.0 — `vendor/github.com/pion/transport/v4/LICENSE`
- github.com/pion/turn/v5 v5.1.0 — `vendor/github.com/pion/turn/v5/LICENSE`
- github.com/pion/webrtc/v4 v4.2.20 — `vendor/github.com/pion/webrtc/v4/LICENSE`
- github.com/thesyncim/gopus v0.1.1 — `vendor/github.com/thesyncim/gopus/LICENSE`
- github.com/wlynxg/anet v0.0.5 — `vendor/github.com/wlynxg/anet/LICENSE`
- github.com/yapingcat/gomedia v0.0.0-20240906162731-17feea57090c — `vendor/github.com/yapingcat/gomedia/LICENSE`
- golang.org/x/crypto v0.54.0 — `vendor/golang.org/x/crypto/LICENSE`
- golang.org/x/net v0.57.0 — `vendor/golang.org/x/net/LICENSE`
- golang.org/x/sys v0.47.0 — `vendor/golang.org/x/sys/LICENSE`
- golang.org/x/time v0.14.0 — `vendor/golang.org/x/time/LICENSE`

The public snapshot must not include CATLINK account credentials, HA auth
files, real device data, private captures, APK/native SDK binaries, or other
deployment secrets. This notice does not grant rights to CATLINK/EZOpen
services, trademarks, proprietary SDKs, or application credentials.
