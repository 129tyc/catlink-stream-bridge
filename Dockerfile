FROM --platform=linux/amd64 golang:1.27.1-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -trimpath -ldflags='-s -w' -o /out/catlink-go-bridge ./cmd/catlink-go-bridge
RUN set -eu; \
    mkdir -p /licenses; \
    find vendor -type f \( -name LICENSE -o -name COPYING -o -name NOTICE -o -name PATENTS \) \
      -exec sh -ec 'for file do relative=${file#vendor/}; target=/licenses/$(dirname "$relative"); mkdir -p "$target"; cp "$file" "$target/"; done' sh {} +

FROM --platform=linux/amd64 gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=build /out/catlink-go-bridge /catlink-go-bridge
COPY --from=build /licenses /licenses
COPY LICENSE /LICENSE
COPY THIRD_PARTY_NOTICES.md /THIRD_PARTY_NOTICES.md
COPY config/streams.go.example.json /config/streams.json.example

USER nonroot:nonroot
VOLUME ["/config"]
EXPOSE 8554 8080
ENTRYPOINT ["/catlink-go-bridge"]
