#!/usr/bin/env bash
# Cross-compile VoidScout Uploader for Windows/Mac/Linux using golang:alpine.
# Run from /home/liquid/voidscout-uploader on the NAS, or via the wrapper below.
set -euo pipefail

VERSION="${VERSION:-dev}"
OUTDIR="${OUTDIR:-./dist}"
mkdir -p "$OUTDIR"

build() {
    local goos=$1 goarch=$2 ext=$3
    local name="voidscout-uploader-${goos}-${goarch}${ext}"
    echo "→ building $name"
    docker run --rm \
        -v "$(pwd)":/src \
        -w /src \
        -e GOOS=$goos -e GOARCH=$goarch \
        -e CGO_ENABLED=0 \
        golang:1.22-alpine \
        sh -c "go mod tidy && go build -ldflags '-s -w -X main.version=$VERSION' -o $OUTDIR/$name ."
    ls -lh "$OUTDIR/$name"
}

case "${1:-all}" in
    win|windows)   build windows amd64 .exe ;;
    mac|darwin)    build darwin amd64 ""; build darwin arm64 "" ;;
    linux)         build linux amd64 "" ;;
    all)
        build windows amd64 .exe
        build linux amd64 ""
        build darwin amd64 ""
        build darwin arm64 ""
        ;;
    *)  echo "usage: $0 [win|mac|linux|all]"; exit 1 ;;
esac

echo ""
echo "Built artifacts:"
ls -lh "$OUTDIR/"
