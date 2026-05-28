# VoidScout Uploader

Companion app for the [VoidScout](https://voidscout.io) WoW addon. Watches your `SavedVariables/VoidScout.lua` file and uploads new fight scores to api.voidscout.io.

**Single binary. ~6 MB. Zero install dependencies.**

## Install

1. **Install the addon** ([CurseForge / GitHub Releases](https://voidscout.io/install))
2. **Download the uploader** for your OS:

| Platform | Download |
|---|---|
| Windows (x64) | [voidscout-uploader-windows-amd64.exe](https://github.com/bughatti/voidscout-uploader/releases/latest/download/voidscout-uploader-windows-amd64.exe) |
| macOS (Intel) | [voidscout-uploader-darwin-amd64](https://github.com/bughatti/voidscout-uploader/releases/latest/download/voidscout-uploader-darwin-amd64) |
| macOS (Apple Silicon) | [voidscout-uploader-darwin-arm64](https://github.com/bughatti/voidscout-uploader/releases/latest/download/voidscout-uploader-darwin-arm64) |
| Linux (x64) | [voidscout-uploader-linux-amd64](https://github.com/bughatti/voidscout-uploader/releases/latest/download/voidscout-uploader-linux-amd64) |

3. **Run it** — double-click on Windows, or `./voidscout-uploader-darwin-*` in terminal on Mac/Linux

The uploader will:
- Auto-detect your WoW SavedVariables folder
- Watch `VoidScout.lua` for changes (5s polling)
- Upload any new fight scores to api.voidscout.io
- Auto-update itself when new versions ship

## What about Windows SmartScreen warning?

First launch on Windows shows "Windows protected your PC". Click **More info** → **Run anyway**. The binary is unsigned (signing costs $200+/yr — we're keeping the project free). After the first run, Windows remembers and won't ask again.

## Options

```
voidscout-uploader.exe [flags]

Flags:
  -api <url>           API base URL (default https://api.voidscout.io)
  -log <path>          Write logs to file (default stderr only)
  -v                   Verbose logging
  -no-update           Skip auto-update check on launch
  -dry-run             Parse + show what would upload, but don't POST
  -once <path>         Parse this SavedVariables file once and exit (testing)
```

## What gets uploaded?

Only the fight data from `VoidScout.lua` SavedVariables. That's:
- Encounter ID, name, difficulty
- Fight outcome (kill/wipe), duration, timestamp
- Roster (player names in the pull)
- Per-player 8-axis scores (Damage / Interrupts / Dispels / Avoidance / Activity / Survival / Teamwork)

**Not uploaded:** chat messages, your personal info, account credentials, anything outside this single file.

## Privacy

VoidScout follows the same opt-out model as RaiderIO and WarcraftLogs — character names are public (pseudonymous WoW identifiers), with an opt-out mechanism at voidscout.io for any player who wants their data removed.

See [PRIVACY.md](https://voidscout.io/privacy) for full details.

## How it auto-updates

On launch, the binary queries GitHub Releases API for the latest tag. If newer than the running version, it downloads the new binary, atomically replaces itself, and logs a "please restart" message. Next time you run it, you're on the new version.

No telemetry. No background phone-home. Just a single GitHub API call at startup.

## Build from source

```bash
git clone https://github.com/bughatti/voidscout-uploader
cd voidscout-uploader
go build .
```

Requires Go 1.22+.

## License

MIT
