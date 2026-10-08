# cb-downloader

Game files downloader for the [CBServers](https://cbservers.xyz/) CDN.

## Build

```bash
go build -o cb-downloader.exe .
```

Dependencies: `github.com/zeebo/xxh3` plus the Go standard library. The
manifest (`games.json`) is embedded at build time via `//go:embed`, so no
runtime data files are required.

Run it from the folder where you want the game files installed.

## Thanks

Thanks to [CBServers](https://cbservers.xyz/) for hosting the CDN that
serves the game file trees this tool downloads.
