# IMPR Relay

A drop-in upload relay for **IamMusicPlayer / IamMusicPlayerRenewed (IMPR)**, the Minecraft music player mod.

In the mod's Music Manager, a player picks a local MP3. The client sends it to a relay, which stores it and returns a URL the mod then streams from. The original relay was a single hosted service that is no longer running. This is a self-hostable replacement.

- Stores files on **Discord** (via webhook) or on **local disk**
- Range requests supported, so seeking in the mod works
- Rate limits, size limit, MP3 check, dedupe, optional allowlist and retention

## Quick start

```sh
nano compose.yaml    # set DISCORD_WEBHOOK_URL and PUBLIC_URL
docker compose up -d --build
```

Check it works:

```sh
curl https://relay.example.com/
# {"Status":"Ok","Name":"IMPR Relay","Version":"1.0.0","MaxFileSize":8388608,"Time":{"ResponseSpeed":0}}
```

Then in the mod's client config set **Relay server url** (`relayServerURL`) to your base URL **with a trailing slash**:

```
https://relay.example.com/
```

## How it works

```
Minecraft client                         relay                        storage
      |  GET  /                            |                             |
      |  <-- {"Status":"Ok", ...}          |                             |
      |  POST /music-upload  (mc-uuid)     |   upload as <id>.mp3        |
      |  raw MP3 bytes ------------------> | --------------------------> | Discord webhook
      |  <-- {"url": ".../f/<id>.mp3"}     |                             |  or local disk
      |                                    |                             |
      |  GET  /f/<id>.mp3 (Range) -------> | <-- fresh CDN URL / file -- |
      |  <-- audio/mpeg stream             |                             |
```

The mod can also be pointed at a JSON file with `"Status": "Transfer"` that redirects to your server. This relay answers `Status: Ok` directly, so no redirect file is needed.

### Why files are proxied

Discord CDN links are signed and expire after about a day. If the relay returned them directly, songs would stop playing. Instead it returns its own permanent `/f/<id>.mp3` link. When someone plays it, the relay fetches a fresh signed URL through the webhook (cached until shortly before expiry) and streams the file through.

## Configuration

All settings are environment variables (see `.env.example`).

| Variable | Default | Description |
|---|---|---|
| `STORAGE` | `discord` | `discord` or `local` |
| `DISCORD_WEBHOOK_URL` | | Required for `discord`. Server Settings → Integrations → Webhooks |
| `PUBLIC_URL` | derived from `Host` | Public base URL, trailing slash. Set this when behind a proxy |
| `PORT` | `3009` | Listen port |
| `RELAY_NAME` | `IMPR Relay` | Name shown in the Music Manager |
| `MAX_FILE_SIZE` | `8388608` | Max upload in bytes. Keep at or below your Discord server's attachment limit |
| `RATE_LIMIT_PER_HOUR` | `20` | Uploads per player UUID per hour (per IP: 5×) |
| `RETENTION_DAYS` | `0` | Delete files older than N days. `0` keeps everything |
| `ALLOWED_UUIDS` | empty | Comma-separated player UUIDs. Empty means anyone may upload |
| `TRUST_PROXY` | `0` | Set `1` behind a reverse proxy to trust `X-Forwarded-*` |
| `DATA_DIR` | `./data` | Holds `files.json` and, for `local`, the `files/` folder |

## API

| Route | Description |
|---|---|
| `GET /` or `/status` | `{"Status","Name","Version","MaxFileSize","Time":{"ResponseSpeed"}}` |
| `POST /music-upload` | Body: raw MP3. Header: `mc-uuid`. Returns `{"url": "..."}` or `{"Error","Message"}` |
| `GET /f/<id>.mp3` | Serves the file. Supports `Range` and `HEAD` |

Error responses use HTTP status codes 400, 403, 413, 415, 429 and 502 together with the `Error`/`Message` JSON the client displays.

## Notes and limits

- **Discord.** Webhooks are rate limited, and using a Discord channel as bulk file hosting is a grey area in their terms. It is fine for a small community. For anything larger use `STORAGE=local`.
- **Abuse.** The mod only sends an unauthenticated `mc-uuid`, which anyone can fake. The per-IP limit, size limit and MP3 check are what actually protect the server. Use `ALLOWED_UUIDS` for a private relay.
- **Privacy.** `data/files.json` records which UUID uploaded which file. Anyone with a link can download that file, so players should not upload private material. `RETENTION_DAYS` cleans up old files, including the Discord messages.
- **Backups.** Keep `data/files.json`. Without it the relay cannot map IDs to Discord messages or local files.
- **Compatibility.** Built against the upstream TeamFelNull client protocol (relay version 1).

## License

MIT
