# Deployment

This app can be pushed to GitHub and deployed as a web service. The dashboard reads CSVs from `data/` and the Refresh Data button runs `data_fetcher.py` on the server to update them.

## Required Secrets

Set these as environment variables on the host. Do not commit real keys.

| Variable | Purpose |
| --- | --- |
| `FRED_API_KEY` | FRED macro data |
| `BEA_API_KEY` | BEA trade data |
| `EIA_API_KEY` | EIA petroleum data |

Optional:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `5003` | Port used by Flask/gunicorn |
| `HOST` | `127.0.0.1` locally, `0.0.0.0` in Docker | Bind address |
| `DATA_DIR` | `./data` | Alternate writable CSV directory |
| `SYNC_BUNDLED_DATA_ON_STARTUP` | `1` | When `DATA_DIR` points at a Render disk, copy newer CSVs bundled by a GitHub redeploy into that disk on startup. Set to `0` to disable. |

## Recommended: Render From GitHub

1. Push this repository to GitHub.
2. In Render, create a new Blueprint or Web Service from the repo.
3. If using the included `render.yaml`, Render will build from the Dockerfile.
4. Add `FRED_API_KEY`, `BEA_API_KEY`, and `EIA_API_KEY` in Render environment variables.
5. Deploy, then open `/macro`.
6. Click Refresh Data to update CSVs on the running Render instance.

### GitHub CSV updates vs. in-app refreshes

Render does not `git pull` inside a running container. CSV updates committed to GitHub reach the app only after Render builds and deploys a new image from that commit. The in-app **Refresh Data** button is separate: it runs `data_fetcher.py` on the running instance and writes CSVs into `DATA_DIR`.

If `DATA_DIR` points at a persistent Render disk, those disk files can hide the newer CSVs bundled in a GitHub redeploy. On startup, the app now compares the bundled data refresh timestamp with the external `DATA_DIR` timestamp and copies the bundled CSVs into `DATA_DIR` only when the deployed GitHub data is newer. Check `/api/status` for `data_dir`, `bundled_data_dir`, `uses_external_data_dir`, and `bundled_data_sync` to diagnose which source the app is reading.

## Local Production Run

```powershell
$env:FRED_API_KEY = "..."
$env:BEA_API_KEY = "..."
$env:EIA_API_KEY = "..."
$env:HOST = "0.0.0.0"
$env:PORT = "5003"
gunicorn app:app --bind 0.0.0.0:5003 --timeout 900 --workers 1
```

On Windows without gunicorn, use the Flask development entry point:

```powershell
$env:FRED_API_KEY = "..."
$env:BEA_API_KEY = "..."
$env:EIA_API_KEY = "..."
python app.py
```

## Docker Run

```powershell
docker build -t macro-data-tracker .
docker run --rm -p 5003:5003 `
  -e FRED_API_KEY="..." `
  -e BEA_API_KEY="..." `
  -e EIA_API_KEY="..." `
  macro-data-tracker
```

## Notes

- Some hosts have ephemeral filesystems. The committed CSVs are enough to load the dashboard after deploy, and the Refresh Data button updates CSVs for the running instance. For durable refreshed CSVs across restarts, configure a persistent disk and set `DATA_DIR` to that disk path. With a persistent disk, leave `SYNC_BUNDLED_DATA_ON_STARTUP=1` if GitHub CSV commits should replace older disk data after a redeploy.
