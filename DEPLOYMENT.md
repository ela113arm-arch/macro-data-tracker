# Deployment

This app can be pushed to GitHub and deployed as a web service. The dashboard reads CSVs from `data/` and the Refresh Data button runs `data_fetcher.py` on the server to update them.

## Required Secrets

Set these as environment variables on the host. Do not commit real keys.

| Variable | Purpose |
| --- | --- |
| `FRED_API_KEY` | FRED macro data |
| `BEA_API_KEY` | BEA trade data |
| `EIA_API_KEY` | EIA petroleum data |
| `APP_PASSWORD` | Password required to access the dashboard |
| `APP_SECRET_KEY` | Long random string used to sign login sessions |

Optional:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `5003` | Port used by Flask/gunicorn |
| `HOST` | `127.0.0.1` locally, `0.0.0.0` in Docker | Bind address |
| `DATA_DIR` | `./data` | Alternate writable CSV directory |

## Recommended: Render From GitHub

1. Push this repository to GitHub.
2. In Render, create a new Blueprint or Web Service from the repo.
3. If using the included `render.yaml`, Render will build from the Dockerfile.
4. Add `FRED_API_KEY`, `BEA_API_KEY`, `EIA_API_KEY`, `APP_PASSWORD`, and `APP_SECRET_KEY` in Render environment variables.
5. Deploy, then open `/macro`.
6. Enter `APP_PASSWORD` once in the browser. After login, the dashboard and refresh buttons are available.

## Local Production Run

```powershell
$env:FRED_API_KEY = "..."
$env:BEA_API_KEY = "..."
$env:EIA_API_KEY = "..."
$env:APP_PASSWORD = "dashboard-password"
$env:APP_SECRET_KEY = "long-random-session-secret"
$env:HOST = "0.0.0.0"
$env:PORT = "5003"
gunicorn app:app --bind 0.0.0.0:5003 --timeout 900 --workers 1
```

On Windows without gunicorn, use the Flask development entry point:

```powershell
$env:FRED_API_KEY = "..."
$env:BEA_API_KEY = "..."
$env:EIA_API_KEY = "..."
$env:APP_PASSWORD = "dashboard-password"
$env:APP_SECRET_KEY = "long-random-session-secret"
python app.py
```

## Docker Run

```powershell
docker build -t macro-data-tracker .
docker run --rm -p 5003:5003 `
  -e FRED_API_KEY="..." `
  -e BEA_API_KEY="..." `
  -e EIA_API_KEY="..." `
  -e APP_PASSWORD="dashboard-password" `
  -e APP_SECRET_KEY="long-random-session-secret" `
  macro-data-tracker
```

## Notes

- Public deployments should always set `APP_PASSWORD`; otherwise anyone who can access the URL can view the dashboard and start data refreshes.
- `REFRESH_TOKEN` is still accepted as a backward-compatible fallback password if `APP_PASSWORD` is not set, but new deployments should use `APP_PASSWORD`.
- Some hosts have ephemeral filesystems. The committed CSVs are enough to load the dashboard after deploy, and the Refresh Data button updates CSVs for the running instance. For durable refreshed CSVs across restarts, configure a persistent disk and set `DATA_DIR` to that disk path.
