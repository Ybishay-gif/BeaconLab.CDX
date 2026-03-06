# Cloudflare Proxy (Stable Setup)

This folder hosts a Cloudflare Worker reverse proxy for the planning app.

Quick tunnels (`*.trycloudflare.com`) are temporary and will eventually fail with `530`.
Use a **named tunnel + stable hostname** instead.

## Stable architecture

- Local app runs on `http://localhost:8080`
- Named tunnel publishes it to a fixed hostname (example: `planning-api.<your-domain>`)
- Worker origin points to that fixed hostname

## One-time setup

1. Authenticate cloudflared (creates `cert.pem`):
   ```bash
   cloudflared tunnel login
   ```
2. Create named tunnel and credentials:
   ```bash
   ./scripts/create_named_tunnel.sh beaconlab-planning-api
   ```
3. Create DNS route to your hostname:
   ```bash
   cloudflared tunnel route dns beaconlab-planning-api planning-api.<your-domain>
   ```
4. Copy `cloudflared/config.example.yml` to `cloudflared/config.yml` and fill:
   - `TUNNEL_ID`
   - `CREDENTIALS_FILE`
   - `STABLE_HOSTNAME`
5. Start tunnel:
   ```bash
   ./scripts/start_named_tunnel.sh cloudflared/config.yml
   ```

## Deploy worker with stable origin

```bash
./scripts/deploy_stable_worker.sh planning-api.<your-domain>
```

Optional fallback origin:

```bash
./scripts/deploy_stable_worker.sh planning-api.<your-domain> backup-api.<your-domain>
```

## Health check

```bash
./scripts/check_proxy_health.sh planning-api.<your-domain> beaconlab.kissterralab.workers.dev
```

## Notes

- Keep tunnel running as a service for reliability.
- Do not point `ORIGIN_BASE` to `trycloudflare.com`.
