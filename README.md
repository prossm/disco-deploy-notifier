# disco-deploy-notifier

A tiny always-on service that listens to the [Disco](https://letsdisco.dev)
daemon's deployment event stream and posts to a Slack channel whenever a
deployment **completes** or **fails**.

## Why this exists

Disco has no built-in outbound webhooks (confirmed with the maintainers, no
near-term plans). The `disco.json` deploy hooks (`hook:deploy:*`) only run on a
*successful* deploy, so they can't tell you about a **failed** one. The daemon's
Server-Sent Events stream at `/api/disco/events` is the only place that surfaces
both outcomes (`QUEUED → PREPARING → REPLACING → COMPLETE | FAILED`).

That stream is **internal and best-effort** — not officially supported, though
the maintainers consider it stable. Events live in a ~1h in-memory buffer and
are **lost if the daemon restarts** (update, server reboot, etc.). This service
is built around those constraints:

- Reconnects with `Last-Event-ID` (persisted to `/data`) to catch up after *its
  own* restarts.
- Dedupes by deployment so buffer replay doesn't double-post.
- Accepts that a **daemon** restart can drop events — best-effort, not guaranteed.

It runs as its **own** Disco project (not a service inside the app it watches) so it
isn't torn down and restarted every time the app it's watching deploys.

## Setup

### 1. Slack incoming webhook
Create one at <https://api.slack.com/messaging/webhooks> for your target channel.
Copy the `https://hooks.slack.com/services/...` URL.

### 2. Dedicated Disco API key
Don't reuse your personal key. Create a separate invite/key:

```bash
disco invites:create   # then accept it to mint a key, or use your team's flow
```

Auth to the stream is HTTP Basic: the key is the **username**, password is empty
(`API_KEY:`). This service handles that for you.

### 3. Deploy as a Disco project
Push these files to a new GitHub repo, then on your Disco host:

```bash
disco projects:add https://github.com/youruser/disco-deploy-notifier.git
```

Set env vars (see `.env.example`) in the Disco UI or CLI:

```bash
disco env:set --project disco-deploy-notifier \
  DISCO_EVENTS_URL=https://your-disco-host/api/disco/events \
  DISCO_API_KEY=xxxxxxxxxxxxxxxx \
  SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ \
  DISCO_PROJECT_FILTER=my-app
```

`DISCO_PROJECT_FILTER` matters: the daemon streams events for **every** project
on the server, so set it to the exact Disco project name(s) you care about.

## Verifying

On first run, set `DEBUG_EVENTS=1` to log full raw event payloads. Trigger a
deploy of the watched project and confirm:
- the `deployment_status` payload shape matches what `extract_deployment()`
  expects (status / project / number / commit fields), and
- a Slack message lands on `COMPLETE` and on `FAILED`.

Then unset `DEBUG_EVENTS`.

## Run locally

```bash
cp .env.example .env   # fill in values
pip install -r requirements.txt
set -a; . ./.env; set +a
python -u notifier.py
```

## Known limits

- **Daemon restarts drop events** (in-memory buffer). If a deploy's notification
  matters and Disco itself restarted mid-deploy, you may not get a ping.
- The event schema is undocumented; `extract_deployment()` is defensive but may
  need a field tweak if Disco changes payloads. `DEBUG_EVENTS=1` shows the raw shape.
- Upstream feature request for real webhooks is the long-term fix — see the
  Disco daemon GitHub issues.
