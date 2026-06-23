"""
Disco deploy -> Slack notifier.

Connects to the Disco daemon's Server-Sent Events stream and posts a message to
a Slack incoming webhook whenever a deployment reaches a terminal state
(COMPLETE / FAILED).

Why an SSE listener and not a disco.json deploy hook: the `hook:deploy:*`
commands only run on a *successful* deploy, so they cannot observe a FAILED one.
The event stream is the only place that surfaces both outcomes.

The stream is an internal, best-effort feed (per the Disco maintainers): events
live in a ~1h in-memory buffer on the daemon and are lost if the daemon
restarts. We therefore:
  - reconnect with Last-Event-ID to catch up after our *own* restarts,
  - dedupe by deployment so buffer replay does not double-post, and
  - accept that a daemon restart can drop events (best-effort, not guaranteed).

Auth is HTTP Basic with the API key as the username and an empty password.
Create a dedicated Disco invite/API key for this rather than reusing yours.
"""

import json
import logging
import os
import sys
import time

import httpx

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("disco-notifier")


def env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and not val:
        log.error("Missing required env var %s", name)
        sys.exit(1)
    return val


# Full URL to the daemon's SSE endpoint, e.g. https://disco.example.com/api/disco/events
EVENTS_URL = env("DISCO_EVENTS_URL", required=True)
API_KEY = env("DISCO_API_KEY", required=True)
SLACK_WEBHOOK_URL = env("SLACK_WEBHOOK_URL", required=True)

# Persisted across restarts so we resume the stream instead of starting at "now"
# (and missing the COMPLETE event of the deploy that restarted us).
STATE_FILE = env("STATE_FILE", "/data/last_event_id")

# The daemon streams events for *every* project on the server. Restrict to these
# Disco project names (comma-separated). Empty = notify for all projects.
PROJECT_FILTER = {
    p.strip() for p in env("DISCO_PROJECT_FILTER", "").split(",") if p.strip()
}

TERMINAL_STATES = {
    s.strip().upper()
    for s in env("TERMINAL_STATES", "COMPLETE,COMPLETED,FAILED").split(",")
    if s.strip()
}
SUCCESS_STATES = {"COMPLETE", "COMPLETED"}
RECONNECT_DELAY = float(env("RECONNECT_DELAY", "5"))

# In-session dedupe of deployments we've already announced. Replay on reconnect
# re-delivers events, so without this we'd double-post.
notified = set()


# --- state persistence ------------------------------------------------------


def read_last_event_id():
    try:
        with open(STATE_FILE) as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None
    except OSError as e:
        log.warning("Could not read state file %s: %s", STATE_FILE, e)
        return None


def write_last_event_id(event_id):
    if not event_id:
        return
    try:
        os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
        with open(STATE_FILE, "w") as f:
            f.write(event_id)
    except OSError as e:
        log.warning("Could not persist last event id: %s", e)


# --- event parsing ----------------------------------------------------------


def _first(d, *keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return None


def extract_deployment(data):
    """Pull the fields we care about out of a deployment:status payload,
    tolerant of unknown/nested shapes (the schema is not documented).

    Observed shape from the daemon:
      {"type": "deployment:status",
       "data": {"project": {"name": "heritable"},
                "deployment": {"number": 488, "status": "COMPLETE"}}}
    """
    # The real payload wraps everything under a top-level "data" envelope.
    inner = data.get("data") if isinstance(data.get("data"), dict) else data
    dep = inner.get("deployment") if isinstance(inner.get("deployment"), dict) else inner
    project = _first(inner, "project", "projectName", "project_name") or _first(dep, "project")
    # `project` may be a nested object ({"name": ...}) — normalize to its name.
    if isinstance(project, dict):
        project = project.get("name")
    status = (_first(dep, "status", "state") or "").upper()
    return {
        "id": _first(dep, "id", "deploymentId", "deployment_id"),
        "number": _first(dep, "number", "deploymentNumber"),
        "status": status,
        "project": project,
        "commit": _first(dep, "commitHash", "commit", "sha"),
    }


def handle_event(event_type, raw_data, event_id):
    # Persist for every event so reconnect resumes past the last one we saw.
    if event_id:
        write_last_event_id(event_id)

    # Log raw events *before* any filtering so debugging shows everything the
    # stream sends, including event types we'd otherwise drop.
    if os.environ.get("DEBUG_EVENTS"):
        log.info("event=%s id=%s data=%s", event_type, event_id, raw_data)

    # We only care about deployment status transitions. The daemon also emits
    # `deployment:created` and heartbeat comments, which we ignore.
    if event_type and event_type != "deployment:status":
        return

    try:
        data = json.loads(raw_data)
    except json.JSONDecodeError:
        log.debug("Skipping non-JSON event data: %s", raw_data)
        return

    dep = extract_deployment(data)
    status = dep["status"]
    if status not in TERMINAL_STATES:
        return

    if PROJECT_FILTER and dep["project"] not in PROJECT_FILTER:
        log.debug("Ignoring %s deploy for project %s (not in filter)", status, dep["project"])
        return

    key = dep["id"] or f"{dep['project']}:{dep['number']}:{status}"
    if key in notified:
        return
    notified.add(key)

    log.info("Notifying Slack: project=%s status=%s key=%s", dep["project"], status, key)
    try:
        post_to_slack(dep)
    except Exception as e:  # best-effort; don't crash the listener on a Slack hiccup
        log.error("Slack post failed: %s", e)
        notified.discard(key)


# --- slack ------------------------------------------------------------------


def post_to_slack(dep):
    status = dep["status"]
    ok = status in SUCCESS_STATES
    emoji = ":white_check_mark:" if ok else ":x:"
    color = "#36a64f" if ok else "#d00000"
    project = dep.get("project") or "unknown project"

    headline = f"{emoji} *Deploy {status.lower()}* — {project}"
    if dep.get("number"):
        headline += f"  ·  build #{dep['number']}"

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": headline}}]
    if dep.get("commit"):
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"commit `{str(dep['commit'])[:8]}`"}],
            }
        )

    payload = {"attachments": [{"color": color, "blocks": blocks}]}
    resp = httpx.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    resp.raise_for_status()


# --- stream loop ------------------------------------------------------------


def stream():
    headers = {"Accept": "text/event-stream"}
    last_id = read_last_event_id()
    if last_id:
        headers["Last-Event-ID"] = last_id
        log.info("Resuming from Last-Event-ID=%s", last_id)

    # read=None: the SSE connection is long-lived; only connect/write/pool time out.
    timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0)
    with httpx.stream(
        "GET", EVENTS_URL, auth=(API_KEY, ""), headers=headers, timeout=timeout
    ) as r:
        r.raise_for_status()
        log.info("Connected to %s", EVENTS_URL)

        event_type = None
        data_lines = []
        cur_id = None
        for line in r.iter_lines():
            if line == "":  # blank line => dispatch the accumulated event
                if data_lines:
                    handle_event(event_type, "\n".join(data_lines), cur_id)
                event_type = None
                data_lines = []
                cur_id = None
                continue
            if line.startswith(":"):  # SSE comment / heartbeat
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "id":
                cur_id = value
            elif field == "event":
                event_type = value
            elif field == "data":
                data_lines.append(value)


def main():
    log.info(
        "Starting disco-deploy-notifier; url=%s projects=%s terminal=%s",
        EVENTS_URL,
        ",".join(sorted(PROJECT_FILTER)) or "ALL",
        ",".join(sorted(TERMINAL_STATES)),
    )
    while True:
        try:
            stream()
            log.warning("Event stream ended; reconnecting in %ss", RECONNECT_DELAY)
        except httpx.HTTPStatusError as e:
            log.error("HTTP %s from events endpoint: %s", e.response.status_code, e)
        except Exception as e:
            log.error("Stream error (%s): %s", type(e).__name__, e)
        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    main()
