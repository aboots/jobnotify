#!/usr/bin/env python3
"""Watch Slurm jobs, process ids, or a local command and send one alert when they finish.

Configure a channel in ~/.config/jobnotify/config.env. A user systemd timer runs
`jobnotify check` every minute. Secrets stay in that file and are never printed.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import smtplib
import socket
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Mapping


TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
    "SPECIAL_EXIT",
    "LEFT_QUEUE",
    "NOT_FOUND",
    "ENDED",
}
HARD_FAILURES = {
    "FAILED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
    "SPECIAL_EXIT",
    "NOT_FOUND",
    "LEFT_QUEUE",
}
MISSING_POLLS = 3
ABSENT_POLLS = 2
FIELD_RE = re.compile(r"([A-Za-z][A-Za-z0-9]*)=(.*?)(?=\s+[A-Za-z][A-Za-z0-9]*=|$)")
ARRAY_META_RE = re.compile(r"^\d+_\[")
JOB_ID_RE = re.compile(r"^\d+$")
SUBMITTED_RE = re.compile(r"Submitted batch job (\d+)")
PARSABLE_RE = re.compile(r"^(\d+)(?:;\S+)?$")


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def home() -> Path:
    return Path(os.environ.get("HOME") or Path.home())


def config_path() -> Path:
    return Path(os.environ.get("JOBNOTIFY_CONFIG") or home() / ".config" / "jobnotify" / "config.env")


def state_path() -> Path:
    return Path(os.environ.get("JOBNOTIFY_STATE") or home() / ".local" / "share" / "jobnotify" / "state.json")


def log_path() -> Path:
    return Path(os.environ.get("JOBNOTIFY_LOG") or home() / ".local" / "share" / "jobnotify" / "jobnotify.log")


def log(message: str) -> None:
    path = log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{stamp} {message}\n")
    try:
        if path.stat().st_size > 200_000:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-1000:]
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except OSError:
        pass


def load_config() -> dict[str, str]:
    path = config_path()
    values: dict[str, str] = {}
    if path.is_file():
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key.strip()] = value
    for key in list(values):
        override = os.environ.get(key)
        if override:
            values[key] = override
    return values


def base_state(state: str) -> str:
    token = (state or "").strip().split()
    if not token:
        return ""
    return token[0].rstrip("+")


def is_terminal(state: str) -> bool:
    return base_state(state) in TERMINAL_STATES


def task_key_belongs(job_id: str, task_id: str) -> bool:
    return task_id == job_id or task_id.startswith(job_id + "_")


def sort_key(task_id: str) -> tuple:
    match = re.match(r"^(\d+)_(\d+)$", task_id)
    if match:
        return (int(match.group(1)), int(match.group(2)))
    if task_id.isdigit():
        return (int(task_id), -1)
    return (10**18, task_id)


def blank_task(task_id: str, state: str, source: str, reason: str = "") -> dict:
    return {
        "id": task_id,
        "name": "",
        "state": state,
        "exit_code": "",
        "elapsed": "",
        "reason": reason,
        "source": source,
        "absent_polls": 0,
    }


def merge_task(old: dict | None, new: dict) -> dict:
    if old is None:
        merged = dict(new)
        merged["absent_polls"] = 0
        return merged
    new_terminal = is_terminal(str(new.get("state", "")))
    old_terminal = is_terminal(str(old.get("state", "")))
    if (not new_terminal) or (not old_terminal):
        base = dict(new)
        fill_from = old
    elif new.get("source") == "sacct" or (new.get("exit_code") and not old.get("exit_code")):
        base = dict(new)
        fill_from = old
    else:
        base = dict(old)
        fill_from = new
    for key in ("name", "elapsed", "reason", "exit_code"):
        if not base.get(key):
            base[key] = fill_from.get(key) or ""
    base["absent_polls"] = 0
    return base


def protocol_tasks(protocol: Mapping) -> list[dict]:
    seen: set[str] = set()
    found: list[dict] = []
    tasks: dict = protocol.get("tasks") or {}
    for job_id in protocol.get("job_ids") or []:
        matched = [item for item in tasks.values() if task_key_belongs(str(job_id), str(item.get("id", "")))]
        matched.sort(key=lambda item: sort_key(str(item.get("id", ""))))
        for item in matched:
            task_id = str(item.get("id", ""))
            if task_id in seen:
                continue
            seen.add(task_id)
            found.append(item)
    return found


def is_settled(protocol: Mapping) -> bool:
    tasks = protocol_tasks(protocol)
    if not tasks:
        return False
    for job_id in protocol.get("job_ids") or []:
        if not any(task_key_belongs(str(job_id), str(task.get("id", ""))) for task in tasks):
            return False
    return all(is_terminal(str(task.get("state", ""))) for task in tasks)


def has_hard_failure(protocol: Mapping) -> bool:
    return any(base_state(str(task.get("state", ""))) in HARD_FAILURES for task in protocol_tasks(protocol))


def apply_snapshot(protocol: dict, snapshots: Mapping[str, Mapping]) -> None:
    protocol.setdefault("tasks", {})
    protocol.setdefault("missing_polls", {})
    for job_id in protocol.get("job_ids") or []:
        snap = snapshots.get(job_id) or {"tasks": [], "ok": False}
        if not snap.get("ok", False):
            continue
        incoming = list(snap.get("tasks") or [])
        seen: set[str] = set()
        for task in incoming:
            task_id = str(task.get("id", ""))
            if not task_id or not task_key_belongs(job_id, task_id):
                continue
            seen.add(task_id)
            protocol["tasks"][task_id] = merge_task(protocol["tasks"].get(task_id), task)
        for task_id, record in list(protocol["tasks"].items()):
            if not task_key_belongs(job_id, task_id) or task_id in seen:
                continue
            if is_terminal(str(record.get("state", ""))):
                continue
            updated = dict(record)
            updated["absent_polls"] = int(updated.get("absent_polls") or 0) + 1
            if updated["absent_polls"] >= ABSENT_POLLS:
                updated["state"] = "LEFT_QUEUE"
                updated["reason"] = updated.get("reason") or "left the scheduler before an exit code was recorded"
                updated["source"] = "missing"
            protocol["tasks"][task_id] = updated
        mine = [task for task_id, task in protocol["tasks"].items() if task_key_belongs(job_id, task_id)]
        if mine:
            protocol["missing_polls"][job_id] = 0
        else:
            count = int(protocol["missing_polls"].get(job_id) or 0) + 1
            protocol["missing_polls"][job_id] = count
            if count >= MISSING_POLLS:
                protocol["tasks"][job_id] = blank_task(
                    job_id,
                    "NOT_FOUND",
                    "missing",
                    "job id was not in the scheduler",
                )


def summarize(tasks: list[dict]) -> str:
    counts: dict[str, int] = {}
    for task in tasks:
        key = base_state(str(task.get("state", ""))) or "UNKNOWN"
        counts[key] = counts.get(key, 0) + 1
    preferred = [
        "COMPLETED",
        "FAILED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "BOOT_FAIL",
        "CANCELLED",
        "LEFT_QUEUE",
        "NOT_FOUND",
        "ENDED",
        "RUNNING",
        "PENDING",
        "COMPLETING",
    ]
    parts: list[str] = []
    seen: set[str] = set()
    for key in preferred + sorted(counts):
        if key in counts and key not in seen:
            seen.add(key)
            label = key.lower().replace("_", " ")
            parts.append(f"{counts[key]} {label}")
    return ", ".join(parts)


def task_line(task: Mapping) -> str:
    state = base_state(str(task.get("state", "")))
    bits = [str(task.get("id", "")), str(task.get("name") or "-"), state]
    if task.get("elapsed"):
        bits.append(str(task["elapsed"]))
    if task.get("exit_code") and state not in {"NOT_FOUND", "LEFT_QUEUE", "ENDED", "RUNNING", "PENDING"}:
        bits.append("exit " + str(task["exit_code"]))
    reason = str(task.get("reason") or "")
    if reason and reason not in {"None", "null"} and state not in {"COMPLETED", "RUNNING", "PENDING"}:
        bits.append(reason[:180])
    return "  " + "  ".join(bits)


def render_tasks(tasks: list[dict]) -> str:
    interesting = [task for task in tasks if base_state(str(task.get("state", ""))) != "COMPLETED"]
    if len(tasks) > 20 and not interesting:
        lines = [task_line(task) for task in tasks[:10]]
        lines.append(f"  ... {len(tasks) - 10} more completed")
        return "\n".join(lines)
    show = interesting if interesting and len(tasks) > 20 else tasks
    if len(show) > 40:
        hidden = len(show) - 40
        show = show[:40]
        return "\n".join(task_line(task) for task in show) + f"\n  ... {hidden} more"
    return "\n".join(task_line(task) for task in show)


def message_for(protocol: Mapping, early: bool) -> tuple[str, str]:
    name = str(protocol.get("name") or "jobs")
    tasks = protocol_tasks(protocol)
    if early:
        title = f"Job problem: {name}"
    elif has_hard_failure(protocol):
        title = f"Jobs need attention: {name}"
    elif tasks and all(base_state(str(task.get("state", ""))) == "CANCELLED" for task in tasks):
        title = f"Jobs stopped: {name}"
    else:
        title = f"Jobs finished: {name}"
    host = socket.gethostname()
    body = "\n".join(
        [
            f"Host: {host}",
            summarize(tasks) or "no tasks recorded",
            "",
            render_tasks(tasks),
            "",
            "Come check the results.",
        ]
    )
    return title, body[:3500]


def parse_scontrol(text: str) -> list[dict]:
    blocks = re.split(r"\n\s*\n", text.strip())
    tasks: list[dict] = []
    for block in blocks:
        if "JobId=" not in block:
            continue
        flat = " ".join(line.strip() for line in block.splitlines())
        fields = {key: value.strip() for key, value in FIELD_RE.findall(flat)}
        job_id = fields.get("JobId", "")
        array_job = fields.get("ArrayJobId", "")
        array_task = fields.get("ArrayTaskId", "")
        if array_job and array_task.isdigit():
            display = f"{array_job}_{array_task}"
        else:
            display = job_id
        if not display or ARRAY_META_RE.match(display):
            continue
        tasks.append(
            {
                "id": display,
                "name": fields.get("JobName", ""),
                "state": fields.get("JobState", ""),
                "exit_code": fields.get("ExitCode", ""),
                "elapsed": fields.get("RunTime", ""),
                "reason": fields.get("Reason", ""),
                "source": "scontrol",
                "absent_polls": 0,
            }
        )
    return tasks


def parse_squeue_line(line: str) -> dict | None:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 4:
        return None
    task_id = parts[0].strip()
    if not task_id or ARRAY_META_RE.match(task_id):
        return None
    return {
        "id": task_id,
        "name": parts[2].strip(),
        "state": parts[1].strip(),
        "exit_code": "",
        "elapsed": parts[3].strip(),
        "reason": "\t".join(parts[4:]).strip(),
        "source": "squeue",
        "absent_polls": 0,
    }


def parse_sacct(text: str) -> list[dict]:
    tasks: list[dict] = []
    for line in text.splitlines():
        parts = line.rstrip("\n").split("|")
        if len(parts) < 5:
            continue
        task_id = parts[0].strip()
        if not task_id or "." in task_id or ARRAY_META_RE.match(task_id):
            continue
        tasks.append(
            {
                "id": task_id,
                "name": parts[1].strip(),
                "state": parts[2].strip(),
                "exit_code": parts[3].strip(),
                "elapsed": parts[4].strip(),
                "reason": "",
                "source": "sacct",
                "absent_polls": 0,
            }
        )
    return tasks


def run_cmd(args: list[str], timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=False, capture_output=True, text=True, timeout=timeout)


def controller_down(text: str) -> bool:
    lowered = text.lower()
    markers = (
        "connection refused",
        "unable to contact",
        "communication connection failure",
        "slurm_persist_conn",
        "problem talking to the database",
    )
    return any(marker in lowered for marker in markers)


def read_squeue() -> tuple[list[dict], bool]:
    user = os.environ.get("USER") or str(os.getuid())
    try:
        result = run_cmd(["squeue", "-u", user, "-t", "all", "-h", "-o", "%i\t%T\t%j\t%M\t%r"])
    except (OSError, subprocess.TimeoutExpired) as exc:
        log(f"squeue failed: {exc}")
        return [], False
    if result.returncode != 0:
        if controller_down(result.stderr) or controller_down(result.stdout):
            log("squeue could not reach the scheduler")
            return [], False
        log(f"squeue exit {result.returncode}: {result.stderr.strip()[:300]}")
        return [], False
    tasks = []
    for line in result.stdout.splitlines():
        parsed = parse_squeue_line(line)
        if parsed:
            tasks.append(parsed)
    return tasks, True


def read_scontrol(job_id: str) -> tuple[list[dict], str]:
    try:
        result = run_cmd(["scontrol", "show", "job", job_id])
    except (OSError, subprocess.TimeoutExpired) as exc:
        log(f"scontrol {job_id} failed: {exc}")
        return [], "down"
    combined = result.stdout + "\n" + result.stderr
    if "Invalid job id" in combined:
        return [], "missing"
    if result.returncode != 0 or controller_down(combined):
        log(f"scontrol {job_id} unavailable: {result.stderr.strip()[:300]}")
        return [], "down"
    return parse_scontrol(result.stdout), "ok"


def read_sacct(job_ids: list[str]) -> list[dict] | None:
    if not job_ids or shutil.which("sacct") is None:
        return None
    try:
        result = run_cmd(
            [
                "sacct",
                "-j",
                ",".join(job_ids),
                "-S",
                "1970-01-01",
                "-X",
                "-P",
                "-n",
                "--format=JobID,JobName,State,ExitCode,Elapsed",
            ],
            timeout=40,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        log(f"sacct failed: {exc}")
        return None
    combined = result.stderr + "\n" + result.stdout
    if result.returncode != 0 or controller_down(combined):
        return None
    return parse_sacct(result.stdout)


def collect_tasks(job_id: str, *groups: list[dict]) -> list[dict]:
    by_id: dict[str, dict] = {}
    for group in groups:
        for task in group:
            task_id = str(task.get("id", ""))
            if task_id and task_key_belongs(job_id, task_id):
                by_id[task_id] = merge_task(by_id.get(task_id), task)
    return list(by_id.values())


def fetch_slurm_snapshots(job_ids: list[str]) -> dict[str, dict]:
    squeue_tasks, squeue_ok = read_squeue()
    sacct_tasks = read_sacct(job_ids) or []
    snapshots: dict[str, dict] = {}
    for job_id in job_ids:
        scontrol_tasks, status = read_scontrol(job_id)
        ok = squeue_ok or status in {"ok", "missing"}
        tasks = collect_tasks(job_id, sacct_tasks, scontrol_tasks, squeue_tasks) if ok else []
        snapshots[job_id] = {"tasks": tasks, "ok": ok}
    return snapshots


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def fetch_pid_snapshots(protocol: Mapping) -> dict[str, dict]:
    exit_files = protocol.get("exit_files") or {}
    snapshots: dict[str, dict] = {}
    for raw_pid in protocol.get("job_ids") or []:
        pid_text = str(raw_pid)
        try:
            pid = int(pid_text)
        except ValueError:
            snapshots[pid_text] = {
                "ok": True,
                "tasks": [blank_task(pid_text, "NOT_FOUND", "pid", "pid is not a number")],
            }
            continue
        exit_code = ""
        exit_path = exit_files.get(pid_text)
        if exit_path and Path(exit_path).is_file():
            exit_code = Path(exit_path).read_text(encoding="utf-8").strip().splitlines()[-1:]
            exit_code = exit_code[0].strip() if exit_code else ""
        if pid_alive(pid):
            state = "RUNNING"
        elif exit_code == "":
            state = "ENDED"
        elif exit_code == "0":
            state = "COMPLETED"
        else:
            state = "FAILED"
        snapshots[pid_text] = {
            "ok": True,
            "tasks": [
                {
                    "id": pid_text,
                    "name": str(protocol.get("command") or "process"),
                    "state": state,
                    "exit_code": exit_code,
                    "elapsed": "",
                    "reason": "",
                    "source": "pid",
                    "absent_polls": 0,
                }
            ],
        }
    return snapshots


class StateStore:
    def __init__(self) -> None:
        self.path = state_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_suffix(".lock")
        self._lock = open(self.lock_path, "a", encoding="utf-8")
        self.data = {"version": 1, "protocols": []}

    def __enter__(self) -> "StateStore":
        fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX)
        if self.path.is_file():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                loaded = {"version": 1, "protocols": []}
            if isinstance(loaded, dict) and isinstance(loaded.get("protocols"), list):
                self.data = loaded
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.save()
        fcntl.flock(self._lock.fileno(), fcntl.LOCK_UN)
        self._lock.close()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self.path.parent,
            prefix=".state-",
            delete=False,
        )
        try:
            json.dump(self.data, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.close()
            os.replace(handle.name, self.path)
        finally:
            if os.path.exists(handle.name):
                os.unlink(handle.name)

    def protocols(self) -> list[dict]:
        return self.data["protocols"]

    def find(self, name: str) -> dict | None:
        for protocol in self.protocols():
            if protocol.get("name") == name:
                return protocol
        return None


def new_protocol(name: str, kind: str, job_ids: list[str], command: str = "") -> dict:
    return {
        "name": name,
        "kind": kind,
        "job_ids": job_ids,
        "command": command,
        "tasks": {},
        "missing_polls": {},
        "exit_files": {},
        "status": "watching",
        "early_sent": False,
        "created": now_iso(),
        "finished_at": None,
        "last_error": "",
    }


def validate_job_ids(job_ids: list[str], kind: str) -> list[str]:
    cleaned = []
    for job_id in job_ids:
        text = str(job_id).strip()
        if kind == "slurm" and not JOB_ID_RE.fullmatch(text):
            raise SystemExit(f"Slurm job id must be digits: {text}")
        if kind == "pid" and not JOB_ID_RE.fullmatch(text):
            raise SystemExit(f"pid must be digits: {text}")
        if text not in cleaned:
            cleaned.append(text)
    if not cleaned:
        raise SystemExit("Provide at least one id")
    return cleaned


def clean_name(name: str) -> str:
    text = " ".join(name.split())
    if not text:
        raise SystemExit("Protocol name is empty")
    if len(text) > 120:
        raise SystemExit("Protocol name is longer than 120 characters")
    return text


def configured_channels(cfg: Mapping[str, str]) -> list[str]:
    found = []
    if cfg.get("NTFY_TOPIC"):
        found.append("ntfy")
    if cfg.get("TELEGRAM_BOT_TOKEN") and cfg.get("TELEGRAM_CHAT_ID"):
        found.append("telegram")
    if cfg.get("SMTP_HOST") and cfg.get("SMTP_TO") and cfg.get("SMTP_PASSWORD"):
        found.append("email")
    if cfg.get("WEBHOOK_URL"):
        found.append("webhook")
    return found


def http_json(url: str, payload: dict | None = None, headers: dict | None = None, method: str | None = None) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError("URL must start with http:// or https://")
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request_headers = {"User-Agent": "jobnotify"}
    if payload is not None:
        request_headers["Content-Type"] = "application/json"
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            response.read(256)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}") from None
    except urllib.error.URLError as exc:
        raise RuntimeError(str(exc.reason)) from None


def send_ntfy(cfg: Mapping[str, str], title: str, body: str, problem: bool) -> None:
    topic = cfg["NTFY_TOPIC"]
    if topic.startswith("http://") or topic.startswith("https://"):
        parsed = urllib.parse.urlparse(topic)
        server = f"{parsed.scheme}://{parsed.netloc}"
        topic_name = parsed.path.lstrip("/")
    else:
        server = (cfg.get("NTFY_SERVER") or "https://ntfy.sh").rstrip("/")
        topic_name = topic
    http_json(
        server,
        {
            "topic": topic_name,
            "title": title[:250],
            "message": body,
            "priority": 5 if problem else 3,
            "tags": ["warning" if problem else "white_check_mark"],
        },
    )


def send_telegram(cfg: Mapping[str, str], title: str, body: str) -> None:
    token = cfg["TELEGRAM_BOT_TOKEN"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    http_json(
        url,
        {
            "chat_id": cfg["TELEGRAM_CHAT_ID"],
            "text": f"{title}\n\n{body}"[:4000],
            "disable_web_page_preview": True,
        },
    )


def send_email(cfg: Mapping[str, str], title: str, body: str) -> None:
    message = EmailMessage()
    message["Subject"] = title
    message["From"] = cfg.get("SMTP_FROM") or cfg.get("SMTP_USER") or cfg["SMTP_TO"]
    message["To"] = cfg["SMTP_TO"]
    message.set_content(f"{title}\n\n{body}")
    host = cfg["SMTP_HOST"]
    port = int(cfg.get("SMTP_PORT") or "587")
    security = (cfg.get("SMTP_SECURITY") or "starttls").lower()
    if security == "ssl":
        client = smtplib.SMTP_SSL(host, port, timeout=30)
    else:
        client = smtplib.SMTP(host, port, timeout=30)
        client.ehlo()
        if security != "none":
            client.starttls()
            client.ehlo()
    try:
        user = cfg.get("SMTP_USER")
        if user:
            client.login(user, cfg.get("SMTP_PASSWORD", ""))
        client.send_message(message)
    finally:
        try:
            client.quit()
        except smtplib.SMTPException:
            pass


def send_webhook(cfg: Mapping[str, str], title: str, body: str) -> None:
    url = cfg["WEBHOOK_URL"]
    if "{message}" in url or "{title}" in url:
        filled = url.replace("{title}", urllib.parse.quote(title[:180])).replace(
            "{message}", urllib.parse.quote(f"{title}\n{body}"[:1500])
        )
        http_json(filled, method="GET")
        return
    http_json(url, {"title": title, "body": body})


def send_all(cfg: Mapping[str, str], title: str, body: str, problem: bool) -> tuple[bool, list[str]]:
    channels = configured_channels(cfg)
    if not channels:
        return False, ["no notification channel is configured"]
    errors: list[str] = []
    sent = 0
    if "ntfy" in channels:
        try:
            send_ntfy(cfg, title, body, problem)
        except Exception as exc:  # noqa: BLE001 - report every channel and keep going
            errors.append(f"ntfy: {exc}")
        else:
            sent += 1
    if "telegram" in channels:
        try:
            send_telegram(cfg, title, body)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"telegram: {exc}")
        else:
            sent += 1
    if "email" in channels:
        try:
            send_email(cfg, title, body)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"email: {exc}")
        else:
            sent += 1
    if "webhook" in channels:
        try:
            send_webhook(cfg, title, body)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"webhook: {exc}")
        else:
            sent += 1
    return sent > 0, errors


def deliver(protocol: dict, early: bool) -> bool:
    title, body = message_for(protocol, early=early)
    ok, errors = send_all(load_config(), title, body, problem=early or has_hard_failure(protocol))
    if ok:
        protocol["last_error"] = "; ".join(errors)
        log(f"sent {'early' if early else 'final'} alert for {protocol['name']}")
        if errors:
            log("partial send failure: " + "; ".join(errors))
        return True
    protocol["last_error"] = "; ".join(errors)
    log(f"alert failed for {protocol['name']}: {protocol['last_error']}")
    return False


def poll_protocol(protocol: dict) -> None:
    if protocol.get("status") != "watching":
        return
    kind = protocol.get("kind") or "slurm"
    if kind == "pid":
        snapshots = fetch_pid_snapshots(protocol)
    else:
        snapshots = fetch_slurm_snapshots([str(job_id) for job_id in protocol.get("job_ids") or []])
    apply_snapshot(protocol, snapshots)
    protocol["last_check"] = now_iso()
    settled = is_settled(protocol)
    if settled:
        if deliver(protocol, early=False):
            protocol["status"] = "finished"
            protocol["finished_at"] = now_iso()
            protocol["early_sent"] = True
        return
    if has_hard_failure(protocol) and not protocol.get("early_sent"):
        if deliver(protocol, early=True):
            protocol["early_sent"] = True


def add_watch(name: str, kind: str, job_ids: list[str], command: str = "", exit_file: str = "") -> dict:
    name = clean_name(name)
    job_ids = validate_job_ids(job_ids, kind)
    with StateStore() as store:
        protocol = store.find(name)
        if protocol and protocol.get("status") == "finished":
            raise SystemExit(f"{name} already finished. Choose a new protocol name.")
        if protocol is None:
            protocol = new_protocol(name, kind, job_ids, command)
            store.protocols().append(protocol)
        else:
            if protocol.get("kind") not in {None, kind}:
                raise SystemExit(f"{name} is a {protocol.get('kind')} watch")
            for job_id in job_ids:
                if job_id not in protocol["job_ids"]:
                    protocol["job_ids"].append(job_id)
            if command:
                protocol["command"] = command
        if exit_file and job_ids:
            protocol.setdefault("exit_files", {})[job_ids[-1]] = exit_file
        return json.loads(json.dumps(protocol))


def strip_remainder(args: list[str]) -> list[str]:
    if args and args[0] == "--":
        return args[1:]
    return args


def parse_sbatch_ids(text: str) -> list[str]:
    ids = SUBMITTED_RE.findall(text)
    if ids:
        return ids
    found = []
    for line in text.splitlines():
        match = PARSABLE_RE.fullmatch(line.strip())
        if match:
            found.append(match.group(1))
    return found


def cmd_watch(args: argparse.Namespace) -> int:
    protocol = add_watch(args.name, args.kind, args.job_ids)
    print(f"Watching {protocol['name']}: {', '.join(protocol['job_ids'])}")
    print("One message is sent when every listed job has finished.")
    return 0


def cmd_sbatch(args: argparse.Namespace) -> int:
    sbatch_args = strip_remainder(args.sbatch_args)
    if not sbatch_args:
        raise SystemExit("Usage: jobnotify sbatch --name NAME -- script.slurm")
    result = subprocess.run(["sbatch", *sbatch_args], check=False, text=True, capture_output=True)
    if result.stdout:
        sys.stdout.write(result.stdout if result.stdout.endswith("\n") else result.stdout + "\n")
    if result.stderr:
        sys.stderr.write(result.stderr if result.stderr.endswith("\n") else result.stderr + "\n")
    if result.returncode != 0:
        return result.returncode
    job_ids = parse_sbatch_ids(result.stdout)
    if not job_ids:
        raise SystemExit("sbatch succeeded but no job id was found in its output")
    protocol = add_watch(args.name, "slurm", job_ids)
    print(f"Watching {protocol['name']}: {', '.join(protocol['job_ids'])}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    command = strip_remainder(args.command)
    if not command:
        raise SystemExit("Usage: jobnotify run --name NAME -- command arg")
    if args.detach:
        exit_dir = state_path().parent / "exits"
        exit_dir.mkdir(parents=True, exist_ok=True)
        exit_file = exit_dir / f"{os.getpid()}-{int(datetime.now().timestamp())}.txt"
        script = 'code=0; "$@" || code=$?; printf "%s\\n" "$code" > "$JOBNOTIFY_EXIT_FILE"; exit "$code"'
        proc = subprocess.Popen(
            ["bash", "-c", script, "bash", *command],
            start_new_session=True,
            env={**os.environ, "JOBNOTIFY_EXIT_FILE": str(exit_file)},
        )
        add_watch(args.name, "pid", [str(proc.pid)], command=" ".join(command), exit_file=str(exit_file))
        print(f"Started pid {proc.pid} and will notify when {args.name} exits.")
        return 0
    result = subprocess.run(command, check=False)
    state = "COMPLETED" if result.returncode == 0 else "FAILED"
    with StateStore() as store:
        protocol = store.find(clean_name(args.name))
        if protocol and protocol.get("status") == "finished":
            raise SystemExit(f"{args.name} already finished. Choose a new protocol name.")
        if protocol is None:
            protocol = new_protocol(clean_name(args.name), "pid", [str(os.getpid())], " ".join(command))
            store.protocols().append(protocol)
        protocol["tasks"] = {
            "local": {
                "id": "local",
                "name": " ".join(command)[:80],
                "state": state,
                "exit_code": str(result.returncode),
                "elapsed": "",
                "reason": "",
                "source": "run",
                "absent_polls": 0,
            }
        }
        protocol["job_ids"] = ["local"]
        if deliver(protocol, early=False):
            protocol["status"] = "finished"
            protocol["finished_at"] = now_iso()
    return result.returncode


def cmd_check(_: argparse.Namespace) -> int:
    with StateStore() as store:
        for protocol in store.protocols():
            try:
                poll_protocol(protocol)
            except Exception as exc:  # noqa: BLE001 - one protocol must not block the others
                protocol["last_error"] = str(exc)
                log(f"check failed for {protocol.get('name')}: {exc}")
    return 0


def cmd_list(_: argparse.Namespace) -> int:
    with StateStore() as store:
        protocols = store.protocols()
        if not protocols:
            print("No protocols are being watched.")
            return 0
        for protocol in protocols:
            tasks = protocol_tasks(protocol)
            summary = summarize(tasks) if tasks else "waiting for the first scheduler check"
            print(f"{protocol.get('status', 'watching'):9} {protocol.get('name')}")
            print(f"          {summary}")
            print(f"          ids: {', '.join(protocol.get('job_ids') or [])}")
            if protocol.get("last_error"):
                print(f"          last error: {protocol['last_error']}")
    return 0


def cmd_forget(args: argparse.Namespace) -> int:
    name = clean_name(args.name)
    with StateStore() as store:
        before = len(store.protocols())
        store.data["protocols"] = [item for item in store.protocols() if item.get("name") != name]
        if len(store.protocols()) == before:
            raise SystemExit(f"No protocol named {name}")
    print(f"Forgot {name}")
    return 0


def cmd_channels(_: argparse.Namespace) -> int:
    cfg = load_config()
    rows = [
        ("ntfy", bool(cfg.get("NTFY_TOPIC"))),
        ("telegram", bool(cfg.get("TELEGRAM_BOT_TOKEN") and cfg.get("TELEGRAM_CHAT_ID"))),
        ("email", bool(cfg.get("SMTP_HOST") and cfg.get("SMTP_TO") and cfg.get("SMTP_PASSWORD"))),
        ("webhook", bool(cfg.get("WEBHOOK_URL"))),
    ]
    for name, ready in rows:
        print(f"{name:10} {'ready' if ready else 'unset'}")
    if cfg.get("SMTP_HOST") and cfg.get("SMTP_TO") and not cfg.get("SMTP_PASSWORD"):
        print("email password is still empty")
    return 0


def cmd_test(_: argparse.Namespace) -> int:
    host = socket.gethostname()
    title = f"jobnotify test from {host}"
    body = "The checker can reach this channel. Job alerts will use the same path."
    ok, errors = send_all(load_config(), title, body, problem=False)
    if ok:
        print("Test alert sent.")
        if errors:
            print("Some channels failed: " + "; ".join(errors))
        return 0
    print("Test alert was not sent: " + "; ".join(errors), file=sys.stderr)
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jobnotify", description="Notify when a named set of jobs finishes.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    watch = sub.add_parser("watch", help="Watch Slurm job ids or pids")
    watch.add_argument("--name", required=True, help="Protocol name included in the alert")
    watch.add_argument("--kind", choices=("slurm", "pid"), default="slurm")
    watch.add_argument("job_ids", nargs="+")
    watch.set_defaults(func=cmd_watch)

    sbatch = sub.add_parser("sbatch", help="Run sbatch and watch the submitted job")
    sbatch.add_argument("--name", required=True)
    sbatch.add_argument("sbatch_args", nargs=argparse.REMAINDER)
    sbatch.set_defaults(func=cmd_sbatch)

    run = sub.add_parser("run", help="Run a local command and notify when it exits")
    run.add_argument("--name", required=True)
    run.add_argument("--detach", action="store_true")
    run.add_argument("command", nargs=argparse.REMAINDER)
    run.set_defaults(func=cmd_run)

    check = sub.add_parser("check", help="Poll watched jobs once")
    check.set_defaults(func=cmd_check)
    listing = sub.add_parser("list", help="Show watched protocols")
    listing.set_defaults(func=cmd_list)
    forget = sub.add_parser("forget", help="Stop watching a protocol")
    forget.add_argument("name")
    forget.set_defaults(func=cmd_forget)
    channels = sub.add_parser("channels", help="Show which channels are configured")
    channels.set_defaults(func=cmd_channels)
    test = sub.add_parser("test", help="Send a test alert")
    test.set_defaults(func=cmd_test)
    self_test = sub.add_parser("self-test", help="Run internal checks")
    self_test.set_defaults(func=cmd_self_test)
    return parser


def cmd_self_test(_: argparse.Namespace) -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobNotifyTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


class JobNotifyTests(unittest.TestCase):
    def test_states(self) -> None:
        self.assertTrue(is_terminal("CANCELLED by 100"))
        self.assertFalse(is_terminal("COMPLETING"))
        self.assertEqual(base_state("FAILED+"), "FAILED")

    def test_scontrol_array(self) -> None:
        text = """
JobId=20057 ArrayJobId=20018 ArrayTaskId=3 ArrayTaskThrottle=2 JobName=v49-stageB
   JobState=PENDING Reason=Nodes_required ExitCode=0:0 RunTime=00:00:00

JobId=20056 ArrayJobId=20018 ArrayTaskId=2 JobName=v49-stageB
   JobState=RUNNING Reason=None ExitCode=0:0 RunTime=05:44:55
"""
        tasks = parse_scontrol(text)
        self.assertEqual([task["id"] for task in tasks], ["20018_3", "20018_2"])
        self.assertEqual(tasks[1]["state"], "RUNNING")
        self.assertEqual(tasks[1]["elapsed"], "05:44:55")

    def test_belongs_and_absence(self) -> None:
        protocol = new_protocol("demo", "slurm", ["20018"])
        running = {
            "20018": {
                "ok": True,
                "tasks": [{"id": "20018_2", "name": "stage", "state": "RUNNING", "exit_code": "", "elapsed": "1:00", "reason": "", "source": "squeue"}],
            }
        }
        apply_snapshot(protocol, running)
        self.assertFalse(is_settled(protocol))
        apply_snapshot(protocol, {"20018": {"ok": True, "tasks": []}})
        self.assertEqual(protocol["tasks"]["20018_2"]["state"], "RUNNING")
        apply_snapshot(protocol, {"20018": {"ok": True, "tasks": []}})
        self.assertEqual(protocol["tasks"]["20018_2"]["state"], "LEFT_QUEUE")
        self.assertTrue(is_settled(protocol))
        self.assertTrue(has_hard_failure(protocol))

    def test_cancelled_does_not_page_early(self) -> None:
        protocol = new_protocol("demo", "slurm", ["20018"])
        apply_snapshot(
            protocol,
            {
                "20018": {
                    "ok": True,
                    "tasks": [
                        {"id": "20018_2", "name": "stage", "state": "RUNNING", "exit_code": "0:0", "elapsed": "", "reason": "", "source": "squeue"},
                        {"id": "20018_4", "name": "stage", "state": "CANCELLED", "exit_code": "0:0", "elapsed": "0:00", "reason": "JobHeldUser", "source": "squeue"},
                    ],
                }
            },
        )
        self.assertFalse(is_settled(protocol))
        self.assertFalse(has_hard_failure(protocol))

    def test_failed_while_running_is_early(self) -> None:
        protocol = new_protocol("demo", "slurm", ["9"])
        apply_snapshot(
            protocol,
            {
                "9": {
                    "ok": True,
                    "tasks": [
                        {"id": "9_0", "name": "fit", "state": "FAILED", "exit_code": "1:0", "elapsed": "0:02", "reason": "", "source": "sacct"},
                        {"id": "9_1", "name": "fit", "state": "RUNNING", "exit_code": "", "elapsed": "0:10", "reason": "", "source": "squeue"},
                    ],
                }
            },
        )
        self.assertFalse(is_settled(protocol))
        self.assertTrue(has_hard_failure(protocol))
        title, body = message_for(protocol, early=True)
        self.assertIn("demo", title)
        self.assertIn("9_0", body)

    def test_not_found_after_grace(self) -> None:
        protocol = new_protocol("missing", "slurm", ["42"])
        for _ in range(MISSING_POLLS - 1):
            apply_snapshot(protocol, {"42": {"ok": True, "tasks": []}})
            self.assertFalse(is_settled(protocol))
        apply_snapshot(protocol, {"42": {"ok": True, "tasks": []}})
        self.assertEqual(protocol["tasks"]["42"]["state"], "NOT_FOUND")
        self.assertTrue(is_settled(protocol))

    def test_controller_error_does_not_advance(self) -> None:
        protocol = new_protocol("demo", "slurm", ["7"])
        apply_snapshot(protocol, {"7": {"ok": False, "tasks": []}})
        self.assertEqual(protocol["missing_polls"].get("7", 0), 0)
        self.assertFalse(is_settled(protocol))

    def test_config_and_sbatch_parse(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.env"
            path.write_text('# comment\nexport NTFY_TOPIC="abc def"\nSMTP_PASSWORD=\n', encoding="utf-8")
            os.environ["JOBNOTIFY_CONFIG"] = str(path)
            try:
                loaded = load_config()
            finally:
                os.environ.pop("JOBNOTIFY_CONFIG", None)
        self.assertEqual(loaded["NTFY_TOPIC"], "abc def")
        self.assertEqual(loaded["SMTP_PASSWORD"], "")
        self.assertEqual(parse_sbatch_ids("Submitted batch job 20020\n"), ["20020"])
        self.assertEqual(parse_sbatch_ids("20018;cluster\n"), ["20018"])

    def test_merge_keeps_exit_code(self) -> None:
        sacct = {
            "id": "9_0",
            "name": "fit",
            "state": "FAILED",
            "exit_code": "1:0",
            "elapsed": "00:02:00",
            "reason": "",
            "source": "sacct",
        }
        live = {
            "id": "9_0",
            "name": "fit",
            "state": "FAILED",
            "exit_code": "",
            "elapsed": "2:00",
            "reason": "NonZeroExitCode",
            "source": "squeue",
        }
        merged = merge_task(sacct, live)
        self.assertEqual(merged["exit_code"], "1:0")
        self.assertEqual(merged["reason"], "NonZeroExitCode")

    def test_finished_message_names_the_protocol(self) -> None:
        protocol = new_protocol("V49 remaining", "slurm", ["20020"])
        apply_snapshot(
            protocol,
            {
                "20020": {
                    "ok": True,
                    "tasks": [
                        {"id": "20020", "name": "v49-summary", "state": "COMPLETED", "exit_code": "0:0", "elapsed": "00:03:00", "reason": "", "source": "sacct"}
                    ],
                }
            },
        )
        title, body = message_for(protocol, early=False)
        self.assertEqual(title, "Jobs finished: V49 remaining")
        self.assertIn("Come check the results.", body)
        self.assertIn("v49-summary", body)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130) from None
