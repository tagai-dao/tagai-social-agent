"""OS integration for the desktop collector."""
import os
import sys
from pathlib import Path

import psutil
from filelock import FileLock, Timeout

from tagagent import config


def chrome_path():
    override = config.hermes_env("TAGAI_CHROME_PATH")
    if override:
        candidates = [Path(override)]
    elif sys.platform == "win32":
        candidates = [Path(os.environ[k]) / "Google/Chrome/Application/chrome.exe"
                      for k in ("ProgramW6432", "ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA")
                      if os.environ.get(k)]
    else:
        candidates = [Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")]
    for path in candidates:
        if path.is_file():
            return str(path.resolve())
    raise RuntimeError("Install Google Chrome or set TAGAI_CHROME_PATH in .env")


def dedicated_chrome(profile):
    exe = Path(chrome_path())
    matches = []
    for proc in psutil.process_iter(["exe", "cmdline"]):
        args = proc.info["cmdline"] or []
        if not proc.info["exe"] or Path(proc.info["exe"]) != exe:
            continue
        if "--remote-debugging-port=9223" not in args or any(a.startswith("--type=") for a in args):
            continue
        if any(Path(a.split("=", 1)[1]) == profile for a in args if a.startswith("--user-data-dir=")):
            matches.append(proc)
    return matches


def singleton(role):
    folder = config.RUNTIME_ROOT / "social-callouts"
    folder.mkdir(parents=True, exist_ok=True)
    return FileLock(str(folder / (role + ".lock")), timeout=0)


def busy(role):
    try:
        with singleton(role):
            return False
    except Timeout:
        return True
