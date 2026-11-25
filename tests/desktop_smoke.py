"""Run on a clean Windows host only; no real feeds, credentials or imports."""
import os
import subprocess
import sys
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory() as temp:
    os.environ["TAGAI_SOCIAL_RUNTIME"] = temp
    from tagagent.host import busy, dedicated_chrome, singleton
    from tagagent.agents.social.persistent_feed_worker import PersistentFeedWorker, PROFILE
    from tagagent.agents.social import callout_monitor
    with singleton("importer"):
        assert busy("importer")
        child = subprocess.run([sys.executable, "-m", callout_monitor.__name__], capture_output=True)
        assert child.returncode and b"Timeout" in child.stderr
    assert not busy("importer")
    worker = PersistentFeedWorker(headless=True)
    try:
        worker.start()
        assert dedicated_chrome(PROFILE)
        page = worker.context.new_page()
        page.goto("data:text/html,<title>offline smoke</title>")
        assert page.title() == "offline smoke"
    finally:
        worker.stop()

