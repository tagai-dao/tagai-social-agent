"""Persistent Chrome worker for platform-wide social callout feeds.

The heartbeat remains a scheduler/health check.  This process owns a dedicated
Chrome profile, keeps platform sessions alive, freezes visible feed cards, and
persists conservative checkpoints.  Ambiguous cards are never reported as a
complete scan.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
from tagagent.host import chrome_path, dedicated_chrome, singleton, busy
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, quote, urlparse

from playwright.sync_api import BrowserContext, Page, TimeoutError, sync_playwright

from tagagent import config
from tagagent.agents.social.callout_monitor import _effective_lookback
from tagagent.agents.social.feed_browser_batch import FeedBrowserBatchError, persist_batch
from tagagent.tools.callout_platforms import X_STATUS_RE
from tagagent.wiki import load_json, save_json


LOG = logging.getLogger("tagagent.social.feed-worker")
# Chrome is resolved at startup.
PROFILE = config.RUNTIME_ROOT / "social-callouts" / "chrome-worker-profile"
STATE = config.RUNTIME_ROOT / "social-callouts" / "persistent-worker-state.json"
HEALTH = config.RUNTIME_ROOT / "social-callouts" / "persistent-worker-health.json"
DIAGNOSTICS = config.RUNTIME_ROOT / "social-callouts" / "worker-diagnostics"
TOKEN_RE = re.compile(r"/(bsc|robinhood)/token(?:s)?/(0x[a-f0-9]{40})", re.I)
FOMO_RE = re.compile(r"/tokens/(bnb|robinhood)/(0x[a-f0-9]{40})", re.I)
PUMP_RE = re.compile(r"/(?:coin|coins|token|tokens)/(0x[a-f0-9]{40})(?:[/?#]|$)", re.I)
PUMP_CALLOUT_RE = re.compile(
    r"/callouts/(0x[a-f0-9]{40})/([0-9a-f]{8}-[0-9a-f-]{27,})", re.I
)
RELATIVE_RE = re.compile(
    r"(?<!\w)(now|刚刚|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs|d|day|days|w|week|weeks|秒|分|分钟|时|小时|天|周))(?!\w)",
    re.I,
)
FOMO_FEED_LABEL_RE = re.compile(r"^(?:Feed|动态)$", re.I)
FOMO_FILTER_LABEL_RE = re.compile(r"^(?:Filter|筛选)(?:\s*\(?\d+/8\)?)?$", re.I)
FOMO_TRADES_LABEL_RE = re.compile(r"^(?:Trades|交易)$", re.I)
FOMO_NEW_ACTIVITY_LABEL_RE = re.compile(
    r"^(?:(?:Show\s+)?New activity|新动态|新活动)$", re.I,
)
PURE_PERCENT_RE = re.compile(r"^[+\-]?\d+(?:\.\d+)?%$")
MARKET_VALUE_RE = re.compile(r"^\$?[\d,.]+(?:\.\d+)?[KMBT]?$", re.I)
FOMO_MARKET_CAP_RE = re.compile(r"\$\s*([\d,.]+(?:\.\d+)?[KMBT]?)\s*MC\b", re.I)
GMGN_X_PAGE_SIZE = 100
GMGN_X_POLL_INTERVAL_SECONDS = 10


class BrowserAccessBlockedError(RuntimeError):
    """An HTTP access challenge is not a broken browser session."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dom_data_icon(locator) -> str:
    """Read a rendered icon without waiting for an optional descendant."""
    return str(locator.evaluate(
        "el => el.querySelector('svg')?.getAttribute('data-icon') || ''"
    ) or "")


def _relative_seconds(value: str) -> int | None:
    match = RELATIVE_RE.search(value or "")
    if not match:
        return None
    raw = match.group(1).lower().replace(" ", "")
    if raw in {"now", "刚刚"}:
        return 0
    amount = int(re.match(r"\d+", raw).group())
    if raw.endswith(("w", "week", "weeks")):
        return amount * 7 * 86400
    if raw.endswith("周"):
        return amount * 7 * 86400
    if raw.endswith(("d", "day", "days")):
        return amount * 86400
    if raw.endswith("天"):
        return amount * 86400
    if raw.endswith(("h", "hr", "hrs")):
        return amount * 3600
    if raw.endswith(("时", "小时")):
        return amount * 3600
    if raw.endswith(("m", "min", "mins")):
        return amount * 60
    if raw.endswith(("分", "分钟")):
        return amount * 60
    return amount


def _card_ages(card: dict) -> list[int]:
    """Return every top-level relative clock captured from a frozen card."""
    values = card.get("relativeTimes")
    if not isinstance(values, list):
        values = RELATIVE_RE.findall(str(card.get("text") or ""))
    ages = [_relative_seconds(str(value)) for value in values]
    return [age for age in ages if age is not None]


def _eligible() -> set[tuple[str, str]]:
    payload = load_json(config.SOCIAL_BROWSER_SCAN_QUEUE, {})
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    return {
        (str(row.get("chain") or "").lower(), str(row.get("tokenCa") or "").lower())
        for row in entries if isinstance(row, dict) and row.get("tokenCa")
    }


def _stable_card_text(text: str) -> str:
    """Remove GMGN/FOMO UI fields that change while one card stays visible."""
    stable: list[str] = []
    skip_value_after: str | None = None
    for raw_line in str(text or "").splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if lowered in {"pnl", "mc"}:
            skip_value_after = lowered
            continue
        if skip_value_after:
            if PURE_PERCENT_RE.fullmatch(line) or MARKET_VALUE_RE.fullmatch(line):
                skip_value_after = None
                continue
            skip_value_after = None
        if RELATIVE_RE.fullmatch(line) or PURE_PERCENT_RE.fullmatch(line):
            continue
        stable.append(lowered)
    return " ".join(stable)


def _card_key(source: str, chain: str, ca: str, text: str, published_at: str = "") -> str:
    # Relative card clocks and live PnL/MC values are deliberately excluded.
    # They previously generated one post ID per second for the same callout.
    # When the platform exposes an absolute publication time, keep it in the
    # identity.  FOMO authors commonly repeat short theses such as "entry" or
    # "still holding" for the same token; dropping a trustworthy timestamp
    # caused every later post with the same body to be suppressed forever by
    # the import ledger.  Callers deliberately pass an empty value when time
    # was inferred from a changing relative clock, preserving the old stable
    # identity in that less reliable case.
    raw = "|".join((
        source, chain, ca.lower(), _stable_card_text(text), published_at,
    ))
    return f"{source}-worker:" + hashlib.sha256(raw.encode()).hexdigest()[:32]


def _scroll_step_budget(source: str, window_seconds: int) -> int:
    """Return a bounded virtual-feed budget for the requested recovery span.

    GMGN's list density changes substantially during busy periods.  The old
    six-seconds-per-step estimate was only just large enough and occasionally
    exhausted the loop one viewport before the requested time boundary.  Keep
    a full-cycle floor and reserve roughly two steps per six seconds while
    retaining a hard cap so a broken list cannot monopolize the worker.
    """
    if source == "gmgn":
        return max(600, min(1200, (window_seconds + 2) // 3))
    if source == "fomo":
        return max(180, min(360, window_seconds // 10))
    return 80


def _market_value(value: str) -> float:
    text = str(value or "").replace(",", "").strip().upper()
    scale = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 1_000_000_000_000}
    suffix = text[-1:] if text[-1:] in scale else ""
    number = text[:-1] if suffix else text
    return float(number) * scale.get(suffix, 1)


def _normalize_fomo_trending_links(rows: list[dict]) -> list[dict]:
    """Normalize the visible FOMO discovery rail without trusting page state."""
    candidates: list[dict] = []
    seen: set[tuple[str, str]] = set()
    ranks: dict[str, int] = {"bsc": 0, "robinhood": 0}
    for row in rows:
        href = str(row.get("href") or "")
        match = FOMO_RE.search(href)
        if not match:
            continue
        chain = "bsc" if match.group(1).lower() == "bnb" else "robinhood"
        token = match.group(2).lower()
        key = (chain, token)
        if key in seen:
            continue
        text = " ".join(str(row.get("text") or "").split())
        market_cap = FOMO_MARKET_CAP_RE.search(text)
        if market_cap is None:
            continue
        try:
            market_cap_usd = _market_value(market_cap.group(1))
        except ValueError:
            continue
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            symbol = text.split(" ", 1)[0]
        ranks[chain] += 1
        seen.add(key)
        candidates.append({
            "chain": chain,
            "token": token,
            "address": token,
            "symbol": symbol,
            "name": symbol,
            "market_cap": str(market_cap_usd),
            "market_cap_usd": str(market_cap_usd),
            "rank": ranks[chain],
            "source_url": href,
        })
    return candidates


class PersistentFeedWorker:
    def __init__(self, *, headless: bool = False):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.chrome_process: subprocess.Popen | None = None
        self.owns_chrome = False
        self.context: BrowserContext | None = None
        self.pages: dict[str, Page] = {}
        self.progress_callback: Callable[[], None] | None = None
        # The authenticated GMGN X request URL is stable across SPA remounts.
        # Keep it at Worker scope so an empty/virtualized panel does not make
        # every continuity checkpoint depend on a visible React XItemCard.
        self.gmgn_x_request_url = ""

    def start(self) -> None:
        PROFILE.mkdir(parents=True, exist_ok=True)
        DIAGNOSTICS.mkdir(parents=True, exist_ok=True)
        self.playwright = sync_playwright().start()
        command = [
            chrome_path(), f"--user-data-dir={PROFILE}", "--remote-debugging-port=9223",
            "--no-first-run", "--no-default-browser-check",
            "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
        ]
        # Chrome does not honor HTTP(S)_PROXY environment variables.  Relying
        # only on the macOS system proxy makes the dedicated browser inherit a
        # transient "offline" network service when the proxy application
        # reconnects.  Pin this worker to the same explicit proxy configured
        # for its LaunchAgent; restarting the dedicated browser can then
        # recreate the network service without waiting for a system-proxy
        # change notification.
        proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        if proxy_url:
            parsed_proxy = urlparse(proxy_url)
            if parsed_proxy.scheme in {"http", "https", "socks4", "socks5"} and parsed_proxy.hostname:
                command.append(f"--proxy-server={proxy_url}")
        if self.headless:
            # The default headless viewport is narrow enough to collapse the
            # desktop Feed rail on FOMO and Pump. Keep the same desktop layout
            # used by the authenticated headed worker.
            command.extend(("--headless=new", "--window-size=1500,900"))
        local_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        endpoint_ready = False
        try:
            with local_opener.open("http://127.0.0.1:9223/json/version", timeout=1):
                endpoint_ready = True
        except Exception:
            pass
        if endpoint_ready and not dedicated_chrome(PROFILE):
            raise RuntimeError("Port 9223 is occupied by another browser; refusing to attach")
        if not endpoint_ready:
            self.chrome_process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.owns_chrome = True
        for _ in range(60):
            try:
                with local_opener.open("http://127.0.0.1:9223/json/version", timeout=1):
                    break
            except Exception:
                time.sleep(0.25)
        else:
            raise RuntimeError("dedicated Chrome did not expose its local debugging endpoint")
        self.browser = self.playwright.chromium.connect_over_cdp("http://127.0.0.1:9223")
        if not self.browser.contexts:
            raise RuntimeError("dedicated Chrome has no persistent browser context")
        self.context = self.browser.contexts[0]

    def stop(self) -> None:
        # Do not call Browser.close() for a CDP attachment: that terminates the
        # dedicated Chrome and invalidates the persistent session for the next
        # LaunchAgent process. Stopping Playwright cleanly detaches the client.
        if self.owns_chrome and self.chrome_process and self.chrome_process.poll() is None:
            self.chrome_process.terminate()
            try:
                self.chrome_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.chrome_process.kill()
        if self.playwright:
            self.playwright.stop()

    def ensure_connected(self) -> None:
        try:
            healthy = bool(
                self.browser is not None
                and self.browser.is_connected()
                and self.context is not None
                and self.context in self.browser.contexts
            )
        except Exception:
            healthy = False
        if healthy:
            return
        self.reconnect()

    def reconnect(self) -> None:
        """Detach stale Playwright handles and reattach to dedicated Chrome.

        Chromium can keep the profile process and debugging port alive after
        invalidating the CDP BrowserContext. `Browser.is_connected()` alone is
        therefore insufficient: old Page objects keep raising TargetClosedError
        forever. Recreating Playwright/CDP handles recovers without discarding
        the authenticated profile.
        """
        self.pages.clear()
        if self.playwright:
            try:
                self.playwright.stop()
            except Exception:
                pass
        self.playwright = self.browser = self.context = None

        # Chrome can briefly expose /json/version while its persistent context
        # is still being recreated. A single connect_over_cdp() at that point
        # fails with Browser.setDownloadBehavior or ECONNREFUSED and used to
        # terminate the whole LaunchAgent loop. Retry the attachment itself;
        # the authenticated profile remains untouched.
        errors: list[str] = []
        for attempt in range(3):
            try:
                self.start()
                return
            except Exception as exc:
                errors.append(str(exc))
                if self.playwright:
                    try:
                        self.playwright.stop()
                    except Exception:
                        pass
                self.playwright = self.browser = self.context = None
                if attempt < 2:
                    time.sleep(attempt + 1)
        # A Chrome process can keep serving /json/version after its default
        # persistent context has become unusable. In that state every fresh
        # CDP client fails with Browser.setDownloadBehavior and ordinary
        # retries can never recover. Restart only the dedicated profile's main
        # process, preserving its authenticated profile data.
        self._restart_dedicated_chrome()
        for attempt in range(5):
            try:
                self.start()
                return
            except Exception as exc:
                errors.append(str(exc))
                if self.playwright:
                    try:
                        self.playwright.stop()
                    except Exception:
                        pass
                self.playwright = self.browser = self.context = None
                if attempt < 4:
                    time.sleep(min(5, attempt + 1))
        raise RuntimeError(
            "dedicated Chrome reattachment failed after profile-process restart: "
            + errors[-1]
        )

    def restart_dedicated_browser(self) -> None:
        """Restart the dedicated Chrome process while preserving its profile.

        CDP can remain connected while Chromium's network service or an app
        renderer is irrecoverably wedged.  Reattaching to that same process is
        then a false recovery: every fresh Page still fails or hangs.  A
        profile-scoped Chrome restart keeps login state but recreates the
        browser network service and renderers.
        """
        self.pages.clear()
        if self.playwright:
            try:
                self.playwright.stop()
            except Exception:
                pass
        self.playwright = self.browser = self.context = None
        self._restart_dedicated_chrome()
        errors: list[str] = []
        for attempt in range(5):
            try:
                self.start()
                return
            except Exception as exc:
                errors.append(str(exc))
                if self.playwright:
                    try:
                        self.playwright.stop()
                    except Exception:
                        pass
                self.playwright = self.browser = self.context = None
                if attempt < 4:
                    time.sleep(min(5, attempt + 1))
        raise RuntimeError(
            "dedicated Chrome restart failed: " + errors[-1]
        )

    @staticmethod
    def _restart_dedicated_chrome() -> None:
        """Restart only Chrome's main process for this worker profile."""
        import psutil
        for process in dedicated_chrome(PROFILE):
            try:
                process.terminate()
                try:
                    process.wait(8)
                except psutil.TimeoutExpired:
                    process.kill()
                    process.wait(8)
            except psutil.NoSuchProcess:
                pass

    @staticmethod
    def _connection_was_closed(error: Exception) -> bool:
        message = str(error).lower()
        return any(marker in message for marker in (
            "targetclosederror",
            "target page, context or browser has been closed",
            "browser has been closed",
            "browser disconnected",
            "connection closed",
        ))

    @staticmethod
    def _transient_navigation_error(error: Exception) -> bool:
        message = str(error).lower()
        return (
            ("timeout" in message and "exceeded" in message)
            or any(marker in message for marker in (
            "err_connection_reset",
            "err_connection_closed",
            "err_network_changed",
            "err_timed_out",
            "err_internet_disconnected",
            "navigation timeout",
            "timeout exceeded",
            ))
        )

    @staticmethod
    def _browser_is_offline(error: Exception) -> bool:
        """Return whether Chromium's network service is explicitly offline."""
        return "err_internet_disconnected" in str(error).lower()

    @staticmethod
    def _rendered_network_error(page: Page) -> str:
        """Return an explicit network error rendered as a browser document.

        Chromium sometimes resolves ``goto`` after replacing the target with
        its error page. Waiting for SPA controls then misclassifies a transport
        outage as a missing FOMO Feed. Only explicit Chromium error tokens or
        an offline navigator state count as evidence here.
        """
        try:
            evidence = page.evaluate(r"""() => {
              const text=`${document.title || ''}\n${document.body?.innerText || ''}`;
              const match=text.match(/ERR_(?:INTERNET_DISCONNECTED|NETWORK_CHANGED|CONNECTION_(?:RESET|CLOSED)|TIMED_OUT)/i);
              if (match) return match[0].toUpperCase();
              if (/\bYou are offline\b/i.test(text) &&
                  /\bneeds a network connection\b/i.test(text)) {
                return 'ERR_INTERNET_DISCONNECTED';
              }
              return navigator.onLine === false ? 'ERR_INTERNET_DISCONNECTED' : '';
            }""")
        except Exception:
            return ""
        return str(evidence or "")

    @staticmethod
    def _retry_backoff(page: Page, delay_ms: int) -> None:
        """Wait between retries even when the renderer was just invalidated.

        Playwright's ``page.wait_for_timeout`` is itself a page operation.  If
        Chromium closes the target between a transient network failure and the
        retry, that wait raises ``TargetClosedError`` and used to abort the
        recovery path before the caller could discard/reconnect the page.  A
        process-level sleep is a safe fallback because it does not touch the
        browser session or mutate the authenticated profile.
        """
        try:
            page.wait_for_timeout(delay_ms)
        except Exception:
            time.sleep(delay_ms / 1000)

    def _goto_with_retry(
        self, page: Page, url: str, *, timeout: int = 60_000, attempts: int = 3,
    ) -> object:
        """Navigate within the dedicated profile with bounded transient retry."""
        errors: list[str] = []
        for attempt in range(attempts):
            try:
                return page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            except Exception as exc:
                errors.append(str(exc))
                if attempt + 1 >= attempts or not self._transient_navigation_error(exc):
                    raise
                LOG.warning(
                    "transient navigation failure for %s (attempt %d/%d): %s",
                    url, attempt + 1, attempts, exc,
                )
                self._retry_backoff(page, min(4_000, 750 * (2 ** attempt)))
        raise RuntimeError(f"navigation failed after {attempts} attempts: {errors[-1]}")

    def page(self, source: str) -> Page:
        # A CDP transport can remain nominally connected after Chromium has
        # invalidated its persistent BrowserContext.  Validate membership on
        # every page acquisition so checkpoint-only traffic cannot spin on a
        # dead context forever between full collection cycles.
        self.ensure_connected()
        assert self.context is not None
        page = self.pages.get(source)
        if page is None or page.is_closed():
            hosts = {
                "fomo": {"fomo.family", "www.fomo.family"},
                "gmgn": {"gmgn.ai", "www.gmgn.ai"},
                "gmgn_x": {"gmgn.ai", "www.gmgn.ai"},
                "pump": {"pump.fun", "www.pump.fun"},
            }[source]
            reserved = set(self.pages.values())
            candidates = [
                candidate for candidate in self.context.pages
                if not candidate.is_closed()
                and candidate not in reserved
                and urlparse(candidate.url).hostname in hosts
            ]
            # LaunchAgent restarts used to create another tab every time. After
            # enough retries Chrome accumulated dozens of heavyweight app
            # tabs, then Page.new_page/navigation began hanging or crashing.
            # This profile is dedicated to the worker, so keep one tab per
            # source and close older duplicates before reuse.
            try:
                page = candidates[-1] if candidates else self.context.new_page()
            except Exception as exc:
                if not self._connection_was_closed(exc):
                    raise
                LOG.warning("%s page acquisition found a stale CDP context; reconnecting", source)
                self.reconnect()
                assert self.context is not None
                page = self.context.new_page()
            for stale in candidates[:-1]:
                try:
                    stale.close(run_before_unload=False)
                except Exception:
                    pass
            self.pages[source] = page
        return page

    def discard_page(self, source: str) -> None:
        """Forget a broken renderer so the next attempt gets a clean target."""
        page = self.pages.pop(source, None)
        if page is not None and not page.is_closed():
            try:
                page.close(run_before_unload=False)
            except Exception:
                # A crashed renderer commonly rejects Page.close(). Removing it
                # from the cache is the important recovery action.
                pass

    @staticmethod
    def _safe_screenshot(page: Page, filename: str) -> None:
        """Save diagnostics without allowing a wedged renderer to mask the fault."""
        try:
            page.screenshot(path=str(DIAGNOSTICS / filename), timeout=5_000)
        except Exception:
            LOG.warning("diagnostic screenshot failed for %s", filename, exc_info=True)

    def _report_progress(self) -> None:
        if self.progress_callback is not None:
            self.progress_callback()

    @staticmethod
    def _fomo_feed_control(page: Page):
        # The public landing page advertises a section named "FEED". It is
        # not the authenticated left rail, and accepting its text as a control
        # made the collector click the marketing page and then report a
        # misleading missing Filter error.
        if not FOMO_RE.search(urlparse(page.url).path):
            return None
        control = PersistentFeedWorker._visible(
            page.get_by_role("button", name=FOMO_FEED_LABEL_RE)
        )
        if control is not None:
            return control
        label = PersistentFeedWorker._visible(
            page.get_by_text(FOMO_FEED_LABEL_RE, exact=True)
        )
        if label is None:
            return None
        button = label.locator("xpath=ancestor::button[1]")
        return PersistentFeedWorker._visible(button)

    @staticmethod
    def _fomo_public_landing(page: Page) -> bool:
        if FOMO_RE.search(urlparse(page.url).path):
            return False
        try:
            return bool(page.evaluate(r"""() => {
              const text=document.body?.innerText || '';
              return /(?:Start trading|开始交易)/i.test(text) &&
                /(?:Login|登录)/i.test(text);
            }"""))
        except Exception:
            return False

    @staticmethod
    def _retry_fomo_offline_page(page: Page) -> bool:
        """Use FOMO's own retry action when its hydrated shell is offline.

        A successful document navigation can still render FOMO's offline
        component. Navigating to another token route leaves that component in
        place, whereas its Retry button rechecks the app's live connection.
        Never click an unrelated button (notably Login) on an unknown page.
        """
        try:
            return bool(page.evaluate(r"""() => {
              const text=document.body?.innerText || '';
              if (!/\bYou are offline\b/i.test(text) ||
                  !/\bneeds a network connection\b/i.test(text)) return false;
              const button=[...document.querySelectorAll('button')].find(el =>
                /^(?:Retry|重试)$/i.test((el.textContent || '').trim()) &&
                el.getBoundingClientRect().width > 0 &&
                el.getBoundingClientRect().height > 0);
              if (!button) return false;
              button.click();
              return true;
            }"""))
        except Exception:
            return False

    def _open_fomo_shell(self, page: Page, eligible: set[tuple[str, str]]):
        """Reuse the hydrated app first, then recover through stable entry routes."""
        parsed = urlparse(page.url)
        if (
            parsed.hostname in {"fomo.family", "www.fomo.family"}
            and re.search(r"/tokens/(?:bnb|robinhood)/0x[a-f0-9]{40}", parsed.path, re.I)
        ):
            control = self._fomo_feed_control(page)
            if control is not None:
                return control

        routes = []
        for chain, ca in sorted(eligible):
            route = "robinhood" if chain == "robinhood" else "bnb"
            routes.append(f"https://fomo.family/tokens/{route}/{ca}")
            if len(routes) >= 2:
                break
        routes.append("https://fomo.family/")
        failures: list[str] = []
        for route in routes:
            try:
                self._goto_with_retry(page, route, timeout=45_000)
                if self._fomo_public_landing(page):
                    failures.append(f"{route}: redirected to public landing page")
                    continue
                deadline = time.monotonic() + 25
                while time.monotonic() < deadline:
                    self._report_progress()
                    control = self._fomo_feed_control(page)
                    if control is not None:
                        return control
                    network_error = self._rendered_network_error(page)
                    if network_error:
                        if network_error == "ERR_INTERNET_DISCONNECTED":
                            # The app's offline component has a bounded,
                            # explicit recovery action. Give it a chance to
                            # recover before trying other routes or rebuilding
                            # Chrome's network service.
                            for _ in range(2):
                                if not self._retry_fomo_offline_page(page):
                                    break
                                page.wait_for_timeout(1_500)
                                self._report_progress()
                                control = self._fomo_feed_control(page)
                                if control is not None:
                                    return control
                                if not self._rendered_network_error(page):
                                    break
                            else:
                                raise RuntimeError(
                                    "FOMO offline page persisted after two Retry attempts"
                                )
                            if not self._rendered_network_error(page):
                                continue
                        raise RuntimeError(
                            f"FOMO navigation rendered transient network error {network_error}"
                        )
                    page.wait_for_timeout(500)
                failures.append(f"{route}: Feed absent after hydration")
            except Exception as exc:
                failures.append(f"{route}: {exc}")
        self._safe_screenshot(page, "fomo-shell-unavailable.png")
        if (
            failures
            and any("public landing page" in failure for failure in failures)
            and all(
                "public landing page" in failure or "Feed absent after hydration" in failure
                for failure in failures
            )
        ):
            raise RuntimeError(
                "FOMO authenticated Feed unavailable: token routes did not expose Feed "
                "and the public landing page was shown; check and manually sign in "
                "to FOMO in the dedicated browser profile"
            )
        raise RuntimeError(
            "FOMO application shell did not expose Feed after base-route recovery: "
            + "; ".join(failures)
        )

    def _browser_json_request(
        self, page: Page, url: str, *, credentials: str = "omit", attempts: int = 3,
    ) -> dict:
        """Fetch JSON through Chromium, with a navigation fallback.

        Both Pump and GMGN occasionally reject an otherwise valid renderer
        ``fetch`` with a synthetic HTTP 0 / ``Failed to fetch`` while direct
        navigation from the same persistent Chromium profile still succeeds.
        Retrying first avoids opening tabs for ordinary transient failures; the
        short-lived navigation page avoids CORS without switching to a separate
        HTTP client or losing the browser session.
        """
        failures: list[str] = []
        for attempt in range(attempts):
            try:
                fetched = page.evaluate(r"""async ({url, credentials}) => {
                  const controller = new AbortController();
                  const timer = setTimeout(() => controller.abort(), 10000);
                  try {
                    const response=await fetch(url, {
                      credentials, cache:'no-store', signal:controller.signal
                    });
                    const text=await response.text();
                    if (!response.ok) return {ok:false,status:response.status,text:text.slice(0,500)};
                    try { return {ok:true,status:response.status,payload:JSON.parse(text)}; }
                    catch (error) { return {ok:false,status:response.status,error:`invalid JSON: ${error}`}; }
                  } catch (error) { return {ok:false,status:0,error:String(error)}; }
                  finally { clearTimeout(timer); }
                }""", {"url": url, "credentials": credentials})
            except Exception as exc:
                fetched = {"ok": False, "status": 0, "error": str(exc)}
            if isinstance(fetched, dict) and fetched.get("ok") is True:
                return fetched
            failures.append(
                f"HTTP {fetched.get('status', 0) if isinstance(fetched, dict) else 0} "
                f"{fetched.get('error') or fetched.get('text') if isinstance(fetched, dict) else fetched}"
            )
            if attempt + 1 < attempts:
                self._retry_backoff(page, 500 * (attempt + 1))

        assert self.context is not None
        navigation_page = self.context.new_page()
        try:
            # The fallback itself is still a network navigation and can hit
            # the same short-lived connection reset as the renderer fetch.
            # Retry it in this same dedicated Chromium profile before failing
            # the platform batch.
            response = self._goto_with_retry(navigation_page, url, timeout=30_000)
            # Read the network response, not Chrome's JSON viewer DOM (which
            # can still be empty at DOMContentLoaded). Preserve HTTP evidence
            # instead of disguising an HTML challenge as a JSON parse error.
            status = response.status if response is not None else 200
            headers = response.headers if response is not None else {}
            body = (response.text() if response is not None else
                    navigation_page.locator("body").inner_text(timeout=15_000))
            if headers.get("cf-mitigated") == "challenge" or (
                status in {401, 403} and "text/html" in headers.get("content-type", "")
            ):
                raise BrowserAccessBlockedError(
                    f"browser access blocked: HTTP {status} at {urlparse(url).hostname}; "
                    "HTML access/challenge page, not JSON; manual review required"
                )
            if status >= 400:
                raise RuntimeError(f"HTTP {status} at {urlparse(url).hostname}")
            payload = json.loads(body)
            return {
                "ok": True,
                "status": status,
                "payload": payload,
                "transport": "chromium-navigation",
                "fetchFailures": failures,
            }
        except BrowserAccessBlockedError:
            raise
        except Exception as exc:
            failures.append(f"Chromium navigation: {exc}")
            raise RuntimeError("browser JSON request failed: " + "; ".join(failures)) from exc
        finally:
            try:
                navigation_page.close(run_before_unload=False)
            except Exception:
                pass

    @staticmethod
    def _visible(locator) -> object | None:
        """Return the first visible match without relying on strict-mode order."""
        for index in range(locator.count()):
            candidate = locator.nth(index)
            if candidate.is_visible():
                return candidate
        return None

    @staticmethod
    def _activate(locator, *, timeout: int = 10_000) -> None:
        """Activate a virtualized tab, tolerating an already-selected rerender."""
        if locator.get_attribute("aria-selected") == "true":
            return
        try:
            locator.click(timeout=timeout, force=True)
        except TimeoutError:
            # React may replace the tab node immediately after the pointer
            # event.  A selected replacement proves that activation happened.
            if locator.get_attribute("aria-selected") != "true":
                raise

    @staticmethod
    def _feed_root(page: Page, source: str):
        """Locate and mark the platform's dedicated feed scroll container.

        All three applications render several independent virtualized panes.
        Mouse coordinates accidentally scrolled whichever pane happened to be
        under the cursor.  Score only real scroll containers and heavily favor
        the source-specific pane geometry and labels.
        """
        details = page.evaluate(r"""source => {
          document.querySelectorAll('[data-tagagent-feed-root]').forEach(el =>
            el.removeAttribute('data-tagagent-feed-root'));
          const visible = el => {
            const r=el.getBoundingClientRect(), s=getComputedStyle(el);
            return r.width >= 220 && r.height >= 220 && r.bottom > 0 && r.right > 0 &&
              s.visibility !== 'hidden' && s.display !== 'none';
          };
          // Pump coin pages expose the global Callouts rail directly. Prefer
          // its stable contract over geometry scoring, which can otherwise
          // select the token-specific Callouts panel lower on the page.
          if (source === 'pump') {
            const rail=document.querySelector('[data-testid="coin-alerts-rail-callouts-scroll"]');
            if (rail && visible(rail)) {
              const r=rail.getBoundingClientRect(), text=(rail.innerText||'').slice(0,180);
              rail.setAttribute('data-tagagent-feed-root', source);
              return {score:1000,left:r.left,top:r.top,width:r.width,height:r.height,
                scrollHeight:rail.scrollHeight,preview:text,
                selector:'coin-alerts-rail-callouts-scroll'};
            }
          }
          // FOMO's Feed lives in the narrow left rail. Select the actual
          // overflow container containing top-level Thesis role=link cards,
          // not the much larger token-detail Thesis table on the right.
          if (source === 'fomo') {
            const fomoVisible=el => {
              const r=el.getBoundingClientRect(), s=getComputedStyle(el);
              return r.width>=220 && r.height>=80 && r.bottom>0 && r.right>0 &&
                s.visibility!=='hidden' && s.display!=='none';
            };
            const rails=[...document.querySelectorAll('div')]
              .filter(el => {
                const r=el.getBoundingClientRect();
                return fomoVisible(el) && r.left < 180 && r.right < 560;
              })
              .filter(el => {
                const overflow=getComputedStyle(el).overflowY;
                return el.scrollHeight > el.clientHeight + 20 ||
                  overflow === 'auto' || overflow === 'scroll';
              })
              .map(el => {
                const text=(el.innerText||'').slice(0,4000);
                const classes=(el.className||'').toString();
                const hasThesis=[...el.querySelectorAll('div[role="link"]')].some(card =>
                  [...card.querySelectorAll('*')].some(node =>
                    node.children.length===0 && /^(?:Thesis|观点)$/i.test((node.textContent||'').trim())));
                const hasThesisLabel=[...el.querySelectorAll('*')].some(node =>
                  node.children.length===0 && /^(?:Thesis|观点)$/i.test((node.textContent||'').trim()));
                // During Feed hydration there may temporarily be no Thesis
                // card.  The rail itself is still identifiable by FOMO's
                // narrow left-column overflow contract and its Feed content.
                const exactOverflow=(/\boverflow-y-auto\b/.test(classes) ||
                  ['auto','scroll'].includes(getComputedStyle(el).overflowY)) &&
                  (/\boverflow-x-hidden\b/.test(classes) || el.getBoundingClientRect().width < 520);
                const feedEvidence=hasThesis || hasThesisLabel || /(?:^|\n)(?:Pinned|置顶|Recap:)(?:\n|$)/i.test(text) ||
                  /(?:^|\n)(?:Closed|Bought|Sold|is up|已平仓|买入|卖出)(?:\n|$)/i.test(text);
                return {el, hasThesis:hasThesis || hasThesisLabel, exactOverflow, feedEvidence};
              })
              .filter(row => row.feedEvidence && (row.hasThesis || row.exactOverflow));
            const rail=rails.sort((a,b) => {
              if (a.hasThesis !== b.hasThesis) return a.hasThesis ? -1 : 1;
              if (a.exactOverflow !== b.exactOverflow) return a.exactOverflow ? -1 : 1;
              const ar=a.el.getBoundingClientRect(), br=b.el.getBoundingClientRect();
              return ar.width-br.width;
            })[0]?.el;
            if (rail) {
              const r=rail.getBoundingClientRect(), text=(rail.innerText||'').slice(0,180);
              rail.setAttribute('data-tagagent-feed-root', source);
              return {score:1000,left:r.left,top:r.top,width:r.width,height:r.height,
                scrollHeight:rail.scrollHeight,preview:text,
                selector:'left-feed-overflow'};
            }
            // Never let the generic fallback select the token-detail Thesis
            // table on the right.  A missing left rail is a transient FOMO
            // hydration failure and must be retried as such.
            return null;
          }
          // GMGN's left Callout panel is a react-virtuoso list. Prefer its
          // explicit scroller contract; generic geometry scoring sometimes
          // selected the neighboring token table or an outer wrapper after a
          // panel remount.
          if (source === 'gmgn') {
            // After dragging the floating tool, GMGN removes its data-id from
            // the Rnd wrapper. The panel component remains the stable anchor.
            const panel=document.querySelector('[data-sentry-component="GlobalCalloutPanel"]') ||
              document.querySelector('[data-id="KEY_GLOBAL_CALLOUT_RND"]');
            const explicit=[...(panel?.querySelectorAll('[data-testid="virtuoso-scroller"]') || [])];
            const inferred=[...(panel?.querySelectorAll('div') || [])].filter(el => {
              const overflow=getComputedStyle(el).overflowY;
              return el.querySelector('[data-sentry-component="CalloutItem"], [data-sentry-component="CalloutFeedCard"]') &&
                (el.scrollHeight > el.clientHeight + 10 || overflow==='auto' || overflow==='scroll');
            });
            const scrollers=[...new Set([...explicit, ...inferred])]
              // A user-docked Callout pane can leave only one card (under
              // 220px) visible. The generic visibility threshold must not
              // reject this authoritative virtual-list scroller.
              .filter(el => {
                const r=el.getBoundingClientRect(), s=getComputedStyle(el);
                return r.width>=220 && r.height>=48 && r.bottom>0 &&
                  r.top<innerHeight && r.right>0 && r.left<innerWidth &&
                  s.visibility!=='hidden' && s.display!=='none';
              })
              .filter(el => el.querySelector('[data-sentry-component="CalloutItem"], [data-sentry-component="CalloutFeedCard"]'));
            const scroller=scrollers.sort((a,b) => {
              const ar=a.getBoundingClientRect(), br=b.getBoundingClientRect();
              return (ar.width*ar.height)-(br.width*br.height);
            })[0];
            if (scroller) {
              const r=scroller.getBoundingClientRect(), text=(scroller.innerText||'').slice(0,180);
              scroller.setAttribute('data-tagagent-feed-root', source);
              return {score:1000,left:r.left,top:r.top,width:r.width,height:r.height,
                scrollHeight:scroller.scrollHeight,preview:text,
                selector:'virtuoso-scroller'};
            }
            return null;
          }
          const candidates=[...document.querySelectorAll('div,section,main,aside')]
            .filter(el => visible(el) && (
              el.scrollHeight > el.clientHeight + 80 ||
              (source === 'pump' && /\bCallouts\b/i.test(el.innerText||''))
            ))
            .map(el => {
              const r=el.getBoundingClientRect(), text=(el.innerText||'').slice(0,12000);
              let score=Math.min(20, (el.scrollHeight-el.clientHeight)/200);
              if (source === 'fomo') {
                if (r.left < 430) score += 80;
                if (r.width < 520) score += 30;
                if (/\bThesis\b/i.test(text)) score += 80;
                if (/Filter\s*\(7\/8\)/i.test(text)) score += 50;
                if (/Market cap|Your positions|Indicators/i.test(text)) score -= 80;
              } else if (source === 'gmgn') {
                if (r.left < 760) score += 45;
                if (r.width < 760) score += 25;
                if (/GMGN全部|GMGN\s*全部/i.test(text)) score += 70;
                if (/Pnl|查看原文|买入/i.test(text)) score += 45;
                if (/推特监控|TG监控|收藏/i.test(text)) score -= 35;
              } else {
                if (r.left > 180 && r.right < innerWidth-120) score += 35;
                if (/\bCallouts\b/i.test(text)) score += 80;
                if (/new callouts|Show more updates|Net PNL/i.test(text)) score += 45;
                if (/Trending|Watchlist|Top PNL/i.test(text)) score -= 35;
              }
              return {el,score,r,text:text.slice(0,180)};
            }).sort((a,b) => b.score-a.score);
          const best=candidates[0];
          if (!best || best.score < 60) return null;
          best.el.setAttribute('data-tagagent-feed-root', source);
          return {score:best.score,left:best.r.left,top:best.r.top,width:best.r.width,
            height:best.r.height,scrollHeight:best.el.scrollHeight,preview:best.text};
        }""", source)
        if not details:
            raise RuntimeError(f"{source.upper()} dedicated Feed scroll container was not found")
        return page.locator(f'[data-tagagent-feed-root="{source}"]'), details

    @staticmethod
    def _freeze(root, source: str) -> list[dict]:
        if source == "fomo":
            # FOMO's Feed cards are clickable divs, not anchors. The previous
            # generic anchor-upward walk therefore attached unrelated chart or
            # ticker links to a large ancestor and silently skipped many real
            # Thesis cards. Parse the actual top-level card contract directly.
            return root.locator(':scope').evaluate_all(r"""roots => {
              const root=roots[0];
              if (!root) return [];
              const labels=[...root.querySelectorAll('*')].filter(node =>
                node.children.length===0 && /^(?:Thesis|观点)$/i.test((node.textContent||'').trim()));
              const cards=[];
              for (const label of labels) {
                const card=label.closest('[role="link"]');
                if (card && root.contains(card)) cards.push(card);
              }
              return [...new Set(cards)].map(card => {
              const leaves=[...card.querySelectorAll('*')].filter(node => node.children.length===0);
              if (!leaves.some(node => /^(?:Thesis|观点)$/i.test((node.textContent||'').trim()))) return null;
              const tokenNode=[...card.querySelectorAll('[role="link"]')]
                .find(node => node !== card && node.getAttribute('translate')==='no' &&
                  (node.textContent||'').trim());
              const tokenImage=[...card.querySelectorAll('img[src]')]
                .map(img => img.src||'')
                .find(src => /(?:^|[\/_])(56|4663)_(0x[a-f0-9]{40})(?:_|\b)/i.test(src));
              const tokenMatch=tokenImage?.match(/(?:^|[\/_])(56|4663)_(0x[a-f0-9]{40})(?:_|\b)/i);
              // Some Mobula logos are opaque hashes (including Solana cards).
              // The token link's own React props carry exact chain+CA, while
              // ticker text and the currently open token page do not.
              let reactToken=null;
              if (tokenNode) {
                const fiberKey=Object.keys(tokenNode).find(key => key.startsWith('__reactFiber$'));
                for (let fiber=fiberKey && tokenNode[fiberKey], depth=0;
                     fiber && depth<12; fiber=fiber.return,depth++) {
                  const props=fiber.memoizedProps||{};
                  if (props.chainId != null && props.tokenAddress) {
                    reactToken={chainId:Number(props.chainId),address:String(props.tokenAddress)};
                    break;
                  }
                }
              }
              if (reactToken && ![56,4663].includes(reactToken.chainId)) return null;
              if (reactToken && !/^0x[a-f0-9]{40}$/i.test(reactToken.address)) return null;
              if (!reactToken && !tokenMatch) return null;
              if (reactToken && tokenMatch &&
                  (reactToken.chainId!==Number(tokenMatch[1]) ||
                   reactToken.address.toLowerCase()!==tokenMatch[2].toLowerCase())) return null;
              const chainId=reactToken?.chainId||Number(tokenMatch[1]);
              const chain=chainId===56 ? 'bsc' : 'robinhood';
              const ca=(reactToken?.address||tokenMatch[2]).toLowerCase();
              const relative=/^(?:now|刚刚|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs|d|day|days|w|week|weeks|秒|分|分钟|时|小时|天|周))$/i;
              const clock=[...card.querySelectorAll('span[title]')]
                .find(node => /\d/.test(node.getAttribute('title')||'') &&
                  relative.test((node.textContent||'').trim()));
              const relativeTime=clock ? (clock.textContent||'').trim() : '';
              let publishedAt='';
              if (clock) {
                const parsed=new Date(clock.getAttribute('title')||'');
                if (!Number.isNaN(parsed.getTime())) publishedAt=parsed.toISOString();
              }
              const lines=(card.innerText||'').split('\n').map(v=>v.trim()).filter(Boolean);
              const username=lines[0]||'platform-user';
              const preferred=card.querySelector(
                '.line-clamp-6,[class*="whitespace-pre-wrap"],[class*="break-words"]');
              const ignored=value => /^(?:Thesis|观点|now|刚刚|\d+\s*(?:s|m|h|秒|分|分钟|时|小时)|[()\u25b2\u25bc]|[+\-]?\d+(?:\.\d+)?%|\$[\d,.]+)$/i.test(value);
              const contentLeaves=[...card.querySelectorAll('div,p,span')]
                .filter(node => node.children.length===0)
                .map(node => (node.innerText||node.textContent||'').trim())
                .filter(value => value && value!==username && !ignored(value));
              const content=(preferred?.innerText||
                contentLeaves.sort((a,b) => b.length-a.length)[0]||'').trim();
              if (!content || !relativeTime) return null;
              const route=chain==='bsc' ? 'bnb' : 'robinhood';
              const clockIndex=lines.findIndex(value => value===relativeTime);
              const tokenSymbol=(tokenNode?.textContent||
                (clockIndex>=0 ? lines[clockIndex+1] : '')||'').trim();
              return {
                href:`https://fomo.family/tokens/${route}/${ca}`,
                text:card.innerText||'', content, username,
                tokenCa:ca, chain, tokenSymbol,
                relativeTimes:[relativeTime], relativeTime, publishedAt,
                tokenMetadataSource:reactToken ? 'token-link-props' : 'defined-token-image',
                html:card.outerHTML.slice(0,5000)
              };
              }).filter(Boolean);
            }""")
        if source == "gmgn":
            return root.locator('[data-sentry-component="CalloutItem"], [data-sentry-component="CalloutFeedCard"]').evaluate_all(r"""els => els.map(card => {
              const token=[...card.querySelectorAll('a[href]')].find(a =>
                /\/(?:bsc|robinhood)\/token\/0x[a-f0-9]{40}(?:[/?#]|$)/i.test(a.href||''));
              if (!token) return null;
              const match=(token.href||'').match(/\/(bsc|robinhood)\/token\/(0x[a-f0-9]{40})/i);
              if (!match) return null;
              const authorLinks=[...card.querySelectorAll('a[href]')];
              // GMGN used to link every native Callout author to
              // /address/0x... .  The current panel links authenticated
              // profiles to /profile/{handle} instead and renders the @handle
              // as a sibling. Requiring the legacy wallet route made every
              // otherwise-valid CalloutItem parse to null, which was later
              // misreported as an empty Feed. Prefer the old address evidence
              // when available, but accept the platform's profile route.
              const addressAuthor=authorLinks.find(a =>
                /\/address\/0x[a-f0-9]{40}(?:[/?#]|$)/i.test(a.href||'') &&
                (a.innerText||'').trim());
              const profileAuthor=authorLinks.find(a =>
                /\/profile\/[^/?#]+\/?(?:[?#].*)?$/i.test(a.href||'') &&
                (a.innerText||'').trim());
              const author=addressAuthor || profileAuthor;
              const contentNode=[...card.querySelectorAll('div')].find(node =>
                /(?:^|\s)whitespace-pre-wrap(?:\s|$)/.test(node.className||'') &&
                /(?:^|\s)break-words(?:\s|$)/.test(node.className||''));
              const content=(contentNode?.innerText||'').trim();
              const relativeTime=[...card.querySelectorAll('*')]
                .filter(node => node.children.length===0)
                .map(node => (node.textContent||'').trim())
                .find(value => /^(?:now|刚刚|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs|秒|分|分钟|时|小时))$/i.test(value))||'';
              if (!author || !content || !relativeTime) return null;
              const xLink=[...card.querySelectorAll('a[href]')]
                .find(a => /https?:\/\/(?:x|twitter)\.com\/[^/\s]+\/status\/\d{15,22}/i.test(a.href||''));
              const addressMatch=(addressAuthor?.href||'').match(/\/address\/(0x[a-f0-9]{40})/i);
              return {
                href:token.href, text:card.innerText||'', content,
                username:(author.innerText||'').trim(), chain:match[1].toLowerCase(),
                tokenCa:match[2].toLowerCase(), relativeTimes:[relativeTime], relativeTime,
                evmAddress:addressMatch ? addressMatch[1].toLowerCase() : '',
                xStatusUrl:xLink ? xLink.href : '', feedCardType:'callout',
                html:card.outerHTML.slice(0,5000)
              };
            }).filter(Boolean)""")
        if source == "pump":
            # Pump's current Callouts UI does not link the mini token card to
            # /coin/{CA}.  The exact EVM CA and stable callout UUID live in the
            # full-card /callouts/{CA}/{UUID} link; the chain is rendered as an
            # image title inside the position card. Parse both the initial
            # callout and every visible threaded update.
            return root.locator('article[data-testid="feed-callout-card"]').evaluate_all(r"""els => els.flatMap(card => {
              const callout=[...card.querySelectorAll('a[href]')].find(a => {
                const href=a.href||'';
                return /\/callouts\/0x[a-f0-9]{40}\/[0-9a-f-]{30,}/i.test(href) ||
                  (/\/coin\/0x[a-f0-9]{40}(?:[/?#]|$)/i.test(href) &&
                   /[?&]highlight_callout=[0-9a-f-]{30,}/i.test(href));
              });
              if (!callout) return [];
              const url=new URL(callout.href, location.href);
              const routeMatch=url.pathname.match(/\/(?:callouts|coin)\/(0x[a-f0-9]{40})(?:\/([0-9a-f-]{30,}))?/i);
              const highlighted=url.searchParams.get('highlight_callout')||'';
              if (!routeMatch) return [];
              const calloutId=(routeMatch[2]||highlighted).toLowerCase();
              if (!/^[0-9a-f-]{30,}$/i.test(calloutId)) return [];
              const logo=card.querySelector('[data-testid="callout-position-mini-card"] img[title]');
              const title=(logo?.getAttribute('title')||'').toLowerCase();
              const chain=title.includes('robinhood') ? 'robinhood' :
                (title.includes('bsc') || title.includes('bnb')) ? 'bsc' : '';
              if (!chain) return [];
              const ca=routeMatch[1].toLowerCase();
              const freeze=(container, kind, suffix) => {
                const clock=container.querySelector('time[datetime]');
                const relativeTime=(clock?.textContent||'').trim();
                const publishedAt=clock?.getAttribute('datetime')||'';
                const contentNode=container.querySelector(
                  kind==='update' ? '[data-testid="callout-update"]' : '[data-testid="callout-note"]');
                const content=(contentNode?.innerText||'').trim();
                const profiles=[...container.querySelectorAll('a[href^="/profile/"],a[href*="pump.fun/profile/"]')];
                const author=profiles.find(a => (a.innerText||'').trim()) || profiles[0];
                const username=(author?.innerText||author?.querySelector('img')?.alt||'').trim();
                const address=(author?.href||'').match(/\/profile\/(0x[a-f0-9]{40})/i)?.[1]||'';
                if (!relativeTime || !publishedAt || !content || !username) return null;
                return {
                  href:callout.href, text:container.innerText||'', content, username,
                  chain, tokenCa:ca, relativeTimes:[relativeTime], relativeTime,
                  publishedAt, evmAddress:address.toLowerCase(),
                  sourcePostId: suffix ? `${calloutId}:${suffix}` : calloutId,
                  feedCardType:kind, html:container.outerHTML.slice(0,5000)
                };
              };
              const initial=freeze(card, 'callout', '');
              const updates=[...card.querySelectorAll('[data-testid="callout-update-row"]')]
                .map((row,index) => freeze(row, 'update',
                  `${row.querySelector('time[datetime]')?.getAttribute('datetime')||index}`))
                .filter(Boolean);
              return [...(initial ? [initial] : []), ...updates];
            })""")
        return root.locator("a[href]").evaluate_all(r"""(els, source) => els.map(a => {
          const href = a.href || '';
          const supported = source === 'pump'
            ? /\/(?:coin|coins|token|tokens)\/0x[a-f0-9]{40}(?:[/?#]|$)/i.test(href)
            : /\/(?:bsc|robinhood)\/token\/?|\/tokens\/(?:bnb|robinhood)\//i.test(href);
          if (!supported) return null;
          // FOMO's ticker strip and token detail pane contain many token links.
          // The token anchor itself does not contain the Thesis label; the old
          // matcher rejected it before walking up to the actual Feed card.
          let card = a;
          for (let i=0; i<7 && card.parentElement; i++, card=card.parentElement) {
            const t=(card.innerText||'').trim();
            if (t.length >= 20 && t.length <= 2400 && /(?:Thesis|Pnl|ago|\bnow\b|\d+\s*(?:s|m|h)\b)/i.test(t)) {
              if (source === 'fomo' && !/(?:^|\n)(?:Thesis|观点)(?:\n|$)/i.test(t)) continue;
              if (source === 'fomo' && /(?:^|\n)(?:Pinned|置顶|Recap:)(?:\n|$)/i.test(t)) continue;
              const relativeTimes = [...t.matchAll(/(?<!\w)(now|刚刚|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs|d|day|days|w|week|weeks|秒|分|分钟|时|小时|天|周))(?!\w)/ig)]
                .map(m => m[1]);
              return {href, text:t, relativeTimes, html:card.outerHTML.slice(0,5000)};
            }
          }
          return null;
        }).filter(Boolean)""", source)

    @staticmethod
    def _visible_feed_ages(root) -> list[int]:
        """Read clocks from all feed event types, excluding pinned summaries.

        FOMO can have long runs of position/PnL events between Thesis cards. We
        still need those clocks to prove that scrolling crossed the requested
        time boundary, while only Thesis cards are eligible for import.
        """
        values = root.evaluate(r"""el => [...el.querySelectorAll('*')]
          .filter(node => node.children.length === 0)
          .filter(node => /^(?:now|刚刚|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs|d|day|days|w|week|weeks|秒|分|分钟|时|小时|天|周)(?:\s+ago)?)$/i
            .test((node.textContent||'').trim()))
          .filter(node => {
            let parent=node;
            for (let i=0; i<6 && parent; i++, parent=parent.parentElement) {
              const text=(parent.innerText||'').slice(0,1200);
              if (/(?:^|\n)(?:Pinned|置顶|Recap:)(?:\n|$)/i.test(text)) return false;
            }
            return true;
          })
          .map(node => (node.textContent||'').trim())""")
        ages = [_relative_seconds(str(value)) for value in values]
        return [age for age in ages if age is not None]

    @staticmethod
    def _fomo_thesis_evidence(root) -> dict:
        """Separate unsupported-chain Thesis from relevant or unresolved cards.

        A mixed-chain Feed can show only Solana Thesis in a time window. Raw
        Thesis labels alone are therefore not evidence of a missed BSC/RH
        card. Unknown chain evidence remains conservative and prevents an
        empty-window claim.
        """
        return root.evaluate(r"""el => {
          const labels=[...el.querySelectorAll('*')].filter(node =>
            node.children.length===0 && /^(?:Thesis|观点)$/i.test((node.textContent||'').trim()));
          const cards=[...new Set(labels.map(node => node.closest('[role="link"]'))
            .filter(card => card && el.contains(card)))];
          let relevant=0, unsupported=0, unresolved=labels.length-cards.length;
          for (const card of cards) {
            const tokenNode=[...card.querySelectorAll('[role="link"]')]
              .find(node => node!==card && node.getAttribute('translate')==='no' &&
                (node.textContent||'').trim());
            let reactToken=null;
            if (tokenNode) {
              const key=Object.keys(tokenNode).find(value => value.startsWith('__reactFiber$'));
              for (let fiber=key && tokenNode[key], depth=0;
                   fiber && depth<12; fiber=fiber.return,depth++) {
                const props=fiber.memoizedProps||{};
                if (props.chainId!=null && props.tokenAddress) {
                  reactToken={chainId:Number(props.chainId),address:String(props.tokenAddress)};
                  break;
                }
              }
            }
            const logos=[...card.querySelectorAll('img[src]')].map(img => img.src||'');
            const supportedLogo=logos.find(src =>
              /(?:^|[\/_])(56|4663)_(0x[a-f0-9]{40})(?:_|\b)/i.test(src));
            const logoMatch=supportedLogo?.match(/(?:^|[\/_])(56|4663)_(0x[a-f0-9]{40})(?:_|\b)/i);
            if (reactToken) {
              if ([56,4663].includes(reactToken.chainId) &&
                  /^0x[a-f0-9]{40}$/i.test(reactToken.address) &&
                  (!logoMatch || (reactToken.chainId===Number(logoMatch[1]) &&
                    reactToken.address.toLowerCase()===logoMatch[2].toLowerCase()))) relevant++;
              else if (![56,4663].includes(reactToken.chainId)) unsupported++;
              else unresolved++;
            } else if (logoMatch) relevant++;
            else if (logos.some(src => /(?:^|[\/_])solana_solana_/i.test(src))) unsupported++;
            else unresolved++;
          }
          return {raw:labels.length,relevant,unsupported,unresolved};
        }""")

    @staticmethod
    def _fomo_empty_window_proven(cards: list[dict], boundary: bool,
                                  details: dict, window_seconds: int) -> bool:
        return bool(
            not cards and boundary
            and details.get("boundaryProof") == "published-time-boundary"
            and int(details.get("observedFeedClocks") or 0) > 0
            and int(details.get("observedThesisLabels") or 0) == 0
            and int(details.get("oldestFeedClockSeconds") or 0) >= window_seconds
        )

    def _scroll_freeze(self, page: Page, *, source: str, window_seconds: int) -> tuple[list[dict], bool, dict]:
        root, root_details = self._feed_root(page, source)
        # SPA navigation may preserve an inner pane's previous scrollTop.
        # Every incremental scan must begin at the newest card.
        root.evaluate("el => { el.scrollTop = 0; el.dispatchEvent(new Event('scroll')); }")
        page.wait_for_timeout(250)
        frozen: dict[str, dict] = {}
        boundary = False
        end_of_feed = False
        unchanged = 0
        bottom_idle = 0
        # A busy GMGN hour can exceed 200 callouts and 40,000px. Its previous
        # 80 half-viewport steps routinely stopped around 30-40 minutes.
        # Recovery windows can contain hundreds of GMGN cards.  Use the window
        # size to budget enough virtual-list advances instead of silently
        # stopping at the old fixed 180-step cap (observed at 35-50 minutes).
        max_steps = _scroll_step_budget(source, window_seconds)
        steps_used = 0
        fomo_clock_count = 0
        fomo_thesis_labels = 0
        fomo_raw_thesis_labels = 0
        fomo_unsupported_thesis_labels = 0
        fomo_oldest_age = 0
        for _ in range(max_steps):
            steps_used += 1
            self._report_progress()
            before = len(frozen)
            try:
                cards = self._freeze(root, source)
            except TimeoutError:
                # React frequently replaces the virtual list node and removes
                # our marker. Re-resolve immediately instead of waiting on a
                # stale Locator for Playwright's full default timeout.
                root, root_details = self._feed_root(page, source)
                cards = self._freeze(root, source)
            for card in cards:
                key = card["href"] + "|" + _stable_card_text(card["text"])
                frozen.setdefault(key, card)
                if any(age >= window_seconds for age in _card_ages(card)):
                    boundary = True
            # FOMO interleaves non-Thesis activity, so all event clocks are
            # useful for proving its timeline boundary. GMGN CalloutItem also
            # renders a token-age clock (for example "27d"); treating that as
            # the callout timestamp stopped every scan in the first viewport.
            if source == "fomo":
                try:
                    ages = self._visible_feed_ages(root)
                    fomo_clock_count += len(ages)
                    thesis = self._fomo_thesis_evidence(root)
                    fomo_thesis_labels += thesis["relevant"] + thesis["unresolved"]
                    fomo_raw_thesis_labels += thesis["raw"]
                    fomo_unsupported_thesis_labels += thesis["unsupported"]
                    if ages:
                        fomo_oldest_age = max(fomo_oldest_age, *ages)
                    if any(age >= window_seconds for age in ages):
                        boundary = True
                except TimeoutError:
                    root, root_details = self._feed_root(page, source)
            if boundary:
                break
            unchanged = unchanged + 1 if len(frozen) == before else 0
            try:
                moved = root.evaluate("""(el, source) => {
                  const before=el.scrollTop;
                  const distance=(source==='gmgn' || source==='fomo')
                    ? Math.max(900, el.clientHeight*.9)
                    : Math.max(500, el.clientHeight*.75);
                  el.scrollBy(0, distance);
                  el.dispatchEvent(new Event('scroll', {bubbles:true}));
                  return el.scrollTop !== before;
                }""", source)
            except TimeoutError:
                root, root_details = self._feed_root(page, source)
                moved = True
            if moved:
                bottom_idle = 0
                page.wait_for_timeout(300 if source in {"gmgn", "fomo"} else 500)
            else:
                # Infinite/virtual feeds often append only after an async fetch
                # at the bottom. Give them several chances before declaring the
                # boundary unresolved.
                bottom_idle += 1
                page.wait_for_timeout(1250)
            # Do not stop just because no eligible Thesis appeared for several
            # viewports. Other FOMO event types can fill long stretches of the
            # ordered timeline. Only a genuinely stationary bottom ends scan.
            if bottom_idle >= (5 if source in {"gmgn", "fomo"} else 3):
                at_bottom = root.evaluate(
                    "el => el.scrollTop + el.clientHeight >= el.scrollHeight - 2"
                )
                # A stable physical end is equivalent to crossing the time
                # boundary: the platform has explicitly supplied no older
                # cards. GMGN can have a busy but finite feed whose oldest
                # available event is still newer than a one-hour recovery
                # cutoff (for example, 244 cards ending at 51 minutes).
                if source in {"gmgn", "fomo"} and at_bottom and unchanged >= 3:
                    boundary = True
                    end_of_feed = True
                break
        try:
            final_scroll = root.evaluate("""el => ({
              scrollTop:el.scrollTop,scrollHeight:el.scrollHeight,
              clientHeight:el.clientHeight,
              atBottom:el.scrollTop + el.clientHeight >= el.scrollHeight - 2
            })""")
        except TimeoutError:
            final_scroll = {}
        root_details = {
            **root_details,
            "finalScroll": final_scroll,
            "scrollStepsUsed": steps_used,
            "scrollStepBudget": max_steps,
            "boundaryProof": (
                "stationary-end-of-feed" if end_of_feed
                else "published-time-boundary" if boundary
                else "unresolved"
            ),
        }
        if source == "fomo":
            root_details.update({
                "observedFeedClocks": fomo_clock_count,
                "observedThesisLabels": fomo_thesis_labels,
                "observedRawThesisLabels": fomo_raw_thesis_labels,
                "observedUnsupportedThesisLabels": fomo_unsupported_thesis_labels,
                "oldestFeedClockSeconds": fomo_oldest_age,
            })
        return list(frozen.values()), boundary, root_details

    def _scroll_freeze_pump_coin(
        self, page: Page, *, window_seconds: int
    ) -> tuple[list[dict], bool, dict, int]:
        """Freeze Pump's ranked cross-chain Callouts rail on a coin page."""
        root, root_details = self._feed_root(page, "pump")
        root.evaluate("el => { el.scrollTop=0; el.dispatchEvent(new Event('scroll')); }")
        page.wait_for_timeout(300)
        frozen: dict[str, dict] = {}
        all_seen: set[str] = set()
        old_seen = False
        stale_chunks = 0
        bottom_idle = 0
        boundary = False
        for step in range(80):
            try:
                cards = self._freeze(root, "pump")
                article_snapshots = root.locator(
                    'article[data-testid="feed-callout-card"]'
                ).evaluate_all(r"""els => els.map((el,index) => {
                  const link=[...el.querySelectorAll('a[href]')].find(a =>
                    /[?&]highlight_callout=[0-9a-f-]{30,}/i.test(a.href||'') ||
                    /\/callouts\/.+\/[0-9a-f-]{30,}/i.test(a.href||''));
                  const href=link?.href||'';
                  const highlighted=href.match(/[?&]highlight_callout=([0-9a-f-]{30,})/i)?.[1];
                  const routed=href.match(/\/callouts\/.+\/([0-9a-f-]{30,})/i)?.[1];
                  const clocks=[...el.querySelectorAll('time')]
                    .map(node => (node.textContent||'').trim())
                    .filter(value => /^(?:now|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs|d|day|days|w|week|weeks)(?:\s+ago)?)$/i.test(value));
                  return {key:(highlighted||routed||href||`${index}:${(el.innerText||'').slice(0,160)}`).toLowerCase(),clocks};
                })""")
            except TimeoutError:
                root, root_details = self._feed_root(page, "pump")
                continue
            new_ages: list[int] = []
            for snapshot in article_snapshots:
                key = str(snapshot.get("key") or "")
                if not key or key in all_seen:
                    continue
                all_seen.add(key)
                for clock in snapshot.get("clocks") or []:
                    age = _relative_seconds(str(clock))
                    if age is not None:
                        new_ages.append(age)
            for card in cards:
                key = str(card.get("sourcePostId") or "") + "|" + str(
                    card.get("publishedAt") or ""
                )
                frozen.setdefault(key, card)
            has_recent = any(age <= window_seconds for age in new_ages)
            has_old = any(age >= window_seconds for age in new_ages)
            old_seen = old_seen or has_old
            # The coin-page rail is backed by one
            # /home-feed?pageSize=150&chain=all response. Once every slot from
            # that finite browser-loaded page has been observed, there is no
            # later card in this snapshot to miss even though ranking mixes
            # recent and old callouts.
            if len(all_seen) >= 150 and old_seen:
                boundary = True
                break
            # The rail is ranked, not chronological: one old card can precede
            # fresh cards. Require three fully stale viewport chunks.
            if step > 0 and new_ages and has_old and not has_recent:
                stale_chunks += 1
            else:
                stale_chunks = 0
            if stale_chunks >= 3:
                boundary = True
                break
            try:
                moved = root.evaluate("""el => {
                  const before=el.scrollTop;
                  el.scrollBy(0, Math.max(420, el.clientHeight*.8));
                  el.dispatchEvent(new Event('scroll', {bubbles:true}));
                  return el.scrollTop !== before;
                }""")
            except TimeoutError:
                root, root_details = self._feed_root(page, "pump")
                moved = True
            if moved:
                bottom_idle = 0
                page.wait_for_timeout(650)
            else:
                bottom_idle += 1
                page.wait_for_timeout(1200)
                if bottom_idle >= 3:
                    boundary = old_seen
                    break
        return list(frozen.values()), boundary, root_details, len(all_seen)

    def _records(self, source: str, cards: list[dict], *, scanned_at: datetime, eligible: set[tuple[str, str]]) -> tuple[list[dict], int, int]:
        records = []
        seen = set()
        unresolved_seen = set()
        for card in cards:
            href, text = str(card.get("href") or ""), str(card.get("text") or "").strip()
            match = FOMO_RE.search(href) if source == "fomo" else TOKEN_RE.search(href)
            if source == "pump":
                match = PUMP_CALLOUT_RE.search(href) or PUMP_RE.search(href)
            if not match:
                continue
            if source == "pump":
                ca = str(card.get("tokenCa") or match.group(1)).lower()
                chain = str(card.get("chain") or "").lower()
                if not chain:
                    chains = {candidate for candidate, token_ca in eligible if token_ca == ca}
                    if len(chains) != 1:
                        continue
                    chain = next(iter(chains))
            else:
                route, ca = match.group(1).lower(), match.group(2).lower()
                chain = "bsc" if route == "bnb" else route
            if (chain, ca) not in eligible:
                continue
            content = str(card.get("content") or text).strip()
            username = str(card.get("username") or "platform-user").lstrip("@")
            # Distinct platform posts may intentionally repeat the same short
            # thesis.  Keep a trustworthy absolute card time in the in-batch
            # dedupe key; relative-only cards remain body-deduped because their
            # reconstructed time drifts between scans.
            published_value = str(card.get("publishedAt") or "")
            ages = _card_ages(card)
            published = None
            try:
                published = datetime.fromisoformat(published_value.replace("Z", "+00:00"))
                if published.tzinfo is None:
                    published = published.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
            x_match = X_STATUS_RE.search(content)
            if not x_match:
                x_match = X_STATUS_RE.search(str(card.get("xStatusUrl") or ""))
            tweet_id = x_match.group(1) if x_match else ""
            trade_id = parse_qs(urlparse(href).query).get("tradeId", [""])[0]
            # Only platform-provided absolute time is safe for identity.  A
            # timestamp reconstructed from "5m" changes with every scan and
            # would turn one card into a stream of duplicates.
            identity_time = published.isoformat() if published is not None else ""
            post_id = str(card.get("sourcePostId") or "") or trade_id or tweet_id or _card_key(
                source, chain, ca, text, identity_time
            )
            key = (chain, ca, post_id)
            if key in seen:
                continue
            if not ages:
                unresolved_seen.add(key)
                continue
            unresolved_seen.discard(key)
            seen.add(key)
            if published is None:
                # A grouped Pump card can contain older updates. Treat the
                # newest visible update as the frozen content publication time.
                published = scanned_at - timedelta(seconds=min(ages))
            if username == "platform-user":
                author = re.search(r"(?:^|\n)(@?[\w.\-\u0080-\uffff]{2,40})", text)
                if author:
                    username = author.group(1).lstrip("@")
            record = {
                "chain": chain, "tokenCa": ca, "kind": "x" if x_match else "native",
                "username": username, "content": content,
                "publishedAt": published.isoformat(), "sourcePostId": post_id,
                "sourceUrl": x_match.group(0) if x_match else href,
            }
            if source == "fomo":
                record["feedCardType"] = "thesis"
                record["tokenMetadataSource"] = str(
                    card.get("tokenMetadataSource") or "token-page"
                )
            if source == "gmgn":
                record["feedCardType"] = str(card.get("feedCardType") or "callout")
            if source == "pump":
                record["feedCardType"] = str(card.get("feedCardType") or "callout")
            evm_address = str(card.get("evmAddress") or "")
            if re.fullmatch(r"0x[a-f0-9]{40}", evm_address, re.I):
                record["evmAddress"] = evm_address.lower()
            if tweet_id:
                record["originalXTweetId"] = tweet_id
            records.append(record)
        # The counters describe unique source posts after stable-ID
        # deduplication, not the number of DOM representations encountered.
        matched = len(seen | unresolved_seen)
        unresolved = len(unresolved_seen - seen)
        return records, matched, unresolved

    @staticmethod
    def _capture_fomo_trending(page: Page) -> dict:
        # React remounts the left rail while a tab is selected. Holding a
        # Playwright Locator across that remount made this optional snapshot
        # wait for the global 30s timeout and delayed the actual Thesis scan.
        # Locate and click the visible controls atomically in the page instead.
        clicked = page.evaluate(r"""() => {
          const visible = el => {
            const r=el.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
          };
          const click = labels => {
            const el=[...document.querySelectorAll('button,[role="button"]')]
              .find(node => visible(node) && labels.includes((node.textContent || '').trim()));
            if (!el) return false;
            el.click();
            return true;
          };
          click(['Tokens', '代币']);
          return click(['Trending', '热门', '趋势']);
        }""")
        if not clicked:
            # Trending is a market-discovery supplement, not part of the
            # Thesis ingestion contract.  FOMO no longer exposes this control
            # in every account/layout, so absence is an expected capability
            # result rather than a collection failure.
            return {
                "status": "unavailable",
                "reason": "filter-control-not-found",
            }
        page.wait_for_timeout(1200)
        rows = page.evaluate(r"""() => {
          const visible = el => {
            const style = getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden'
              && rect.width > 0 && rect.height > 0;
          };
          const button = [...document.querySelectorAll('button')].find(el =>
            visible(el) && ['Trending', '热门', '趋势'].includes((el.textContent || '').trim()));
          if (!button) return [];
          let root = button.parentElement;
          while (root && [...root.querySelectorAll('a[href*="/tokens/"]')].filter(visible).length < 3) {
            root = root.parentElement;
          }
          if (!root) return [];
          return [...root.querySelectorAll('a[href*="/tokens/"]')]
            .filter(visible)
            .map(el => ({
              href: el.href,
              text: el.innerText || el.textContent || '',
              symbol: (el.innerText || '').split('\n').map(v => v.trim()).filter(Boolean)[0] || '',
            }));
        }""")
        candidates = _normalize_fomo_trending_links(rows if isinstance(rows, list) else [])
        if not candidates:
            return {
                "status": "unavailable",
                "reason": "zero-supported-chain-candidates",
            }
        snapshot = {
            "schemaVersion": 1,
            "complete": True,
            "scannedAt": _utcnow().isoformat(),
            "source": "fomo:trending",
            "pageUrl": page.url,
            "candidates": candidates,
        }
        save_json(config.TRENDING_IMPORT_FOMO_SNAPSHOT, snapshot)
        return snapshot

    def collect_fomo(self, window_seconds: int, eligible: set[tuple[str, str]]) -> dict:
        page = self.page("fomo")
        try:
            feed = self._open_fomo_shell(page, eligible)
        except RuntimeError:
            # Trending is independent of the Thesis Feed.  A missing Feed
            # control must not leave the market-discovery fallback stale.
            try:
                self._capture_fomo_trending(page)
            except Exception as exc:
                LOG.warning("FOMO Trending fallback capture failed: %s", exc)
            raise
        # FOMO remounts the left navigation while the token shell hydrates.
        # Playwright's normal click waits for the node to remain stable and can
        # therefore time out even though the visible button is actionable.
        # Resolve the visible node above, then dispatch the click in-page so a
        # harmless React remount cannot abort the whole platform scan.
        try:
            # Use the same control that shell discovery accepted. The visible
            # label may be nested inside a button or tab, so searching only
            # top-level button text can falsely report an activation failure.
            feed.evaluate("""el => {
              (el.closest('button,[role="button"],[role="tab"],a') || el).click();
            }""", timeout=5_000)
        except Exception as exc:
            # A React remount can detach the first locator between discovery
            # and activation. Re-resolve once without navigating away.
            feed = self._fomo_feed_control(page)
            if feed is None:
                raise RuntimeError("FOMO Feed control could not be activated") from exc
            try:
                feed.evaluate("""el => {
                  (el.closest('button,[role="button"],[role="tab"],a') || el).click();
                }""", timeout=5_000)
            except Exception as retry_exc:
                raise RuntimeError("FOMO Feed control could not be activated") from retry_exc
        page.wait_for_timeout(1000)
        # Feed preferences are persisted in the dedicated profile, but enforce
        # the contract every cycle in case the site resets them.
        filter_button = None
        filter_deadline = time.monotonic() + 10
        while filter_button is None and time.monotonic() < filter_deadline:
            self._report_progress()
            filter_button = self._visible(page.get_by_text(FOMO_FILTER_LABEL_RE))
            if filter_button is not None:
                break
            network_error = self._rendered_network_error(page)
            if network_error:
                raise RuntimeError(f"FOMO went offline after Feed activation: {network_error}")
            page.wait_for_timeout(250)
        if filter_button is None:
            self._safe_screenshot(page, "fomo-filter-unavailable.png")
            raise RuntimeError("FOMO Feed filter control was not found")
        filter_summary = filter_button.locator("xpath=ancestor::button[1]").inner_text()
        trades_disabled = "7/8" in filter_summary
        if not trades_disabled:
            filter_button.click(timeout=10_000)
            # FOMO keeps hidden copies of menu labels in the DOM. `.first`
            # occasionally selected one of those and timed out even though the
            # visible menu was ready. Poll for the actually visible option.
            trades = None
            deadline = time.monotonic() + 10
            while trades is None and time.monotonic() < deadline:
                trades = self._visible(page.get_by_text(FOMO_TRADES_LABEL_RE, exact=True))
                if trades is None:
                    page.wait_for_timeout(250)
            if trades is None:
                raise RuntimeError("FOMO Feed filter opened but visible Trades option did not hydrate")
            trades_button = trades.locator("xpath=ancestor::button[1]")
            trades_checked = "text-accent-primary" in (
                trades_button.locator("svg").get_attribute("class") or ""
            )
            if trades_checked:
                trades_button.click(timeout=10_000)
                page.wait_for_timeout(500)
            # The toggle may rerender both the menu and Filter control, so
            # resolve the visible control again before closing/confirming.
            filter_button = self._visible(
                page.get_by_text(FOMO_FILTER_LABEL_RE)
            )
            if filter_button is None:
                raise RuntimeError("FOMO Feed filter control disappeared after updating Trades")
            if self._visible(page.get_by_text(FOMO_TRADES_LABEL_RE, exact=True)) is not None:
                filter_button.click(timeout=10_000)
            filter_summary = filter_button.locator("xpath=ancestor::button[1]").inner_text()
            trades_disabled = "7/8" in filter_summary
        if not trades_disabled:
            raise RuntimeError("FOMO Feed filter did not confirm Trades disabled (expected 7/8)")
        # A previous scan leaves the persistent rail deep in history. FOMO
        # buffers incoming cards behind this floating control; resetting
        # scrollTop alone does not commit them to the list. Click it first so
        # the frozen snapshot is anchored at the newest available activity.
        # FOMO changed the visible label from "Show new activity" to
        # "New activity" without keeping an accessible aria-label. The old
        # exact-role selector silently left fresh Thesis cards staged above
        # the currently mounted virtual list.
        new_activity = self._visible(page.get_by_role(
            "button", name=FOMO_NEW_ACTIVITY_LABEL_RE,
        ))
        if new_activity is None:
            label = self._visible(page.get_by_text(
                FOMO_NEW_ACTIVITY_LABEL_RE, exact=True,
            ))
            if label is not None:
                new_activity = label.locator("xpath=ancestor::button[1]")
        new_activity_clicked = new_activity is not None
        if new_activity is not None:
            new_activity.evaluate("el => el.click()")
            page.wait_for_timeout(1200)
        # The page shell and Filter can be ready before the left Feed rail has
        # mounted.  Waiting here prevents _scroll_freeze from observing a
        # transient empty frame (or, historically, selecting the right-hand
        # token Thesis table through the generic fallback).
        root_ready = False
        root_deadline = time.monotonic() + 15
        while time.monotonic() < root_deadline:
            try:
                root, details = self._feed_root(page, "fomo")
                root_ready = (
                    details.get("selector") == "left-feed-overflow"
                    and root.evaluate("el => el.isConnected && el.clientHeight > 0")
                )
            except RuntimeError:
                root_ready = False
            if root_ready:
                break
            page.wait_for_timeout(500)
        if not root_ready:
            try:
                self._capture_fomo_trending(page)
            except Exception as exc:
                LOG.warning("FOMO Trending fallback capture failed without Feed rail: %s", exc)
            raise RuntimeError("FOMO left Feed rail did not hydrate within 15 seconds")
        scanned_at = _utcnow()
        cards, boundary, root_details = self._scroll_freeze(
            page, source="fomo", window_seconds=window_seconds
        )
        no_results = self._fomo_empty_window_proven(
            cards, boundary, root_details, window_seconds
        )
        if not cards and not no_results:
            self._safe_screenshot(page, "fomo-zero-thesis-unproven.png")
            raise RuntimeError(
                "FOMO yielded zero parsed Thesis cards without proof of an empty "
                f"time window: {root_details}"
            )
        records, matched, unresolved = self._records("fomo", cards, scanned_at=scanned_at, eligible=eligible)
        if not boundary:
            self._safe_screenshot(page, "fomo-incomplete.png")
        batch = self._batch("fomo", scanned_at, page.url, records, cards, matched, unresolved, boundary, window_seconds, {
            "feedSelected": True, "tradesDisabled": trades_disabled,
            "filterCount": "7/8", "bscScanned": True, "robinhoodScanned": True,
            "newActivityChecked": True, "newActivityClicked": new_activity_clicked,
            "pinnedOfficialSkipped": True,
            "feedContainerLocated": True, "feedContainer": root_details,
            "noResults": no_results,
        })
        # The Thesis feed is the authoritative Social Agent source.  Capture
        # the optional Trending token snapshot only after the complete feed is
        # frozen and converted, so a removed/renamed Trending control cannot
        # delay, remount, or invalidate Thesis ingestion.
        try:
            trending = self._capture_fomo_trending(page)
            batch["verification"]["trendingSnapshot"] = trending.get("status", "captured")
            if trending.get("reason"):
                batch["verification"]["trendingSnapshotReason"] = trending["reason"]
        except Exception as exc:
            batch["verification"]["trendingSnapshot"] = "error"
            batch["verification"]["trendingSnapshotError"] = str(exc)[:300]
            LOG.warning("Optional FOMO Trending snapshot failed after Thesis capture: %s", exc)
        return batch

    @staticmethod
    def _configure_gmgn_chains(page: Page) -> dict:
        details = page.evaluate(r"""() => {
          document.querySelectorAll('[data-tagagent-gmgn-chain-trigger]').forEach(el =>
            el.removeAttribute('data-tagagent-gmgn-chain-trigger'));
          const root=document.querySelector('[data-sentry-component="GlobalCalloutPanel"]') ||
            document.querySelector('[data-id="KEY_GLOBAL_CALLOUT_RND"]');
          // The Callout pane is draggable and may be docked below the chart.
          // Its chain control is not guaranteed to be near the top-left of
          // the viewport. Scope to the pane instead of fixed coordinates;
          // the page header and token tables have similar controls.
          const legacy=root ? [...root.querySelectorAll('[data-testid="chain-multi-select-trigger"]')]
            .map(el => el.closest('button[aria-haspopup="dialog"]')).filter(Boolean) : [];
          const modern=root ? [...root.querySelectorAll('button[aria-haspopup="dialog"]')]
            .filter(el => el.querySelector(
              '[data-icon="IconBsc20pxS"], [data-icon="IconRobinhood16pxS"], [data-icon="IconSolchain16pxS"]'
            )) : [];
          const target=[...legacy, ...modern].find(el => {
            const r=el.getBoundingClientRect();
            const style=getComputedStyle(el);
            return r.width>0 && r.height>0 && r.bottom>0 && r.top<innerHeight &&
              r.right>0 && r.left<innerWidth && style.visibility!=='hidden' &&
              style.display!=='none' && !el.closest('[aria-hidden="true"]');
          });
          if (!target) return null;
          target.setAttribute('data-tagagent-gmgn-chain-trigger','true');
          return {left:target.getBoundingClientRect().left,top:target.getBoundingClientRect().top,
            variant: target.querySelector('[data-testid="chain-multi-select-trigger"]') ? 'legacy' : 'fusion'};
        }""")
        if not details:
            raise RuntimeError("GMGN left Callout chain filter trigger was not found")
        # The trigger sits inside GMGN's floating panel.  Its React wrapper can
        # be geometrically visible while an animating sibling still intercepts
        # Playwright's actionability checks, which previously stalled a whole
        # cycle for eight seconds and left only an incomplete GMGN batch.  The
        # marker above scopes the target precisely, so dispatch the same DOM
        # click used by the working chain toggles below.  Re-query inside the
        # page to tolerate a hydration remount between discovery and click.
        clicked = page.evaluate(r"""() => {
          const target=document.querySelector('[data-tagagent-gmgn-chain-trigger="true"]');
          if (!target || !target.isConnected) return false;
          target.click();
          return true;
        }""")
        if not clicked:
            raise RuntimeError("GMGN chain filter trigger remounted before activation")
        desired = {"bsc", "robinhood"}
        if details["variant"] == "legacy":
            # GMGN can leave the previous Radix popover mounted while opening
            # a new one.  Both nodes may satisfy Playwright's ``visible``
            # predicate, so a global locator enters strict-mode and aborts the
            # whole GMGN cycle.  Mark only the topmost, hit-testable popover
            # that owns chain options and scope every subsequent operation to
            # it.  The DOM-order fallback covers browsers where hit testing is
            # temporarily unavailable during the opening animation.
            panel_details = page.evaluate(r"""() => {
              document.querySelectorAll('[data-tagagent-gmgn-chain-panel]').forEach(el =>
                el.removeAttribute('data-tagagent-gmgn-chain-panel'));
              const panels=[...document.querySelectorAll(
                '[data-testid="chain-multi-select-panel"]'
              )].filter(el => {
                const r=el.getBoundingClientRect();
                const style=getComputedStyle(el);
                return r.width>0 && r.height>0 && style.visibility!=='hidden' &&
                  style.display!=='none' && !el.closest('[aria-hidden="true"]') &&
                  el.querySelector('input[data-testid^="chain-multi-select-option-"]');
              });
              const hitTestable=panels.filter(panel => {
                const r=panel.getBoundingClientRect();
                const x=Math.max(0, Math.min(innerWidth-1, r.left+r.width/2));
                const y=Math.max(0, Math.min(innerHeight-1, r.top+Math.min(r.height/2, 40)));
                return document.elementsFromPoint(x,y).some(el => panel.contains(el));
              });
              const target=(hitTestable.length ? hitTestable : panels).at(-1);
              if (!target) return null;
              target.setAttribute('data-tagagent-gmgn-chain-panel','true');
              const r=target.getBoundingClientRect();
              return {visiblePanels:panels.length, hitTestablePanels:hitTestable.length,
                loginRequired:!!target.querySelector('[data-testid="global-multi-chain-login-tip-icon"]'),
                left:r.left, top:r.top};
            }""")
            if not panel_details:
                raise RuntimeError("GMGN visible chain filter panel was not found")
            if panel_details.get("loginRequired"):
                page.keyboard.press("Escape")
                raise BrowserAccessBlockedError(
                    "GMGN multi-chain selection requires login in the dedicated browser; "
                    "cannot verify exact BSC+Robinhood filter; manual sign-in required"
                )
            panel = page.locator('[data-tagagent-gmgn-chain-panel="true"]')
            panel.wait_for(state="visible", timeout=8_000)
            # React can replace the entire popover on each toggle, dropping
            # our marker and changing option order. Re-discover and mutate
            # only ONE live option per pass; never carry nth() across renders.
            for _ in range(32):
                state = page.evaluate(r"""(desired) => {
                  const panels=[...document.querySelectorAll('[data-testid="chain-multi-select-panel"]')]
                    .filter(el => { const r=el.getBoundingClientRect();
                      return r.width>0 && r.height>0 && getComputedStyle(el).visibility!=='hidden'
                        && !el.closest('[aria-hidden="true"]'); });
                  const live=panels.filter(el => { const r=el.getBoundingClientRect();
                    return document.elementsFromPoint(Math.max(0,Math.min(innerWidth-1,r.left+r.width/2)),
                      Math.max(0,Math.min(innerHeight-1,r.top+Math.min(r.height/2,40))))
                      .some(hit => el.contains(hit)); });
                  const panel=(live.length ? live : panels).at(-1);
                  if (!panel) return {missing:true};
                  const rows=[...panel.querySelectorAll('input[data-testid^="chain-multi-select-option-"]')]
                    .map(input => ({input,name:input.getAttribute('data-testid')
                      .replace('chain-multi-select-option-','').toLowerCase(),
                      selected:input.checked || input.getAttribute('data-selected')==='true'}));
                  const selected=rows.filter(row => row.selected).map(row => row.name);
                  const change=rows.find(row => row.selected !== desired.includes(row.name));
                  if (change) { change.input.click(); return {changed:change.name}; }
                  return {selected};
                }""", sorted(desired))
                if "selected" in state:
                    selected = state["selected"]
                    break
                if state.get("missing"):
                    reopened = page.evaluate(r"""() => {
                      const root=document.querySelector('[data-sentry-component="GlobalCalloutPanel"]') ||
                        document.querySelector('[data-id="KEY_GLOBAL_CALLOUT_RND"]');
                      const trigger=root?.querySelector('[data-testid="chain-multi-select-trigger"]')
                        ?.closest('button[aria-haspopup="dialog"]');
                      if (!trigger) return false;
                      trigger.click(); return true;
                    }""")
                    if not reopened:
                        raise RuntimeError("GMGN chain filter trigger disappeared during selection")
                page.wait_for_timeout(150)
            else:
                raise RuntimeError("GMGN chain filter did not converge after 32 toggles")
            details["panel"] = panel_details
        else:
            # GMGN's September UI removed every chain selector test id.  The
            # Fusion block is the authoritative state: selected icons use the
            # opacity-100 class and each icon's nearest cursor-pointer toggles
            # that chain. This remains scoped to the left Callout dialog.
            result = page.evaluate(r"""(desired) => {
              const dialogs=[...document.querySelectorAll('[role="dialog"]')]
                .filter(el => el.getBoundingClientRect().width>0);
              const dialog=dialogs.find(el => el.querySelector('[data-testid="mix-chain-fusion-block"]'));
              const block=dialog?.querySelector('[data-testid="mix-chain-fusion-block"]');
              if (!block) return {error:'fusion chain dialog not found'};
              const nameFor=(icon) => {
                const value=(icon.getAttribute('data-icon') || '').toLowerCase();
                if (value.includes('robinhood')) return 'robinhood';
                if (value.includes('bsc')) return 'bsc';
                if (value.includes('solchain')) return 'sol';
                if (value.includes('basechain')) return 'base';
                if (value.includes('weth')) return 'eth';
                if (value.includes('arbitrum')) return 'arbitrum';
                if (value.includes('stable')) return 'stable';
                if (value.includes('xlayer')) return 'x layer';
                if (value.includes('hyperevm')) return 'hyperevm';
                if (value.includes('megaeth')) return 'megaeth';
                if (value.includes('monad')) return 'monad';
                if (value.includes('arc')) return 'arc';
                return '';
              };
              const icons=[...block.querySelectorAll('[data-icon]')]
                .map(icon => ({icon, name:nameFor(icon)})).filter(row => row.name);
              for (const {icon,name} of icons) {
                const selected=(icon.getAttribute('class') || '').includes('opacity-100');
                if (selected !== desired.includes(name)) {
                  const toggle=icon.closest('.cursor-pointer');
                  if (!toggle) return {error:`toggle not found for ${name}`};
                  toggle.click();
                }
              }
              return {ok:true};
            }""", sorted(desired))
            if result.get("error"):
                raise RuntimeError(f"GMGN chain filter failed: {result['error']}")
            page.wait_for_timeout(500)
            selected = page.evaluate(r"""() => {
              const block=[...document.querySelectorAll('[data-testid="mix-chain-fusion-block"]')]
                .find(el => el.getBoundingClientRect().width>0);
              const names=[];
              for (const icon of block?.querySelectorAll('[data-icon]') || []) {
                if (!(icon.getAttribute('class') || '').includes('opacity-100')) continue;
                const value=(icon.getAttribute('data-icon') || '').toLowerCase();
                if (value.includes('robinhood')) names.push('robinhood');
                else if (value.includes('bsc')) names.push('bsc');
                else if (value.includes('solchain')) names.push('sol');
                else if (value.includes('basechain')) names.push('base');
                else if (value.includes('weth')) names.push('eth');
                else if (value.includes('arbitrum')) names.push('arbitrum');
                else if (value.includes('stable')) names.push('stable');
                else if (value.includes('xlayer')) names.push('x layer');
                else if (value.includes('hyperevm')) names.push('hyperevm');
                else if (value.includes('megaeth')) names.push('megaeth');
                else if (value.includes('monad')) names.push('monad');
                else if (value.includes('arc')) names.push('arc');
              }
              return names;
            }""")
        page.keyboard.press("Escape")
        if set(selected) != desired:
            raise RuntimeError(f"GMGN chain filter expected only BSC+Robinhood, got {selected}")
        return {
            "selected": sorted(selected),
            "excludedOthers": True,
            "trigger": details,
        }

    @staticmethod
    def _gmgn_native_controls(page: Page, *, activate: bool = False) -> dict:
        """Support legacy combined tab and the split native feed controls.

        Never infer native readiness from the neighboring X monitor or from
        an arbitrary page-wide 'All' label. Re-resolve after every remount.
        """
        return page.evaluate(r"""activate => {
          const visible = el => {
            const r=el.getBoundingClientRect(), s=getComputedStyle(el);
            return r.width>0 && r.height>0 && s.display!=='none' && s.visibility!=='hidden';
          };
          const panel=[...document.querySelectorAll(
            '[data-sentry-component="GlobalCalloutPanel"], [data-id="KEY_GLOBAL_CALLOUT_RND"]'
          )].find(visible);
          if (!panel) return {ready:false};
          const buttons=[...panel.querySelectorAll('button')].filter(visible);
          const legacy=buttons.find(el => /^GMGN\s*全部$/.test((el.textContent||'').trim()));
          if (legacy) {
            if (activate) legacy.click();
            return {ready:true, variant:'legacy', selected:activate};
          }
          const all=buttons.find(el => el.getAttribute('data-sentry-component')==='renderTab' &&
            /^(全部|All)$/i.test((el.textContent||'').trim()));
          const sources=buttons.filter(el => /^(GMGN|FOMO|PUMP)$/i.test(el.getAttribute('aria-label')||'') &&
            el.hasAttribute('aria-pressed'));
          if (!all || !sources.some(el => el.getAttribute('aria-label')==='GMGN')) return {ready:false};
          const allSelected=/(?:^|\s)text-text-100(?:\s|$)/.test(all.className) ||
            all.getAttribute('aria-selected')==='true';
          const selected=sources.filter(el => el.getAttribute('aria-pressed')==='true')
            .map(el=>el.getAttribute('aria-label'));
          if (activate) {
            if (!allSelected) { all.click(); return {ready:true, selected:false}; }
            const mismatch=sources.find(el => (el.getAttribute('aria-pressed')==='true') !==
              (el.getAttribute('aria-label')==='GMGN'));
            if (mismatch) { mismatch.click(); return {ready:true, selected:false}; }
          }
          return {ready:true, variant:'split', selected:allSelected && selected.length===1 && selected[0]==='GMGN',
            nativeSources:selected, allSelected};
        }""", activate)

    def _ensure_gmgn_callout_panel(self, page: Page) -> None:
        """Open the native Callout pane without confusing it with its title.

        GMGN keeps both floating tools mounted and remounts the Callout body
        during hydration.  A global exact-text lookup for ``喊单`` can therefore
        select the title inside the floating pane, while a locator retained
        across the remount becomes stale.  Re-resolve the pane on every poll
        and, when necessary, atomically click only the bottom-dock trigger
        outside the pane.
        """
        def ready() -> bool:
            return bool(self._gmgn_native_controls(page).get("ready"))

        hydration_deadline = time.monotonic() + 20
        while time.monotonic() < hydration_deadline:
            if ready():
                return
            page.wait_for_timeout(250)

        clicked = page.evaluate(r"""() => {
          const visible = el => {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
          };
          const paneReady = [...document.querySelectorAll(
            '[data-sentry-component="GlobalCalloutPanel"], [data-id="KEY_GLOBAL_CALLOUT_RND"]'
          )].some(panel => visible(panel) && [...panel.querySelectorAll('button,span,div')]
            .some(el => visible(el) && (el.textContent || '').trim() === 'GMGN全部'));
          if (paneReady) return true;
          const labels = new Set(['喊单', 'Callout', 'Callouts']);
          const candidates = [...document.querySelectorAll(
            'button,[role="button"],[aria-label],[title],span,div'
          )].filter(el => {
            if (!visible(el) || el.closest(
              '[data-sentry-component="GlobalCalloutPanel"], [data-id="KEY_GLOBAL_CALLOUT_RND"]'
            )) return false;
            const values=[el.textContent, el.getAttribute('aria-label'), el.getAttribute('title')]
              .map(value => (value || '').trim());
            return values.some(value => labels.has(value));
          }).sort((a,b) => b.getBoundingClientRect().bottom - a.getBoundingClientRect().bottom);
          const label = candidates[0];
          if (!label) return false;
          const target = label.closest('button,[role="button"],.cursor-pointer') || label;
          target.click();
          return true;
        }""")
        if not clicked:
            raise RuntimeError("GMGN bottom Callout trigger was not found")

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if ready():
                return
            page.wait_for_timeout(250)
        raise RuntimeError("GMGN native Callout panel did not expose supported feed controls")

    def _collect_gmgn_native(self, window_seconds: int) -> tuple:
        """Read and prove the native rail independently of X coverage."""
        page = self.page("gmgn")
        self._goto_with_retry(page, "https://gmgn.ai/")
        try:
            page.wait_for_function(r"""() => [...document.querySelectorAll('button,span')]
              .some(el => {
                const text=(el.textContent || '').trim();
                const r=el.getBoundingClientRect();
                return r.width>0 && r.height>0 && (text==='喊单' || text==='GMGN全部');
              })""", timeout=15_000)
        except TimeoutError:
            pass
        # SPA navigation preserves both floating tools. Open only the native
        # Callout pane and wait through its hydration/remount before resolving
        # any child locator.
        self._ensure_gmgn_callout_panel(page)
        panel_locator = page.locator(
            '[data-sentry-component="GlobalCalloutPanel"], '
            '[data-id="KEY_GLOBAL_CALLOUT_RND"]'
        )
        chain_filter = self._configure_gmgn_chains(page)
        # Changing the multi-chain filter remounts the Callout pane.  Looking
        # for the tab immediately raced that remount and made otherwise healthy
        # cycles fail.  Wait for the new pane rather than relying on a fixed
        # sub-second delay.
        deadline = time.monotonic() + 15
        native_controls = {}
        while time.monotonic() < deadline:
            native_controls = self._gmgn_native_controls(page, activate=True)
            if native_controls.get("selected"):
                break
            page.wait_for_timeout(250)
        else:
            raise RuntimeError(f"GMGN native All/GMGN-only selection failed: {native_controls}")
        native_card_selector = '[data-sentry-component="CalloutItem"], [data-sentry-component="CalloutFeedCard"]'
        try:
            panel_locator.locator(native_card_selector).first.wait_for(
                state="visible", timeout=20_000
            )
        except TimeoutError as exc:
            self._safe_screenshot(page, "gmgn-zero-callout-items.png")
            raise RuntimeError(
                "GMGN left Callout scroller yielded zero native Callout cards"
            ) from exc
        cards, boundary, root_details = self._scroll_freeze(
            page, source="gmgn", window_seconds=window_seconds
        )
        if not cards:
            rendered = panel_locator.locator(
                native_card_selector
            ).count()
            self._safe_screenshot(page, "gmgn-zero-parsed-callout-items.png")
            raise RuntimeError(
                "GMGN Callout Feed rendered "
                f"{rendered} CalloutItem card(s) but parsed zero valid native cards"
            )
        return page, cards, boundary, root_details, chain_filter, native_controls

    def collect_gmgn(self, window_seconds: int, eligible: set[tuple[str, str]]) -> dict:
        page, cards, boundary, root_details, chain_filter, native_controls = self._collect_gmgn_native(window_seconds)
        x_cards, endpoint_x_boundary, x_verification = self._collect_gmgn_x(
            page, window_seconds=window_seconds
        )
        continuity = self._update_gmgn_x_continuity(
            _utcnow(), x_verification, window_seconds
        )
        x_boundary = endpoint_x_boundary or continuity["boundaryReached"]
        x_verification.update({
            "xSnapshotOverlapProven": continuity["overlapProven"],
            "xContinuousSince": continuity["continuousSince"],
            "xContinuityBoundaryReached": continuity["boundaryReached"],
            "xContinuityByChain": continuity.get("chains", {}),
        })
        all_cards = cards + x_cards
        scanned_at = _utcnow()
        records, matched, unresolved = self._records(
            "gmgn", all_cards, scanned_at=scanned_at, eligible=eligible
        )
        combined_boundary = boundary and x_boundary
        if not combined_boundary:
            self._safe_screenshot(page, "gmgn-incomplete.png")
        return self._batch("gmgn", scanned_at, "https://gmgn.ai/", records, all_cards, matched, unresolved,
            combined_boundary, window_seconds, {
                "calloutPanel": "social_monitor", "chains": ["bsc", "robinhood"],
                "chainFilterSelected": chain_filter["selected"],
                "chainFilterExcludedOthers": chain_filter["excludedOthers"],
                "routeMismatch": False,
                "feedContainerLocated": True, "feedContainer": root_details,
                "nativeCalloutCards": len(cards), "nativeBoundaryReached": boundary,
                "nativeControls": native_controls,
                **x_verification,
            })

    def _collect_gmgn_x(
        self, page: Page, *, window_seconds: int, discovery_timeout_ms: int = 15_000,
    ) -> tuple[list[dict], bool, dict]:
        """Read GMGN's browser-authenticated, CA-only X monitor payload."""
        request_url_script = r"""() => performance.getEntriesByType('resource')
          .map(entry => entry.name)
          .filter(url => url.includes('/vas/api/v1/twitter/messages?'))
          .at(-1) || ''"""
        observed_url = str(page.evaluate(request_url_script) or "")
        base_url = observed_url or self.gmgn_x_request_url
        used_cached_url = bool(base_url and not observed_url)
        panel_visible = False
        only_ca = None

        # A visible XItemCard is deliberately not a prerequisite: GMGN can
        # render an empty/virtualized panel while the authenticated endpoint is
        # healthy. Open the UI only when this Worker has not learned the
        # endpoint yet, then observe the request itself as the source of truth.
        if not base_url:
            root = page.locator('[data-id="KEY_X_SNIPER_RND_V1"]')
            panel_visible = any(root.nth(i).is_visible() for i in range(root.count()))
            if not panel_visible:
                trigger = self._visible(page.get_by_text("社媒监控", exact=True))
                if trigger is None:
                    raise RuntimeError("GMGN Social Monitor trigger was not found")
                trigger.click(force=True, timeout=min(8_000, discovery_timeout_ms))
            only_ca = self._visible(page.get_by_text("只看CA", exact=True))
            if only_ca is not None:
                checkbox = only_ca.locator("xpath=..")
                # ``Locator.get_attribute`` waits for the nested SVG for the
                # full Playwright default timeout. During a panel remount the
                # label can already be visible while the icon is absent,
                # turning a lightweight continuity checkpoint into a 30s
                # stall. A synchronous DOM lookup is sufficient here: a
                # missing icon means the state is not safely identifiable and
                # we leave the control untouched.
                icon = _dom_data_icon(checkbox)
                if icon == "IconSinglechoice16px":
                    checkbox.click(force=True, timeout=min(8_000, discovery_timeout_ms))
            deadline = time.monotonic() + (discovery_timeout_ms / 1000)
            while time.monotonic() < deadline and not base_url:
                page.wait_for_timeout(250)
                base_url = str(page.evaluate(request_url_script) or "")

        if not base_url and only_ca is not None:
            # An already-open Social Monitor can hydrate its cards from React
            # cache without leaving the authenticated request in the current
            # resource-timing buffer.  Re-apply CA-only once to force a fresh
            # request instead of treating a healthy panel as a collector
            # outage.  Resolve the control again after each click because the
            # panel remounts.
            current = self._visible(page.get_by_text("只看CA", exact=True))
            if current is not None:
                current.locator("xpath=..").evaluate("el => el.click()")
                page.wait_for_timeout(300)
            current = self._visible(page.get_by_text("只看CA", exact=True))
            if current is not None:
                current.locator("xpath=..").evaluate("el => el.click()")
                deadline = time.monotonic() + min(5, discovery_timeout_ms / 1000)
                while time.monotonic() < deadline and not base_url:
                    page.wait_for_timeout(250)
                    base_url = str(page.evaluate(request_url_script) or "")
        if not base_url:
            raise RuntimeError("GMGN Social Monitor request URL was not observed")
        self.gmgn_x_request_url = base_url
        request_urls = page.evaluate(r"""({baseUrl, pageSize}) => {
          return ['bsc','robinhood'].map(chain => {
            const url=new URL(baseUrl);
            url.searchParams.set('has_token','true');
            url.searchParams.set('limit', String(pageSize));
            url.searchParams.delete('chain');
            url.searchParams.set('chain', chain);
            return {chain, url:url.toString()};
          });
        }""", {"baseUrl": base_url, "pageSize": GMGN_X_PAGE_SIZE})
        rows: list[dict] = []
        rows_by_chain: dict[str, int] = {}
        for request in request_urls:
            request_chain = str(request.get("chain") or "")
            fetched = self._browser_json_request(
                page, str(request.get("url") or ""), credentials="same-origin",
            )
            payload = fetched.get("payload") if isinstance(fetched, dict) else None
            chain_rows = payload.get("data") if isinstance(payload, dict) else None
            if (
                not isinstance(fetched, dict) or fetched.get("ok") is not True
                or not isinstance(chain_rows, list)
            ):
                if used_cached_url:
                    # Force UI rediscovery on the next checkpoint if GMGN changed
                    # the request shape or invalidated a URL-bound parameter.
                    self.gmgn_x_request_url = ""
                raise RuntimeError(
                    f"GMGN Social Monitor CA Feed failed for {request_chain}: HTTP "
                    f"{fetched.get('status', 0) if isinstance(fetched, dict) else 0}"
                )
            rows_by_chain[request_chain] = len(chain_rows)
            rows.extend(chain_rows)

        now = _utcnow()
        cutoff = now - timedelta(seconds=window_seconds)
        cards: list[dict] = []
        observed_times: list[datetime] = []
        observed_times_by_chain: dict[str, list[datetime]] = {
            "bsc": [], "robinhood": [],
        }
        for row in rows:
            if not isinstance(row, dict):
                continue
            token, user = row.get("token"), row.get("user")
            if not isinstance(token, dict) or not isinstance(user, dict):
                continue
            chain = str(token.get("chain") or "").lower()
            ca = str(token.get("ca") or "").lower()
            tweet_id = str(row.get("tweet_id") or "")
            username = str(user.get("screen_name") or "").lstrip("@")
            if chain not in {"bsc", "robinhood"} or not re.fullmatch(
                r"0x[a-f0-9]{40}", ca, re.I
            ) or not re.fullmatch(r"\d{15,22}", tweet_id) or not username:
                continue
            try:
                published = datetime.fromtimestamp(
                    int(str(row.get("tw_timestamp") or "0")) / 1000,
                    timezone.utc,
                )
            except (TypeError, ValueError, OverflowError):
                continue
            observed_times.append(published)
            observed_times_by_chain[chain].append(published)
            if published < cutoff or published > now + timedelta(seconds=10):
                continue
            content_obj = row.get("content")
            if str(row.get("tw_type") or "") == "repost":
                content_obj = row.get("source_content") or content_obj
            content = (
                str(content_obj.get("text") or "").strip()
                if isinstance(content_obj, dict) else ""
            )
            if not content:
                continue
            age = max(0, int((now - published).total_seconds()))
            x_url = f"https://x.com/{username}/status/{tweet_id}"
            cards.append({
                "href": f"https://gmgn.ai/{chain}/token/{ca}",
                "text": f"{username}\n{age}s\n{content}",
                "content": content, "username": username,
                "chain": chain, "tokenCa": ca,
                "relativeTimes": [f"{age}s"], "relativeTime": f"{age}s",
                "publishedAt": published.isoformat(),
                "sourcePostId": tweet_id, "xStatusUrl": x_url,
                "feedCardType": "social_monitor",
            })
        oldest = min(observed_times) if observed_times else None
        oldest_by_chain = {
            chain: min(times).isoformat() if times else ""
            for chain, times in observed_times_by_chain.items()
        }
        boundary_by_chain = {
            chain: bool(
                rows_by_chain.get(chain, 0) < 20
                or (
                    oldest_by_chain[chain]
                    and datetime.fromisoformat(oldest_by_chain[chain]) <= cutoff
                )
            )
            for chain in ("bsc", "robinhood")
        }
        boundary = all(boundary_by_chain.values())
        return cards, boundary, {
            "xMonitorSelected": True, "xOnlyCa": True,
            "xEndpointDiscovery": "performance" if observed_url else (
                "worker-cache" if used_cached_url else "panel-request"
            ),
            "xItemCardRequired": False,
            "xFeedRows": len(rows), "xWindowCards": len(cards),
            "xRequestedLimit": GMGN_X_PAGE_SIZE,
            "xFeedRowsByChain": rows_by_chain,
            "xOldestPublishedAtByChain": oldest_by_chain,
            "xBoundaryReachedByChain": boundary_by_chain,
            "xBoundaryReached": boundary,
            "xOldestPublishedAt": oldest.isoformat() if oldest else "",
        }

    def collect_gmgn_x_checkpoint(
        self, window_seconds: int, eligible: set[tuple[str, str]], *, quick: bool = False,
    ) -> dict:
        """Persist a lightweight X snapshot between 10-minute full scans.

        GMGN returns at most 20 CA-tagged X messages. At current traffic that
        buffer spans roughly two minutes, so polling it only during a full
        ten-minute platform cycle necessarily loses posts. Overlapping
        snapshots build an explicit continuity proof in persistent state.
        """
        page = self.page("gmgn_x")
        if urlparse(page.url).hostname not in {"gmgn.ai", "www.gmgn.ai"}:
            self._goto_with_retry(page, "https://gmgn.ai/")
            page.wait_for_timeout(3000)
        cards, endpoint_boundary, verification = self._collect_gmgn_x(
            page, window_seconds=window_seconds,
            discovery_timeout_ms=2_000 if quick else 15_000,
        )
        scanned_at = _utcnow()
        continuity = self._update_gmgn_x_continuity(
            scanned_at, verification, window_seconds
        )
        records, matched, unresolved = self._records(
            "gmgn", cards, scanned_at=scanned_at, eligible=eligible
        )
        return self._batch(
            "gmgn", scanned_at, "https://gmgn.ai/", records, cards,
            matched, unresolved, endpoint_boundary or continuity["boundaryReached"],
            window_seconds, {
                "calloutPanel": "social_monitor",
                "chains": ["bsc", "robinhood"],
                "chainFilterSelected": ["bsc", "robinhood"],
                "chainFilterExcludedOthers": True,
                "routeMismatch": False,
                "feedContainerLocated": True,
                **verification,
                "xSnapshotOverlapProven": continuity["overlapProven"],
                "xContinuousSince": continuity["continuousSince"],
                "xContinuityBoundaryReached": continuity["boundaryReached"],
                "xContinuityByChain": continuity.get("chains", {}),
            },
        )

    def collect_gmgn_x_checkpoint_resilient(
        self, window_seconds: int, eligible: set[tuple[str, str]], *, quick: bool = False,
    ) -> dict:
        """Retry one GMGN X checkpoint after recreating its browser target.

        Checkpoints run independently from the full platform scan. A renderer
        can be closed by Chrome immediately after ``page()`` validates it, so
        merely reconnecting in the outer scheduler leaves that checkpoint
        lost until the next poll. Re-run the same snapshot once after recovery
        so the 20-row GMGN X ring buffer keeps its intended overlap.
        """
        try:
            return self.collect_gmgn_x_checkpoint(
                window_seconds, eligible, quick=quick,
            )
        except Exception as exc:
            if self._connection_was_closed(exc):
                self.reconnect()
            else:
                # A stale request URL or a wedged renderer is isolated to the
                # checkpoint tab. Recreate it without disturbing FOMO/Pump.
                self.discard_page("gmgn_x")
            return self.collect_gmgn_x_checkpoint(
                window_seconds, eligible, quick=quick,
            )

    @staticmethod
    def _update_gmgn_x_continuity(
        scanned_at: datetime, verification: dict, window_seconds: int
    ) -> dict:
        oldest_by_chain = verification.get("xOldestPublishedAtByChain")
        boundary_by_chain = verification.get("xBoundaryReachedByChain")
        if isinstance(oldest_by_chain, dict) and isinstance(boundary_by_chain, dict):
            state = load_json(STATE, {})
            state = state if isinstance(state, dict) else {}
            x_state = state.get("gmgnX") if isinstance(state.get("gmgnX"), dict) else {}
            previous_chains = (
                x_state.get("chains") if isinstance(x_state.get("chains"), dict) else {}
            )
            chain_results: dict[str, dict] = {}
            for chain in ("bsc", "robinhood"):
                try:
                    oldest = datetime.fromisoformat(
                        str(oldest_by_chain.get(chain) or "").replace("Z", "+00:00")
                    )
                except ValueError:
                    oldest = scanned_at
                previous = previous_chains.get(chain)
                previous_scan = continuous_since = None
                if isinstance(previous, dict):
                    try:
                        previous_scan = datetime.fromisoformat(
                            str(previous.get("lastScannedAt") or "").replace("Z", "+00:00")
                        )
                        continuous_since = datetime.fromisoformat(
                            str(previous.get("continuousSince") or "").replace("Z", "+00:00")
                        )
                    except ValueError:
                        previous_scan = continuous_since = None
                overlap = bool(
                    previous_scan is not None and continuous_since is not None
                    and oldest <= previous_scan
                    and (scanned_at - previous_scan).total_seconds() <= 240
                )
                direct_boundary = boundary_by_chain.get(chain) is True
                if not overlap:
                    continuous_since = oldest
                reached = bool(
                    direct_boundary
                    or continuous_since <= scanned_at - timedelta(seconds=window_seconds)
                )
                chain_results[chain] = {
                    "lastScannedAt": scanned_at.isoformat(),
                    "continuousSince": continuous_since.isoformat(),
                    "snapshotRows": (
                        verification.get("xFeedRowsByChain", {}).get(chain, 0)
                        if isinstance(verification.get("xFeedRowsByChain"), dict) else 0
                    ),
                    "overlapProven": overlap,
                    "boundaryReached": reached,
                }
            common_since = max(
                datetime.fromisoformat(row["continuousSince"])
                for row in chain_results.values()
            )
            state["gmgnX"] = {
                "lastScannedAt": scanned_at.isoformat(),
                "continuousSince": common_since.isoformat(),
                "snapshotRows": verification.get("xFeedRows", 0),
                "overlapProven": all(
                    row["overlapProven"] or row["boundaryReached"]
                    for row in chain_results.values()
                ),
                "chains": chain_results,
            }
            save_json(STATE, state)
            return {
                "overlapProven": all(
                    row["overlapProven"] or row["boundaryReached"]
                    for row in chain_results.values()
                ),
                "continuousSince": common_since.isoformat(),
                "boundaryReached": all(
                    row["boundaryReached"] for row in chain_results.values()
                ),
                "chains": chain_results,
            }

        oldest_value = str(verification.get("xOldestPublishedAt") or "")
        try:
            oldest = datetime.fromisoformat(oldest_value.replace("Z", "+00:00"))
        except ValueError:
            oldest = scanned_at
        state = load_json(STATE, {})
        x_state = state.get("gmgnX") if isinstance(state, dict) else None
        previous_scan = continuous_since = None
        if isinstance(x_state, dict):
            try:
                previous_scan = datetime.fromisoformat(
                    str(x_state.get("lastScannedAt") or "").replace("Z", "+00:00")
                )
                continuous_since = datetime.fromisoformat(
                    str(x_state.get("continuousSince") or "").replace("Z", "+00:00")
                )
            except ValueError:
                previous_scan = continuous_since = None
        overlap = bool(
            previous_scan is not None and continuous_since is not None
            and oldest <= previous_scan
            and (scanned_at - previous_scan).total_seconds() <= 240
        )
        if not overlap:
            continuous_since = oldest
        state = state if isinstance(state, dict) else {}
        state["gmgnX"] = {
            "lastScannedAt": scanned_at.isoformat(),
            "continuousSince": continuous_since.isoformat(),
            "snapshotRows": verification.get("xFeedRows", 0),
            "overlapProven": overlap,
        }
        save_json(STATE, state)
        return {
            "overlapProven": overlap,
            "continuousSince": continuous_since.isoformat(),
            "boundaryReached": continuous_since <= scanned_at - timedelta(seconds=window_seconds),
        }

    def collect_pump(self, window_seconds: int, eligible: set[tuple[str, str]]) -> dict:
        """Collect Pump's global Callouts rail through its browser Feed payload.

        The coin-page rail is ranked rather than chronological and is heavily
        virtualized.  Reading DOM slots therefore misses fresh updates attached
        to older callouts.  The browser home-feed provides exact chain, CA,
        author and absolute time for its selected coins, including expanded
        updates.  It is *not* an exhaustive list of every eligible coin:
        callout_monitor must use the exact-CA API for coins absent from it.
        The DOM verifies the coin-page entry point requested by the operator.
        """
        page = self.page("pump")
        # PONS is only a stable doorway to the platform-wide left rail; records
        # are still accepted exclusively by the current exact eligible set.
        seed_chain = "robinhood"
        seed_ca = "0x39dbed3a2bd333467115de45665cc57f813c4571"
        seed_url = f"https://pump.fun/coin/{seed_ca}"
        ready = r"""() => {
          const rail=document.querySelector('[data-testid="coin-alerts-rail-callouts-scroll"]');
          if (!rail) return false;
          return rail.querySelectorAll('article[data-testid="feed-callout-card"]').length > 0;
        }"""
        coin_page_navigation_error = ""
        try:
            # This doorway is only visual verification. Give it one bounded
            # attempt; the authoritative browser home-feed below remains able
            # to prove complete platform coverage without spending three
            # minutes retrying a wedged renderer.
            self._goto_with_retry(page, seed_url, timeout=20_000, attempts=1)
        except Exception as exc:
            if not self._transient_navigation_error(exc):
                raise
            # The platform-wide browser home-feed does not depend on the coin
            # page renderer.  A CDN/network timeout on the optional doorway
            # must not suppress every Pump callout in this cycle.
            coin_page_navigation_error = str(exc)
            LOG.warning(
                "Pump coin-page doorway failed after bounded retries; "
                "continuing with Chromium home-feed: %s", exc,
            )
        self._report_progress()
        coin_page_rail_visible = False
        if not coin_page_navigation_error:
            try:
                page.wait_for_function(ready, timeout=20_000)
                coin_page_rail_visible = True
            except TimeoutError:
                # The exact failure this route addresses is a React loading
                # skeleton. The browser Feed endpoint can still be healthy, so do
                # not fall back to the unrelated homepage or declare coverage yet.
                self._report_progress()
                self._safe_screenshot(page, "pump-coin-callouts-skeleton.png")
        self._report_progress()

        route = PUMP_RE.search(page.url)
        coin_page_matched = bool(route and route.group(1).lower() == seed_ca)
        if not coin_page_navigation_error and not coin_page_matched:
            raise RuntimeError("Pump coin-page seed URL did not preserve the exact PONS CA")

        scanned_at = _utcnow()
        endpoint = (
            "https://frontend-api-v3.pump.fun/home-feed"
            "?pageSize=150&chain=all&platform=WEB"
            "&includeFollowedHolders=true&followedHoldersLimit=5"
        )
        payloads: list[dict] = []
        seen_tokens: set[str] = set()
        next_token = ""
        exhausted = False
        # The endpoint presently returns one finite 150-row snapshot. Retain
        # cursor support so a future Pump rollout cannot silently truncate it.
        for _ in range(20):
            url = endpoint + (f"&pageToken={quote(next_token, safe='')}" if next_token else "")
            # Stay in the dedicated Chromium profile. If a cross-origin fetch
            # is rejected with HTTP 0, the helper retries and then navigates a
            # short-lived Chromium tab directly to the same JSON endpoint.
            self._report_progress()
            fetched = self._browser_json_request(page, url, credentials="omit")
            self._report_progress()
            if not isinstance(fetched, dict) or fetched.get("ok") is not True:
                raise RuntimeError(
                    "Pump Chromium home-feed request failed: "
                    f"HTTP {fetched.get('status') if isinstance(fetched, dict) else 0} "
                    f"{fetched.get('error') or fetched.get('text') if isinstance(fetched, dict) else fetched}"
                )
            payload = fetched.get("payload")
            if not isinstance(payload, dict) or not isinstance(payload.get("coins"), list):
                raise RuntimeError("Pump browser home-feed omitted its coins array")
            payloads.append(payload)
            candidate = str(payload.get("nextPageToken") or "")
            if not candidate:
                exhausted = True
                break
            if candidate in seen_tokens:
                raise RuntimeError("Pump browser home-feed repeated its pagination token")
            seen_tokens.add(candidate)
            next_token = candidate
        if not exhausted:
            raise RuntimeError("Pump browser home-feed exceeded the 20-page safety cap")

        rows = [row for payload in payloads for row in payload["coins"] if isinstance(row, dict)]
        if not rows:
            raise RuntimeError("Pump browser home-feed returned zero Callout rows")
        cards = self._pump_home_feed_cards(
            rows, scanned_at=scanned_at, window_seconds=window_seconds
        )
        records, matched, unresolved = self._records(
            "pump", cards, scanned_at=scanned_at, eligible=eligible
        )
        return self._batch(
            "pump", scanned_at, page.url if coin_page_matched else seed_url,
            records, cards, matched, unresolved,
            exhausted, window_seconds, {
                "calloutsSelected": True,
                "feedContainerLocated": True,
                "coinPageFeed": True,
                "globalCrossChainFeed": True,
                "coinPageMatched": coin_page_matched,
                "coinPageNavigationFailed": bool(coin_page_navigation_error),
                "coinPageNavigationError": coin_page_navigation_error,
                "coinPageRailVisible": coin_page_rail_visible,
                "coinPageSeed": {"chain": seed_chain, "tokenCa": seed_ca},
                "browserHomeFeed": True,
                "exactCaApiFallbackRequiredForAbsentCoins": True,
                "chainResolution": "feed-payload",
                "feedPageSize": 150,
                "pagesFetched": len(payloads),
                "nextPageTokenExhausted": exhausted,
                "boundaryProof": "browser-home-feed-page-exhausted",
                "scannedCards": len(rows),
            },
        )

    @staticmethod
    def _pump_home_feed_cards(
        rows: list[dict], *, scanned_at: datetime, window_seconds: int
    ) -> list[dict]:
        """Flatten Pump home-feed callouts and updates inside this scan window."""
        cutoff = scanned_at - timedelta(seconds=window_seconds)
        cards: list[dict] = []

        def append_event(
            *, row: dict, position: dict, callout: dict, event_id: str,
            content: object, published_value: object, event_type: str,
        ) -> None:
            chain = str(row.get("chain") or "").lower()
            ca = str(row.get("coinMint") or "").lower()
            if chain not in {"bsc", "robinhood"} or not re.fullmatch(
                r"0x[a-f0-9]{40}", ca, re.I
            ):
                return
            try:
                published = datetime.fromisoformat(
                    str(published_value or "").replace("Z", "+00:00")
                )
                if published.tzinfo is None:
                    published = published.replace(tzinfo=timezone.utc)
                published = published.astimezone(timezone.utc)
            except ValueError:
                return
            if published < cutoff or published > scanned_at:
                return
            body = str(content or "").strip()
            username = str(
                position.get("userName") or position.get("xUsername") or ""
            ).strip().lstrip("@")
            callout_id = str(callout.get("calloutId") or "").strip()
            if not body or not username or not callout_id or not event_id:
                return
            age = max(0, int((scanned_at - published).total_seconds()))
            href = f"https://pump.fun/coin/{ca}?highlight_callout={callout_id}"
            card = {
                "href": href,
                "text": f"{username}\n{body}",
                "content": body,
                "username": username,
                "chain": chain,
                "tokenCa": ca,
                "publishedAt": published.isoformat(),
                "relativeTimes": [f"{age}s"],
                "relativeTime": f"{age}s",
                "sourcePostId": event_id,
                "feedCardType": event_type,
            }
            address = str(position.get("walletAddress") or "").lower()
            if re.fullmatch(r"0x[a-f0-9]{40}", address, re.I):
                card["evmAddress"] = address
            cards.append(card)

        for row in rows:
            position = row.get("position")
            if not isinstance(position, dict):
                continue
            callout = position.get("callout")
            if not isinstance(callout, dict):
                continue
            callout_id = str(callout.get("calloutId") or "").strip()
            append_event(
                row=row, position=position, callout=callout,
                event_id=callout_id, content=callout.get("thesis"),
                published_value=callout.get("calloutTimestamp"),
                event_type="callout",
            )
            for update in callout.get("updates") or []:
                if not isinstance(update, dict):
                    continue
                update_id = str(update.get("id") or "").strip()
                append_event(
                    row=row, position=position, callout=callout,
                    event_id=f"{callout_id}:{update_id}" if update_id else "",
                    content=update.get("content"),
                    published_value=update.get("createdAt"),
                    event_type="update",
                )
        return cards

    def _collect_pump_home_legacy(self, window_seconds: int, eligible: set[tuple[str, str]]) -> dict:
        page = self.page("pump")
        self._goto_with_retry(page, "https://pump.fun/")
        page.evaluate("window.scrollTo(0, 0)")
        tabs = page.get_by_role("tab", name="Callouts", exact=True)
        try:
            tabs.first.wait_for(state="visible", timeout=20_000)
        except TimeoutError:
            self._safe_screenshot(page, "pump-no-callouts-tab.png")
        callouts = self._visible(tabs)
        if callouts is None:
            callouts = self._visible(page.get_by_text("Callouts", exact=True))
        if callouts is None:
            raise RuntimeError("Pump Callouts tab was not found")
        self._activate(callouts)
        ready = """() => {
          const text=document.body.innerText||'';
          const clocks=/\\d+\\s*(?:s|m|h)\\s+ago/i.test(text);
          const calloutLinks=[...document.querySelectorAll('a[href]')].some(a =>
            /\\/callouts\\/(?:0x[a-f0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,})\\/[0-9a-f-]{30,}/i
              .test(a.getAttribute('href')||''));
          return clocks && calloutLinks;
        }"""
        try:
            page.wait_for_function(ready, timeout=25_000)
        except TimeoutError:
            # Pump occasionally leaves the three-column homepage in a loading
            # skeleton indefinitely. One reload is safe; after that the API
            # fallback must own this cycle rather than claiming browser cover.
            page.reload(wait_until="domcontentloaded", timeout=60_000)
            page.evaluate("window.scrollTo(0, 0)")
            tabs = page.get_by_role("tab", name="Callouts", exact=True)
            tabs.first.wait_for(state="visible", timeout=20_000)
            callouts = self._visible(tabs)
            if callouts is not None:
                self._activate(callouts)
            try:
                page.wait_for_function(ready, timeout=25_000)
            except TimeoutError:
                self._safe_screenshot(page, "pump-loading-skeleton.png")
                raise RuntimeError("Pump Callouts remained in its loading skeleton; API fallback required")
        scanned_at = _utcnow()
        cards_by_key: dict[str, dict] = {}
        boundary_by_chain: dict[str, bool] = {}
        root_details_by_chain: dict[str, dict] = {}
        for chain in ("robinhood", "bsc"):
            chain_filter_locator = page.locator('[data-testid="feed-chain-filter"]')
            chain_filter_locator.first.wait_for(state="visible", timeout=10_000)
            chain_filter = self._visible(chain_filter_locator)
            if chain_filter is None:
                raise RuntimeError("Pump visible Feed chain filter was not found")
            chain_filter.evaluate("el => el.click()")
            option_locator = page.locator(f'[data-testid="feed-chain-option-{chain}"]')
            option_locator.first.wait_for(state="visible", timeout=8_000)
            option = self._visible(option_locator)
            if option is None:
                raise RuntimeError(f"Pump visible {chain} Feed option was not found")
            # The popover closes and remounts the feed synchronously. A normal
            # Playwright click sometimes waits for stability until the option
            # has already detached, even though it is actionable now.
            option.evaluate("el => el.click()")
            page.wait_for_timeout(1000)
            selected_label = "Robinhood" if chain == "robinhood" else "BSC"
            selected_ok = page.evaluate(r"""label => [...document.querySelectorAll(
              '[data-testid="feed-chain-filter"]')].some(el => {
                const r=el.getBoundingClientRect(), s=getComputedStyle(el);
                return r.width>0 && r.height>0 && s.visibility!=='hidden' &&
                  (el.innerText||'').trim().toLowerCase().includes(label.toLowerCase());
              })""", selected_label)
            if not selected_ok:
                raise RuntimeError(f"Pump Feed did not confirm {selected_label} chain filter")

            # Apply Pump's explicit live-update banner before freezing. The
            # feed otherwise leaves new cards staged outside the current DOM.
            live = self._visible(page.get_by_text(re.compile(r"^new callouts?$", re.I)))
            if live is not None:
                live.evaluate("el => el.click()")
                page.wait_for_timeout(700)

            try:
                page.locator('article[data-testid="feed-callout-card"]').first.wait_for(
                    state="visible", timeout=15_000
                )
            except TimeoutError:
                body = page.locator("body").inner_text()
                if not re.search(r"No (?:callouts|results)", body, re.I):
                    raise RuntimeError(f"Pump {chain} Callouts did not load any cards")

            root, root_details = self._feed_root(page, "pump")
            root_details_by_chain[chain] = root_details
            chain_seen: set[str] = set()
            stale_pages = 0
            boundary = False
            # Pump's ranked Callouts feed is not strictly chronological. Read
            # multiple pagination chunks and require two consecutive chunks
            # without a window-age event before declaring the boundary.
            for page_index in range(8):
                try:
                    snapshot = self._freeze(root, "pump")
                except TimeoutError:
                    root, root_details = self._feed_root(page, "pump")
                    snapshot = self._freeze(root, "pump")
                new_cards = []
                for card in snapshot:
                    key = str(card.get("sourcePostId") or "") + "|" + str(card.get("publishedAt") or "")
                    if key in chain_seen:
                        continue
                    chain_seen.add(key)
                    cards_by_key.setdefault(key, card)
                    new_cards.append(card)
                if page_index > 0:
                    has_recent = any(
                        any(age <= window_seconds for age in _card_ages(card))
                        for card in new_cards
                    )
                    stale_pages = 0 if has_recent else stale_pages + 1
                    if stale_pages >= 2:
                        boundary = True
                        break
                more = self._visible(page.get_by_text("Show more callouts", exact=True))
                if more is None:
                    boundary = True
                    break
                before = page.locator('article[data-testid="feed-callout-card"]').count()
                more.evaluate("el => el.click()")
                try:
                    page.wait_for_function(
                        "before => document.querySelectorAll('article[data-testid=\"feed-callout-card\"]').length > before",
                        arg=before, timeout=10_000,
                    )
                except TimeoutError:
                    # A stationary load-more control is equivalent to the end
                    # of the currently available server result set.
                    boundary = True
                    break
                root, root_details = self._feed_root(page, "pump")
            boundary_by_chain[chain] = boundary

        cards = list(cards_by_key.values())
        boundary = all(boundary_by_chain.values())
        if not cards:
            # Clocks elsewhere on the three-column homepage can prove only that
            # the page moved through time; without a frozen Callout token card
            # they cannot prove browser Feed coverage.
            raise RuntimeError("Pump Callouts container yielded zero resolvable token cards; API fallback required")
        records, matched, unresolved = self._records("pump", cards, scanned_at=scanned_at, eligible=eligible)
        if not boundary:
            self._safe_screenshot(page, "pump-incomplete.png")
        return self._batch("pump", scanned_at, page.url, records, cards, matched, unresolved, boundary, window_seconds, {
            "calloutsSelected": True, "feedContainerLocated": True,
            "chainFilterScanned": ["bsc", "robinhood"],
            "boundaryByChain": boundary_by_chain,
            "feedContainer": root_details_by_chain,
        })

    @staticmethod
    def _batch(source: str, scanned_at: datetime, page_url: str, records: list[dict], cards: list[dict], matched: int, unresolved: int, boundary: bool, window_seconds: int, verification: dict) -> dict:
        # Multiple anchors/updates may resolve to one stable sourcePostId. That
        # is a successful deduplication, not an incomplete scan. Only matched
        # cards that lack a usable clock keep coverage open.
        complete = bool(boundary and unresolved == 0)
        scanned_cards = int(verification.get("scannedCards") or len(cards))
        candidate_cards = len(cards)
        return {
            "schemaVersion": 1, "complete": True, "source": source,
            "scannedAt": scanned_at.isoformat(), "pageUrl": page_url,
            "records": records,
            "coverage": {"windowSeconds": window_seconds, "complete": complete,
                         "scannedCards": scanned_cards, "eligibleMatchedCards": matched,
                         "unresolvedCards": unresolved},
            "verification": {**verification, "scannedCards": scanned_cards,
                             "candidateCards": candidate_cards,
                             "notEligibleCandidateCards": max(0, candidate_cards - matched),
                             "resolvedCards": len(records), "eligibleMatchedCards": matched,
                             "unresolvedCards": unresolved,
                             "boundaryReached": boundary},
        }

    def cycle(self, *, source: str | None = None, window_seconds: int | None = None) -> dict:
        self.ensure_connected()
        eligible = _eligible()
        selected = [source] if source else ["fomo", "gmgn", "pump"]
        source_windows = {
            name: int(window_seconds or _effective_lookback(int(time.time()), name))
            for name in selected
        }
        window = max(source_windows.values())
        started_at = _utcnow()
        result = {
            "at": started_at.isoformat(), "updatedAt": started_at.isoformat(),
            "status": "running", "windowSeconds": window,
            "sourceWindowSeconds": source_windows, "sources": {}, "errors": {},
        }
        save_json(HEALTH, result)
        x_snapshots: list[dict] = []
        last_x_snapshot = 0.0
        x_retry_after = 0.0

        def capture_x_continuity(stage: str, *, force: bool = False) -> None:
            nonlocal last_x_snapshot, x_retry_after
            if source is not None:
                return
            now = time.monotonic()
            # A failed checkpoint can take longer than its ten-second polling
            # interval. Without a circuit breaker the next scroll callback
            # immediately retries the same outage and starves the active
            # platform scan.
            if now < x_retry_after:
                return
            if not force and now - last_x_snapshot < GMGN_X_POLL_INTERVAL_SECONDS:
                return
            try:
                batch = self.collect_gmgn_x_checkpoint_resilient(
                    source_windows.get("gmgn", window), eligible, quick=True,
                )
                snapshot_summary = {
                    "stage": stage, **persist_batch(batch),
                    "verification": batch["verification"],
                }
                x_snapshots.append(snapshot_summary)
                last_x_snapshot = time.monotonic()
                result["updatedAt"] = _utcnow().isoformat()
                result["activeStage"] = stage
                result["lastGmgnXCheckpointAt"] = result["updatedAt"]
                save_json(HEALTH, result)
            except Exception as exc:
                LOG.exception("GMGN X continuity snapshot failed at %s", stage)
                x_snapshots.append({"stage": stage, "error": str(exc)})
                result["updatedAt"] = _utcnow().isoformat()
                result["activeStage"] = stage
                result["lastGmgnXCheckpointError"] = str(exc)
                save_json(HEALTH, result)
                # The checkpoint callback also runs while another platform is
                # scrolling.  Recover the shared CDP session here; otherwise a
                # closed context remains cached and every ten-second snapshot
                # fails until the LaunchAgent is manually restarted.  The
                # active platform attempt will detect its invalidated page and
                # perform its normal bounded retry.
                if self._connection_was_closed(exc):
                    try:
                        self.reconnect()
                    except Exception:
                        LOG.exception("browser reattachment after GMGN X checkpoint failed")
                elif self._browser_is_offline(exc):
                    # At cycle start no platform page is active, so replace a
                    # stale offline Chromium network service immediately.  A
                    # restart during another platform's scroll callback would
                    # invalidate that active Page; its ordinary platform retry
                    # performs the restart instead.
                    self.discard_page("gmgn_x")
                    x_retry_after = time.monotonic() + 120
                    if stage == "cycle-start":
                        LOG.warning(
                            "GMGN X checkpoint found Chromium offline; "
                            "restarting dedicated Chrome network service"
                        )
                        self.restart_dedicated_browser()
                elif self._transient_navigation_error(exc):
                    self.discard_page("gmgn_x")
                    x_retry_after = time.monotonic() + 60
                # Avoid retrying a broken X panel on every 300ms scroll step.
                last_x_snapshot = time.monotonic()

        # The CA-only endpoint is a 20-row ring buffer. Snapshot before and
        # between the slower UI scans so a busy X feed cannot roll over while
        # FOMO/GMGN native/Pump are being traversed.
        capture_x_continuity("cycle-start", force=True)
        self.progress_callback = lambda: capture_x_continuity("intra-stage")
        try:
            for name in selected:
                platform_started = time.monotonic()
                errors = []
                for attempt in range(3):
                    try:
                        batch = getattr(self, f"collect_{name}")(
                            source_windows[name], eligible
                        )
                        summary = {**persist_batch(batch), "verification": batch["verification"]}
                        if batch["coverage"]["complete"] is not True:
                            summary["status"] = "incomplete"
                            summary["incompleteReason"] = "feed boundary was not reached or visible cards were unresolved"
                        if errors:
                            summary["recoveredAfterRetry"] = True
                        summary["durationSeconds"] = round(time.monotonic() - platform_started, 3)
                        result["sources"][name] = summary
                        result["updatedAt"] = _utcnow().isoformat()
                        save_json(HEALTH, result)
                        break
                    except Exception as exc:
                        errors.append(str(exc))
                        LOG.exception("%s collection attempt %d failed", name, attempt + 1)
                        if isinstance(exc, BrowserAccessBlockedError):
                            # Do not solve challenges or restart other sources'
                            # healthy pages in response to an access decision.
                            break
                        self.discard_page(name)
                        if attempt < 2:
                            # A fresh Page is essential after Chromium's "Page
                            # crashed" signal; the old object remains open-looking
                            # but can never navigate successfully again.
                            try:
                                if self._connection_was_closed(exc):
                                    LOG.warning("%s lost its CDP target; forcing browser reattachment", name)
                                    self.reconnect()
                                elif self._browser_is_offline(exc):
                                    # A live CDP socket does not imply a live
                                    # Chromium network service.  After the
                                    # collector's own bounded retries, an
                                    # explicit ERR_INTERNET_DISCONNECTED is
                                    # sticky often enough that allocating a
                                    # fresh Page is ineffective.  Give the
                                    # local proxy a short recovery window, then
                                    # rebuild only the dedicated Chrome process
                                    # (the persistent login profile is kept).
                                    x_retry_after = time.monotonic() + 120
                                    LOG.warning(
                                        "%s found Chromium offline; restarting "
                                        "dedicated Chrome network service",
                                        name,
                                    )
                                    time.sleep(5 * (attempt + 1))
                                    self.restart_dedicated_browser()
                                elif self._transient_navigation_error(exc):
                                    # Restarting Chromium cannot repair a host
                                    # network/proxy interruption and discards
                                    # otherwise healthy hydrated tabs. Preserve
                                    # the browser, allow transport recovery, and
                                    # retry with the freshly allocated page.
                                    LOG.warning(
                                        "%s hit a transient network failure; preserving Chrome and retrying",
                                        name,
                                    )
                                    time.sleep(5 * (attempt + 1))
                                else:
                                    # A nominally connected CDP session does
                                    # not prove Chromium's network service or
                                    # hydrated app shell is healthy.  The
                                    # collector has already exhausted its own
                                    # bounded retries, so recreate the
                                    # dedicated browser before the final
                                    # platform attempt.  The persistent profile
                                    # (and login state) is preserved.
                                    LOG.warning(
                                        "%s failed after bounded retries; restarting dedicated Chrome",
                                        name,
                                    )
                                    self.restart_dedicated_browser()
                            except Exception as reconnect_exc:
                                errors.append(f"browser recovery failed: {reconnect_exc}")
                                LOG.exception("%s browser recovery failed", name)
                                break
                if name not in result["sources"]:
                    result["errors"][name] = errors[-1]
                    result["updatedAt"] = _utcnow().isoformat()
                    save_json(HEALTH, result)
                capture_x_continuity(f"after-{name}", force=True)
        finally:
            self.progress_callback = None
        if x_snapshots:
            result["xContinuitySnapshots"] = x_snapshots
        result["status"] = "complete"
        result["updatedAt"] = _utcnow().isoformat()
        result["durationSeconds"] = round((_utcnow() - started_at).total_seconds(), 3)
        save_json(HEALTH, result)
        return result


def _consume(*, timeout: int = 120) -> bool:
    """Consume batches without allowing an API stall to stop future scans."""
    try:
        completed = subprocess.run([
            sys.executable, "-u", "-m",
            "tagagent.agents.social.callout_monitor", "--require-complete",
        ], cwd=config.PKG_ROOT, check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        LOG.error("callout_monitor exceeded %ss; continuing the browser schedule", timeout)
        return False
    if completed.returncode:
        LOG.error("callout_monitor exited with status %s", completed.returncode)
        return False
    return True


def _external_consumer_running() -> bool:
    """Detect an importer owned by a previous Worker/heartbeat process."""
    return busy("importer")


def _start_consumer(
    current: subprocess.Popen | None,
    started_at: float | None,
    *, timeout: int = 480,
) -> tuple[subprocess.Popen | None, float | None]:
    """Keep one non-blocking TagAI importer without delaying Feed capture."""
    if current is not None and current.poll() is None:
        if started_at is not None and time.monotonic() - started_at <= timeout:
            return current, started_at
        LOG.error("callout_monitor exceeded %ss; replacing the stalled importer", timeout)
        current.terminate()
        try:
            current.wait(timeout=10)
        except subprocess.TimeoutExpired:
            current.kill()
    elif _external_consumer_running():
        # LaunchAgent restarts lose the local Popen handle while the child
        # importer continues. Do not create a second process over the same
        # ledger/window-state files; retry after the external process exits.
        return None, None
    process = subprocess.Popen([
        sys.executable, "-u", "-m",
        "tagagent.agents.social.callout_monitor", "--require-complete",
    ], cwd=config.PKG_ROOT)
    return process, time.monotonic()


def _reap_consumer(
    current: subprocess.Popen | None,
    started_at: float | None,
    *,
    timeout: int,
) -> tuple[subprocess.Popen | None, float | None, bool]:
    """Poll and promptly terminate a Worker-owned stalled importer.

    This runs on every scheduler tick, not just at the next 10-minute browser
    cycle. Therefore a 480-second import deadline is an actual deadline rather
    than a deadline plus as much as one full collection interval.
    """
    if current is None:
        return None, None, False
    if current.poll() is not None:
        return None, None, False
    if started_at is None or time.monotonic() - started_at <= timeout:
        return current, started_at, False
    LOG.error("callout_monitor exceeded %ss; terminating the stalled importer", timeout)
    current.terminate()
    try:
        current.wait(timeout=10)
    except subprocess.TimeoutExpired:
        current.kill()
        current.wait(timeout=10)
    return None, None, True


def _cycle_ready_for_import(result: dict, selected: list[str]) -> bool:
    """Return whether the cycle persisted anything the importer can consume.

    The importer deliberately publishes records from an incomplete source
    batch while keeping that source's recovery cursor open.  Requiring every
    platform to reach its boundary here couples otherwise independent feeds:
    a long GMGN recovery scan would prevent a complete FOMO batch from being
    published.  Start the importer after any selected source was persisted;
    ``--require-complete`` can still report the incomplete overall cycle after
    consuming the usable records.
    """
    if result.get("status") != "complete":
        return False
    sources = result.get("sources")
    if not isinstance(sources, dict):
        return False
    for source in selected:
        summary = sources.get(source)
        if isinstance(summary, dict) and summary.get("status") in {"accepted", "incomplete"}:
            return True
    return False


def _abort_stalled_cycle(started_at: str, timeout_seconds: int) -> None:
    """Record a hard cycle timeout and exit for LaunchAgent recovery.

    Playwright timeouts are cooperative and cannot always interrupt a wedged
    CDP transport.  A daemon timer remains independent of the sync Playwright
    greenlet; exiting lets launchd recreate both Python and the dedicated
    Chrome connection instead of leaving a permanently live-looking process.
    """
    now = _utcnow().isoformat()
    health = load_json(HEALTH, {})
    if not isinstance(health, dict):
        health = {}
    errors = health.get("errors")
    if not isinstance(errors, dict):
        errors = {}
    errors["worker"] = (
        f"browser collection cycle exceeded {timeout_seconds}s; "
        "exiting for LaunchAgent recovery"
    )
    health.update({
        "at": health.get("at") or started_at,
        "updatedAt": now,
        "status": "failed",
        "errors": errors,
        "timedOut": True,
    })
    save_json(HEALTH, health)
    LOG.critical(errors["worker"])
    os._exit(75)


def main() -> None:
    parser = argparse.ArgumentParser(description="Persistent FOMO/GMGN/Pump browser Feed worker")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument(
        "--interval",
        type=int,
        default=config.SOCIAL_CALLOUT_INTERVAL_SECONDS,
    )
    parser.add_argument("--source", choices=("fomo", "gmgn", "pump"))
    parser.add_argument("--window-seconds", type=int)
    parser.add_argument(
        "--x-poll-interval", type=int, default=GMGN_X_POLL_INTERVAL_SECONDS,
        help="seconds between lightweight GMGN CA-only X snapshots",
    )
    parser.add_argument("--headless", action="store_true")
    parser.add_argument(
        "--cycle-timeout",
        type=int,
        help="hard seconds allowed for one full browser cycle",
    )
    parser.add_argument(
        "--import-timeout",
        type=int,
        default=480,
        help="hard seconds allowed for one TagAI import process",
    )
    args = parser.parse_args()
    with singleton("worker"):
        _run_worker(args)


def _run_worker(args):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    worker = PersistentFeedWorker(headless=args.headless)
    worker.start()
    consumer: subprocess.Popen | None = None
    consumer_started_at: float | None = None
    try:
        next_full_scan = time.monotonic()
        next_x_snapshot = next_full_scan + args.x_poll_interval
        while True:
            consumer, consumer_started_at, importer_timed_out = _reap_consumer(
                consumer,
                consumer_started_at,
                timeout=max(60, int(args.import_timeout)),
            )
            if importer_timed_out:
                health = load_json(HEALTH, {})
                if isinstance(health, dict):
                    health["updatedAt"] = _utcnow().isoformat()
                    health["lastImporterTimeoutAt"] = health["updatedAt"]
                    health["lastImporterError"] = (
                        f"callout_monitor exceeded {max(60, int(args.import_timeout))}s; "
                        "terminated for next-cycle retry"
                    )
                    save_json(HEALTH, health)
            now = time.monotonic()
            if now >= next_full_scan:
                cycle_started = now
                cycle_started_at = _utcnow().isoformat()
                cycle_timeout = int(args.cycle_timeout or max(
                    180, min(540, args.interval - 30 if args.interval > 210 else args.interval)
                ))
                watchdog = threading.Timer(
                    cycle_timeout,
                    _abort_stalled_cycle,
                    args=(cycle_started_at, cycle_timeout),
                )
                watchdog.daemon = True
                watchdog.start()
                try:
                    cycle_result = worker.cycle(
                        source=args.source, window_seconds=args.window_seconds
                    )
                    print(json.dumps(cycle_result, ensure_ascii=False), flush=True)
                finally:
                    watchdog.cancel()
                if not args.loop:
                    break
                selected = [args.source] if args.source else ["fomo", "gmgn", "pump"]
                if _cycle_ready_for_import(cycle_result, selected):
                    consumer, consumer_started_at = _start_consumer(
                        consumer, consumer_started_at,
                        timeout=max(60, int(args.import_timeout)),
                    )
                else:
                    LOG.warning(
                        "browser cycle persisted no usable batch for %s; importer not started",
                        ",".join(selected),
                    )
                next_full_scan = cycle_started + args.interval
                continue
            if args.source is None and now >= next_x_snapshot:
                window = int(
                    args.window_seconds
                    or _effective_lookback(int(time.time()), "gmgn")
                )
                # A standalone checkpoint runs outside the full-cycle watchdog.
                # CDP page.evaluate/navigation can wedge without honoring its
                # own timeout, leaving a live-looking LaunchAgent that never
                # schedules FOMO/GMGN/Pump again. Bound the entire checkpoint.
                snapshot_started_at = _utcnow().isoformat()
                snapshot_timeout = max(60, min(120, args.interval // 4))
                snapshot_watchdog = threading.Timer(
                    snapshot_timeout,
                    _abort_stalled_cycle,
                    args=(snapshot_started_at, snapshot_timeout),
                )
                snapshot_watchdog.daemon = True
                snapshot_watchdog.start()
                try:
                    batch = worker.collect_gmgn_x_checkpoint_resilient(
                        window, _eligible(),
                    )
                    persisted = persist_batch(batch)
                    summary = {
                        "at": _utcnow().isoformat(), "windowSeconds": window,
                        "gmgnXSnapshot": {
                            **persisted,
                            "verification": batch["verification"],
                        },
                    }
                    print(json.dumps(summary, ensure_ascii=False), flush=True)
                    health = load_json(HEALTH, {})
                    if isinstance(health, dict):
                        health["updatedAt"] = summary["at"]
                        health["lastGmgnXCheckpointAt"] = summary["at"]
                        health.pop("lastGmgnXCheckpointError", None)
                        save_json(HEALTH, health)
                    # A changed snapshot can contain a new X post or establish
                    # the missing completeness transition after recovery. Do
                    # not wait for the next ten-minute UI pass to publish it.
                    # Quiet snapshots are sidecar-only and intentionally do
                    # not wake the importer every ten seconds.
                    if persisted.get("auditAppended") is True:
                        consumer, consumer_started_at = _start_consumer(
                            consumer, consumer_started_at,
                            timeout=max(60, int(args.import_timeout)),
                        )
                except Exception as exc:
                    LOG.exception("GMGN X continuity snapshot failed")
                    health = load_json(HEALTH, {})
                    if isinstance(health, dict):
                        health["updatedAt"] = _utcnow().isoformat()
                        health["lastGmgnXCheckpointError"] = str(exc)
                        save_json(HEALTH, health)
                    try:
                        if worker._connection_was_closed(exc):
                            worker.reconnect()
                        elif worker._transient_navigation_error(exc):
                            worker.discard_page("gmgn_x")
                    except Exception:
                        LOG.exception("browser recovery after GMGN X snapshot failed")
                finally:
                    snapshot_watchdog.cancel()
                next_x_snapshot = time.monotonic() + args.x_poll_interval
                continue
            deadlines = [next_full_scan - now]
            if args.source is None:
                deadlines.append(next_x_snapshot - now)
            time.sleep(max(0.25, min(5, *deadlines)))
    finally:
        if consumer is not None and consumer.poll() is None:
            consumer.terminate()
            try:
                consumer.wait(timeout=10)
            except subprocess.TimeoutExpired:
                consumer.kill()
        worker.stop()


if __name__ == "__main__":
    main()
