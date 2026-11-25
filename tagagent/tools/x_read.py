import json
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Any

from tagagent import config
from tagagent.wiki import append_jsonl

X_READ_AUDIT_LOG_PATH = config.RUNTIME_ROOT / "logs" / "x-read-audit.jsonl"


def _xurl_env() -> dict:
    env = os.environ.copy()
    # Go's proxy selection may prefer ALL_PROXY (commonly a local SOCKS
    # endpoint) over the stable HTTP proxy.  That produced intermittent read
    # failures while the same OAuth2 request succeeded with ALL_PROXY removed.
    env.pop("ALL_PROXY", None)
    env.pop("all_proxy", None)
    if not env.get("HTTPS_PROXY") and not env.get("https_proxy"):
        for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
            env.setdefault(key, config.DEFAULT_PROXY)
    return env


def _audit(api_path: str, ok: bool, **details) -> None:
    try:
        append_jsonl(
            X_READ_AUDIT_LOG_PATH,
            {
                "at": datetime.now(timezone.utc).isoformat(),
                "api_path": api_path,
                "ok": ok,
                **details,
            },
        )
    except OSError:
        pass


def _xurl_bin(xurl_bin: str | None = None) -> str:
    return xurl_bin or os.environ.get("TAGAGENT_XURL_BIN") or config.XURL_BIN


def run_xurl_args(
    args: list[str], timeout: float = 20.0, xurl_bin: str | None = None,
    attempts: int = 3,
) -> Any | None:
    api_path = " ".join(args)
    attempts = max(1, min(5, int(attempts)))
    last_error = "unknown"
    last_returncode = None
    for attempt in range(1, attempts + 1):
        try:
            proc = subprocess.run(
                [
                    _xurl_bin(xurl_bin),
                    "--app",
                    config.X_API_APP,
                    "--auth",
                    "oauth2",
                    "--username",
                    config.X_API_USERNAME,
                    *args,
                ],
                capture_output=True,
                env=_xurl_env(),
                text=True,
                timeout=timeout,
                check=False,
            )
            last_returncode = proc.returncode
            if proc.returncode == 0:
                try:
                    parsed = json.loads(proc.stdout)
                except json.JSONDecodeError as exc:
                    last_error = f"json_decode:{exc.msg}"
                else:
                    _audit(api_path, True, attempts=attempt)
                    return parsed
            else:
                detail = str(proc.stderr or proc.stdout or "").strip().replace("\n", " ")
                last_error = f"xurl_exit:{proc.returncode}:{detail[:240]}"
        except subprocess.TimeoutExpired:
            last_error = "timeout"
        except FileNotFoundError:
            last_error = "binary_not_found"
            break
        except OSError as exc:
            last_error = f"os_error:{type(exc).__name__}:{str(exc)[:180]}"
        if attempt < attempts:
            time.sleep(0.25 * attempt)
    _audit(
        api_path, False, attempts=attempts, error=last_error,
        returncode=last_returncode,
    )
    return None


def run_xurl(
    api_path: str, timeout: float = 20.0, xurl_bin: str | None = None
) -> Any | None:
    return run_xurl_args([api_path], timeout=timeout, xurl_bin=xurl_bin)


def _clamp_max_results(max_results: int) -> int:
    return max(5, min(100, int(max_results)))


# Wider user fields on the SAME calls we already make (no extra X requests):
# TagAI forwarding needs author name/avatar/metrics, so ask for them here
# instead of ever making a dedicated user lookup.
_USER_FIELDS = "username,name,profile_image_url,public_metrics"


def fetch_mentions(max_results: int = 5) -> dict | None:
    n = _clamp_max_results(max_results)
    api_path = (
        f"/2/users/{config.AGENT_USER_ID}/mentions?max_results={n}"
        "&expansions=author_id,attachments.media_keys,referenced_tweets.id"
        "&tweet.fields=author_id,conversation_id,created_at,attachments,entities,referenced_tweets,note_tweet"
        "&media.fields=media_key,type,url,preview_image_url,width,height"
        f"&user.fields={_USER_FIELDS}"
    )
    return run_xurl(api_path)


def fetch_tweet(tweet_id: str) -> dict | None:
    api_path = (
        f"/2/tweets/{tweet_id}?expansions=author_id,attachments.media_keys,referenced_tweets.id"
        "&tweet.fields=author_id,conversation_id,created_at,attachments,entities,referenced_tweets,note_tweet"
        "&media.fields=media_key,type,url,preview_image_url,width,height"
        f"&user.fields={_USER_FIELDS}"
    )
    return run_xurl(api_path)


def fetch_user_posts(username: str, n: int = 5) -> Any | None:
    return run_xurl_args(["posts", username, "-n", str(n)])


def user_map(payload) -> dict[str, dict]:
    try:
        users = payload.get("includes", {}).get("users", [])
    except AttributeError:
        return {}
    if not isinstance(users, list):
        return {}
    return {str(user["id"]): user for user in users if isinstance(user, dict) and "id" in user}


def normalize_mention(tweet: dict, users: dict) -> dict:
    tweet_id = str(tweet["id"])
    author_id = str(tweet.get("author_id") or "")
    author = users.get(author_id) or {}
    handle = author.get("username") or ""
    if handle:
        tweet_url = f"https://x.com/{handle}/status/{tweet_id}"
    else:
        tweet_url = f"https://x.com/i/web/status/{tweet_id}"

    normalized = {
        "tweet_id": tweet_id,
        "tweet_url": tweet_url,
        "text": tweet.get("text") or "",
        "author_id": author_id,
        "author_handle": handle,
        "conversation_id": str(tweet.get("conversation_id") or tweet["id"]),
        "created_at": tweet.get("created_at"),
    }
    # X keeps the public destination of t.co links under entities.urls.  The
    # plain tweet text intentionally contains only the short URL, so dropping
    # entities loses repository identity and other continuation context.
    entity_urls = []
    entities = tweet.get("entities") or {}
    for value in entities.get("urls") or []:
        if not isinstance(value, dict):
            continue
        short_url = str(value.get("url") or "")
        expanded_url = str(value.get("unwound_url") or value.get("expanded_url") or short_url)
        if not expanded_url:
            continue
        entity_urls.append(
            {
                "url": short_url,
                "expanded_url": expanded_url,
                "display_url": str(value.get("display_url") or ""),
                "title": str(value.get("title") or ""),
                "description": str(value.get("description") or ""),
            }
        )
    if entity_urls:
        normalized["urls"] = entity_urls
        expanded_text = normalized["text"]
        for value in entity_urls:
            if value["url"]:
                expanded_text = expanded_text.replace(value["url"], value["expanded_url"])
        normalized["expanded_text"] = expanded_text
    referenced_tweets = tweet.get("referenced_tweets")
    if isinstance(referenced_tweets, list) and referenced_tweets:
        normalized["referenced_tweets"] = referenced_tweets
    # Carry the raw X user object through when the payload already had it
    # (webhook includes / widened user.fields) — free author metrics for the
    # TagAI collect payload, no dedicated user lookup ever needed.
    if author:
        normalized["author"] = author
    return normalized


def normalize_payload(payload) -> list[dict]:
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if isinstance(data, dict):
        tweets = [data]
    elif isinstance(data, list):
        tweets = data
    else:
        return []

    users = user_map(payload)
    media = {
        str(item.get("media_key")): item
        for item in (payload.get("includes", {}).get("media", []) or [])
        if isinstance(item, dict) and item.get("media_key")
    }
    included_tweets = {
        str(item.get("id")): item
        for item in (payload.get("includes", {}).get("tweets", []) or [])
        if isinstance(item, dict) and item.get("id")
    }
    normalized = []
    for tweet in tweets:
        if not isinstance(tweet, dict) or "id" not in tweet:
            continue
        try:
            item = normalize_mention(tweet, users)
            keys = ((tweet.get("attachments") or {}).get("media_keys") or [])
            attached = [media[str(key)] for key in keys if str(key) in media]
            if attached:
                item["media"] = attached
            replied_to = next(
                (
                    ref for ref in (tweet.get("referenced_tweets") or [])
                    if isinstance(ref, dict) and ref.get("type") == "replied_to"
                ),
                None,
            )
            if replied_to:
                parent_id = str(replied_to.get("id") or "")
                item["replied_to_tweet_id"] = parent_id
                parent = included_tweets.get(parent_id) or {}
                if parent.get("text"):
                    item["replied_to_text"] = str(parent["text"])
            normalized.append(item)
        except (KeyError, TypeError):
            continue
    return normalized
