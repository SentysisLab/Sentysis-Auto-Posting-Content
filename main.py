"""
WatcherGuru -> X (Twitter) + Threads auto-poster.

Reads the latest posts from the WatcherGuru Telegram channel and publishes
them to our X and Threads accounts through the Zernio API.

Run:
    python main.py            # publish for real
    python main.py --dry-run  # show what would be posted, publish nothing
"""

import argparse
import asyncio
import json
import mimetypes
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from zernio import Zernio, ZernioAPIError

load_dotenv()


# ============================================================================
# Config
# ============================================================================
def env(name: str, default: str = "") -> str:
    """Read an env var, also accepting its lowercase spelling."""
    return os.getenv(name) or os.getenv(name.lower()) or default


TELEGRAM_API_ID = int(env("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = env("TELEGRAM_API_HASH")
ZERNIO_API_KEY = env("ZERNIO_API_KEY")

# Optional overrides; if unset, account IDs are looked up from Zernio.
TWITTER_ACCOUNT_ID = env("ZERNIO_TWITTER_ACCOUNT_ID")
THREADS_ACCOUNT_ID = env("ZERNIO_THREADS_ACCOUNT_ID")

SOURCE_CHAT = "WatcherGuru"
SESSION_FILE = "telegram_session"
RESULT_FILE = Path("results.json")
FETCH_LIMIT = 20                # latest Telegram messages to check per run
MAX_POSTS_PER_RUN = max(1, int(env("MAX_POSTS_PER_RUN", "2")))  # newest N get posted; older backlog is skipped
MIN_WORDS = 3                   # skip very short messages
SLEEP_BETWEEN_POSTS_SEC = 2

# Platform limits (docs.zernio.com/platforms/*)
TEXT_LIMIT = {"twitter": 280, "threads": 500}

# Call to action added to the end of every post
CTA = "Create your Narrative, stake conviction, and launch token within 48 hours."
MAX_IMAGES = 4                  # X allows 4 images; Threads allows more
TEMP_DIR = Path("temp_media")


# ============================================================================
# Text helpers
# ============================================================================
def normalize(s: str) -> str:
    """Normalize text for de-duplication."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def clean_caption(text: str) -> str:
    """Strip mentions, links and markdown links from the Telegram text."""
    cleaned = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)  # [label](url) -> label
    cleaned = cleaned.replace("**", "").replace("__", "")      # markdown bold
    cleaned = re.sub(r"https?://\S+", "", cleaned)
    cleaned = re.sub(r"@WatcherGuru\b", "", cleaned, flags=re.IGNORECASE)  # source credit
    cleaned = re.sub(r"@(\w+)", r"\1", cleaned)                            # @PayPal -> PayPal (no tagging)
    cleaned = re.sub(r"^\s*(JUST IN|BREAKING)\s*:\s*", "", cleaned, flags=re.IGNORECASE)  # drop news labels
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n\s*\n+", "\n\n", cleaned)
    return cleaned.strip()


def x_len(text: str) -> int:
    """Approximate X's weighted length: emoji and most non-Latin characters count as 2."""
    return sum(2 if ord(ch) > 0x10FF else 1 for ch in text)


def fit(text: str, limit: int, measure=len) -> str:
    """Trim text to a character limit, on a word boundary."""
    if measure(text) <= limit:
        return text
    cut = text
    while cut and measure(cut + "…") > limit:
        cut = cut[:-1]
    cut = cut.rsplit(" ", 1)[0].rstrip() if " " in cut else cut
    return cut + "…"


def one_cashtag(text: str) -> str:
    """X allows one cashtag per post: keep the first $TICKER, drop the $ from the rest ($67,000 is untouched)."""
    seen = False

    def repl(m: re.Match) -> str:
        nonlocal seen
        if seen:
            return m.group(1)
        seen = True
        return m.group(0)

    return re.sub(r"(?<![\w$])\$([A-Za-z][A-Za-z0-9_]{0,9})\b", repl, text)


def compose(caption: str, platform: str) -> str:
    """Caption + CTA, trimming only the caption so the CTA is always shown in full."""
    if platform == "twitter":
        caption = one_cashtag(caption)
    measure = x_len if platform == "twitter" else len
    suffix = f"\n\n{CTA}"
    body = fit(caption, TEXT_LIMIT[platform] - measure(suffix), measure)
    return body + suffix


# ============================================================================
# Results store (results.json)
# ============================================================================
def load_results() -> list[dict]:
    try:
        return json.loads(RESULT_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_results(entries: list[dict]) -> None:
    RESULT_FILE.write_text(json.dumps(entries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def upsert_result(entries: list[dict], entry: dict) -> None:
    """Replace the entry for the same Telegram message, or append it."""
    for i, existing in enumerate(entries):
        if existing.get("telegram_id") == entry["telegram_id"]:
            entries[i] = entry
            return
    entries.append(entry)


def already_done(entries: list[dict]) -> tuple[set[int], set[str]]:
    """IDs and texts that were published (fully or partly) — never repost those."""
    done = [e for e in entries if e.get("status") in ("published", "partial", "duplicate", "skipped")]
    return {e["telegram_id"] for e in done}, {normalize(e.get("original_text", "")) for e in done}


# ============================================================================
# Telegram source
# ============================================================================
@dataclass
class SourcePost:
    telegram_id: int
    text: str
    images: list[Path] = field(default_factory=list)
    videos: list[Path] = field(default_factory=list)

    def cleanup(self) -> None:
        for p in self.images + self.videos:
            p.unlink(missing_ok=True)


async def download_media(client: TelegramClient, messages: list) -> tuple[list[Path], list[Path]]:
    images, videos = [], []
    TEMP_DIR.mkdir(exist_ok=True)
    for m in messages:
        try:
            if isinstance(m.media, MessageMediaPhoto):
                path = TEMP_DIR / f"{m.id}.jpg"
                await client.download_media(m.media, file=str(path))
                images.append(path)
            elif isinstance(m.media, MessageMediaDocument) and "video" in (m.file.mime_type or ""):
                path = TEMP_DIR / f"{m.id}.mp4"
                await client.download_media(m.media, file=str(path))
                videos.append(path)
        except Exception as e:
            print(f"[Telegram] Media download failed for message {m.id}: {e}")
    return images, videos


async def album_messages(client: TelegramClient, msg) -> list:
    """All messages in the same album as msg, in order."""
    nearby = await client.get_messages(SOURCE_CHAT, ids=list(range(msg.id - 10, msg.id + 11)))
    return sorted((m for m in nearby if m and m.grouped_id == msg.grouped_id), key=lambda m: m.id)


# ============================================================================
# Zernio publisher
# ============================================================================
class Publisher:
    def __init__(self, dry_run: bool = False):
        self.client = Zernio(api_key=ZERNIO_API_KEY)
        self.dry_run = dry_run
        self.accounts = self._resolve_accounts()

    def _resolve_accounts(self) -> dict[str, str]:
        accounts = {"twitter": TWITTER_ACCOUNT_ID, "threads": THREADS_ACCOUNT_ID}
        if not all(accounts.values()):
            for a in self.client.accounts.list_accounts().get("accounts", []):
                platform = a.get("platform")
                if platform in accounts and not accounts[platform] and a.get("isActive"):
                    accounts[platform] = a["_id"]
        missing = [p for p, acc in accounts.items() if not acc]
        if missing:
            raise RuntimeError(f"No connected Zernio account for: {', '.join(missing)}")
        return accounts

    def upload(self, path: Path) -> str:
        """Upload a local file via presigned URL (works for any size). Returns its public URL."""
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        presign = self.client.media.get_media_presigned_url(
            filename=path.name, content_type=content_type, size=path.stat().st_size
        )
        with path.open("rb") as f:
            r = requests.put(presign["uploadUrl"], data=f, headers={"Content-Type": content_type}, timeout=300)
        r.raise_for_status()
        return presign["publicUrl"]

    def publish(self, post: SourcePost) -> dict:
        # X can't mix video and images: prefer a single video, else up to 4 images.
        if post.videos:
            media = [("video", post.videos[0])]
        else:
            media = [("image", p) for p in post.images[:MAX_IMAGES]]
        content = compose(post.text, "threads")
        platforms = []
        for platform, account_id in self.accounts.items():
            item = {"platform": platform, "accountId": account_id}
            text = compose(post.text, platform)
            if text != content:
                item["customContent"] = text
            platforms.append(item)

        if self.dry_run:  # Zernio's own dry_run only supports TikTok, so stop here
            print(f"[DRY RUN] would post {len(media)} media item(s): {content!r}")
            return {"post": {"status": "dry_run", "platforms": []}}

        media_items = [{"type": kind, "url": self.upload(path)} for kind, path in media]
        return self.client.posts.create_post(
            content=content,
            media_items=media_items or None,
            platforms=platforms,
            publish_now=True,
            idempotency_key=f"watcherguru-{post.telegram_id}",
        )


def summarize(response: dict) -> tuple[str, dict]:
    """Overall status + per-platform status/url/error from a create_post response."""
    p = response.get("post", {})
    per_platform = {}
    for item in p.get("platforms", []):
        per_platform[item.get("platform")] = {
            "status": item.get("status"),
            "url": item.get("platformPostUrl"),
            "error": item.get("errorMessage"),
        }
    return p.get("status", "unknown"), per_platform


# ============================================================================
# Main
# ============================================================================
async def run(dry_run: bool) -> None:
    if not (TELEGRAM_API_ID and TELEGRAM_API_HASH and ZERNIO_API_KEY):
        raise RuntimeError("Missing env vars: TELEGRAM_API_ID, TELEGRAM_API_HASH, ZERNIO_API_KEY")

    publisher = Publisher(dry_run=dry_run)
    results = load_results()
    done_ids, done_texts = already_done(results)
    seen_albums = set()

    client = TelegramClient(SESSION_FILE, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start()
    try:
        # Collect new posts, oldest first, so they go out in the same order as on Telegram.
        candidates = []
        messages = [m async for m in client.iter_messages(SOURCE_CHAT, limit=FETCH_LIMIT)]
        for msg in reversed(messages):
            original = (msg.raw_text or "").strip()
            if len(original.split()) < MIN_WORDS:
                continue
            if msg.id in done_ids or normalize(original) in done_texts:
                continue
            if msg.grouped_id:
                if msg.grouped_id in seen_albums:
                    continue
                seen_albums.add(msg.grouped_id)
            caption = clean_caption(original)
            if caption:
                candidates.append((msg, original, caption))

        # Only post the newest few; older backlog is marked skipped so it never floods the feed.
        backlog, candidates = candidates[:-MAX_POSTS_PER_RUN], candidates[-MAX_POSTS_PER_RUN:]
        for msg, original, caption in backlog:
            print(f"[SKIPPED] backlog: {caption[:60]}...")
            if not dry_run:
                upsert_result(results, {"telegram_id": msg.id, "original_text": original, "caption": caption,
                                        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                        "status": "skipped"})
        if backlog and not dry_run:
            save_results(results)

        for msg, original, caption in candidates:
            media_msgs = await album_messages(client, msg) if msg.grouped_id else [msg]
            images, videos = await download_media(client, media_msgs)
            post = SourcePost(msg.id, caption, images, videos)

            entry = {
                "telegram_id": msg.id,
                "original_text": original,
                "caption": caption,
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            try:
                response = publisher.publish(post)
                status, per_platform = summarize(response)
                entry.update(status=status, zernio_post_id=response.get("post", {}).get("_id"),
                             platforms=per_platform)
                print(f"[{status.upper()}] {caption[:60]}...")
                for platform, info in per_platform.items():
                    if info.get("error"):
                        print(f"    {platform}: {info['error']}")
            except ZernioAPIError as e:
                status = "duplicate" if e.status_code == 409 else "failed"
                entry.update(status=status, error=str(e))
                print(f"[{status.upper()}] {caption[:60]}... -> {e}")
            except Exception as e:
                entry.update(status="failed", error=str(e))
                print(f"[FAILED] {caption[:60]}... -> {e}")
            finally:
                post.cleanup()

            if not dry_run:
                upsert_result(results, entry)
                save_results(results)
            time.sleep(SLEEP_BETWEEN_POSTS_SEC)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="show what would be posted without publishing")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run or env("DRY_RUN") == "1"))
