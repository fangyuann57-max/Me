"""
Card List Cleaner Bot — aiogram v3 (Ready to Use — single file)

Getting started:
  1) Add BOT_TOKEN + ADMIN_IDS + API_ID + API_HASH to the .env file (or use start.bat)
  2) pip install -r requirements.txt
  3) python bot.py

Features: clean/extract, SPLIT (by count/brand/country), FILTER BIN,
MIX, COMBINE, FORMAT, CLEAN (CC|MM|YY|CVC only), Admin/VIP lock, progress bar,
rate-limit guard, auto-backup, broadcast, user history, ban/unban, feedback,
file rename, usage stats dashboard, BIN lookup, proxy checker,
CC scraper (/scr). UI: /start feature hub, inline navigation with Back/Home.
"""

from __future__ import annotations

import asyncio
import csv
import html
import io
import json
import logging
import os
import random
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv

load_dotenv()

# 🎯 SCR Userbot အတွက် မဖြစ်မနေ ထည့်သွင်းရမည့်အပိုင်း
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    RPCError,
    AuthKeyDuplicatedError,
    RpcCallFailError,
)
from telethon.errors.rpcerrorlist import ServerError, TimedOutError
API_ID   = int(os.getenv("API_ID", "36485237"))
API_HASH = os.getenv("API_HASH", "206e64a95f2b4acece834e8eea933cf2")
if not os.getenv("API_ID") or not os.getenv("API_HASH"):
    logging.getLogger(__name__).warning(
        "API_ID / API_HASH not set in .env — falling back to hardcoded defaults. "
        "Move these to .env: this is a Telethon account credential and should "
        "never ship inside the source file."
    )

# Concurrency control — StringSession supports multiple *simultaneous*
# connections from the same account, so we no longer need one global lock.
# Instead: a semaphore caps how many /scr jobs run at once (protects against
# too many parallel Telegram API connections / flood risk), and each job gets
# its own TelegramClient so users A, B, C don't block each other.
_SCR_MAX_CONCURRENT = 1   # regular (non-Premium) accounts get hit with SEVERE
                          # FloodWait (10-30+ min) when 2-3 connections hammer
                          # the API at once — FloodWait is account-wide, not
                          # per-connection, so concurrency makes it worse, not
                          # better. Users B/C queue instead of running parallel.
_SCR_SEMAPHORE: asyncio.Semaphore | None = None

def _get_scr_semaphore() -> asyncio.Semaphore:
    global _SCR_SEMAPHORE
    if _SCR_SEMAPHORE is None:
        _SCR_SEMAPHORE = asyncio.Semaphore(_SCR_MAX_CONCURRENT)
    return _SCR_SEMAPHORE

# Live scrapes, keyed by user id, so /cancel can stop a running job cleanly.
_SCR_ACTIVE: dict[int, asyncio.Event] = {}
_SCR_SEND_RETRY = 3   # retries for a single Telegram send (own FloodWait)

# ── Robustness / reliability tuning ──────────────────────────────────────────
_SCR_PACE_MAX        = 8.0    # upper bound for the adaptive per-request delay
_SCR_TRANSIENT_RETRY = 4      # retries for network/RPC errors before giving up
_SCR_FLOOD_BAIL_SEC  = 300    # FloodWait longer than this → stop early, keep cards


def _scr_register(uid: int) -> asyncio.Event:
    """Register a cancellation token for this user's scrape (replacing any old one)."""
    ev = asyncio.Event()
    _SCR_ACTIVE[uid] = ev
    return ev


def _scr_unregister(uid: int) -> None:
    _SCR_ACTIVE.pop(uid, None)


def _scr_cancel(uid: int) -> bool:
    """Signal cancellation for a running scrape. Returns True if one was active."""
    ev = _SCR_ACTIVE.get(uid)
    if ev is not None:
        ev.set()
        return True
    return False


async def _scr_sleep_cancellable(seconds: float, cancel_ev: asyncio.Event) -> bool:
    """Sleep for `seconds`, but wake immediately if the scrape is cancelled.

    Returns True if the full duration elapsed, False if it was cancelled early.
    This keeps /cancel responsive even during long FloodWait back-offs.
    """
    if seconds <= 0:
        return not cancel_ev.is_set()
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True
        if cancel_ev.is_set():
            return False
        try:
            await asyncio.wait_for(cancel_ev.wait(), timeout=min(remaining, 1.0))
            return False   # event was set
        except asyncio.TimeoutError:
            continue


async def _scr_send_document(
    message: Message,
    filename: str,
    lines: list[str],
    caption: str,
) -> bool:
    """Send a result file, retrying on FloodWait. Returns False only if every
    attempt failed (so callers can decide whether to keep going)."""
    body = "\n".join(lines).encode("utf-8")
    for attempt in range(_SCR_SEND_RETRY):
        try:
            await message.answer_document(
                BufferedInputFile(body, filename=filename),
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
            return True
        except FloodWaitError as fw:
            wait = min(fw.seconds + 2, _SCR_FLOOD_BAIL_SEC)
            logger.warning("send FloodWait %ss (attempt %d)", wait, attempt + 1)
            await asyncio.sleep(wait)
        except Exception as e:
            logger.debug("send document attempt %d failed: %s", attempt + 1, e)
            await asyncio.sleep(1.5 * (attempt + 1))
    return False

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "8714885475:AAE6R6SR7nLckEIBd1oVEs1FT1uwIVjNEDE").strip()
ACCESS_DENIED = (
    "🔒 <b>Access Denied</b>\n\n"
    "This bot is for <b>Admin / VIP</b> access only.\n"
    "Send your ID to an Admin to request approval.\n\n"
    "🆔 Your ID: <code>{uid}</code>"
)
BIN_CSV_PATH    = Path(__file__).with_name("bins.csv")
BIN_CACHE_FILE  = Path(__file__).with_name("bin_cache.json")
ACCESS_FILE     = Path(__file__).with_name("allowed_users.json")
FORCE_JOIN_FILE = Path(__file__).with_name("force_join.json")   # /setjoin command data
# ── BIN lookup providers (all queried concurrently, first hit wins per field) ──
# Trust order:  HandyAPI  >  Binlist.io  >  Binlist.net  >  Bincheck
HANDYAPI_BIN_URL = "https://data.handyapi.com/bin/{bin}"        # free, no key
BINLIST_IO_URL   = "https://binlist.io/lookup/{bin}"            # free, no key — reliable
BINLIST_URL      = "https://lookup.binlist.net/{bin}"           # legacy — often down
BINCHECK_URL     = "https://api.bincheck.io/api/{bin}"          # free, no key needed
MAX_FILE_LINES = 100_000
MAX_FILE_BYTES = 50 * 1024 * 1024
MAX_BIN_LOOKUPS_PER_JOB = 20_000   # a job may look up nearly every unique BIN.
                                   # (was 500 — that silently dumped every card
                                   # whose BIN fell outside the first 500 into
                                   # the "Unknown" bucket, which is why Country
                                   # splits often showed 70%+ Unknown.)
_BIN_LOOKUP_CONCURRENCY = 20       # parallel BIN lookups per job
_BIN_RETRY_PASS = True             # after the first pass, retry every BIN that
                                   # came back unresolved (cheap — they are the
                                   # minority and often succeed on a 2nd try).
BIN_CACHE_TTL_SEC = 86_400 * 30   # 30 days — re-fetch stale entries
THREAD_POOL = ThreadPoolExecutor(max_workers=4)

# Shared HTTP session — reused by every command instead of each one opening
# its own connection pool. Created once in main() at startup, closed at
# shutdown. Falls back to a fresh session only if something calls a helper
# before startup (should not normally happen).
HTTP_SESSION: aiohttp.ClientSession | None = None


def get_http_session() -> aiohttp.ClientSession:
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        HTTP_SESSION = aiohttp.ClientSession()
    return HTTP_SESSION

BIN_CACHE: dict[str, dict[str, str]] = {}
LOCAL_BIN_DB: dict[str, dict[str, str]] = {}
# FSM clear မလုပ်ဘဲ session မပျောက်အောင် backup
USER_CARDS: dict[int, list[str]] = {}
USER_PROFILES: dict[int, str] = {}          # user_id -> latest Telegram username

USER_HISTORY: dict[int, list[dict]] = {}          # per-user upload history
RATE_LIMIT: dict[int, list[float]] = {}           # rate-limit timestamps
BANNED_IDS: set[int] = set()                      # banned user IDs
FEEDBACK_LOG: list[dict] = []                     # feedback from users
BOT_START_TIME: float = time.monotonic()          # uptime tracking
RATE_LIMIT_MAX = 10                               # max uploads per window
RATE_LIMIT_WINDOW = 60.0                          # seconds
BACKUP_DIR = Path(__file__).with_name("backups") # auto-backup folder

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
router = Router()

try:
    BACKUP_DIR.mkdir(exist_ok=True)
except Exception as e:
    logging.error("Failed to create backups directory: %s", e)

def _he(text: str) -> str:
    """HTML-escape a string for safe use in Telegram HTML messages."""
    return html.escape(str(text))

def _format_uptime() -> str:
    """Return bot uptime as 'Xh Ym Zs' string."""
    sec = int(time.monotonic() - BOT_START_TIME)
    h, rem = divmod(sec, 3600)
    m, s   = divmod(rem, 60)
    return f"{h}h {m}m {s}s"

# Admin / VIP Access Control
def _parse_id_list(raw: str) -> set[int]:
    out: set[int] = set()
    for part in (raw or "").replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


ADMIN_IDS: set[int] = _parse_id_list(os.getenv("ADMIN_IDS", "5566718291"))
ENV_RESTRICTED = os.getenv("ACCESS_RESTRICTED", "true").strip().lower() in ("1", "true", "yes", "on")
_vip_ids: set[int] = set()
_restricted: bool = ENV_RESTRICTED


def load_access() -> None:
    global _vip_ids, _restricted
    if not ACCESS_FILE.is_file():
        _restricted = ENV_RESTRICTED
        _vip_ids = set()
        save_access()
        return
    try:
        data = json.loads(ACCESS_FILE.read_text(encoding="utf-8"))
        _restricted = bool(data.get("restricted", ENV_RESTRICTED))
        _vip_ids = {int(x) for x in data.get("vip", []) if str(x).isdigit()}
    except Exception:
        logger.exception("access file load failed")
        _restricted = ENV_RESTRICTED
        _vip_ids = set()


def save_access() -> None:
    ACCESS_FILE.write_text(
        json.dumps({"restricted": _restricted, "vip": sorted(_vip_ids)}, indent=2),
        encoding="utf-8",
    )


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def is_vip(user_id: int) -> bool:
    return user_id in _vip_ids


def is_restricted() -> bool:
    return _restricted


def set_restricted(value: bool) -> None:
    global _restricted
    _restricted = value
    save_access()


def role_label(user_id: int) -> str:
    if is_admin(user_id):
        return "👑 Admin"
    if is_vip(user_id):
        return "⭐ VIP"
    if not _restricted:
        return "🌐 Public"
    return "🚫 No access"


def can_use_bot(user_id: int) -> bool:
    if is_admin(user_id) or is_vip(user_id):
        return True
    return not _restricted


def add_vip(user_id: int) -> bool:
    if user_id in _vip_ids:
        return False
    _vip_ids.add(user_id)
    save_access()
    return True


def remove_vip(user_id: int) -> bool:
    if user_id not in _vip_ids:
        return False
    _vip_ids.discard(user_id)
    save_access()
    return True


def list_vip() -> list[int]:
    return sorted(_vip_ids)


def access_stats() -> str:
    mode = "🔒 Private (Admin+VIP)" if _restricted else "🌐 Public (everyone)"
    admins = ", ".join(str(x) for x in sorted(ADMIN_IDS)) or "(none — set ADMIN_IDS in .env)"
    vips = ", ".join(str(x) for x in list_vip()) or "(empty)"
    return (
        f"<b>Access mode:</b> {mode}\n"
        f"<b>Admins (.env):</b> <code>{admins}</code>\n"
        f"<b>VIP ({len(_vip_ids)}):</b> <code>{vips}</code>"
    )


# ─── Force Join Channel (managed entirely via /setjoin — no .env needed) ──────
# FORCE_JOIN_CHANNEL: id (int) or @username used for get_chat_member() calls.
# FORCE_JOIN_LINK:    a clickable t.me link shown on the Join button.
# FORCE_JOIN_TITLE:   display name shown in messages / /joininfo.
FORCE_JOIN_CHANNEL: "int | str | None" = None
FORCE_JOIN_LINK: str | None = None
FORCE_JOIN_TITLE: str | None = None


def load_force_join() -> None:
    """Load /setjoin data from disk at startup."""
    global FORCE_JOIN_CHANNEL, FORCE_JOIN_LINK, FORCE_JOIN_TITLE
    if not FORCE_JOIN_FILE.is_file():
        return
    try:
        data = json.loads(FORCE_JOIN_FILE.read_text(encoding="utf-8"))
        FORCE_JOIN_CHANNEL = data.get("channel")
        FORCE_JOIN_LINK    = data.get("link")
        FORCE_JOIN_TITLE   = data.get("title")
    except Exception:
        logger.exception("force_join.json load failed")
        FORCE_JOIN_CHANNEL = FORCE_JOIN_LINK = FORCE_JOIN_TITLE = None


def _save_force_join() -> None:
    try:
        FORCE_JOIN_FILE.write_text(
            json.dumps(
                {"channel": FORCE_JOIN_CHANNEL, "link": FORCE_JOIN_LINK, "title": FORCE_JOIN_TITLE},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception as e:
        logger.debug("force_join.json save failed: %s", e)


def set_force_join_channel(channel: "int | str", link: str | None, title: str | None) -> None:
    global FORCE_JOIN_CHANNEL, FORCE_JOIN_LINK, FORCE_JOIN_TITLE
    FORCE_JOIN_CHANNEL, FORCE_JOIN_LINK, FORCE_JOIN_TITLE = channel, link, title
    _save_force_join()


def clear_force_join_channel() -> None:
    global FORCE_JOIN_CHANNEL, FORCE_JOIN_LINK, FORCE_JOIN_TITLE
    FORCE_JOIN_CHANNEL = FORCE_JOIN_LINK = FORCE_JOIN_TITLE = None
    _save_force_join()
# ──────────────────────────────────────────────────────────────────────────────


# Progress Bar UI
def render_bar(percent: float, width: int = 10) -> str:
    percent = max(0.0, min(100.0, percent))
    filled = min(width, max(0, int(round(width * percent / 100.0))))
    return f"[{'■' * filled}{'□' * (width - filled)}] {percent:.0f}%"


class ProgressMessage:
    def __init__(self, message: Message, title: str, *, min_interval: float = 0.65) -> None:
        self.message = message
        self.title = title
        self.min_interval = min_interval
        self._last_edit = 0.0

    def _build(self, done: int, total: int, subtitle: str = "") -> str:
        total = max(total, 1)
        bar = render_bar(done / total * 100.0)
        text = f"⏳ <b>{self.title}</b>\n<code>{bar}</code>\n📦 <b>{done:,}</b> / <b>{total:,}</b>"
        if subtitle:
            text += f"\n<i>{subtitle}</i>"
        return text

    async def update(self, done: int, total: int, subtitle: str = "", *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and done < total and (now - self._last_edit) < self.min_interval:
            return
        self._last_edit = now
        try:
            await self.message.edit_text(self._build(done, total, subtitle), parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.debug("progress update skipped: %s", e)

    async def finish(self, text: str) -> None:
        try:
            await self.message.edit_text(text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.debug("progress finish skipped: %s", e)

    async def delete(self) -> None:
        try:
            await self.message.delete()
        except Exception as e:
            logger.debug("progress delete skipped: %s", e)


# Card parsing

# ပိုမိုလျှင်မြန် extract — | : / space tab နှင့် line တစ်ကြောင်းလုံး
CC_PATTERNS = [
    # NOTE: a second whitespace-only pattern used to exist here, but the
    # separator class below already includes \s — it was a strict subset
    # that never matched anything new and just doubled scan time per file.
    re.compile(
        r"(?<!\d)(\d{13,19})[\s|:,/\\-]+(\d{1,2})[\s|:,/\\-]+(\d{2,4})[\s|:,/\\-]+(\d{3,4})(?!\d)",
        re.I,
    ),
]

CB_SPLIT = "act:split"
CB_SPLIT_COUNT = "act:split:count"
CB_SPLIT_BRAND = "act:split:brand"
CB_SPLIT_COUNTRY = "act:split:country"
CB_FILTER = "act:filter"
CB_MIX = "act:mix"
CB_COMBINE = "act:combine"
CB_FORMAT = "act:format"
CB_CLEAN = "act:clean"
CB_CLOSE = "act:close"
CB_COMBINE_DONE = "act:combine_done"
CB_COMBINE_CANCEL = "act:combine_cancel"
CB_RENAME    = "act:rename"

# ── Navigation (hub / menus / back) ───────────────────────────────────────────
CB_NAV_HOME  = "nav:home"     # feature catalog (shown by /start)
CB_NAV_TOOLS = "nav:tools"    # command-free tools hub
CB_NAV_MENU  = "nav:menu"     # main card workflow menu
CB_NAV_BACK  = "nav:back"     # back to the previous screen
CB_NAV_HELP  = "nav:help"     # help / how-to
CB_NAV_STATS = "nav:stats"    # this user's stats

# ── Tools-menu entry points (mirror the slash commands) ───────────────────────
CB_TOOL_BIN     = "tool:bin"
CB_TOOL_PROXY   = "tool:proxy"
CB_TOOL_SCR     = "tool:scr"
CB_TOOL_FEEDBACK= "tool:feedback"
CB_TOOL_MYID    = "tool:myid"
CB_TOOL_REGISTER= "tool:register"


class SplitState(StatesGroup):
    waiting_lines = State()


class FilterState(StatesGroup):
    waiting_bin = State()


class CombineState(StatesGroup):
    collecting = State()

class RenameState(StatesGroup):
    waiting_name = State()

class FeedbackState(StatesGroup):
    waiting_text = State()


class BinState(StatesGroup):
    waiting_bin = State()


class ProxyState(StatesGroup):
    waiting_proxy = State()


@dataclass
class CardLine:
    number: str
    month: str
    year: str
    cvc: str

    @property
    def bin6(self) -> str:
        return self.number[:6]

    @property
    def expiry_key(self) -> tuple[int, int]:
        y = int(self.year)
        if y < 100:
            y += 2000
        return (y, int(self.month))

    def normalized(self, year_digits: int = 2) -> str:
        mm = str(int(self.month)).zfill(2)
        yy = int(self.year)
        if yy >= 100:
            yy_str = str(yy)[-2:] if year_digits == 2 else str(yy)
        else:
            yy_str = str(yy).zfill(2) if year_digits == 2 else str(2000 + yy)
        return f"{self.number}|{mm}|{yy_str}|{self.cvc}"


def pan_guess_brand(pan: str) -> str:
    if not pan:
        return "UNKNOWN"
    if pan.startswith("4"):
        return "VISA"
    if pan[:2] in ("51", "52", "53", "54", "55") or (len(pan) >= 4 and 2221 <= int(pan[:4]) <= 2720):
        return "MASTERCARD"
    if pan[:2] in ("34", "37"):
        return "AMEX"
    if pan.startswith("6"):
        return "DISCOVER"
    if pan.startswith("35"):
        return "JCB"
    if pan.startswith("30") or pan.startswith("36") or pan.startswith("38"):
        return "DINERS"
    return "UNKNOWN"


def _normalize_tier(raw: str) -> str:
    t = (raw or "").strip().upper()
    if not t or t in ("UNKNOWN", "N/A", "—"):
        return "STANDARD"
    aliases = {
        "CLASSIC": "CLASSIC",
        "STANDARD": "STANDARD",
        "GOLD": "GOLD",
        "PLATINUM": "PLATINUM",
        "SIGNATURE": "SIGNATURE",
        "INFINITE": "INFINITE",
        "WORLD": "WORLD",
        "WORLD ELITE": "WORLD ELITE",
        "BUSINESS": "BUSINESS",
        "CORPORATE": "CORPORATE",
        "PREPAID": "PREPAID",
        "DEBIT": "DEBIT",
        "CREDIT": "CREDIT",
    }
    for key, val in aliases.items():
        if key in t:
            return val
    return t[:48]


def _current_ym() -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    return now.year, now.month


def _line_is_expired(line: str) -> bool:
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < 4:
        return True
    try:
        card = CardLine(number=parts[0], month=parts[1], year=parts[2], cvc=parts[3])
        y, m = card.expiry_key
        cy, cm = _current_ym()
        return y < cy or (y == cy and m < cm)
    except (ValueError, TypeError):
        return True


def _parse_handyapi(payload: dict[str, Any]) -> dict[str, str]:
    country = payload.get("Country") or payload.get("country") or {}
    if isinstance(country, dict):
        country_name = country.get("Name") or country.get("name") or country.get("A2") or "UNKNOWN"
        country_a2   = country.get("A2") or country.get("a2") or ""
    else:
        country_name = str(country or "UNKNOWN")
        country_a2   = ""
    scheme = payload.get("Scheme") or payload.get("scheme") or "UNKNOWN"
    tier_raw = payload.get("CardTier") or payload.get("cardTier") or payload.get("Level") or "STANDARD"
    # Use alpha-2 as country value if available (better for flag lookup); store display name too
    return {
        "brand":           str(scheme).upper(),
        "country":         str(country_a2 or country_name),
        "country_display": str(country_name),
        "bank":            str(payload.get("Issuer") or payload.get("issuer") or payload.get("bank") or "UNKNOWN"),
        "tier":            _normalize_tier(str(tier_raw)),
    }


def _parse_binlist(payload: dict[str, Any], pan: str) -> dict[str, str]:
    country = payload.get("country") or {}
    if isinstance(country, dict):
        ca2   = country.get("alpha2") or ""
        cname = country.get("name") or ca2 or "UNKNOWN"
    else:
        ca2, cname = "", "UNKNOWN"
    bank = payload.get("bank") or {}
    bname = bank.get("name") if isinstance(bank, dict) else str(bank or "UNKNOWN")
    tier_hint = payload.get("type") or payload.get("brand") or "STANDARD"
    return {
        "brand":           str(payload.get("scheme") or payload.get("brand") or pan_guess_brand(pan)).upper(),
        "country":         str(ca2 or cname or "UNKNOWN"),
        "country_display": str(cname or "UNKNOWN"),
        "bank":            str(bname or "UNKNOWN"),
        "tier":            _normalize_tier(str(tier_hint)),
    }


def _parse_binlist_io(payload: dict[str, Any], pan: str) -> dict[str, str] | None:
    """Parse a binlist.io response.

    Shape:
      {
        "success": true,
        "scheme": "VISA/DANKORT",
        "type": "DEBIT",
        "category": "CLASSIC",
        "country": {"alpha2": "DK", "alpha3": "DNK", "name": "DENMARK", "emoji": "🇩🇰"},
        "bank": {"name": "VESTJYSK BANK A/S", ...},
        "number": {"iin": "457173", "length": 16, "luhn": true}
      }
    Returns None when the BIN could not be resolved.
    """
    if not isinstance(payload, dict):
        return None
    # Explicit failure flag (with all-null fields) — treat as unresolved.
    if payload.get("success") is False:
        return None
    # A "hit" may omit `success` but still carry a scheme + country; only treat
    # as valid if we actually got something useful.
    country = payload.get("country") or {}
    ca2, cname = "", ""
    if isinstance(country, dict):
        ca2   = country.get("alpha2") or ""
        cname = country.get("name") or ""
    bank  = payload.get("bank") or {}
    bname = bank.get("name") if isinstance(bank, dict) else str(bank or "")
    scheme = payload.get("scheme") or ""
    if not (scheme or ca2 or bname):
        return None
    # Prefer alpha-2 as the country value; keep the display name for the label.
    return {
        "brand":           str(scheme or pan_guess_brand(pan)).upper(),
        "country":         str(ca2 or cname or "UNKNOWN"),
        "country_display": str(cname or ca2 or "UNKNOWN"),
        "bank":            str(bname or "UNKNOWN"),
        "tier":            _normalize_tier(str(payload.get("type") or payload.get("category") or "STANDARD")),
    }


def _load_local_bin_db() -> None:
    if not BIN_CSV_PATH.is_file():
        return
    try:
        with BIN_CSV_PATH.open(encoding="utf-8", errors="ignore", newline="") as f:
            for row in csv.DictReader(f):
                bin_key = (row.get("bin") or row.get("BIN") or "").strip()[:6]
                if len(bin_key) < 6:
                    continue
                LOCAL_BIN_DB[bin_key] = {
                    "brand": (row.get("brand") or row.get("scheme") or "UNKNOWN").strip().upper(),
                    "country": (row.get("country") or row.get("country_name") or "UNKNOWN").strip(),
                    "bank": (row.get("bank") or row.get("issuer") or "UNKNOWN").strip(),
                    "tier": _normalize_tier(row.get("tier") or row.get("level") or "STANDARD"),
                }
        logger.info("bins.csv loaded: %s entries", len(LOCAL_BIN_DB))
    except Exception:
        logger.exception("bins.csv load failed")


def _luhn_ok(num: str) -> bool:
    if not num.isdigit():
        return False
    s = 0
    alt = False
    for d in reversed(num):
        n = int(d)
        if alt:
            n *= 2
            if n > 9:
                n -= 9
        s += n
        alt = not alt
    return s % 10 == 0


def _parse_cards_sync(text: str, luhn_check: bool = False) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for pattern in CC_PATTERNS:
        for m in pattern.finditer(text):
            num, mm, yy, cvc = m.group(1), m.group(2), m.group(3), m.group(4)
            if luhn_check and not _luhn_ok(num):
                continue
            try:
                mm_i = int(mm)
            except ValueError:
                continue
            if mm_i < 1 or mm_i > 12:
                continue
            card = CardLine(number=num, month=str(mm_i), year=yy, cvc=cvc)
            line = card.normalized(2)
            if line not in seen:
                seen.add(line)
                out.append(line)
            if len(out) >= MAX_FILE_LINES:
                return out
    return out


async def parse_cards(text: str) -> list[str]:
    loop = asyncio.get_running_loop()
    cards = await loop.run_in_executor(THREAD_POOL, _parse_cards_sync, text, False)
    if cards:
        return cards
    return await loop.run_in_executor(THREAD_POOL, _parse_cards_sync, text, True)



def _pan_from_line(line: str) -> str:
    return line.split("|", 1)[0].strip()


def _strict_clean_cards(cards: list[str]) -> list[str]:
    """Combo (Name|Address|...) ဖယ်ပြီး CC|MM|YY|CVC သာ ထားသည်။"""
    out: list[str] = []
    seen: set[str] = set()
    for line in cards:
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 4:
            continue
        strict = f"{parts[0]}|{parts[1]}|{parts[2]}|{parts[3]}"
        if strict not in seen:
            seen.add(strict)
            out.append(strict)
    return out


def _remove_expired_cards(cards: list[str]) -> tuple[list[str], int]:
    alive: list[str] = []
    seen: set[str] = set()
    dropped = 0
    for line in cards:
        if _line_is_expired(line):
            dropped += 1
            continue
        if line not in seen:
            seen.add(line)
            alive.append(line)
    return alive, dropped


def _bucket_cards_by_meta(
    cards: list[str], bin_info: dict[str, dict[str, str]], key_name: str
) -> dict[str, list[str]]:
    buckets: defaultdict[str, list[str]] = defaultdict(list)
    for line in cards:
        b6 = _pan_from_line(line)[:6]
        label = str(bin_info.get(b6, {}).get(key_name, "UNKNOWN")).upper()
        buckets[label].append(line)
    return dict(buckets)


def _safe_filename_label(label: str) -> str:
    return re.sub(r"[^\w\-.]+", "_", label).strip("_")[:48] or "unknown"


async def _send_split_buckets(
    bot: Bot,
    chat_id: int,
    buckets: dict[str, list[str]],
    file_prefix: str,
    prog: ProgressMessage | None = None,
) -> int:
    items = [(label, chunk) for label, chunk in sorted(buckets.items()) if chunk]
    for i, (label, chunk) in enumerate(items, 1):
        if prog:
            await prog.update(i, len(items), f"{label} — {len(chunk)} cards")
        fname = f"{file_prefix}_{_safe_filename_label(label)}.txt"
        await bot.send_document(
            chat_id,
            _make_txt_file(chunk, fname),
            caption=f"{label} — {len(chunk)} cards",
        )
    return len(items)


def split_mode_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📦 By Count", callback_data=CB_SPLIT_COUNT),
                InlineKeyboardButton(text="💳 By Brand", callback_data=CB_SPLIT_BRAND),
            ],
            [
                InlineKeyboardButton(text="🌍 By Country", callback_data=CB_SPLIT_COUNTRY),
            ],
            [
                InlineKeyboardButton(text="⬅️ Back", callback_data=CB_NAV_BACK),
                InlineKeyboardButton(text="🏠 Home",  callback_data=CB_NAV_HOME),
            ],
        ]
    )


def main_menu_keyboard() -> InlineKeyboardMarkup:
    """The workflow menu shown after cards are loaded."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔍 Filter BIN", callback_data=CB_FILTER),
                InlineKeyboardButton(text="✂️ Split",       callback_data=CB_SPLIT),
            ],
            [
                InlineKeyboardButton(text="🔀 Mix",        callback_data=CB_MIX),
                InlineKeyboardButton(text="🔄 Combine",    callback_data=CB_COMBINE),
            ],
            [
                InlineKeyboardButton(text="🎨 Format",     callback_data=CB_FORMAT),
                InlineKeyboardButton(text="🧹 Clean",      callback_data=CB_CLEAN),
            ],
            [
                InlineKeyboardButton(text="✏️ Rename",     callback_data=CB_RENAME),
                InlineKeyboardButton(text="🏠 Home",       callback_data=CB_NAV_HOME),
            ],
        ]
    )


# Backwards-compatible alias (older code paths call action_keyboard()).
action_keyboard = main_menu_keyboard


def hub_keyboard(uid: int) -> InlineKeyboardMarkup:
    """The /start feature hub keyboard — compact 2-column layout."""
    rows = [
        [
            InlineKeyboardButton(text="📂 Card Tools", callback_data=CB_NAV_MENU),
            InlineKeyboardButton(text="🧰 Extra Tools", callback_data=CB_NAV_TOOLS),
        ],
        [
            InlineKeyboardButton(text="📊 My Stats",  callback_data=CB_NAV_STATS),
            InlineKeyboardButton(text="❓ Help",       callback_data=CB_NAV_HELP),
        ],
    ]
    if is_admin(uid):
        rows.append([
            InlineKeyboardButton(text="👑 Admin Panel", callback_data="adm:panel"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tools_keyboard() -> InlineKeyboardMarkup:
    """Command-free access to every non-card tool."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔎 BIN Lookup",  callback_data=CB_TOOL_BIN),
                InlineKeyboardButton(text="🛰️ Proxy Check", callback_data=CB_TOOL_PROXY),
            ],
            [
                InlineKeyboardButton(text="🕸️ CC Scraper",  callback_data=CB_TOOL_SCR),
                InlineKeyboardButton(text="✉️ Feedback",    callback_data=CB_TOOL_FEEDBACK),
            ],
            [
                InlineKeyboardButton(text="🆔 My ID",       callback_data=CB_TOOL_MYID),
                InlineKeyboardButton(text="📝 Register",    callback_data=CB_TOOL_REGISTER),
            ],
            [
                InlineKeyboardButton(text="🏠 Home",        callback_data=CB_NAV_HOME),
            ],
        ]
    )


def combine_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Finish merge", callback_data=CB_COMBINE_DONE),
                InlineKeyboardButton(text="❌ Cancel", callback_data=CB_COMBINE_CANCEL),
            ]
        ]
    )


def success_caption(count: int, filename: str | None = None) -> str:
    file_line = f"📄 {html.escape(filename)}\n" if filename else ""
    return (
        f"✅ <b>Cards loaded</b>\n"
        f"└───────────────────\n"
        f"{file_line}"
        f"💳 <b>{count:,}</b> cards ready\n\n"
        f"👇 Pick an action:"
    )


async def get_cards(user_id: int, state: FSMContext) -> list[str]:
    data = await state.get_data()
    cards = data.get("cards")
    if cards:
        return list(cards)
    return list(USER_CARDS.get(user_id) or [])


async def save_cards(user_id: int, state: FSMContext, cards: list[str]) -> None:
    USER_CARDS[user_id] = cards
    await state.update_data(cards=cards)


async def require_cards(call: CallbackQuery, state: FSMContext) -> list[str] | None:
    cards = await get_cards(call.from_user.id, state)
    if not cards:
        await call.answer("Session expired — Please upload the .txt file again", show_alert=True)
        return None
    return cards


def _make_txt_file(lines: list[str], filename: str) -> BufferedInputFile:
    body = "\n".join(lines)
    if body and not body.endswith("\n"):
        body += "\n"
    return BufferedInputFile(body.encode("utf-8"), filename=filename)


async def _read_upload_bytes(message: Message, bot: Bot) -> bytes:
    doc = message.document
    if not doc:
        raise ValueError("File not found")
    if doc.file_size and doc.file_size > MAX_FILE_BYTES:
        raise ValueError(f"File too large (Max {MAX_FILE_BYTES // (1024 * 1024)} MB)")
    name = (doc.file_name or "").lower()
    if not name.endswith(".txt"):
        raise ValueError("Only .txt files are accepted")
    tg_file = await bot.get_file(doc.file_id)
    buf = io.BytesIO()
    await bot.download_file(tg_file.file_path, buf)
    return buf.getvalue()


async def _decode_text(data: bytes) -> str:
    def _dec() -> str:
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                return data.decode(enc)
            except UnicodeDecodeError:
                continue
        return data.decode("utf-8", errors="ignore")

    return await asyncio.get_running_loop().run_in_executor(THREAD_POOL, _dec)


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict | None = None,
    retries: int = 1,  # ⚡ ပိုမြန်စေရန် Retry ကို 1 အထိ လျှော့ချလိုက်သည်
) -> dict | None:
    """GET → JSON. Retries on 429 with exponential backoff."""
    delay = 1.0
    for attempt in range(retries + 1):
        try:
            # ⚡ Timeout ကို 10 မှ 5 စက္ကန့်သို့ လျှော့ချလိုက်သည် (သေနေသော API များကြောင့် အချိန်မကြန့်ကြာစေရန်)
            async with session.get(
                url,
                headers=headers or {},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                if resp.status == 429:
                    await asyncio.sleep(delay)
                    delay *= 1.5
                    continue
                return None
        except Exception:
            if attempt < retries:
                await asyncio.sleep(delay)
                delay *= 1.5
                continue
            return None
    return None

_BIN_CACHE_SAVE_DELAY = 5.0          # seconds to wait before flushing
_bin_cache_save_task: asyncio.Task | None = None


def _save_bin_cache() -> None:
    """Synchronous one-shot save — use only at shutdown or startup."""
    try:
        BIN_CACHE_FILE.write_text(
            json.dumps(BIN_CACHE, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        logger.debug("bin_cache save failed: %s", e)


def _schedule_bin_cache_save() -> None:
    """Schedule a debounced async write — coalesces rapid saves into one."""
    global _bin_cache_save_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No event loop yet (startup) — fall back to sync save
        _save_bin_cache()
        return

    if _bin_cache_save_task and not _bin_cache_save_task.done():
        _bin_cache_save_task.cancel()

    async def _flush() -> None:
        await asyncio.sleep(_BIN_CACHE_SAVE_DELAY)
        _save_bin_cache()
        logger.debug("bin_cache flushed (%d entries)", len(BIN_CACHE))

    _bin_cache_save_task = loop.create_task(_flush())


def _load_bin_cache() -> None:
    """Load previously saved BIN_CACHE from disk.
    Entries without _ts are treated as age=0 so they stay valid until TTL expires."""
    if not BIN_CACHE_FILE.is_file():
        return
    try:
        data = json.loads(BIN_CACHE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            now = time.monotonic()
            loaded = expired = 0
            for k, v in data.items():
                if not isinstance(v, dict):
                    continue
                ts = float(v.get("_ts", 0))
                # Convert wall-clock ts stored on disk to monotonic equivalent
                # We store wall-clock on save; convert back approximately
                age = time.time() - ts if ts > 1_000_000 else BIN_CACHE_TTL_SEC  # old format → treat as stale
                if age < BIN_CACHE_TTL_SEC:
                    v["_ts"] = now - age   # adjust to monotonic
                    BIN_CACHE[k] = v
                    loaded += 1
                else:
                    expired += 1
            logger.info("bin_cache.json loaded: %s fresh, %s expired/dropped", loaded, expired)
    except Exception as e:
        logger.debug("bin_cache load failed: %s", e)


def _parse_bincheck(payload: dict[str, Any], pan: str) -> dict[str, str] | None:
    """Parse bincheck.io response. Returns None if unusable."""
    if not isinstance(payload, dict):
        return None
    scheme = payload.get("scheme") or payload.get("brand") or ""
    bank   = payload.get("bank_name") or payload.get("bank") or ""
    country = payload.get("country_name") or payload.get("country") or ""
    a2      = payload.get("country_code") or payload.get("country") or ""
    tier    = payload.get("type") or payload.get("card_type") or "STANDARD"
    if not scheme and not bank:
        return None
    return {
        "brand":           str(scheme).upper() or pan_guess_brand(pan),
        "country":         str(a2 or country or "UNKNOWN"),
        "country_display": str(country or a2 or "UNKNOWN"),
        "bank":            str(bank or "UNKNOWN"),
        "tier":            _normalize_tier(str(tier)),
    }


def _ensure_bin_meta(info: dict[str, str]) -> dict[str, str]:
    info.setdefault("tier", "STANDARD")
    info.setdefault("brand", "UNKNOWN")
    info.setdefault("country", "UNKNOWN")
    info.setdefault("bank", "UNKNOWN")
    info.setdefault("_ts", 0.0)
    return info


def _bin_cache_fresh(info: dict[str, str]) -> bool:
    """True if cached entry is still within TTL."""
    ts = float(info.get("_ts", 0))
    return (time.monotonic() - ts) < BIN_CACHE_TTL_SEC


def _bin_stamp(info: dict) -> dict:
    """Stamp entry with current wall-clock time for on-disk TTL."""
    info["_ts"] = time.time()
    return info


async def lookup_bin(
    bin6: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    force_refresh: bool = False,
    retries: int = 1,
) -> dict[str, str]:
    """Resolve one BIN. `force_refresh=True` skips cached/known-empty results so
    the caller can genuinely retry a previously-unresolved BIN. `retries` is the
    per-API retry budget (the retry pass uses a higher value)."""
    # 0) Already-resolved cache hit (skipped when the caller wants a fresh try).
    if not force_refresh:
        cached = BIN_CACHE.get(bin6)
        if cached and _bin_cache_fresh(cached):
            return _ensure_bin_meta(cached)

    # 1) Local CSV DB (authoritative — honour it even on a forced refresh).
    if bin6 in LOCAL_BIN_DB:
        entry = _ensure_bin_meta(dict(LOCAL_BIN_DB[bin6]))
        entry["_ts"] = time.monotonic()
        BIN_CACHE[bin6] = entry
        return entry

    async with sem:
        # 🎯 Smart Merge အတွက် Base Dictionary ပြင်ဆင်ခြင်း
        best_info = {
            "brand": pan_guess_brand(bin6),
            "country": "UNKNOWN",
            "country_display": "UNKNOWN",
            "bank": "UNKNOWN",
            "tier": "STANDARD",
        }

        # လိုအပ်နေသော Data များကို အခြား API များမှ ဖြည့်စွက်ပေးမည့် Helper Function
        def merge_info(new_info: dict) -> None:
            if new_info.get("brand") and new_info["brand"] != "UNKNOWN":
                best_info["brand"] = new_info["brand"]
            if new_info.get("country") and new_info["country"] != "UNKNOWN":
                best_info["country"] = new_info["country"]
                best_info["country_display"] = new_info.get("country_display", new_info["country"])
            if new_info.get("bank") and new_info["bank"] != "UNKNOWN":
                best_info["bank"] = new_info["bank"]
            if new_info.get("tier") and new_info["tier"] != "STANDARD":
                best_info["tier"] = new_info["tier"]

        # ⚡ 4 APIs အားလုံးကို တစ်ပြိုင်နက် concurrent ခေါ်မည် (waterfall မဟုတ်တော့ပါ) —
        # worst-case latency ကို sum(timeouts) မှ max(timeouts) အဖြစ်သို့ လျှော့ချသည်။
        # Merge priority (first non-empty value wins) — most-trusted first:
        #   HandyAPI > Binlist.io > Binlist.net > Bincheck
        p1, p2, p3, p4 = await asyncio.gather(
            _fetch_json(session, HANDYAPI_BIN_URL.format(bin=bin6), retries=retries),
            _fetch_json(session, BINLIST_IO_URL.format(bin=bin6), retries=retries),
            _fetch_json(session, BINLIST_URL.format(bin=bin6),
                        headers={"Accept-Version": "3"}, retries=retries),
            _fetch_json(session, BINCHECK_URL.format(bin=bin6), retries=retries),
            return_exceptions=True,
        )
        if isinstance(p1, BaseException):
            p1 = None
        if isinstance(p2, BaseException):
            p2 = None
        if isinstance(p3, BaseException):
            p3 = None
        if isinstance(p4, BaseException):
            p4 = None

        if p1 and str(p1.get("Status", "")).upper() == "SUCCESS":
            merge_info(_parse_handyapi(p1))
        if p2:
            parsed_io = _parse_binlist_io(p2, bin6)
            if parsed_io:
                merge_info(parsed_io)
        if p3:
            merge_info(_parse_binlist(p3, bin6))
        if p4:
            parsed4 = _parse_bincheck(p4, bin6)
            if parsed4:
                merge_info(parsed4)

        best_info = _ensure_bin_meta(best_info)
        
        # 🎯 Cache ထဲသို့ အလွတ်ကြီးများ မဝင်သွားစေရန် (Bank သို့ Country တစ်ခုခုပါမှ Cache မှတ်မည်)
        if best_info["bank"] != "UNKNOWN" or best_info["country"] != "UNKNOWN":
            best_info = _bin_stamp(best_info)
            BIN_CACHE[bin6] = best_info
            _schedule_bin_cache_save()

        return best_info

async def enrich_bins(
    cards: list[str],
    progress: ProgressMessage | None = None,
) -> dict[str, dict[str, str]]:
    """Resolve the issuer metadata (brand/country/bank/tier) for every unique
    BIN in `cards`, returning {bin6: {..}} for ALL BINs found.

    Robustness notes:
      * Every unique BIN is looked up (up to a very high safety cap), so cards
        no longer silently fall into "Unknown" just because their BIN was
        outside an arbitrary first-N window.
      * A BIN that fails on the first pass is retried once more, which rescues
        the many cases caused by a single flaky/timed-out API call.
    """
    unique_pans = [_pan_from_line(c) for c in cards]
    bins = sorted({p[:6] for p in unique_pans if len(p) >= 6})
    if len(bins) > MAX_BIN_LOOKUPS_PER_JOB:
        bins = bins[:MAX_BIN_LOOKUPS_PER_JOB]
    total = max(len(bins), 1)

    # 🚀 Concurrency (raised) — API calls are I/O bound.
    sem = asyncio.Semaphore(_BIN_LOOKUP_CONCURRENCY)
    result: dict[str, dict[str, str]] = {}
    done_count = 0
    if progress:
        await progress.update(0, total, "Starting BIN lookup...", force=True)

    async def _lookup_one(session: aiohttp.ClientSession, b: str) -> None:
        nonlocal done_count
        info = await lookup_bin(b, session, sem)
        result[b] = info
        done_count += 1

        if progress:
            # ⚡ Throttle UI updates to avoid Telegram flood limits.
            if done_count % 5 == 0 or done_count == total:
                await progress.update(done_count, total, f"BIN {b}")

    session = get_http_session()
    await asyncio.gather(*[_lookup_one(session, b) for b in bins])

    # ── Second pass: retry the BINs that came back unresolved ─────────────────
    if _BIN_RETRY_PASS:
        unresolved = [
            b for b in bins
            if result.get(b, {}).get("country", "UNKNOWN") == "UNKNOWN"
        ]
        if unresolved and progress:
            await progress.update(
                0, len(unresolved),
                f"Retrying {len(unresolved)} unresolved BIN(s)…", force=True,
            )
        if unresolved:
            # A short pause lets a transient rate-limit clear before retrying.
            await asyncio.sleep(1.0)
            retry_done = 0

            async def _retry_one(b: str) -> None:
                nonlocal retry_done
                # Bypass the cache read so we actually hit the APIs again.
                info = await lookup_bin(b, session, sem, force_refresh=True, retries=2)
                if info.get("country", "UNKNOWN") != "UNKNOWN":
                    result[b] = info
                retry_done += 1
                if progress and (retry_done % 5 == 0 or retry_done == len(unresolved)):
                    await progress.update(
                        retry_done, len(unresolved), f"Retry {b}"
                    )

            await asyncio.gather(*[_retry_one(b) for b in unresolved])

    resolved = sum(
        1 for b in bins if result.get(b, {}).get("country", "UNKNOWN") != "UNKNOWN"
    )
    if progress:
        await progress.update(
            total, total,
            f"Resolved {resolved}/{len(bins)} BINs", force=True,
        )

    # ── Ensure every card's BIN has an entry (unknown ones get a placeholder) ──
    for c in cards:
        pan = _pan_from_line(c)
        b6 = pan[:6]
        if b6 not in result:
            result[b6] = {
                "brand": pan_guess_brand(pan),
                "country": "UNKNOWN",
                "bank": "UNKNOWN",
                "tier": "STANDARD",
            }
        elif "tier" not in result[b6]:
            result[b6]["tier"] = "STANDARD"

    return result


def result_nav_keyboard() -> InlineKeyboardMarkup:
    """Buttons attached to a result file so the user can chain another action."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🏠 Main Menu", callback_data=CB_NAV_MENU),
                InlineKeyboardButton(text="🧰 Tools",     callback_data=CB_NAV_TOOLS),
            ],
        ]
    )


async def send_result_file(
    target: Message | CallbackQuery,
    bot: Bot,
    lines: list[str],
    filename: str,
    caption: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    if not lines:
        if isinstance(target, CallbackQuery):
            await target.answer("No results found.", show_alert=True)
        else:
            await target.answer("No results found.")
        return
    doc = _make_txt_file(lines, filename)
    chat_id = target.message.chat.id if isinstance(target, CallbackQuery) else target.chat.id
    await bot.send_document(
        chat_id, doc, caption=caption,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup if reply_markup is not None else result_nav_keyboard(),
    )
    if isinstance(target, CallbackQuery):
        await target.answer("✅ ပြီးပါပြီ")


async def show_menu(
    message: Message, state: FSMContext, cards: list[str],
    filename: str | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else message.chat.id
    await state.set_state(None)
    await save_cards(uid, state, cards)
    await message.answer(
        success_caption(len(cards), filename),
        parse_mode=ParseMode.HTML,
        reply_markup=action_keyboard(),
    )


# ─── Force Join Helpers ───────────────────────────────────────────────────────

def _force_join_keyboard() -> InlineKeyboardMarkup:
    """Join link (stored link preferred, falls back to channel id) + Check button."""
    link = FORCE_JOIN_LINK or f"https://t.me/{str(FORCE_JOIN_CHANNEL).lstrip('@')}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Channel Join ဝင်ပါ", url=link)],
            [InlineKeyboardButton(text="✅ Join ပြီးပြီ — စစ်ဆေးပါ", callback_data="fj:check")],
        ]
    )


async def _check_member(bot: Bot, user_id: int) -> bool:
    """User သည် FORCE_JOIN_CHANNEL ၏ member ဟုတ်/မဟုတ် စစ်ဆေးသည်။
    Channel မသတ်မှတ်ထားလျှင် (/setjoin မလုပ်ရသေးလျှင်) True ပြန်သည် — disabled."""
    if not FORCE_JOIN_CHANNEL:
        return True
    try:
        member = await bot.get_chat_member(FORCE_JOIN_CHANNEL, user_id)
        return member.status not in ("left", "kicked", "banned")
    except Exception:
        # Bot ကို channel ထဲ Admin အဖြစ် ထည့်မထားလျှင် fail ဖြစ်နိုင် — block မလုပ်ဘဲ ဆက်ခွင့်ပြု
        logger.warning("force-join check failed for uid=%s — is the bot an admin of %s?", user_id, FORCE_JOIN_CHANNEL)
        return True

# ──────────────────────────────────────────────────────────────────────────────


class AccessMiddleware(BaseMiddleware):
    """Admin + VIP သာ bot သုံးခွင့် (public mode ပိတ်ထားချိန်)"""

    PUBLIC_COMMANDS = frozenset({"/start", "/help", "/myid", "/cancel", "/register", "/feedback"})

    async def __call__(
        self,
        handler: Callable[..., Awaitable[Any]],
        event: Any,
        data: dict[str, Any],
    ) -> Any:
        user = None
        if isinstance(event, Message) and event.from_user:
            user = event.from_user
        elif isinstance(event, CallbackQuery) and event.from_user:
            user = event.from_user
        if user is None:
            return await handler(event, data)

        if user.username:
            USER_PROFILES[user.id] = user.username.lstrip("@")

        _touch_user(user.id)   # ⚡ cleanup loop အတွက် last-seen update

        # ── Force Join Check (admin ကို ကျော်သည်; /setjoin ဖြင့် disable ဖြစ်နေရင် skip) ──
        if FORCE_JOIN_CHANNEL and not is_admin(user.id):
            bot: Bot = data.get("bot")  # type: ignore[assignment]
            if bot is not None:
                joined = await _check_member(bot, user.id)
                if not joined:
                    kb = _force_join_keyboard()
                    if isinstance(event, Message):
                        await event.answer(
                            "📢 <b>Bot သုံးရန် Channel Join လိုအပ်သည်!</b>\n\n"
                            "အောက်ပါ channel ကို Join ပြုလုပ်ပြီး\n"
                            "<b>✅ Join ပြီးပြီ — စစ်ဆေးပါ</b> ကို နှိပ်ပါ။",
                            reply_markup=kb,
                            parse_mode=ParseMode.HTML,
                        )
                    elif isinstance(event, CallbackQuery):
                        if event.data == "fj:check":
                            await event.answer(
                                "❌ Channel ကို Join မရသေး!\nJoin ဝင်ပြီးမှ ထပ်ကြိုးစားပါ။",
                                show_alert=True,
                            )
                        else:
                            await event.answer("📢 Channel Join ဦးဆုံးလုပ်ပါ!", show_alert=True)
                    return None
        # ── End Force Join Check ──────────────────────────────────────────────

        if can_use_bot(user.id):
            return await handler(event, data)

        if isinstance(event, Message) and event.text:
            cmd = event.text.split()[0].split("@")[0].lower()
            if cmd in self.PUBLIC_COMMANDS:
                return await handler(event, data)
            if is_admin(user.id) and cmd.startswith("/"):
                return await handler(event, data)

        if isinstance(event, Message):
            await event.answer(
                ACCESS_DENIED.format(uid=user.id),
                parse_mode=ParseMode.HTML,
            )
            return None
        if isinstance(event, CallbackQuery):
            await event.answer("🔒 Admin / VIP access only.", show_alert=True)
            return None
        return None


@router.callback_query(F.data == "fj:check")
async def fj_check_callback(call: CallbackQuery, bot: Bot) -> None:
    """User က 'Join ပြီးပြီ' ကို နှိပ်လျှင် membership ပြန်စစ်ဆေးသည်"""
    uid = call.from_user.id
    joined = await _check_member(bot, uid)
    if joined:
        await call.message.edit_text(
            "✅ <b>Channel Join အောင်မြင်သည်!</b>\n\n"
            "ယခု /start နှိပ်ပြီး bot ကို စသုံးနိုင်ပါပြီ 🎉",
            parse_mode=ParseMode.HTML,
        )
        await call.answer("✅ Welcome!", show_alert=False)
    else:
        await call.answer(
            "❌ Channel ကို Join မရသေးပါ!\nJoin ဝင်ပြီးမှ ထပ်ကြိုးစားပါ။",
            show_alert=True,
        )


@router.message(Command("setjoin"))
async def cmd_setjoin(message: Message, bot: Bot) -> None:
    """Admin — Force Join channel ကို Telegram ထဲကပဲ သတ်မှတ်ပါ (.env မလို)
    သုံးပုံ: /setjoin @channelusername   သို့   /setjoin -1001234567890
    Bot ကို channel ၏ Admin အဖြစ် အရင်ထည့်ထားရမည်။"""
    uid = message.from_user.id
    if not is_admin(uid):
        return await message.answer("🚫 Admin only command ဖြစ်သည်။")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer(
            "📝 <b>Force Join Channel သတ်မှတ်ရန်</b>\n\n"
            "သုံးပုံ:\n"
            "<code>/setjoin @channelusername</code>\n"
            "<code>/setjoin -1001234567890</code>\n\n"
            "⚠️ Bot ကို channel ၏ <b>Admin</b> အဖြစ် အရင်ထည့်ထားပါ။\n"
            "ပိတ်ရန်: /unsetjoin\n"
            "စစ်ရန်: /joininfo",
            parse_mode=ParseMode.HTML,
        )

    raw = args[1].strip()
    target: "int | str" = int(raw) if raw.lstrip("-").isdigit() else (raw if raw.startswith("@") else f"@{raw}")

    try:
        chat = await bot.get_chat(target)
    except Exception as e:
        return await message.answer(
            "❌ Channel ကို ရှာမတွေ့ပါ။\n\n"
            "• Channel username / ID မှန်ကန်ကြောင်း စစ်ပါ\n"
            "• Bot ကို channel ထဲ <b>Admin</b> အဖြစ် အရင်ထည့်ထားပါ\n"
            f"<code>{_he(e)}</code>",
            parse_mode=ParseMode.HTML,
        )

    # Bot က channel ရဲ့ admin ဟုတ်/မဟုတ် စစ်ပြီး link ကို ဖန်တီး/ရှာသည်
    warn = ""
    link: str | None = f"https://t.me/{chat.username}" if chat.username else None
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        if member.status not in ("administrator", "creator"):
            warn = "\n\n⚠️ <b>သတိ:</b> Bot သည် ဤ channel ၏ Admin မဟုတ်သေးပါ — membership check fail ဖြစ်နိုင်သည်။"
        elif link is None:
            # Private channel — invite link ကို bot admin အနေဖြင့် ထုတ်ပေးနိုင်သည်
            try:
                link = await bot.export_chat_invite_link(chat.id)
            except Exception:
                warn = "\n\n⚠️ Private channel အတွက် invite link ကို auto ထုတ်၍ မရပါ — 'Invite Users via Link' admin right ပေးထားပါ။"
    except Exception:
        warn = "\n\n⚠️ Bot ၏ channel membership status ကို စစ်ဆေး၍ မရပါ — Bot ကို Admin ထည့်ထားကြောင်း သေချာပါစေ။"

    title = chat.title or chat.username or str(chat.id)
    set_force_join_channel(chat.id, link, title)
    await message.answer(
        f"✅ <b>Force Join Channel သတ်မှတ်ပြီးပါပြီ</b>\n\n"
        f"📢 Channel: <b>{_he(title)}</b>\n"
        f"🆔 ID: <code>{chat.id}</code>\n"
        f"🔗 Link: {link or '(မရှိ)'}{warn}",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("unsetjoin"))
async def cmd_unsetjoin(message: Message) -> None:
    """Admin — Force Join ကို ပိတ်ပါ"""
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Admin only command ဖြစ်သည်။")
    if not FORCE_JOIN_CHANNEL:
        return await message.answer("ℹ️ Force Join channel ဘာမှ သတ်မှတ်မထားသေးပါ။")
    clear_force_join_channel()
    await message.answer("✅ Force Join ကို ပိတ်လိုက်ပါပြီ — user များ Channel Join မလိုအပ်တော့ပါ။")


@router.message(Command("joininfo"))
async def cmd_joininfo(message: Message) -> None:
    """Admin — Force Join status ကြည့်ရန်"""
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Admin only command ဖြစ်သည်။")
    if not FORCE_JOIN_CHANNEL:
        return await message.answer("🔓 Force Join: <b>Disabled</b>\n\nသတ်မှတ်ရန်: /setjoin @channel", parse_mode=ParseMode.HTML)
    await message.answer(
        f"🔒 Force Join: <b>Enabled</b>\n"
        f"📢 Channel: <b>{_he(FORCE_JOIN_TITLE or '—')}</b>\n"
        f"🆔 ID: <code>{FORCE_JOIN_CHANNEL}</code>\n"
        f"🔗 Link: {FORCE_JOIN_LINK or '(မရှိ)'}",
        parse_mode=ParseMode.HTML,
    )


# ─── Feature Hub / Help / Tools / Stats text ────────────────────────────────────

def feature_hub_text(uid: int) -> str:
    """The /start landing screen — clean and minimal."""
    role = role_label(uid)
    return (
        "💳 <b>Card Tool Bot</b>\n"
        f"┌ Status: <b>{role}</b>\n"
        "└───────────────────\n\n"
        "📤 Send a <b>.txt</b> file to get started\n"
        "<code>4111111111111111|12|28|123</code>\n\n"
        "👇 Or pick a tool below."
    )


def tools_menu_text() -> str:
    return (
        "🧰 <b>Extra Tools</b>\n"
        "└───────────────────\n\n"
        "🔎 BIN Lookup  ·  🛰️ Proxy Checker\n"
        "🕸️ CC Scraper  ·  ✉️ Feedback\n"
        "🆔 My ID  ·  📝 Register\n\n"
        "👇 Pick a tool."
    )


def help_text(uid: int) -> str:
    role = role_label(uid)
    body = (
        "❓ <b>How to use Card Tool Bot</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🔐 Status: <b>{role}</b>\n\n"
        "<b>1️⃣ Send your cards</b>\n"
        "Upload a <b>.txt</b> file containing one card per line:\n"
        "<code>4111111111111111|12|28|123</code>\n\n"
        "<b>2️⃣ Pick an action</b>\n"
        "After the file is read you get a menu:\n"
        "  🔍 Filter BIN — keep only one BIN\n"
        "  ✂️ Split — by count / brand / country\n"
        "  🔀 Mix — shuffle lines\n"
        "  🔄 Combine — merge several files\n"
        "  🎨 Format — normalise CC|MM|YY|CVC\n"
        "  🧹 Clean — remove dupes &amp; expired\n"
        "  ✏️ Rename — change the file name\n\n"
        "<b>3️⃣ Standalone tools</b>\n"
        "  🔎 /bin 453201 — BIN lookup\n"
        "  🛰️ /proxy ip:port — proxy checker\n"
        "  🕸️ /scr — CC scraper\n"
        "  ✉️ /feedback — contact admin\n"
        "  🆔 /myid — your user ID\n"
        "  ⏹ /cancel — stop the current action"
    )
    if is_admin(uid):
        body += (
            "\n\n👑 <b>Admin</b>\n"
            "  /adminpanel — inline admin panel\n"
            "  /addvip · /delvip · /viplist\n"
            "  /ban · /unban · /broadcast\n"
            "  /usagestats · /access · /accessinfo"
        )
    return body


def dashboard_text(uid: int) -> str:
    """A small personal dashboard."""
    role = role_label(uid)
    hist = USER_HISTORY.get(uid) or []
    files = len(hist)
    total_cards = sum(int(h.get("count", 0)) for h in hist if isinstance(h, dict))
    last = ""
    if hist:
        last_rec = hist[-1]
        if isinstance(last_rec, dict):
            last = f"\n🕒 Last: <b>{html.escape(str(last_rec.get('file', '—')))}</b> ({last_rec.get('count', 0):,})"
    return (
        f"📊 <b>My Stats</b>\n"
        f"┌ Role: <b>{role}</b>\n"
        f"├ ID: <code>{uid}</code>\n"
        f"├ Files: <b>{files:,}</b>\n"
        f"└ Cards: <b>{total_cards:,}</b>"
        f"{last}"
    )


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    uid = message.from_user.id
    if not can_use_bot(uid):
        return await message.answer(
            ACCESS_DENIED.format(uid=uid)
            + "\n\nSend your ID to an Admin to request VIP access.\n"
            + "📝 /register — Request access",
            parse_mode=ParseMode.HTML,
        )
    await message.answer(
        feature_hub_text(uid),
        parse_mode=ParseMode.HTML,
        reply_markup=hub_keyboard(uid),
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    uid = message.from_user.id
    if not can_use_bot(uid):
        return await cmd_start(message)
    await message.answer(
        help_text(uid),
        parse_mode=ParseMode.HTML,
        reply_markup=hub_keyboard(uid),
    )


@router.message(Command("myid"))
async def cmd_myid(message: Message) -> None:
    uid = message.from_user.id
    uname = message.from_user.username or "—"
    await message.answer(
        f"🆔 <b>Your Telegram ID</b>\n"
        f"<code>{uid}</code>\n"
        f"Username: @{uname}\n"
        f"Role: <b>{role_label(uid)}</b>",
        parse_mode=ParseMode.HTML,
    )


async def _admin_only(message: Message) -> bool:
    if is_admin(message.from_user.id):
        return True
    # သာမန် User တွေ Admin Command လာရိုက်ရင် ဘာမှပြန်မပြောဘဲ လျစ်လျူရှုမည်
    return False


@router.message(Command("addvip"))
async def cmd_addvip(message: Message) -> None:
    if not await _admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("Usage: <code>/addvip 123456789</code>", parse_mode=ParseMode.HTML)
    uid = int(parts[1])
    if is_admin(uid):
        return await message.answer("ℹ️ This ID is already an admin.")
    if add_vip(uid):
        await message.answer(f"⭐ VIP ထည့်ပြီး: <code>{uid}</code>", parse_mode=ParseMode.HTML)
    else:
        await message.answer(f"ℹ️ <code>{uid}</code> is already a VIP.", parse_mode=ParseMode.HTML)


@router.message(Command("delvip"))
async def cmd_delvip(message: Message) -> None:
    if not await _admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("Usage: <code>/delvip 123456789</code>", parse_mode=ParseMode.HTML)
    uid = int(parts[1])
    if remove_vip(uid):
        await message.answer(f"🗑️ VIP removed: <code>{uid}</code>", parse_mode=ParseMode.HTML)
    else:
        await message.answer(f"⚠️ <code>{uid}</code> is not a VIP.", parse_mode=ParseMode.HTML)


@router.message(Command("viplist"))
async def cmd_viplist(message: Message) -> None:
    if not await _admin_only(message):
        return
    vips = list_vip()
    body = "\n".join(f"  • <code>{v}</code>" for v in vips) or "  (empty)"
    await message.answer(
        f"⭐ <b>VIP List ({len(vips)})</b>\n{body}\n\n{access_stats()}",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("accessinfo"))
async def cmd_accessinfo(message: Message) -> None:
    if not await _admin_only(message):
        return
    await message.answer(access_stats(), parse_mode=ParseMode.HTML)


@router.message(Command("access"))
async def cmd_access(message: Message) -> None:
    if not await _admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.answer(
            "Usage:\n"
            "<code>/access private</code> — Admin+VIP သာ\n"
            "<code>/access public</code> — လူတိုင်း",
            parse_mode=ParseMode.HTML,
        )
    mode = parts[1].lower()
    if mode in ("private", "on", "closed", "restrict"):
        set_restricted(True)
        await message.answer("🔒 <b>Private mode</b> — Only Admin + VIP can use the bot now.", parse_mode=ParseMode.HTML)
    elif mode in ("public", "off", "open"):
        set_restricted(False)
        await message.answer("🌐 <b>Public mode</b> — Everyone can use the bot now.", parse_mode=ParseMode.HTML)
    else:
        await message.answer("⚠️ Please enter private or public.", parse_mode=ParseMode.HTML)


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.set_state(None)
    stopped = _scr_cancel(message.from_user.id)
    if stopped:
        await message.answer("🛑 Scrape cancelled — sending what was found so far…")
    else:
        await message.answer("❌ Action cancelled. You can upload a new file now.")


@router.message(F.document, ~(F.caption & F.caption.casefold().contains("proxy")))
async def on_document(message: Message, state: FSMContext, bot: Bot) -> None:
    uid = message.from_user.id
    if uid in BANNED_IDS:
        return await message.answer("🚫 You are banned.")
    if not _check_rate_limit(uid):
        return await message.answer(f"⏳ Rate limit — {RATE_LIMIT_WINDOW:.0f}s တွင် {RATE_LIMIT_MAX} ကြိမ်သာ upload ခွင့်ပြုသည်")
    current = await state.get_state()

    if current == CombineState.collecting.state:
        try:
            data = await _read_upload_bytes(message, bot)
            text = await _decode_text(data)
            new_cards = await parse_cards(text)
            if not new_cards:
                return await message.answer("⚠️ No valid cards found.")
            existing = await get_cards(uid, state)
            merged = list(dict.fromkeys(existing + new_cards))
            await save_cards(uid, state, merged)
            return await message.answer(
                f"➕ Added <b>{len(new_cards)}</b> → Total <b>{len(merged)}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=combine_keyboard(),
            )
        except ValueError as e:
            return await message.answer(f"⚠️ {e}")
        except Exception:
            logger.exception("combine_upload")
            return await message.answer("⚠️ Error reading file.")

    status = await message.answer(
        f"⏳ <b>Reading file...</b>\n<code>{render_bar(0)}</code>",
        parse_mode=ParseMode.HTML,
    )
    prog = ProgressMessage(status, "Cleaning cards")
    try:
        await prog.update(1, 4, "Downloading file...", force=True)
        data = await _read_upload_bytes(message, bot)
        await prog.update(2, 4, "Decoding text...", force=True)
        text = await _decode_text(data)
        await prog.update(3, 4, "Extracting cards (regex)...", force=True)
        cards = await parse_cards(text)
        await prog.update(4, 4, "Done", force=True)
        if not cards:
            await status.edit_text(
                "⚠️ No cards found.\n"
                "Example:\n<code>4111111111111111|12|28|123</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        fname = (message.document.file_name or "cards.txt") if message.document else "cards.txt"
        _record_history(uid, fname, len(cards), message.from_user.username if message.from_user else None)
        _auto_backup(uid, cards, fname)
        await status.delete()
        await show_menu(message, state, cards, fname)
    except ValueError as e:
        await status.edit_text(f"⚠️ {e}")
    except Exception:
        logger.exception("document_handler")
        await status.edit_text("⚠️ Error processing file.")


@router.callback_query(F.data == CB_CLOSE)
async def cb_close(call: CallbackQuery) -> None:
    await call.answer()
    try:
        await call.message.delete()
    except Exception:
        await call.message.edit_reply_markup(reply_markup=None)


# ─── Navigation handlers (hub ⇄ tools ⇄ menu ⇄ back) ───────────────────────────

async def _safe_edit(call: CallbackQuery, text: str, markup: InlineKeyboardMarkup) -> None:
    """Edit the current message in place, falling back to a fresh message."""
    try:
        await call.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    except Exception:
        try:
            await call.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        except Exception:
            pass


@router.callback_query(F.data == CB_NAV_HOME)
async def cb_nav_home(call: CallbackQuery) -> None:
    uid = call.from_user.id
    if not can_use_bot(uid):
        return await call.answer("🚫 No access.", show_alert=True)
    await call.answer()
    await _safe_edit(call, feature_hub_text(uid), hub_keyboard(uid))


@router.callback_query(F.data == CB_NAV_TOOLS)
async def cb_nav_tools(call: CallbackQuery) -> None:
    if not can_use_bot(call.from_user.id):
        return await call.answer("🚫 No access.", show_alert=True)
    await call.answer()
    await _safe_edit(call, tools_menu_text(), tools_keyboard())


@router.callback_query(F.data == CB_NAV_MENU)
async def cb_nav_menu(call: CallbackQuery, state: FSMContext) -> None:
    uid = call.from_user.id
    cards = await get_cards(uid, state)
    if not cards:
        await call.answer("⚠️ No cards loaded — send a .txt first.", show_alert=True)
        return await _safe_edit(call, feature_hub_text(uid), hub_keyboard(uid))
    await call.answer()
    await _safe_edit(call, success_caption(len(cards)), main_menu_keyboard())


@router.callback_query(F.data == CB_NAV_BACK)
async def cb_nav_back(call: CallbackQuery, state: FSMContext) -> None:
    """Back = workflow menu if cards are loaded, otherwise the feature hub."""
    uid = call.from_user.id
    cards = await get_cards(uid, state)
    await call.answer()
    if cards:
        await _safe_edit(call, success_caption(len(cards)), main_menu_keyboard())
    else:
        await _safe_edit(call, feature_hub_text(uid), hub_keyboard(uid))


@router.callback_query(F.data == CB_NAV_HELP)
async def cb_nav_help(call: CallbackQuery) -> None:
    uid = call.from_user.id
    await call.answer()
    await _safe_edit(call, help_text(uid), hub_keyboard(uid))


@router.callback_query(F.data == CB_NAV_STATS)
async def cb_nav_stats(call: CallbackQuery) -> None:
    uid = call.from_user.id
    await call.answer()
    await _safe_edit(call, dashboard_text(uid), hub_keyboard(uid))


# ─── Tools-menu entry points ───────────────────────────────────────────────────

@router.callback_query(F.data == CB_TOOL_BIN)
async def cb_tool_bin(call: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(call.from_user.id):
        return await call.answer("🚫 No access.", show_alert=True)
    await state.set_state(BinState.waiting_bin)
    await call.answer()
    await call.message.answer("🔎 <b>BIN Lookup</b>\nSend a 6–8 digit BIN — <code>453201</code>", parse_mode=ParseMode.HTML)


@router.callback_query(F.data == CB_TOOL_PROXY)
async def cb_tool_proxy(call: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(call.from_user.id):
        return await call.answer("🚫 No access.", show_alert=True)
    await state.set_state(ProxyState.waiting_proxy)
    await call.answer()
    await call.message.answer(
        "🛰️ <b>Proxy Checker</b>\nSend a proxy — <code>ip:port</code>\n"
        "(or upload a .txt of proxies with caption <code>proxy</code>)",
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data == CB_TOOL_FEEDBACK)
async def cb_tool_feedback(call: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(call.from_user.id):
        return await call.answer("🚫 No access.", show_alert=True)
    await state.set_state(FeedbackState.waiting_text)
    await call.answer()
    await call.message.answer("💬 <b>Feedback</b>\nPlease type your message. ( /cancel to stop )", parse_mode=ParseMode.HTML)


@router.callback_query(F.data == CB_TOOL_MYID)
async def cb_tool_myid(call: CallbackQuery) -> None:
    uid = call.from_user.id
    await call.answer()
    await call.message.answer(
        f"🆔 <b>Your User ID</b>\n<code>{uid}</code>\n📄 <code>{uid}</code>\n"
        "Send this ID to an admin to request access.",
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data == CB_TOOL_REGISTER)
async def cb_tool_register(call: CallbackQuery, bot: Bot) -> None:
    # Delegate to the existing /register flow.
    await call.answer()
    try:
        await cmd_register(call.message)
    except Exception:
        logger.exception("tool_register")
        await call.message.answer("⚠️ Could not open registration. Try /register")


# ─── State handlers for hub-driven tools (typed input) ─────────────────────────

@router.message(BinState.waiting_bin)
async def bin_input_from_tool(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id
    await state.set_state(None)
    if not can_use_bot(uid):
        return
    raw = re.sub(r"[^\d]", "", message.text or "")[:8]
    if len(raw) < 6:
        return await message.answer(
            "⚠️ Please send a 6–8 digit BIN — e.g. <code>453201</code>",
            parse_mode=ParseMode.HTML,
        )
    bin6 = raw[:6]
    wait = await message.answer(
        f"🔍 Looking up BIN <code>{bin6}</code>...", parse_mode=ParseMode.HTML
    )
    try:
        info = await lookup_bin(bin6, get_http_session(), asyncio.Semaphore(1))
        brand           = _he(info.get("brand", "UNKNOWN"))
        country         = info.get("country", "UNKNOWN")
        country_display = _he(info.get("country_display", country))
        bank            = _he(info.get("bank", "UNKNOWN"))
        tier            = _he(info.get("tier", "STANDARD"))
        guessed_brand   = pan_guess_brand(bin6)
        source = "🌐 API" if info.get("bank") not in ("UNKNOWN", "", None) else "🧠 Local/Guess"
        flag = _country_flag(country)
        await wait.edit_text(
            f"🔍 <b>BIN Lookup — <code>{bin6}</code></b>\n\n"
            f"💳 Brand:   <b>{brand}</b>  <i>(guess: {guessed_brand})</i>\n"
            f"🏦 Bank:    <b>{bank}</b>\n"
            f"🌍 Country: {flag} <b>{country_display}</b>\n"
            f"💎 Tier:    <b>{tier}</b>\n"
            f"📡 Source:  {source}",
            parse_mode=ParseMode.HTML,
            reply_markup=hub_keyboard(uid),
        )
    except Exception:
        logger.exception("bin_input_from_tool")
        await wait.edit_text("⚠️ BIN lookup failed — Please try again later")


@router.message(ProxyState.waiting_proxy)
async def proxy_input_from_tool(message: Message, state: FSMContext, bot: Bot) -> None:
    uid = message.from_user.id
    await state.set_state(None)
    if not can_use_bot(uid):
        return
    text = (message.text or "").strip()
    proxy_list = _parse_proxy_text(text)
    if not proxy_list:
        return await message.answer(
            "⚠️ Could not parse that proxy.\nExample: <code>1.2.3.4:8080</code>",
            parse_mode=ParseMode.HTML,
        )
    if len(proxy_list) > _PROXY_MAX_MANUAL:
        proxy_list = proxy_list[:_PROXY_MAX_MANUAL]
    await _check_proxy_batch(
        message, bot, uid, proxy_list,
        title="Proxy Check",
        source_note=f"🧰 From tools menu — {len(proxy_list):,} proxies",
    )


@router.callback_query(F.data == CB_TOOL_SCR)
async def cb_tool_scr(call: CallbackQuery) -> None:
    if not can_use_bot(call.from_user.id):
        return await call.answer("🚫 No access.", show_alert=True)
    await call.answer()
    try:
        await cmd_scr(call.message)
    except Exception:
        logger.exception("tool_scr")
        await call.message.answer("⚠️ Could not open the scraper. Try /scr")


@router.callback_query(F.data == CB_MIX)
async def cb_mix(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    cards = await require_cards(call, state)
    if not cards:
        return
    await call.answer("🔀 Mixing...")
    shuffled = cards[:]
    random.shuffle(shuffled)
    await save_cards(call.from_user.id, state, shuffled)
    await send_result_file(call, bot, shuffled, "mixed_cards.txt", f"🔀 Mixed — {len(shuffled)} cards")



@router.callback_query(F.data == CB_FORMAT)
async def cb_format(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    cards = await require_cards(call, state)
    if not cards:
        return
    await call.answer("🎨 Formatting...")
    formatted: list[str] = []
    for line in cards:
        parts = line.split("|")
        if len(parts) == 4:
            try:
                obj = CardLine(number=parts[0], month=parts[1], year=parts[2], cvc=parts[3])
                formatted.append(obj.normalized(2))
                continue
            except Exception:
                pass  # unparseable format — keep original line as-is
        formatted.append(line)  # unparseable → keep original
    await save_cards(call.from_user.id, state, formatted)
    await send_result_file(call, bot, formatted, "formatted_cards.txt", f"🎨 {len(formatted)} cards")


@router.callback_query(F.data == CB_CLEAN)
async def cb_clean(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    cards = await require_cards(call, state)
    if not cards:
        return
    await call.answer("🧹 Cleaning...")
    cleaned = _strict_clean_cards(cards)
    if not cleaned:
        return await call.answer("⚠️ No valid CC|MM|YY|CVC entries found.", show_alert=True)
    alive, exp_dropped = _remove_expired_cards(cleaned)
    if not alive:
        return await call.answer("⚠️ All cards have expired.", show_alert=True)
    await save_cards(call.from_user.id, state, alive)
    combo_cut = len(cards) - len(cleaned)
    await send_result_file(
        call,
        bot,
        alive,
        "strictly_cleaned_cards.txt",
        f"🧹 {len(alive)} live cards | -{exp_dropped} expired | -{combo_cut} combo/dup",
    )




@router.callback_query(F.data == CB_SPLIT)
async def cb_split_menu(call: CallbackQuery, state: FSMContext) -> None:
    if not await require_cards(call, state):
        return
    await call.answer()
    await _safe_edit(
        call,
        "✂️ <b>Advanced Split</b>\nChoose a split method:",
        split_mode_keyboard(),
    )


@router.callback_query(F.data == CB_SPLIT_COUNT)
async def cb_split_count(call: CallbackQuery, state: FSMContext) -> None:
    if not await require_cards(call, state):
        return
    await state.set_state(SplitState.waiting_lines)
    await call.answer()
    await call.message.answer(
        "📦 <b>Split by Count</b>\nHow many cards per file?\nExample: <code>500</code>",
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data == CB_SPLIT_BRAND)
async def cb_split_brand(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    cards = await require_cards(call, state)
    if not cards:
        return
    await call.answer("💳 Split by brand...")
    wait = await call.message.answer(
        f"⏳ <b>Split by Brand</b>\n<code>{render_bar(0)}</code>",
        parse_mode=ParseMode.HTML,
    )
    prog = ProgressMessage(wait, "Split by Brand")
    try:
        info = await enrich_bins(cards, progress=prog)
        buckets = _bucket_cards_by_meta(cards, info, "brand")
        n = await _send_split_buckets(bot, call.message.chat.id, buckets, "split_brand", prog)
        await prog.finish(f"✅ Brand split — {n} file(s)")
    except Exception:
        logger.exception("split_brand")
        await prog.finish("⚠️ Split Error")


@router.callback_query(F.data == CB_SPLIT_COUNTRY)
async def cb_split_country(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    cards = await require_cards(call, state)
    if not cards:
        return
    await call.answer("🌍 Split by country...")
    wait = await call.message.answer(
        f"⏳ <b>Split by Country</b>\n"
        f"<i>Looking up issuer country for {len(cards):,} cards…</i>\n"
        f"<code>{render_bar(0)}</code>",
        parse_mode=ParseMode.HTML,
    )
    prog = ProgressMessage(wait, "Split by Country")
    try:
        info = await enrich_bins(cards, progress=prog)

        # Group by canonical country, merging every spelling of the same
        # country that the different BIN sources may have returned.
        buckets = _bucket_cards_by_country(cards, info)
        if not buckets:
            return await prog.finish("⚠️ No cards to split.")

        # Largest group first, UNKNOWN always at the very end.
        def _sort_key(item):
            (a2, name), chunk = item
            is_unknown = 1 if a2 == "UNKNOWN" else 0
            return (is_unknown, -len(chunk), name)

        ordered = sorted(buckets.items(), key=_sort_key)

        summary_lines: list[str] = []
        sent = 0
        total = len(ordered)
        for i, ((a2, name), chunk) in enumerate(ordered, 1):
            flag = _country_flag(a2)
            await prog.update(i, total, f"{flag} {name} — {len(chunk)} cards")
            fname = f"split_{_safe_filename_label(a2)}_{_safe_filename_label(name)}.txt"
            await bot.send_document(
                call.message.chat.id,
                _make_txt_file(chunk, fname),
                caption=f"{flag} <b>{_he(name)}</b> ({a2}) — {len(chunk):,} cards",
                parse_mode=ParseMode.HTML,
            )
            sent += 1
            pct = (len(chunk) / len(cards) * 100) if cards else 0
            summary_lines.append(
                f"{flag} <b>{_he(name)}</b> — <b>{len(chunk):,}</b> ({pct:.1f}%)"
            )

        # Unknown bucket size → resolved percentage for the header.
        unknown_n = len(buckets.get(("UNKNOWN", "Unknown"), []))
        resolved_n = len(cards) - unknown_n
        resolved_pct = (resolved_n / len(cards) * 100) if cards else 0.0

        header = (
            f"✅ <b>Country Split Complete</b>\n\n"
            f"💳 Total cards: <b>{len(cards):,}</b>\n"
            f"🌍 Countries: <b>{sent}</b>\n"
            f"📦 Files sent: <b>{sent}</b>\n"
            f"🎯 Resolved: <b>{resolved_n:,}</b> ({resolved_pct:.1f}%)"
        )
        if unknown_n:
            header += (
                f"  ·  🌐 Unknown: <b>{unknown_n:,}</b>\n"
                "<i>Unknown cards have a BIN no public source could resolve "
                "(or an offline/timeout API). Re-running often resolves more.</i>"
            )
        summary = header + "\n━━━━━━━━━━━━━━━━\n" + "\n".join(summary_lines)
        await prog.finish(summary)
    except Exception:
        logger.exception("split_country")
        await prog.finish("⚠️ Split Error")


@router.message(SplitState.waiting_lines)
async def split_lines_input(message: Message, state: FSMContext, bot: Bot) -> None:
    uid = message.from_user.id
    cards = await get_cards(uid, state)
    if not cards:
        await state.set_state(None)
        return await message.answer("Session expired — Please upload file again")
    try:
        n = int((message.text or "").strip())
        if n < 1:
            raise ValueError
    except ValueError:
        return await message.answer("⚠️ Enter a whole number (e.g. 500).")
    chunks = [cards[i : i + n] for i in range(0, len(cards), n)]
    await state.set_state(None)
    prog_msg = await message.answer(
        f"⏳ <b>Splitting files</b>\n<code>{render_bar(0)}</code>",
        parse_mode=ParseMode.HTML,
    )
    prog = ProgressMessage(prog_msg, "Splitting files")
    for i, chunk in enumerate(chunks, 1):
        await prog.update(i, len(chunks), f"Sending part {i}/{len(chunks)}")
        await bot.send_document(
            message.chat.id,
            _make_txt_file(chunk, f"split_{i}_of_{len(chunks)}.txt"),
            caption=f"Part {i}/{len(chunks)} — {len(chunk)}",
        )
    await prog.finish(f"✅ Split complete — {len(chunks)} files")


@router.callback_query(F.data == CB_FILTER)
async def cb_filter(call: CallbackQuery, state: FSMContext) -> None:
    if not await require_cards(call, state):
        return
    await state.set_state(FilterState.waiting_bin)
    await call.answer()
    await call.message.answer("🔍 Enter a 6-digit BIN — <code>411111</code>", parse_mode=ParseMode.HTML)


@router.message(FilterState.waiting_bin)
async def filter_bin_input(message: Message, state: FSMContext, bot: Bot) -> None:
    uid = message.from_user.id
    cards = await get_cards(uid, state)
    if not cards:
        await state.set_state(None)
        return await message.answer("Session expired — Please upload file again")
    bin6 = (message.text or "").strip()
    if not re.fullmatch(r"\d{6}", bin6):
        return await message.answer("⚠️ Enter a 6-digit BIN.")
    filtered = [c for c in cards if _pan_from_line(c).startswith(bin6)]
    await state.set_state(None)
    await save_cards(uid, state, filtered)
    if not filtered:
        return await message.answer(f"⚠️ No matches for BIN {bin6}.")
    await send_result_file(message, bot, filtered, f"bin_{bin6}.txt", f"🔍 {len(filtered)} cards")



@router.callback_query(F.data == CB_COMBINE)
async def cb_combine(call: CallbackQuery, state: FSMContext) -> None:
    if not await require_cards(call, state):
        return
    await state.set_state(CombineState.collecting)
    await call.answer()
    await call.message.answer(
        "🔄 <b>COMBINE</b> — .txt ထပ်တင်ပါ → Finish merge\n/cancel",
        parse_mode=ParseMode.HTML,
        reply_markup=combine_keyboard(),
    )


@router.callback_query(F.data == CB_COMBINE_CANCEL)
async def cb_combine_cancel(call: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(None)
    await call.answer("Cancelled")
    await call.message.edit_text("❌ Combine cancelled")


@router.callback_query(F.data == CB_COMBINE_DONE)
async def cb_combine_done(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    cards = await require_cards(call, state)
    if not cards:
        return
    merged = list(dict.fromkeys(cards))
    await save_cards(call.from_user.id, state, merged)
    await state.set_state(None)
    await call.answer("Merging...")
    await send_result_file(call, bot, merged, "combined_master.txt", f"🔄 {len(merged)} unique cards")



def _check_rate_limit(user_id: int) -> bool:
    """True = allowed, False = rate-limited."""
    if is_admin(user_id) or is_vip(user_id):
        return True
    now = time.monotonic()
    stamps = RATE_LIMIT.get(user_id, [])
    stamps = [t for t in stamps if now - t < RATE_LIMIT_WINDOW]
    if len(stamps) >= RATE_LIMIT_MAX:
        RATE_LIMIT[user_id] = stamps
        return False
    stamps.append(now)
    RATE_LIMIT[user_id] = stamps
    return True


@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    if not await _admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("Usage: <code>/ban 123456789</code>", parse_mode=ParseMode.HTML)
    uid = int(parts[1])
    BANNED_IDS.add(uid)
    await message.answer(f"🚫 Banned: <code>{uid}</code>", parse_mode=ParseMode.HTML)


@router.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    if not await _admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("Usage: <code>/unban 123456789</code>", parse_mode=ParseMode.HTML)
    uid = int(parts[1])
    BANNED_IDS.discard(uid)
    await message.answer(f"✅ Unbanned: <code>{uid}</code>", parse_mode=ParseMode.HTML)


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, bot: Bot) -> None:
    if not await _admin_only(message):
        return
    text = (message.text or "").split(None, 1)
    if len(text) < 2:
        return await message.answer("Usage: <code>/broadcast မက်ဆေ့ message</code>", parse_mode=ParseMode.HTML)
    msg = text[1]
    targets = list(USER_HISTORY.keys())
    ok = fail = 0
    for uid in targets:
        try:
            await bot.send_message(uid, "📢 <b>Broadcast</b>\n\n" + msg, parse_mode=ParseMode.HTML)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await message.answer(f"📢 Broadcast ပြီး — ✅ {ok} | ❌ {fail}")


@router.message(Command("usagestats"))
async def cmd_usagestats(message: Message) -> None:
    if not await _admin_only(message):
        return
    total_uploads = sum(len(v) for v in USER_HISTORY.values())
    total_cards = sum(
        sum(e.get("count", 0) for e in v) for v in USER_HISTORY.values()
    )
    await message.answer(
        "📊 <b>Usage Stats</b>\n\n"
        f"⏱ Uptime: <b>{_format_uptime()}</b>\n"
        f"👥 Unique users: <b>{len(USER_HISTORY)}</b>\n"
        f"📤 Total uploads: <b>{total_uploads}</b>\n"
        f"💳 Total cards processed: <b>{total_cards:,}</b>\n"
        f"🚫 Banned: <b>{len(BANNED_IDS)}</b>\n"
        f"💬 Feedback received: <b>{len(FEEDBACK_LOG)}</b>",
        parse_mode=ParseMode.HTML,
    )


def _admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👥 User List",    callback_data="adm:userlist"),
                InlineKeyboardButton(text="⭐ VIP List",     callback_data="adm:viplist"),
            ],
            [
                InlineKeyboardButton(text="🚫 Banned List",  callback_data="adm:banlist"),
                InlineKeyboardButton(text="📊 Stats",        callback_data="adm:stats"),
            ],
            [
                InlineKeyboardButton(text="🔒 → Private",   callback_data="adm:setprivate"),
                InlineKeyboardButton(text="🌐 → Public",    callback_data="adm:setpublic"),
            ],
            [
                InlineKeyboardButton(text="💬 Feedbacks",   callback_data="adm:feedbacks"),
                InlineKeyboardButton(text="❌ Close",        callback_data="adm:close"),
            ],
            [
                InlineKeyboardButton(text="🏠 Home",        callback_data=CB_NAV_HOME),
            ],
        ]
    )


@router.message(Command("adminpanel"))
async def cmd_adminpanel(message: Message) -> None:
    if not await _admin_only(message):
        return
    mode   = "🔒 Private" if is_restricted() else "🌐 Public"
    vip_c  = len(list_vip())
    ban_c  = len(BANNED_IDS)
    user_c = len(USER_HISTORY)
    await message.answer(
        f"╔══════════════════════╗\n"
        f"║  👑  ADMIN PANEL      ║\n"
        f"╚══════════════════════╝\n\n"
        f"🔐 Mode: <b>{mode}</b>\n"
        f"👥 Active users: <b>{user_c}</b>\n"
        f"⭐ VIP: <b>{vip_c}</b>  |  🚫 Banned: <b>{ban_c}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=_admin_panel_keyboard(),
    )


@router.callback_query(F.data.startswith("adm:"))
async def cb_admin_panel(call: CallbackQuery, bot: Bot) -> None:
    if not is_admin(call.from_user.id):
        return await call.answer("⛔ Admin သာ သုံးခွင့်ရှိသည်", show_alert=True)
    action = call.data.split(":", 1)[1]

    if action == "close":
        await call.answer()
        try:
            await call.message.delete()
        except Exception:
            pass
        return

    if action == "panel":
        await call.answer()
        mode  = "🔒 Private" if is_restricted() else "🌐 Public"
        vip_c  = len(list_vip())
        ban_c  = len(BANNED_IDS)
        user_c = len(USER_HISTORY)
        await _safe_edit(
            call,
            f"╔══════════════════════╗\n"
            f"║  👑  ADMIN PANEL      ║\n"
            f"╚══════════════════════╝\n\n"
            f"🔐 Access: <b>{mode}</b>\n"
            f"👥 Users: <b>{user_c}</b>  ·  ⭐ VIP: <b>{vip_c}</b>  ·  🚫 Banned: <b>{ban_c}</b>",
            _admin_panel_keyboard(),
        )
        return

    if action == "stats":
        await call.answer()
        total_uploads = sum(len(v) for v in USER_HISTORY.values())
        total_cards   = sum(sum(e.get("count", 0) for e in v) for v in USER_HISTORY.values())
        await call.message.answer(
            f"📊 <b>Usage Stats</b>\n\n"
            f"⏱ Uptime: <b>{_format_uptime()}</b>\n"
            f"👥 Unique users: <b>{len(USER_HISTORY)}</b>\n"
            f"📤 Total uploads: <b>{total_uploads}</b>\n"
            f"💳 Total cards: <b>{total_cards:,}</b>\n"
            f"🚫 Banned: <b>{len(BANNED_IDS)}</b>\n"
            f"💬 Feedbacks: <b>{len(FEEDBACK_LOG)}</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "userlist":
        await call.answer()
        if not USER_HISTORY:
            return await call.message.answer("👥 User မရှိသေးပါ")
        lines = []
        for uid, hist in list(USER_HISTORY.items())[-20:]:
            role = "👑" if is_admin(uid) else ("⭐" if is_vip(uid) else ("🚫" if uid in BANNED_IDS else "👤"))
            last = hist[-1].get("ts", "—") if hist else "—"
            username = USER_PROFILES.get(uid) or (hist[-1].get("username", "") if hist else "")
            uname_text = f"@{username}" if username else "—"
            lines.append(f"{role} <code>{uid}</code> — {uname_text} — {len(hist)} uploads | Last: {last}")
        await call.message.answer(
            f"👥 <b>Recent Users ({len(USER_HISTORY)} total)</b>\n\n" + "\n".join(lines),
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "viplist":
        await call.answer()
        vips = list_vip()
        if not vips:
            return await call.message.answer("⭐ VIP မရှိသေးပါ")
        body = "\n".join(f"  ⭐ <code>{v}</code>" for v in vips)
        await call.message.answer(
            f"⭐ <b>VIP List ({len(vips)})</b>\n\n{body}\n\n"
            f"<i>ဖယ်ရှားရန်: /delvip &lt;id&gt;</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "banlist":
        await call.answer()
        if not BANNED_IDS:
            return await call.message.answer("🚫 Banned user မရှိပါ")
        body = "\n".join(f"  🚫 <code>{v}</code>" for v in sorted(BANNED_IDS))
        await call.message.answer(
            f"🚫 <b>Banned List ({len(BANNED_IDS)})</b>\n\n{body}\n\n"
            f"<i>ဖြေရှင်းရန်: /unban &lt;id&gt;</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "setprivate":
        set_restricted(True)
        await call.answer("🔒 Private mode ဖွင့်ပြီး", show_alert=True)
        await call.message.edit_text(
            call.message.text.replace("🌐 Public", "🔒 Private"),
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_panel_keyboard(),
        )
        return

    if action == "setpublic":
        set_restricted(False)
        await call.answer("🌐 Public mode ဖွင့်ပြီး", show_alert=True)
        await call.message.edit_text(
            call.message.text.replace("🔒 Private", "🌐 Public"),
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_panel_keyboard(),
        )
        return

    if action == "feedbacks":
        await call.answer()
        if not FEEDBACK_LOG:
            return await call.message.answer("💬 Feedback မရှိသေးပါ")
        lines = []
        for fb in FEEDBACK_LOG[-10:]:
            lines.append(f"👤 <code>{fb.get('uid','?')}</code> [{fb.get('ts','?')}]\n  {fb.get('text','')}")
        await call.message.answer(
            f"💬 <b>Latest Feedbacks ({len(FEEDBACK_LOG)} total)</b>\n\n" + "\n\n".join(lines),
            parse_mode=ParseMode.HTML,
        )
        return

    await call.answer("Unknown action", show_alert=True)


def _record_history(user_id: int, filename: str, count: int, username: str | None = None) -> None:
    if username:
        USER_PROFILES[user_id] = username.lstrip("@")

    entry = {
        "file": filename,
        "count": count,
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "username": USER_PROFILES.get(user_id, ""),
    }
    USER_HISTORY.setdefault(user_id, []).append(entry)
    if len(USER_HISTORY[user_id]) > 20:
        USER_HISTORY[user_id] = USER_HISTORY[user_id][-20:]




def _auto_backup(user_id: int, cards: list[str], filename: str) -> None:
    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe = re.sub(r"[^\w\-.]", "_", filename)[:40]
        path = BACKUP_DIR / f"{user_id}_{ts}_{safe}.txt"
        path.write_text("\n".join(cards), encoding="utf-8")
        # keep only last 5 backups per user
        user_backups = sorted(BACKUP_DIR.glob(f"{user_id}_*.txt"))
        for old in user_backups[:-5]:
            old.unlink(missing_ok=True)
    except Exception as e:
        logger.error("Auto-backup failed for user %s: %s", user_id, e)


@router.callback_query(F.data == CB_RENAME)
async def cb_rename_prompt(call: CallbackQuery, state: FSMContext) -> None:
    if not await require_cards(call, state):
        return
    await state.set_state(RenameState.waiting_name)
    await call.answer()
    await call.message.answer("✏️ Enter output file name (e.g.: <code>mylist</code>)\n.txt will be auto-added", parse_mode=ParseMode.HTML)


@router.message(RenameState.waiting_name)
async def rename_input(message: Message, state: FSMContext, bot: Bot) -> None:
    uid = message.from_user.id
    cards = await get_cards(uid, state)
    if not cards:
        await state.set_state(None)
        return await message.answer("Session expired — Please upload file again")
    raw = (message.text or "").strip()
    if not raw:
        return await message.answer("⚠️ Please enter a name")
    safe = re.sub(r"[^\w\-. ]", "_", raw)[:48].strip() + ".txt"
    await state.set_state(None)
    await send_result_file(message, bot, cards, safe, f"✏️ {len(cards)} cards → {safe}")


@router.message(Command("feedback"))
async def cmd_feedback_prompt(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id):
        return
    await state.set_state(FeedbackState.waiting_text)
    await message.answer("💬 Please write your feedback or suggestion:", parse_mode=ParseMode.HTML)


@router.message(FeedbackState.waiting_text)
async def feedback_input(message: Message, state: FSMContext, bot: Bot) -> None:
    await state.set_state(None)
    text = (message.text or "").strip()
    if not text:
        return await message.answer("⚠️ No feedback provided.")
    uid = message.from_user.id
    uname = message.from_user.username or "—"
    FEEDBACK_LOG.append({"uid": uid, "uname": uname, "text": text,
                          "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")})
    await message.answer("✅ Feedback sent — thank you!")
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"💬 <b>Feedback</b> from @{uname} (<code>{uid}</code>)\n\n{text}",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.debug("feedback delivery to admin failed: %s", e)


@router.message(Command("feedbacklist"))
async def cmd_feedbacklist(message: Message) -> None:
    if not await _admin_only(message):
        return
    if not FEEDBACK_LOG:
        return await message.answer("💬 No feedback yet.")
    lines = [f"💬 <b>Feedback Log ({len(FEEDBACK_LOG)})</b>"]
    for i, e in enumerate(FEEDBACK_LOG[-15:], 1):
        lines.append(f"{i}. @{e['uname']} (<code>{e['uid']}</code>) [{e['ts']}]\n   {e['text'][:120]}")
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("export"))
async def cmd_export_all_cards(message: Message, bot: Bot) -> None:
    # 🎯 Admin သာလျှင် သုံးခွင့်ရှိအောင် တင်းကျပ်ထားခြင်း
    if not await _admin_only(message):
        return
        
    wait_msg = await message.answer("⏳ <b>Exporting all users' cards...</b>", parse_mode=ParseMode.HTML)
    
    # 🎯 Bot ထဲတွင်ရှိသော User အားလုံး၏ ကတ်များကို တစ်စုတစ်စည်းတည်း ဆွဲထုတ်မည်
    all_cards = []
    for uid, cards in USER_CARDS.items():
        if cards:
            all_cards.extend(cards)
            
    # 🎯 ကတ်အထပ်တွေ (Duplicates) ပါနေရင် ဖယ်ထုတ်မည်
    all_cards = list(dict.fromkeys(all_cards))
    
    if not all_cards:
        return await wait_msg.edit_text("⚠️ No recorded cards from any user are available yet.")
        
    # 🎯 ကတ်အားလုံးကို .txt ဖိုင်အဖြစ် ပြောင်းလဲပြီး Admin ထံသို့ ပို့ပေးမည်
    await send_result_file(
        message, 
        bot, 
        all_cards, 
        f"all_users_{len(all_cards)}_cards.txt", 
        f"📦 <b>All Users' Cards Exported</b>\n💳 စုစုပေါင်း: <b>{len(all_cards):,}</b> cards"
    )
    await wait_msg.delete()

 
#
#  Limits  :  Admin → unlimited  |  VIP/User → 5,000
#  Usage   :  /scr @channel  |  /scr @channel 3000  |  /scr https://t.me/ch 500
#
#  Speed fixes vs old version:
#    • Batch fetch 200 messages per Telethon API call (was 1-at-a-time)
#    • Parse the whole batch as one joined string → single regex pass
#      instead of N separate thread-pool dispatches
#    • wait_time=0 removes Telethon's artificial inter-request sleep
#    • Progress edit only every 5 s (not every message) → no Telegram rate-limit
#
#  "database is locked" fix:
#    • Global asyncio.Lock → only one scrape at a time, no SQLite contention
#    • try/finally guarantees client.disconnect() even on crash
#    • StringSession("") used so session is in-memory; the .session file is
#      only written on clean shutdown, never left half-locked

SCR_USER_LIMIT  = 5_000    # hard cap for non-admins
_SCR_BATCH      = 100      # Telegram's real per-request cap — avoids Telethon
                            # silently splitting one call into 4 sub-requests
_SCR_CHUNK_SIZE = 5_000    # send a partial file every N cards — more frequent
                            # checkpoints so a late FloodWait doesn't cost 10k
_SCR_PACE_SEC   = 1.0      # delay between requests — 0.35s was still tripping
                            # severe (1500s+) FloodWait on a regular account.
                            # 1s/request ≈ 100 msgs/sec scan rate, well under
                            # Telegram's threshold for sustained get_messages calls


@router.message(Command("scr"))
async def cmd_scr(message: Message, bot: Bot) -> None:
    uid = message.from_user.id
    if not can_use_bot(uid):
        return

    args = (message.text or "").split()
    if len(args) < 2:
        limit_hint = "unlimited" if is_admin(uid) else f"{SCR_USER_LIMIT:,}"
        return await message.answer(
            "⚠️ <b>Syntax:</b>\n"
            "<code>/scr @channel</code>\n"
            "<code>/scr @channel 3000</code>\n"
            "<code>/scr https://t.me/channel 5000</code>\n\n"
            f"🔒 Your limit: <b>{limit_hint}</b> cards",
            parse_mode=ParseMode.HTML,
        )

    # ── Parse channel ──────────────────────────────────────────────────────────
    raw = args[1].strip()
    if raw.startswith("https://t.me/") or raw.startswith("http://t.me/"):
        after = raw.split("t.me/", 1)[1]
        if after.startswith("+") or "joinchat" in after:
            channel = raw               # private invite link — pass as-is
        else:
            channel = after.split("/")[0].split("?")[0]
    elif raw.startswith("t.me/"):
        after = raw.split("t.me/", 1)[1]
        channel = raw if (after.startswith("+") or "joinchat" in after) else after.split("/")[0].split("?")[0]
    else:
        channel = raw

    # ── Requested card count ───────────────────────────────────────────────────
    if len(args) >= 3 and args[2].isdigit():
        requested = max(1, int(args[2]))
    else:
        requested = SCR_USER_LIMIT

    if is_admin(uid):
        limit = requested
    else:
        limit = min(requested, SCR_USER_LIMIT)
        if requested > SCR_USER_LIMIT:
            await message.answer(
                f"⚠️ Non-admin cap is <b>{SCR_USER_LIMIT:,}</b>. "
                f"Capping to <b>{limit:,}</b>.",
                parse_mode=ParseMode.HTML,
            )

    # ── Concurrency guard ──────────────────────────────────────────────────────
    sem = _get_scr_semaphore()
    if sem.locked():   # all slots currently taken
        await message.answer(
            f"⏳ {_SCR_MAX_CONCURRENT} scrape(s) already running — "
            "yours is queued and will start automatically.\n"
            "Send /cancel to abort it while it waits.",
            parse_mode=ParseMode.HTML,
        )

    wait_msg = await message.answer(
        f"🔍 <b>Scraping</b> <code>{_he(channel)}</code>\n"
        f"🎯 Target: <b>{limit:,}</b> cards\n"
        f"<code>{render_bar(0)}</code>",
        parse_mode=ParseMode.HTML,
    )

    async with sem:
        client: TelegramClient | None = None
        cancel_ev = _scr_register(uid)
        try:
            if cancel_ev.is_set():
                return await wait_msg.edit_text("❌ Scrape cancelled.")

            SCR_SESSION = "1AZWarzoBu8DWgSoxz1hzjUv2iKctAVIUv3MLcW-EvBSH-wzO9dYKQ5y_c6B4Ds1jdyudqpd_ofijc1dCO1mZ3yvblydiGk-Kq0wWmEbLLIESN_LuFCQg0mh6SclUWFDdDRr3aqo2G-pHtyWaVeipOhrfOhWTeUMkUOe3NNp8MjJ0ITgkFuOKw2YA4k9yNqbizx73ZKkX_MIiVjeNczit5LYPpqkrb8z0cHgHaCUxVcCxIycTTD5bTQb42k0j4tiriCpuTgQ1fDlSx_ZN8gMe5NmwM-jDQkStSzKfyhg-CTu1VANbEjK1V-d2B4aP2UmzyWspwAaEdwyydyMnyyOpVp9LjKcBw70="
            client = TelegramClient(StringSession(SCR_SESSION), API_ID, API_HASH)
            await client.start()

            all_cards:    list[str] = []
            seen:         set[str]  = set()
            msgs_scanned  = 0
            offset_id     = 0
            last_edit     = 0.0
            chunk_number  = 0
            sent_count    = 0          # cards already delivered in files
            pace          = _SCR_PACE_SEC
            done          = False
            loop          = asyncio.get_running_loop()
            safe          = f"CardSync7_Bot_u{uid}"

            async def _flush(force: bool = False) -> None:
                """Send any not-yet-delivered cards as the next chunk file."""
                nonlocal chunk_number, sent_count
                backlog = len(all_cards) - sent_count
                if backlog <= 0:
                    return
                if not force and backlog < _SCR_CHUNK_SIZE:
                    return
                chunk_cards = all_cards[sent_count: sent_count + _SCR_CHUNK_SIZE]
                chunk_number += 1
                part_fname = f"scrd_{safe}_part{chunk_number}_{len(chunk_cards)}.txt"
                ok = await _scr_send_document(
                    message,
                    part_fname,
                    chunk_cards,
                    f"📦 <b>Part {chunk_number}</b> — {len(chunk_cards):,} cards\n"
                    f"💳 Total so far: <b>{len(all_cards):,}</b> / <b>{limit:,}</b>",
                )
                if ok:
                    sent_count += len(chunk_cards)

            while not done:
                if cancel_ev.is_set():
                    await _flush(force=True)
                    return await wait_msg.edit_text(
                        f"⏹ <b>Scrape cancelled</b>\n"
                        f"💳 Cards found: <b>{len(all_cards):,}</b>\n"
                        f"📦 Files sent: <b>{chunk_number}</b>",
                        parse_mode=ParseMode.HTML,
                    )

                # ── Batch fetch ────────────────────────────────────────────────
                # FloodWait is handled separately from ordinary transient
                # errors: a flood wait retries the SAME offset_id indefinitely
                # (bounded only by cancellation and the severe-cooldown bail),
                # while network/RPC errors get a small bounded retry budget.
                batch = None
                transient = 0
                while True:
                    if cancel_ev.is_set():
                        break
                    try:
                        if not client.is_connected():
                            await client.connect()
                        batch = await client.get_messages(
                            channel,
                            limit=_SCR_BATCH,
                            offset_id=offset_id,
                        )
                        break
                    except FloodWaitError as fw:
                        wait_secs = fw.seconds + 2
                        logger.warning("FloodWait %ss — pausing scrape", wait_secs)

                        if wait_secs > _SCR_FLOOD_BAIL_SEC:
                            # Severe, account-wide cooldown: stop early but keep
                            # whatever we already found.
                            await _flush(force=True)
                            mins = wait_secs // 60
                            return await wait_msg.edit_text(
                                f"⏳ <b>Telegram rate limit hit — ~{mins} min cooldown</b>\n"
                                f"💳 Found before stopping: <b>{len(all_cards):,}</b>\n"
                                f"📦 Files sent: <b>{chunk_number}</b>\n"
                                "The account is rate-limited account-wide right now. "
                                "Please try again later instead of waiting here.",
                                parse_mode=ParseMode.HTML,
                            )

                        # Moderate cooldown: back off, raise the pace so the next
                        # requests are gentler, then retry the SAME offset_id.
                        pace = min(_SCR_PACE_MAX, max(pace * 1.5, wait_secs / 4))
                        try:
                            await wait_msg.edit_text(
                                f"⏳ <b>Scraping paused</b> (Telegram rate limit)\n"
                                f"💳 Found so far: <b>{len(all_cards):,}</b>\n"
                                f"⏱ Resuming in <b>{wait_secs}s</b>…",
                                parse_mode=ParseMode.HTML,
                            )
                        except Exception:
                            pass
                        await _scr_sleep_cancellable(wait_secs, cancel_ev)
                        continue

                    except (AuthKeyDuplicatedError, RpcCallFailError,
                            ServerError, TimedOutError,
                            RPCError, asyncio.TimeoutError,
                            ConnectionError, OSError) as e:
                        transient += 1
                        logger.warning(
                            "transient SCR error (attempt %d/%d): %s",
                            transient, _SCR_TRANSIENT_RETRY, e,
                        )
                        if transient > _SCR_TRANSIENT_RETRY:
                            raise
                        # Drop the connection so the next attempt reconnects.
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                        await _scr_sleep_cancellable(min(2 ** transient, 15), cancel_ev)
                        continue

                if cancel_ev.is_set():
                    await _flush(force=True)
                    return await wait_msg.edit_text(
                        f"⏹ <b>Scrape cancelled</b>\n"
                        f"💳 Cards found: <b>{len(all_cards):,}</b>\n"
                        f"📦 Files sent: <b>{chunk_number}</b>",
                        parse_mode=ParseMode.HTML,
                    )
                if not batch:
                    break   # end of channel history

                # ── Parse text + captions (channels often post cards as captions)
                parts: list[str] = []
                for msg in batch:
                    if getattr(msg, "text", None):
                        parts.append(msg.text)
                    if getattr(msg, "caption", None):
                        parts.append(msg.caption)
                combined = "\n".join(parts)
                msgs_scanned += len(batch)
                offset_id     = batch[-1].id

                if combined:
                    new_cards: list[str] = await loop.run_in_executor(
                        THREAD_POOL, _parse_cards_sync, combined, False
                    )
                    for card in new_cards:
                        if card not in seen:
                            seen.add(card)
                            all_cards.append(card)
                            if len(all_cards) >= limit:
                                done = True
                                break

                # ── Chunked delivery ──────────────────────────────────────────
                await _flush(force=done)

                # ── Progress update (max once per 5 s) ────────────────────────
                now = time.monotonic()
                if now - last_edit >= 5.0 or done:
                    last_edit = now
                    pct = min(100, int(len(all_cards) / limit * 100)) if limit else 0
                    try:
                        await wait_msg.edit_text(
                            f"🔍 <b>Scraping</b> <code>{_he(channel)}</code>\n"
                            f"💳 Found : <b>{len(all_cards):,}</b> / <b>{limit:,}</b>\n"
                            f"📨 Scanned: <b>{msgs_scanned:,}</b> msgs\n"
                            f"📦 Parts sent: <b>{chunk_number}</b>\n"
                            f"<code>{render_bar(pct)}</code>",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

                # ── Adaptive pace (gentler if we've been throttled) ────────────
                if not await _scr_sleep_cancellable(pace, cancel_ev):
                    continue
                # Slowly relax the pace back toward the base when things are calm.
                if pace > _SCR_PACE_SEC:
                    pace = max(_SCR_PACE_SEC, pace * 0.9)

            # Deliver anything still pending (final chunk).
            await _flush(force=True)

            if not all_cards:
                return await wait_msg.edit_text(
                    f"😔 No cards found in <code>{_he(channel)}</code>.\n"
                    f"📨 Scanned <b>{msgs_scanned:,}</b> messages.\n"
                    "Make sure the Userbot has joined this channel.",
                    parse_mode=ParseMode.HTML,
                )

            await wait_msg.edit_text(
                f"✅ <b>Scrape Complete</b>\n"
                f"📡 Channel : <code>{_he(channel)}</code>\n"
                f"💳 Total   : <b>{len(all_cards[:limit]):,}</b> cards\n"
                f"📦 Files   : <b>{chunk_number}</b> parts\n"
                f"📨 Scanned : <b>{msgs_scanned:,}</b> messages",
                parse_mode=ParseMode.HTML,
            )

        except Exception as e:
            err = str(e)
            logger.exception("SCR failed for uid=%s channel=%s", uid, channel)
            try:
                await wait_msg.edit_text(
                    f"❌ <b>Scrape failed</b>\n<code>{_he(err[:300])}</code>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        finally:
            # Always release the Telegram connection and the cancellation slot,
            # no matter how we leave (normal end, cancel, bail, or crash).
            try:
                if client is not None:
                    await client.disconnect()
            except Exception:
                pass
            client = None
            _scr_unregister(uid)


@router.message(Command("register"))
async def cmd_register(message: Message, bot: Bot) -> None:
    uid = message.from_user.id
    uname = message.from_user.username or "—"
    if can_use_bot(uid):
        return await message.answer("✅ You already have access!")
    if uid in BANNED_IDS:
        return await message.answer("🚫 You are banned.")
    # Notify admins for approval
    approval_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Approve VIP", callback_data=f"approve:{uid}"),
        InlineKeyboardButton(text="🚫 Deny", callback_data=f"deny:{uid}"),
    ]])
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"📝 <b>Access Request</b>\n@{uname} (<code>{uid}</code>) is requesting bot access.",
                parse_mode=ParseMode.HTML,
                reply_markup=approval_kb,
            )
        except Exception as e:
            logger.debug("register notify to admin failed: %s", e)
    await message.answer("📝 Request sent — please wait for admin approval.")


@router.callback_query(F.data.startswith("approve:"))
async def cb_approve(call: CallbackQuery, bot: Bot) -> None:
    if not is_admin(call.from_user.id):
        return await call.answer("⛔️ Admin only")
    uid = int(call.data.split(":")[1])
    add_vip(uid)
    await call.answer(f"✅ {uid} approved")
    await call.message.edit_text(f"✅ <code>{uid}</code> VIP ထည့်ပြီး", parse_mode=ParseMode.HTML)
    try:
        await bot.send_message(uid, "🎉 <b>Access Approved!</b>\nYou can now use the bot — /start", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.debug("approve notify to user %s failed: %s", uid, e)


@router.callback_query(F.data.startswith("deny:"))
async def cb_deny(call: CallbackQuery, bot: Bot) -> None:
    if not is_admin(call.from_user.id):
        return await call.answer("⛔️ Admin only")
    uid = int(call.data.split(":")[1])
    await call.answer(f"🚫 {uid} denied")
    await call.message.edit_text(f"🚫 <code>{uid}</code> ငြင်းပယ်ပြီး", parse_mode=ParseMode.HTML)
    try:
        await bot.send_message(uid, "❌ <b>Access Denied</b>\nYour request was denied by an admin.", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.debug("deny notify to user %s failed: %s", uid, e)



_COUNTRY_TO_A2: dict[str, str] = {
    "UNITED STATES": "US", "USA": "US", "US": "US",
    "UNITED KINGDOM": "GB", "UK": "GB", "GB": "GB",
    "AUSTRALIA": "AU", "AU": "AU",
    "CANADA": "CA", "CA": "CA",
    "GERMANY": "DE", "DE": "DE",
    "FRANCE": "FR", "FR": "FR",
    "JAPAN": "JP", "JP": "JP",
    "CHINA": "CN", "CN": "CN",
    "INDIA": "IN", "IN": "IN",
    "BRAZIL": "BR", "BR": "BR",
    "MEXICO": "MX", "MX": "MX",
    "RUSSIA": "RU", "RU": "RU",
    "SOUTH KOREA": "KR", "KOREA": "KR", "KR": "KR",
    "INDONESIA": "ID", "ID": "ID",
    "TURKEY": "TR", "TR": "TR",
    "SAUDI ARABIA": "SA", "SA": "SA",
    "NETHERLANDS": "NL", "NL": "NL",
    "SWITZERLAND": "CH", "CH": "CH",
    "SWEDEN": "SE", "SE": "SE",
    "NORWAY": "NO", "NO": "NO",
    "DENMARK": "DK", "DK": "DK",
    "FINLAND": "FI", "FI": "FI",
    "POLAND": "PL", "PL": "PL",
    "SPAIN": "ES", "ES": "ES",
    "ITALY": "IT", "IT": "IT",
    "PORTUGAL": "PT", "PT": "PT",
    "BELGIUM": "BE", "BE": "BE",
    "AUSTRIA": "AT", "AT": "AT",
    "SINGAPORE": "SG", "SG": "SG",
    "HONG KONG": "HK", "HK": "HK",
    "TAIWAN": "TW", "TW": "TW",
    "THAILAND": "TH", "TH": "TH",
    "MALAYSIA": "MY", "MY": "MY",
    "PHILIPPINES": "PH", "PH": "PH",
    "VIETNAM": "VN", "VN": "VN",
    "MYANMAR": "MM", "MM": "MM",
    "CAMBODIA": "KH", "KH": "KH",
    "PAKISTAN": "PK", "PK": "PK",
    "BANGLADESH": "BD", "BD": "BD",
    "SRI LANKA": "LK", "LK": "LK",
    "NEPAL": "NP", "NP": "NP",
    "NIGERIA": "NG", "NG": "NG",
    "SOUTH AFRICA": "ZA", "ZA": "ZA",
    "KENYA": "KE", "KE": "KE",
    "GHANA": "GH", "GH": "GH",
    "EGYPT": "EG", "EG": "EG",
    "ETHIOPIA": "ET", "ET": "ET",
    "ARGENTINA": "AR", "AR": "AR",
    "COLOMBIA": "CO", "CO": "CO",
    "CHILE": "CL", "CL": "CL",
    "PERU": "PE", "PE": "PE",
    "VENEZUELA": "VE", "VE": "VE",
    "UAE": "AE", "UNITED ARAB EMIRATES": "AE", "AE": "AE",
    "ISRAEL": "IL", "IL": "IL",
    "IRAN": "IR", "IR": "IR",
    "IRAQ": "IQ", "IQ": "IQ",
    "UKRAINE": "UA", "UA": "UA",
    "CZECH REPUBLIC": "CZ", "CZECHIA": "CZ", "CZ": "CZ",
    "HUNGARY": "HU", "HU": "HU",
    "ROMANIA": "RO", "RO": "RO",
    "GREECE": "GR", "GR": "GR",
    "NEW ZEALAND": "NZ", "NZ": "NZ",
    "IRELAND": "IE", "IE": "IE",
    "LUXEMBOURG": "LU", "LU": "LU",
}


def _country_flag(country_name: str) -> str:
    """Return flag emoji for a country name or alpha-2 code. Falls back to 🌐."""
    upper = country_name.strip().upper()
    # If already 2-letter alpha code
    a2 = _COUNTRY_TO_A2.get(upper)
    if not a2:
        # Try first two chars if it looks like an alpha-2 code itself
        if len(upper) == 2 and upper.isalpha():
            a2 = upper
    if not a2:
        return "🌐"
    # Convert alpha-2 to regional indicator emoji (flag)
    try:
        return chr(ord("🇦") + ord(a2[0]) - ord("A")) + chr(ord("🇦") + ord(a2[1]) - ord("A"))
    except Exception:
        return "🌐"


# ── Country normalization for "Split by Country" ─────────────────────────────
# Build a reverse map a2 -> human-readable name from _COUNTRY_TO_A2, preferring
# the longest (most descriptive) spelling seen for each code.
_A2_TO_COUNTRY: dict[str, str] = {}
for _name, _a2 in _COUNTRY_TO_A2.items():
    if _a2 not in _A2_TO_COUNTRY or len(_name) > len(_A2_TO_COUNTRY[_a2]):
        _A2_TO_COUNTRY[_a2] = _name.title()


def _canonical_country(value: str) -> tuple[str, str]:
    """Normalize any country value (alpha-2 code, "US", "United States", "usa",
    "UNKNOWN", "") into a canonical (alpha2_code, Display Name) pair.

    Returns ("UNKNOWN", "Unknown") when the value cannot be resolved, so all
    unresolvable cards share a single clearly-labelled bucket instead of
    fragmenting into name/code variants.
    """
    raw = (value or "").strip()
    if not raw:
        return "UNKNOWN", "Unknown"
    upper = raw.upper()

    if upper in ("UNKNOWN", "N/A", "NA", "—", "-", "NONE", "ZZ", "XX"):
        return "UNKNOWN", "Unknown"

    if len(upper) == 2 and upper.isalpha():
        name = _A2_TO_COUNTRY.get(upper)
        return (upper, name) if name else (upper, upper)

    a2 = _COUNTRY_TO_A2.get(upper)
    if a2:
        return a2, _A2_TO_COUNTRY.get(a2, upper.title())

    return upper, raw.title()


def _bucket_cards_by_country(
    cards: list[str], bin_info: dict[str, dict[str, str]]
) -> dict[tuple[str, str], list[str]]:
    """Group cards by canonical (a2_code, Display Name), merging every spelling
    of the same country that the different BIN sources may have produced."""
    buckets: defaultdict[tuple[str, str], list[str]] = defaultdict(list)
    for line in cards:
        b6 = _pan_from_line(line)[:6]
        meta = bin_info.get(b6, {})
        value = meta.get("country") or meta.get("country_display") or "UNKNOWN"
        key = _canonical_country(value)
        buckets[key].append(line)
    return dict(buckets)



def _luhn_complete(prefix: str) -> str:
    """Given a digit string where the LAST char is the Luhn check placeholder,
    compute and append the correct check digit.
    Any 'x' in the prefix (not the last position) must already be resolved.
    Returns a fully-numeric string of length len(prefix)+1 that passes Luhn.
    """
    # Append check digit 0-9 until Luhn passes
    for d in range(10):
        candidate = prefix + str(d)
        if _luhn_ok(candidate):
            return candidate
    return prefix + "0"   # should never happen for valid prefix



def _extrap_parse_card(text: str) -> tuple[str, str, str, str] | None:
    """Parse 'cc|mm|yy|cvv', 'cc mm yy cvv', or just 'cc'.
    Returns (cc, mm, yy, cvv) — missing fields become empty string."""
    text = text.strip()
    # Try delimiter split first
    for sep in ("|", "/", ":", " "):
        parts = [p.strip() for p in text.split(sep) if p.strip()]
        if len(parts) >= 1 and parts[0].isdigit() and len(parts[0]) >= 8:
            cc  = parts[0]
            mm  = parts[1] if len(parts) > 1 else ""
            yy  = parts[2] if len(parts) > 2 else ""
            cvv = parts[3] if len(parts) > 3 else ""
            return cc, mm, yy, cvv
    return None


def _generate_extrap_cards(cc: str, mm: str, yy: str, original_cvv: str, count: int) -> list[str]:
    """Ultimate Extrapolator: Bank-Specific Batch-Skip & Dynamic Sub-BIN Locking"""
    target_len = len(cc) if 13 <= len(cc) <= 19 else 16
    is_amex = cc.startswith("34") or cc.startswith("37")
    
    # ၁။ Dynamic Sub-BIN Locking
    # AMEX သည် 15 လုံးသာရှိပြီး Batch သတ်မှတ်ချက် ပိုရှည်သဖြင့် ၁၁ လုံး အသေဖမ်းမည်
    # Visa/Mastercard သည် ၁၀ လုံး အသေဖမ်းမည်
    fixed_len = 11 if is_amex else (10 if target_len >= 15 else 8)
    fixed_prefix = cc[:fixed_len]
    if len(fixed_prefix) < fixed_len:
        fixed_prefix = cc[:len(cc)-1]
        
    rand_len = (target_len - 1) - len(fixed_prefix)

    # ၂။ Expiry Date (မူရင်းလနှင့်နှစ်ကိုသာ အတိအကျ သုံးမည်)
    now = datetime.now(timezone.utc)
    if mm and mm.isdigit() and 1 <= int(mm) <= 12:
        out_mm = mm.zfill(2)
    else:
        out_mm = str(random.randint(now.month, 12)).zfill(2)
        
    if yy and yy.isdigit():
        y = int(yy)
        out_yy = str(y) if y >= 2000 else str(2000 + y)
    else:
        out_yy = str(now.year + random.randint(1, 3))

    results: list[str] = []
    seen: set[str] = set()
    
    # ၃။ မူရင်းအလယ်ဂဏန်းကို ဖမ်းယူခြင်း
    original_middle_str = cc[fixed_len:target_len-1]
    try:
        original_middle = int(original_middle_str)
    except ValueError:
        original_middle = random.randint(0, (10**rand_len)-1)
        
    # ၄။ 🎯 Bank-Specific Batch-Skip Logic
    offsets = []
    
    if is_amex:
        # AMEX သည် ကပ်လျက်မထုတ်ပေးသောကြောင့် Prime Gaps များကို အသုံးပြုမည်
        for step in [3, 7, 11, 15, 23, 45, 80]:
            for i in range(1, (count // 5) + 2):
                offsets.extend([step * i, -(step * i)])
    else:
        # Visa / Mastercard များအတွက် Micro & Macro Offsets
        for i in range(1, (count // 3) + 1):
            offsets.extend([i, -i])
        for step in [10, 20, 50, 100]:
            for i in range(1, (count // 4) + 2):
                offsets.extend([step * i, -(step * i)])

    # ထပ်နေသော offset များကို Order မပျက်စေဘဲ ရှင်းလင်းမည်
    offsets = list(dict.fromkeys(offsets))

    for offset in offsets:
        if len(results) >= count:
            break
            
        new_middle_int = original_middle + offset
        if new_middle_int < 0 or new_middle_int >= (10 ** rand_len):
            continue
            
        new_middle_str = str(new_middle_int).zfill(rand_len)
        prefix = fixed_prefix + new_middle_str
        num = _luhn_complete(prefix)
        
        if len(num) != target_len or num in seen:
            continue
        seen.add(num)
        
        # ၅။ CVV Correlation Logic
        cvv_len = 4 if is_amex else 3
        if original_cvv and original_cvv.isdigit() and len(original_cvv) == cvv_len and not is_amex:
            # Visa/MC အတွက်သာ CVV ကို Offset အတိုင်း အစဉ်လိုက် တွက်ချက်မည်
            new_cvv_int = (int(original_cvv) + offset) % (10 ** cvv_len)
            c_cvv = str(new_cvv_int).zfill(cvv_len)
        else:
            # AMEX ဖြစ်နေလျှင် CID ၄ လုံးကို လုံးဝ Random ယူမည်
            c_cvv = "".join(str(random.randint(0, 9)) for _ in range(cvv_len))
         
        # သက်တမ်းနှစ်ကို ၂ လုံးအဖြစ်သာ (YY) ပြသမည်
        results.append(f"{num}|{out_mm}|{out_yy[-2:]}|{c_cvv}")

    # လိုအပ်သော အရေအတွက် မပြည့်သေးပါက Random ဖြင့် ဆက်ဖြည့်မည်
    attempts = 0
    while len(results) < count and attempts < 1000:
        attempts += 1
        rand_part = "".join(str(random.randint(0, 9)) for _ in range(rand_len))
        prefix = fixed_prefix + rand_part
        num = _luhn_complete(prefix)
        
        if len(num) != target_len or num in seen:
            continue
        seen.add(num)
        
        cvv_len = 4 if is_amex else 3
        c_cvv = "".join(str(random.randint(0, 9)) for _ in range(cvv_len))
        
        results.append(f"{num}|{out_mm}|{out_yy[-2:]}|{c_cvv}")

    return results

@router.message(Command("extrap"))
async def cmd_extrap(message: Message) -> None:
    """Usage: /extrap <card> [amount]
    Example: /extrap 4111111111111111|12|2026|123 15"""
    uid = message.from_user.id
    if not can_use_bot(uid):
        return
    if not _check_rate_limit(uid):
        return await message.answer("⏳ Rate limit — ခဏစောင့်ပါ")

    args = (message.text or "").split(maxsplit=2)[1:]  # drop /extrap
    if not args:
        return await message.answer(
            "ℹ️ <b>Usage:</b> <code>/extrap 4111111111111111|12|26|123 15</code>\n\n"
            "• Card: CC|MM|YY|CVV (or) CC only\n"
            "• Amount: 1–300 (default 15)\n"
            "• If MM/YY is missing, it will be randomized",
            parse_mode=ParseMode.HTML,
        )

    # Parse amount (last token if digit)
    count = 15
    card_raw = args[0]
    if len(args) == 2 and args[1].isdigit():
        count = max(1, min(300, int(args[1])))
    elif " " in card_raw:
        # /extrap cc amount  (no pipe, space separated)
        parts = card_raw.rsplit(" ", 1)
        if parts[1].isdigit():
            card_raw = parts[0]
            count = max(1, min(300, int(parts[1])))

    parsed = _extrap_parse_card(card_raw)
    if not parsed:
        return await message.answer("⚠️ ကတ်နံပါတ် မမှန်ကန်ပါ — ဂဏန်း ≥8 လုံး ထည့်ပါ")

    cc, mm, yy, original_cvv = parsed
    if not cc.isdigit() or len(cc) < 8:
        return await message.answer("⚠️ ကတ်နံပါတ် အနည်းဆုံး ဂဏန်း ၈ လုံး ရှိရမည်")

    # Check source Luhn
    src_luhn = _luhn_ok(cc)
    luhn_icon = "✅" if src_luhn else "⚠️"

    # Mask source card for display
    masked = cc[:6] + "x" * (len(cc) - 10) + cc[-4:] if len(cc) > 10 else cc[:4] + "xxxx" + cc[-2:]

    wait = await message.answer("⏳ Extrapolating cards…")

    # BIN lookup
    sem = asyncio.Semaphore(4)
    sess = get_http_session()
    bin_info = await lookup_bin(cc[:6], sess, sem)

    brand   = bin_info.get("brand", pan_guess_brand(cc))
    bank    = bin_info.get("bank", "UNKNOWN")
    country = bin_info.get("country_display") or bin_info.get("country", "UNKNOWN")
    tier    = bin_info.get("tier", "STANDARD")
    flag    = _country_flag(bin_info.get("country", ""))

    # Generate cards
    cards = await asyncio.get_running_loop().run_in_executor(
        THREAD_POOL, _generate_extrap_cards, cc, mm, yy, original_cvv, count
    )

    header = (
        f"🎰 <b>Advanced Extrap</b> — <code>{masked}</code>\n"
        f"💳 Brand:  <b>{brand}</b>  |  💎 Tier: <b>{tier}</b>\n"
        f"🏦 Bank:   <b>{bank}</b>\n"
        f"🌍 Country: {flag} <b>{country}</b>\n"
        f"🔢 Sub-BIN: <code>{cc[:10]}</code>  |  Luhn: {luhn_icon}\n"
        f"💠 Generated: <b>{len(cards)}</b> cards\n"
        f"💥 <i>Tip: Use fresh live cards for higher hit rates!</i>"
    )

    if len(cards) > 80:
        # Send as txt file to avoid Telegram message size limit
        txt_content = "\n".join(cards)
        doc = BufferedInputFile(txt_content.encode("utf-8"), filename="extrap_cards.txt")
        await wait.delete()
        await message.answer_document(doc, caption=header, parse_mode=ParseMode.HTML)
    else:
        card_lines = "\n".join(f"<code>{c}</code>" for c in cards)
        await wait.edit_text(
            f"{header}\n\n{card_lines}",
            parse_mode=ParseMode.HTML,
        )
@router.message(Command("inter"))
async def cmd_inter(message: Message, bot: Bot) -> None:
    """Usage: /inter <card1> <card2>"""
    uid = message.from_user.id
    if not can_use_bot(uid):
        return
    if not _check_rate_limit(uid):
        return await message.answer("⏳ Rate limit — ခဏစောင့်ဆိုင်းသင့်သည်။")

    args = (message.text or "").split()[1:]
    if len(args) < 2:
        return await message.answer(
            "🎯 <b>Advanced Interpolation (Live Finder)</b>\n\n"
            "Usage: <code>/inter &lt;card1&gt; &lt;card2&gt;</code>\n\n"
            "Accurately catch hidden cards between two Live cards.\n"
            "Example: <code>/inter 411111xxxxx1234|12|26|111 411111xxxxx1238|12|26|115</code>",
            parse_mode=ParseMode.HTML,
        )

    c1 = _extrap_parse_card(args[0])
    c2 = _extrap_parse_card(args[1])

    if not c1 or not c2:
        return await message.answer("⚠️ ကတ်ပုံစံ မမှန်ကန်ပါ။ (CC|MM|YY|CVV) ပုံစံဖြင့် ထည့်သွင်းရပါမည်။")

    cc1, mm1, yy1, cvv1 = c1
    cc2, mm2, yy2, cvv2 = c2

    if len(cc1) != len(cc2):
        return await message.answer("⚠️ ကတ်နှစ်ခု၏ ဂဏန်းအရေအတွက် (Length) တူညီရပါမည်။")
    if cc1[:6] != cc2[:6]:
        return await message.answer("⚠️ BIN (ပထမ ၆ လုံး) တူညီရပါမည်။")

    # Luhn check digit ကိုဖယ်၍ အရှေ့ဂဏန်းများကိုသာ base အဖြစ်ယူမည်
    base1 = int(cc1[:-1])
    base2 = int(cc2[:-1])

    if base1 == base2:
        return await message.answer("⚠️ ကတ်နှစ်ခု တူညီနေပါသည်။ (ကြားထဲတွင် ကတ်မရှိပါ)")

    diff = abs(base1 - base2)
    if diff > 1000:
        return await message.answer(f"⚠️ ကတ်နှစ်ခုကြား အကွာအဝေး ({diff}) များလွန်းပါသည်။ (အများဆုံး ၁၀၀၀ သာ ခွင့်ပြုသည်)")

    wait = await message.answer("⏳ Interpolating hidden cards...", parse_mode=ParseMode.HTML)

    # 🎯 Directional Step: C1 မှ C2 သို့ လားရာကို သတ်မှတ်ခြင်း (ရှေ့တိုး/နောက်ဆုတ်)
    step = 1 if base1 < base2 else -1

    results: list[str] = []
    # base1 မှ base2 အထိ (step အတိုင်း) တွက်ချက်ခြင်း
    for b in range(base1 + step, base2, step):
        prefix = str(b).zfill(len(cc1) - 1)
        num = _luhn_complete(prefix)

        # Expiry Date သတ်မှတ်ခြင်း
        out_mm = mm1.zfill(2) if mm1 else str(random.randint(1, 12)).zfill(2)
        out_yy = yy1 if yy1 else str(datetime.now(timezone.utc).year + random.randint(1, 3))
        if len(out_yy) == 2:
            out_yy = "20" + out_yy

        # CVV တွက်ချက်ခြင်း (AMEX Check ပါဝင်သည်)
        is_amex = num.startswith("34") or num.startswith("37")
        cvv_len = 4 if is_amex else 3

        if cvv1 and cvv1.isdigit() and len(cvv1) == cvv_len and not is_amex:
            # C1 မှ လက်ရှိရောက်နေသော အကွာအဝေးကို တိုင်းတာခြင်း (အပေါင်း/အနှုတ် အတိအကျရမည်)
            offset = b - base1 
            new_cvv_int = (int(cvv1) + offset) % (10 ** cvv_len)
            c_cvv = str(new_cvv_int).zfill(cvv_len)
        else:
            # AMEX ဖြစ်နေလျှင် (သို့) မူရင်း CVV မပါလျှင် လုံးဝ Random ယူမည်
            c_cvv = "".join(str(random.randint(0, 9)) for _ in range(cvv_len))

        results.append(f"{num}|{out_mm}|{out_yy}|{c_cvv}")

    if not results:
        return await wait.edit_text("⚠️ ကတ်နှစ်ခုကြားတွင် ခြားနေသော ကတ်မရှိပါ။ (ကပ်လျက်ဖြစ်နေပါသည်)")

    # BIN lookup
    sem = asyncio.Semaphore(4)
    sess = get_http_session()
    bin_info = await lookup_bin(cc1[:6], sess, sem)

    brand   = _he(bin_info.get("brand", pan_guess_brand(cc1)))
    bank    = _he(bin_info.get("bank", "UNKNOWN"))
    country = _he(bin_info.get("country_display") or bin_info.get("country", "UNKNOWN"))
    tier    = _he(bin_info.get("tier", "STANDARD"))
    flag    = _country_flag(bin_info.get("country", ""))

    header = (
        f"🎯 <b>Advanced Interpolation</b>\n"
        f"💳 Brand:  <b>{brand}</b>  |  💎 Tier: <b>{tier}</b>\n"
        f"🏦 Bank:   <b>{bank}</b>\n"
        f"🌍 Country: {flag} <b>{country}</b>\n"
        f"🎯 Generated: <b>{len(results)}</b> hidden cards\n"
        f"💥 <i>Tip: Checked these cards, they are extremely high hit rate!</i>"
    )

    if len(results) > 80:
        txt_content = "\n".join(results)
        doc = BufferedInputFile(txt_content.encode("utf-8"), filename="inter_cards.txt")
        await wait.delete()
        await message.answer_document(doc, caption=header, parse_mode=ParseMode.HTML)
    else:
        card_lines = "\n".join(f"<code>{c}</code>" for c in results)
        await wait.edit_text(f"{header}\n\n{card_lines}", parse_mode=ParseMode.HTML)




@router.message(Command("bin"))
async def cmd_bin(message: Message) -> None:
    """Usage: /bin 411111  or  /bin 4111111111111111"""
    if not can_use_bot(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.answer(
            "🔍 <b>BIN Lookup</b>\n"
            "Usage: <code>/bin 411111</code> (6–8 digits)\n\n"
            "Example: <code>/bin 453201</code>",
            parse_mode=ParseMode.HTML,
        )
    raw = re.sub(r"[^\d]", "", parts[1])[:8]  # digits only, max 8
    if len(raw) < 6:
        return await message.answer("⚠️ အနည်းဆုံး BIN ၆ လုံး ထည့်ပါ", parse_mode=ParseMode.HTML)
    bin6 = raw[:6]
    wait = await message.answer(f"🔍 Looking up BIN <code>{bin6}</code>...", parse_mode=ParseMode.HTML)
    try:
        session = get_http_session()
        sem = asyncio.Semaphore(1)
        info = await lookup_bin(bin6, session, sem)
        
        # HTML Error မတက်စေရန် ရှင်းလင်းခြင်း
        brand            = _he(info.get("brand", "UNKNOWN"))
        country          = info.get("country", "UNKNOWN")
        country_display  = _he(info.get("country_display", country))
        bank             = _he(info.get("bank", "UNKNOWN"))
        tier             = _he(info.get("tier", "STANDARD"))
        guessed_brand    = pan_guess_brand(bin6)
        source           = "🌐 API" if info.get("bank") not in ("UNKNOWN", "", None) else "🧠 Local/Guess"
        flag             = _country_flag(country)
        
        await wait.edit_text(
            f"🔍 <b>BIN Lookup — <code>{bin6}</code></b>\n\n"
            f"💳 Brand:   <b>{brand}</b>  <i>(guess: {guessed_brand})</i>\n"
            f"🏦 Bank:    <b>{bank}</b>\n"
            f"🌍 Country: {flag} <b>{country_display}</b>\n"
            f"💎 Tier:    <b>{tier}</b>\n"
            f"📡 Source:  {source}",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        logger.exception("cmd_bin")
        await wait.edit_text("⚠️ BIN lookup failed — Please try again later")

# Supported formats:
#   host:port
#   host:port:user:pass
#   socks5://user:pass@host:port
_PROXY_SESSIONS: dict[int, list[str]] = {}   # uid → proxy list

# Tracks last-seen timestamp (monotonic) per user — used by cleanup loop
_USER_LAST_SEEN: dict[int, float] = {}
_INACTIVE_EVICT_SEC = 7_200.0  # 2 hours — evict user state after this idle period


def _touch_user(uid: int) -> None:
    """Call this whenever a user sends any message to update their last-seen time."""
    _USER_LAST_SEEN[uid] = time.monotonic()


async def _memory_cleanup_loop() -> None:
    """Background task: runs every hour and evicts stale in-memory state.

    Evicts:
    - BIN_CACHE entries whose TTL has expired (in-memory; disk copy managed separately)
    - RATE_LIMIT entries for users with no timestamps in the current window
    - USER_CARDS / _PROXY_SESSIONS for users inactive for >_INACTIVE_EVICT_SEC
    """
    while True:
        try:
            await asyncio.sleep(3_600)   # run every hour
            now = time.monotonic()

            # 1) BIN_CACHE — remove in-memory stale entries
            stale_bins = [k for k, v in list(BIN_CACHE.items()) if not _bin_cache_fresh(v)]
            for k in stale_bins:
                BIN_CACHE.pop(k, None)

            # 2) RATE_LIMIT — remove users with no recent timestamps
            cutoff = time.time() - RATE_LIMIT_WINDOW
            stale_rl = [uid for uid, ts_list in list(RATE_LIMIT.items())
                        if not any(t > cutoff for t in ts_list)]
            for uid in stale_rl:
                RATE_LIMIT.pop(uid, None)

            # 3) USER_CARDS / _PROXY_SESSIONS — remove inactive users
            inactive = [uid for uid, last in list(_USER_LAST_SEEN.items())
                        if (now - last) > _INACTIVE_EVICT_SEC]
            for uid in inactive:
                USER_CARDS.pop(uid, None)
                _PROXY_SESSIONS.pop(uid, None)
                _USER_LAST_SEEN.pop(uid, None)

            logger.debug(
                "memory_cleanup: evicted %d BIN entries, %d rate-limit entries, %d inactive users",
                len(stale_bins), len(stale_rl), len(inactive),
            )
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("memory_cleanup_loop error")


# ── Proxy-check tuning ────────────────────────────────────────────────────────
_PROXY_MAX_MANUAL   = 100     # max proxies accepted via an inline /proxy message
_PROXY_CONCURRENCY  = 24      # parallel checks (I/O-bound; higher than before)
_PROXY_TIMEOUT      = 8.0     # per-request timeout (seconds)
_PROXY_FILE_LIMIT   = 2_000_000  # 2 MB text cap for uploaded proxy files
# When a file has more lines than this we still check everything, but we
# stream the file in chunks so we never hold every proxy + result in RAM at
# once. Large files are effectively "unlimited" for the user.
_PROXY_STREAM_CHUNK = 2_000


def _parse_proxy_line(line: str) -> dict | None:
    """Parse any common proxy format into a dict.

    Supported formats:
      host:port
      host:port:user:pass
      user:pass@host:port          ← long tokens / special chars OK
      scheme://[user:pass@]host:port
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    def _valid_port(p) -> bool:
        return str(p).isdigit() and 1 <= int(p) <= 65535

    def _split_auth(auth: str) -> tuple[str, str]:
        # Credentials may themselves contain "@" (e.g. a token with @), so we
        # split on the FIRST ":" (user:pass) — the host is everything after the
        # LAST "@", handled by the caller's greedy match.
        return auth.split(":", 1) if ":" in auth else (auth, "")

    # ── 1. Explicit scheme URI: scheme://[user:pass@]host:port ───────────────
    # Auth may contain "@", so we match lazily and then split on the LAST "@".
    uri_m = re.match(
        r"^(socks5|socks4a?|socks4|http|https)://(.+)$",
        line, re.I,
    )
    if uri_m:
        scheme, rest = uri_m.groups()
        if "@" in rest:
            auth, hostport = rest.rsplit("@", 1)
            user, pwd = _split_auth(auth)
        else:
            user, pwd, hostport = "", "", rest
        hm = re.match(r"^([^:/]+):(\d{1,5})$", hostport)
        if hm and _valid_port(hm.group(2)):
            return {"scheme": scheme.lower(), "host": hm.group(1),
                    "port": int(hm.group(2)), "user": user, "password": pwd,
                    "raw": line}

    # ── 2. user:pass@host:port (no scheme, any chars in credentials) ─────────
    # Split on the LAST "@" so passwords/tokens containing "@" still work.
    if "@" in line and "://" not in line:
        auth, hostport = line.rsplit("@", 1)
        hm = re.match(r"^([^:/]+):(\d{1,5})$", hostport)
        if hm and _valid_port(hm.group(2)):
            user, pwd = _split_auth(auth)
            return {"scheme": "http", "host": hm.group(1), "port": int(hm.group(2)),
                    "user": user, "password": pwd, "raw": line}

    # ── 3. host:port (plain, no auth) ────────────────────────────────────────
    plain_m = re.match(r"^([^:@]+):(\d{1,5})$", line)
    if plain_m and _valid_port(plain_m.group(2)):
        host, port = plain_m.groups()
        return {"scheme": "http", "host": host, "port": int(port),
                "user": "", "password": "", "raw": line}

    # ── 4. host:port:user:pass (colon-delimited, pass may contain colons) ────
    parts = line.split(":")
    if len(parts) >= 4 and "@" not in parts[0] and parts[1].isdigit() and _valid_port(parts[1]):
        host, port = parts[0], parts[1]
        user = parts[2]
        pwd  = ":".join(parts[3:])
        return {"scheme": "http", "host": host, "port": int(port),
                "user": user, "password": pwd, "raw": line}

    return None


# Latency thresholds for speed rating
_PROXY_DEAD_MS   = 8000.0   # over this → treat as dead
_PROXY_FAST_MS   = 800.0    # under this → Fast
_PROXY_MEDIUM_MS = 2500.0   # under this → Medium, else Slow


def _proxy_speed_label(ms: float) -> str:
    if ms < _PROXY_FAST_MS:
        return "⚡ Fast"
    if ms < _PROXY_MEDIUM_MS:
        return "🟡 Medium"
    return "🔴 Slow"


def _proxy_anonymity(real_ip: str, proxy_ip: str, via_header: str, forwarded: str) -> str:
    """Detect anonymity level from ip-api extra headers (best-effort)."""
    if via_header or forwarded:
        return "🔍 Transparent"
    if real_ip and proxy_ip and real_ip != proxy_ip:
        return "🕵️ Anonymous"
    return "👻 Elite"


async def _check_one_proxy(
    session: aiohttp.ClientSession,
    proxy: dict,
    timeout: float = 10.0,
    retries: int = 1,
) -> tuple[bool, str, float, str, str, str, str, str, str, str]:
    """Returns (alive, ip, latency_ms, city, isp, timezone, country_name, country_a2, speed_label, anonymity).
    Primary: ip-api.com (full geo). Fallback: plain IP only. Retries once on failure."""
    if proxy["scheme"] in ("socks5", "socks4", "socks4a"):
        auth = ""
        if proxy["user"]:
            auth = f"{proxy['user']}:{proxy['password']}@"
        proxy_url = f"{proxy['scheme']}://{auth}{proxy['host']}:{proxy['port']}"
        proxy_auth = None
    else:
        proxy_url = f"http://{proxy['host']}:{proxy['port']}"
        proxy_auth = aiohttp.BasicAuth(proxy["user"], proxy["password"]) if proxy.get("user") and proxy["user"] else None

    DEAD_RESULT = (False, "timeout/refused", 0.0, "", "", "", "", "", "", "")

    for attempt in range(retries + 1):
        t0 = time.monotonic()
        # Primary: ip-api.com — full geo + anonymity hints
        try:
            async with session.get(
                "http://ip-api.com/json?fields=query,country,countryCode,city,isp,org,timezone",
                proxy=proxy_url,
                proxy_auth=proxy_auth,
                timeout=aiohttp.ClientTimeout(total=timeout),
                ssl=False,
            ) as resp:
                latency = (time.monotonic() - t0) * 1000
                if latency > _PROXY_DEAD_MS:
                    break  # too slow → dead
                if resp.status == 200:
                    data      = await resp.json(content_type=None)
                    ip        = str(data.get("query") or "?")
                    city      = str(data.get("city") or "")
                    isp       = str(data.get("isp") or data.get("org") or "")
                    tz        = str(data.get("timezone") or "")
                    cname     = str(data.get("country") or "")
                    a2        = str(data.get("countryCode") or "")
                    via       = resp.headers.get("Via", "")
                    fwd       = resp.headers.get("X-Forwarded-For", "")
                    anon      = _proxy_anonymity("", ip, via, fwd)
                    speed     = _proxy_speed_label(latency)
                    return True, ip, latency, city, isp, tz, cname, a2, speed, anon
        except Exception as e:
            latency = (time.monotonic() - t0) * 1000
            logger.debug("ip-api attempt %d failed for %s:%s — %s", attempt+1, proxy["host"], proxy["port"], e)

        # Fallback: plain IP only
        for test_url, ip_field in [
            ("https://api.ipify.org?format=json", "ip"),
            ("https://ifconfig.me/ip", None),
        ]:
            try:
                async with session.get(
                    test_url,
                    proxy=proxy_url,
                    proxy_auth=proxy_auth,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    ssl=False,
                ) as resp:
                    latency = (time.monotonic() - t0) * 1000
                    if resp.status == 200:
                        if ip_field is None:
                            ip = (await resp.text()).strip()[:45] or "?"
                        else:
                            data = await resp.json(content_type=None)
                            ip   = str(data.get(ip_field) or "?")
                        speed = _proxy_speed_label(latency)
                        return True, ip, latency, "", "", "", "", "", speed, "❓ Unknown"
            except Exception:
                continue

        # All failed — wait before retry
        if attempt < retries:
            await asyncio.sleep(0.5)

    return DEAD_RESULT


def _parse_proxy_text(raw_text: str) -> list[dict]:
    """Parse every line of a blob of text into proxy dicts (skips junk)."""
    out: list[dict] = []
    for ln in raw_text.splitlines():
        p = _parse_proxy_line(ln)
        if p:
            out.append(p)
    return out


async def _check_proxy_batch(
    message: Message,
    bot: Bot,
    uid: int,
    proxy_list: list[dict],
    *,
    title: str = "Proxy Check",
    source_note: str = "",
    unlimited: bool = False,
) -> None:
    """Check a batch of proxies concurrently and report the result.

    Shared by /proxy (inline), reply-based /proxy, and file uploads so the
    behaviour — progress bar, sorting, live/dead reporting and the output
    file — is identical everywhere.

    `unlimited=True` means the caller already decided not to cap the batch
    (file uploads); we only guard against an empty list here.
    """
    total = len(proxy_list)
    if total == 0:
        return await message.answer("⚠️ No valid proxies found.")

    wait = await message.answer(
        f"🌐 <b>{title}</b>\n"
        + (f"{source_note}\n" if source_note else "")
        + f"<code>{render_bar(0)}</code>\n0 / {total:,}",
        parse_mode=ParseMode.HTML,
    )
    prog = ProgressMessage(wait, title)

    live: list[tuple[float, str]] = []   # (latency_ms, output line)
    dead: list[str] = []
    lock = asyncio.Lock()
    done = 0
    sem = asyncio.Semaphore(_PROXY_CONCURRENCY)

    async def _one(sess: aiohttp.ClientSession, proxy: dict) -> None:
        nonlocal done
        async with sem:
            alive, ip, ms, city, isp, tz, cname, a2, speed, anon = await _check_one_proxy(
                sess, proxy, timeout=_PROXY_TIMEOUT
            )
            async with lock:
                done += 1
                if alive:
                    flag   = _country_flag(a2 or cname)
                    geo    = " | ".join(filter(None, [f"{flag} {cname}".strip(), city, isp]))
                    clabel = f"[{geo}]" if geo else ""
                    line = (
                        f"{proxy['raw']}  # {ip} {ms:.0f}ms {speed} {anon} {clabel}"
                    ).strip()
                    live.append((ms, line))
                else:
                    dead.append(proxy["raw"])
                idx = done

        # Progress outside the lock (network already done for this item).
        label = f"✅ {speed}" if alive else "❌"
        try:
            await prog.update(idx, total, f"{proxy['host']}:{proxy['port']} {label}")
        except Exception:
            pass

    connector = aiohttp.TCPConnector(limit=_PROXY_CONCURRENCY * 2, ttl_dns_cache=300)
    timeout_cfg = aiohttp.ClientTimeout(total=_PROXY_TIMEOUT * 2)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as sess:
        await asyncio.gather(*[_one(sess, p) for p in proxy_list])

    # Fastest live proxies first.
    live.sort(key=lambda x: x[0])
    live_lines = [line for _, line in live]

    # Persist live proxies for this user.
    _PROXY_SESSIONS[uid] = [p.split("#")[0].strip() for p in live_lines]

    summary = (
        f"✅ <b>{title} ပြီး</b>\n\n"
        f"🟢 Live: <b>{len(live_lines):,}</b>\n"
        f"🔴 Dead: <b>{len(dead):,}</b>\n"
        f"📊 Total: <b>{total:,}</b>"
    )
    await prog.finish(summary)

    if live_lines:
        live_txt = "\n".join(live_lines)
        await bot.send_document(
            message.chat.id,
            BufferedInputFile(live_txt.encode("utf-8"), filename="live_proxies.txt"),
            caption=f"🟢 {len(live_lines):,} Live Proxies — sorted by speed",
        )


@router.message(Command("proxy"))
async def cmd_proxy(message: Message, bot: Bot) -> None:
    """Proxy checker.

    Three ways to use it:
      1. Inline:  /proxy 1.2.3.4:8080   (or paste many, one per line)
      2. Reply:   reply to a .txt file  (or a text message full of proxies)
                  with the single word  /proxy
      3. Upload:  send a .txt file with a caption containing "proxy"
    """
    if not can_use_bot(message.from_user.id):
        return
    uid = message.from_user.id
    raw_text = (message.text or "").split(None, 1)
    arg = raw_text[1].strip() if len(raw_text) > 1 else ""

    # ── 1. REPLY mode: /proxy as a reply to a file or a text message ───────────
    reply = message.reply_to_message
    if not arg and reply is not None:
        # Reply to a document (.txt) → download and check it (unlimited).
        if reply.document:
            name = (reply.document.file_name or "").lower()
            if not name.endswith(".txt"):
                return await message.answer("⚠️ Reply to a .txt file to check its proxies.")
            if reply.document.file_size and reply.document.file_size > _PROXY_FILE_LIMIT:
                return await message.answer(
                    f"⚠️ File too large (limit {_PROXY_FILE_LIMIT // 1_000_000} MB)."
                )
            await message.answer("📥 Reading proxies from your file…")
            tg_file = await bot.get_file(reply.document.file_id)
            buf = io.BytesIO()
            await bot.download_file(tg_file.file_path, buf)
            text = buf.getvalue().decode("utf-8", errors="ignore")
            proxy_list = _parse_proxy_text(text)
            if not proxy_list:
                return await message.answer("⚠️ No valid proxies found in that file.")
            return await _check_proxy_batch(
                message, bot, uid, proxy_list,
                title="Proxy Check",
                source_note=f"📄 From replied file — {len(proxy_list):,} proxies",
                unlimited=True,
            )

        # Reply to a text message → parse proxies out of it (manual cap 100).
        if reply.text:
            proxy_list = _parse_proxy_text(reply.text)
            if not proxy_list:
                return await message.answer("⚠️ No valid proxies found in that message.")
            if len(proxy_list) > _PROXY_MAX_MANUAL:
                await message.answer(
                    f"ℹ️ Manual cap is {_PROXY_MAX_MANUAL} — checking the first "
                    f"{_PROXY_MAX_MANUAL} of {len(proxy_list):,}. "
                    "For unlimited, put them in a .txt file."
                )
                proxy_list = proxy_list[:_PROXY_MAX_MANUAL]
            return await _check_proxy_batch(
                message, bot, uid, proxy_list,
                title="Proxy Check",
                source_note=f"💬 From replied message — {len(proxy_list):,} proxies",
            )

        return await message.answer("⚠️ Reply to a .txt file or a text message with proxies.")

    # ── 2. No argument and no reply → show help ────────────────────────────────
    if not arg:
        return await message.answer(
            "🌐 <b>Proxy Checker</b>\n\n"
            "Paste a proxy — any format works:\n"
            "<code>/proxy 1.2.3.4:8080</code>\n"
            "<code>/proxy user:pass@host:port</code>\n"
            "<code>/proxy host:port:user:pass</code>\n"
            "<code>/proxy socks5://user:pass@host:port</code>\n\n"
            "<b>Bulk (one per line):</b>\n"
            "<code>/proxy\n1.2.3.4:3128\nuser:pass@5.6.7.8:1080</code>\n\n"
            f"📝 Manual limit: <b>{_PROXY_MAX_MANUAL}</b> proxies\n\n"
            "<b>📄 File check (unlimited):</b>\n"
            "• Reply to a .txt file with <code>/proxy</code>, or\n"
            "• Upload a .txt with caption <code>proxy</code>",
            parse_mode=ParseMode.HTML,
        )

    # ── 3. INLINE mode ─────────────────────────────────────────────────────────
    proxy_list = _parse_proxy_text(arg)
    if not proxy_list:
        return await message.answer(
            "⚠️ Failed to parse proxy.\n\n"
            "ပုံစံများ:\n"
            "<code>1.2.3.4:8080</code>\n"
            "<code>user:pass@host:port</code>\n"
            "<code>host:port:user:pass</code>\n"
            "<code>socks5://user:pass@host:port</code>",
            parse_mode=ParseMode.HTML,
        )

    if len(proxy_list) > _PROXY_MAX_MANUAL:
        await message.answer(
            f"ℹ️ Manual cap is {_PROXY_MAX_MANUAL} — checking the first "
            f"{_PROXY_MAX_MANUAL} of {len(proxy_list):,}.\n"
            "For unlimited, send them as a .txt file (caption <code>proxy</code>) "
            "or reply to a file with <code>/proxy</code>.",
            parse_mode=ParseMode.HTML,
        )
        proxy_list = proxy_list[:_PROXY_MAX_MANUAL]

    # Single proxy → quick detailed card (nicer than the batch view).
    if len(proxy_list) == 1:
        proxy = proxy_list[0]
        wait = await message.answer(
            f"🔄 Checking <code>{proxy['host']}:{proxy['port']}</code> "
            f"({proxy['scheme'].upper()})…",
            parse_mode=ParseMode.HTML,
        )
        async with aiohttp.ClientSession() as sess:
            alive, ip_info, ms, city, isp, tz, cname, a2, speed, anon = await _check_one_proxy(
                sess, proxy, timeout=_PROXY_TIMEOUT
            )
        if alive:
            _PROXY_SESSIONS.setdefault(uid, [])
            if proxy["raw"] not in _PROXY_SESSIONS[uid]:
                _PROXY_SESSIONS[uid].append(proxy["raw"])
        status_emoji = "✅ LIVE" if alive else "❌ DEAD"
        flag = _country_flag(a2 or cname) if alive else ""
        country_line = f"{flag} <b>{cname}</b>" if cname else "<i>unknown</i>"
        city_line    = f"🏙 City:      <b>{city}</b>\n" if city else ""
        isp_line     = f"📡 ISP:       <b>{isp}</b>\n"  if isp else ""
        tz_line      = f"🕐 Timezone:  <b>{tz}</b>\n"   if tz else ""
        speed_line   = f"{speed}  {anon}\n"             if alive else ""
        await wait.edit_text(
            f"{status_emoji}  <code>{proxy['host']}:{proxy['port']}</code>\n"
            f"🔌 Type:     <b>{proxy['scheme'].upper()}</b>\n"
            f"🌐 IP:       <code>{ip_info}</code>\n"
            f"🌍 Country:  {country_line}\n"
            f"{city_line}{isp_line}{tz_line}"
            f"{speed_line}"
            f"⚡ Latency:  <b>{ms:.0f} ms</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    await _check_proxy_batch(
        message, bot, uid, proxy_list,
        title="Proxy Check",
        source_note=f"📋 {len(proxy_list):,} proxies",
    )

@router.message(F.document & F.caption & F.caption.casefold().contains("proxy"))
async def proxy_file_upload(message: Message, bot: Bot) -> None:
    """Handle a .txt file uploaded with a caption containing 'proxy'.

    Unlimited: every valid proxy in the file is checked (no 50/500 cap).
    """
    if not can_use_bot(message.from_user.id):
        return
    uid = message.from_user.id
    doc = message.document
    if not doc or not (doc.file_name or "").lower().endswith(".txt"):
        return await message.answer("⚠️ Only .txt files are accepted.")
    if doc.file_size and doc.file_size > _PROXY_FILE_LIMIT:
        return await message.answer(
            f"⚠️ File too large (limit {_PROXY_FILE_LIMIT // 1_000_000} MB)."
        )

    await message.answer("📥 Reading proxies from your file…")
    tg_file = await bot.get_file(doc.file_id)
    buf = io.BytesIO()
    await bot.download_file(tg_file.file_path, buf)
    raw_text = buf.getvalue().decode("utf-8", errors="ignore")
    del buf   # free the bytes early

    proxy_list = _parse_proxy_text(raw_text)
    del raw_text
    if not proxy_list:
        return await message.answer("⚠️ No valid proxies found.")

    await _check_proxy_batch(
        message, bot, uid, proxy_list,
        title="Proxy Check",
        source_note=f"📄 From file — {len(proxy_list):,} proxies (unlimited)",
        unlimited=True,
    )


async def _register_commands(bot: Bot) -> None:
    user_cmds = [
        BotCommand(command="start",    description="Start the bot"),
        BotCommand(command="register", description="Request bot access"),
        BotCommand(command="help",     description="Show instructions"),
        BotCommand(command="myid",     description="Check your User ID"),
        BotCommand(command="cancel",   description="Cancel current action"),        
        BotCommand(command="feedback", description="Send feedback"),
        BotCommand(command="bin",      description="BIN lookup (6-8 digits)"),
        BotCommand(command="proxy",    description="Proxy checker (HTTP/SOCKS4/5)"),
        BotCommand(command="scr",      description="Scrape CC from channels"),
    ]



    admin_cmds = user_cmds + [
        BotCommand(command="export",       description="Export current cards"),
        BotCommand(command="adminpanel",   description="👑 Admin Panel (Inline UI)"),
        BotCommand(command="ban",          description="User ban"),
        BotCommand(command="unban",        description="User unban"),
        BotCommand(command="broadcast",    description="Broadcast message"),
        BotCommand(command="usagestats",   description="Usage dashboard"),
        BotCommand(command="feedbacklist", description="Feedback list"),
        BotCommand(command="addvip",       description="Add VIP"),
        BotCommand(command="delvip",       description="Remove VIP"),
        BotCommand(command="viplist",      description="VIP list"),
        BotCommand(command="access",       description="Change access mode"),
        BotCommand(command="accessinfo",   description="Access status"),
        BotCommand(command="setjoin",      description="Set force-join channel"),
        BotCommand(command="unsetjoin",    description="Disable force-join"),
        BotCommand(command="joininfo",     description="Force-join status"),
    ]

    # Set default (user) commands for all private chats
    await bot.set_my_commands(user_cmds, scope=BotCommandScopeAllPrivateChats())

    # Set admin-only commands per admin chat ID
    for admin_id in ADMIN_IDS:
        try:
            await bot.set_my_commands(
                admin_cmds,
                scope=BotCommandScopeChat(chat_id=admin_id),
            )
        except Exception as e:
            logger.debug("set admin commands for %s failed: %s", admin_id, e)

    # Also clear group/channel scope (bot ကို group မှာ သုံးရင် command မပြ)
    try:
        await bot.set_my_commands([], scope=BotCommandScopeDefault())
    except Exception as e:
        logger.debug("clear default scope failed: %s", e)


async def main() -> None:
    import re as _re
    if not BOT_TOKEN or not _re.fullmatch(r"\d{8,10}:[A-Za-z0-9_-]{35}", BOT_TOKEN):
        raise SystemExit(
            "BOT_TOKEN မရှိ/မမှန် — .env ဖိုင်တွင် BotFather token ထည့်ပါ\n"
            "ဥပမာ: BOT_TOKEN=7123456789:AAH..."
        )
    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS မသတ်မှတ်ထား — .env တွင် သင့် Telegram ID ထည့်ပါ")

    load_access()
    load_force_join()
    _load_local_bin_db()
    _load_bin_cache()
    global HTTP_SESSION
    HTTP_SESSION = aiohttp.ClientSession()
    cleanup_task = asyncio.create_task(_memory_cleanup_loop())
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.middleware(AccessMiddleware())
    dp.callback_query.middleware(AccessMiddleware())
    dp.include_router(router)

    await _register_commands(bot)
    me = await bot.get_me()
    mode = "PRIVATE" if is_restricted() else "PUBLIC"
    logger.info("Running @%s | access=%s | admins=%s", me.username, mode, ADMIN_IDS)

    try:
        while True:
            try:
                await dp.start_polling(bot, drop_pending_updates=True)
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Polling crashed; restarting in 5 seconds.")
                await asyncio.sleep(5)
    finally:
        cleanup_task.cancel()
        if HTTP_SESSION and not HTTP_SESSION.closed:
            await HTTP_SESSION.close()
        _save_bin_cache()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())