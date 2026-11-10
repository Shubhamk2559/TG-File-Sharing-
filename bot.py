from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from contextlib import asynccontextmanager
from functools import lru_cache
import asyncio, copy, html, io, json, logging, os, re, secrets, time

try:  # faster event loop (optional) - must be installed BEFORE pyrogram is imported
    import uvloop
    uvloop.install()
except Exception:
    pass

from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram import errors as tg_errors
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.types import InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup

logging.basicConfig(level=logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.WARNING)  # less log I/O under load

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
DB_CHANNEL = int(os.environ["DB_CHANNEL"])
ADMINS = {int(x) for x in os.environ.get("ADMINS", "").split() if x}
BACKUP_CHANNEL = int(os.environ.get("BACKUP_CHANNEL", "0"))
CHANNELS = [DB_CHANNEL] + ([BACKUP_CHANNEL] if BACKUP_CHANNEL else [])
WORKER_URL = os.environ["WORKER_URL"].rstrip("/")
API_SECRET = os.environ["API_SECRET"]
FORCE_SUB = os.environ.get("FORCE_SUB", "")
FORCE_SUB_LINK = os.environ.get("FORCE_SUB_LINK", "")
AUTO_DELETE = int(os.environ.get("AUTO_DELETE", "0"))
PROTECT = os.environ.get("PROTECT_CONTENT", "0") == "1"
PORT = int(os.environ.get("PORT", "8000"))
CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID", "")   # optional: for exact Cloudflare numbers in /stats
CF_API_TOKEN = os.environ.get("CF_API_TOKEN", "")     # optional: token with "Account Analytics: Read"

# ---- performance knobs (all optional) ----
WORKERS = int(os.environ.get("WORKERS", "100"))        # concurrent update handlers (cheap: coroutines)
SEND_RATE = float(os.environ.get("SEND_RATE", "25"))   # max Telegram sends/sec (limit is ~30) -> no FloodWait storms
CACHE_TTL = int(os.environ.get("CACHE_TTL", "300"))    # seconds a code stays cached (saves Cloudflare KV reads)
CACHE_MAX = int(os.environ.get("CACHE_MAX", "5000"))   # max codes kept in RAM

app = Client("filebot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, workers=WORKERS)
http: aiohttp.ClientSession = None

CODE_RE = re.compile(r"#([A-Za-z0-9_-]{1,60})")
MEDIA_ATTRS = ("document", "video", "audio", "photo", "animation", "voice", "video_note")
is_admin = filters.create(lambda _, __, m: m.from_user and m.from_user.id in ADMINS)
media = (filters.document | filters.video | filters.audio | filters.photo
         | filters.animation | filters.voice | filters.video_note)

# errors caused by the *user* (blocked bot, deleted account...) - no point trying other locations
USER_ERRORS = tuple(getattr(tg_errors, n) for n in (
    "UserIsBlocked", "InputUserDeactivated", "UserDeactivated", "UserDeactivatedBan",
    "ChatWriteForbidden") if hasattr(tg_errors, n))


# ---------- small building blocks ----------
class TTLCache:
    """Tiny LRU + TTL cache. get() returns None on miss."""

    def __init__(self, maxsize):
        self.d, self.max = OrderedDict(), maxsize

    def get(self, key):
        e = self.d.get(key)
        if e is None:
            return None
        if e[0] < time.monotonic():
            del self.d[key]
            return None
        self.d.move_to_end(key)
        return e[1]

    def set(self, key, val, ttl):
        self.d[key] = (time.monotonic() + ttl, val)
        self.d.move_to_end(key)
        while len(self.d) > self.max:
            self.d.popitem(last=False)

    def pop(self, key):
        self.d.pop(key, None)


class Limiter:
    """Spreads Telegram calls evenly (lock-free, single event loop). pause() = global FloodWait."""

    def __init__(self, rate):
        self.gap, self.next = 1.0 / max(rate, 1), 0.0

    async def wait(self):
        now = time.monotonic()
        t = max(now, self.next)
        self.next = t + self.gap
        if t > now:
            await asyncio.sleep(t - now)

    def pause(self, sec):
        self.next = max(self.next, time.monotonic() + sec)


class KeyedLock:
    """One lock per code (uploads to different codes don't block each other); cleans itself up."""

    def __init__(self):
        self.d = {}

    @asynccontextmanager
    async def __call__(self, key):
        e = self.d.setdefault(key, [asyncio.Lock(), 0])
        e[1] += 1
        try:
            async with e[0]:
                yield
        finally:
            e[1] -= 1
            if e[1] == 0:
                self.d.pop(key, None)


limiter = Limiter(SEND_RATE)
code_lock = KeyedLock()
items_cache = TTLCache(CACHE_MAX)      # code -> items (positive hits CACHE_TTL, "not found" 30s)
member_cache = TTLCache(50000)         # uid -> True (joined FORCE_SUB), 2 min
dead_locs = TTLCache(20000)            # (chat, msg_id) that failed to copy -> skipped for 5 min
inflight = {}                          # code -> running fetch (many users, one KV read)
busy = set()                           # users whose delivery is in progress
bg_tasks = set()                       # keep references to background tasks
restoring = False


# ---------- Cloudflare Worker storage ----------
async def api(method, path, **kw):
    err = None
    for attempt in (0, 1):  # one quick retry on network error / 5xx (all calls are idempotent)
        try:
            async with http.request(method, f"{WORKER_URL}/api/{path}",
                                    headers={"x-secret": API_SECRET}, **kw) as r:
                text = await r.text()
                if r.status == 200:
                    return text
                err = RuntimeError(f"worker {r.status}: {text[:100]}")
                if r.status < 500:
                    raise err
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            err = e
        if attempt == 0:
            await asyncio.sleep(0.3)
    raise err


async def _load(code):
    items = json.loads(await api("GET", "get", params={"code": code}))
    items_cache.set(code, items, CACHE_TTL if items else 30)
    return items


async def get_items(code, fresh=False):
    """Cached read. fresh=True bypasses cache (used before writing; callers must deepcopy)."""
    if fresh:
        return await _load(code)
    hit = items_cache.get(code)
    if hit is not None:
        return hit
    task = inflight.get(code)
    if task is None:
        task = inflight[code] = asyncio.ensure_future(_load(code))
        task.add_done_callback(lambda _, c=code: inflight.pop(c, None))
    return await asyncio.shield(task)


async def put_items(code, items):
    try:
        await api("POST", "put", params={"code": code}, data=json.dumps(items))
    except Exception:
        items_cache.pop(code)
        raise
    items_cache.set(code, items, CACHE_TTL)


async def del_code(code):
    await api("POST", "delete", params={"code": code})
    items_cache.pop(code)


async def list_codes():
    return [k for k in json.loads(await api("GET", "list")) if not k.startswith("__")]


# ---------- helpers ----------
stat = {"day": "", "dl": 0, "files": 0, "up": 0}


def bump(key, n=1):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if stat["day"] != today:
        stat.update(day=today, dl=0, files=0, up=0)
    stat[key] += n


async def retry(fn, *a, **k):
    """Rate-limited Telegram call; on FloodWait everybody pauses together, then retries."""
    for i in range(5):
        await limiter.wait()
        try:
            return await fn(*a, **k)
        except FloodWait as e:
            if i == 4:
                raise
            limiter.pause(e.value + 1)


def media_of(m):
    for a in MEDIA_ATTRS:
        o = getattr(m, a, None)
        if o:
            return o.file_id, o.file_unique_id


async def index_messages(msgs):
    """Save messages under their #code (one KV write per code, none if nothing changed).
    Same file (unique id) -> one item with many locations."""
    groups = {}
    for m in msgs:
        cm, mi = CODE_RE.search(m.caption or ""), media_of(m)
        if not cm or not mi:
            continue
        code = cm.group(1).lower()
        if code.startswith("__"):
            continue
        groups.setdefault(code, []).append((mi, m.caption.html, [m.chat.id, m.id]))
    result = None
    for code, entries in groups.items():
        async with code_lock(code):
            items = copy.deepcopy(await get_items(code, fresh=True))
            changed = False
            for (fid, uid), cap, loc in entries:
                for it in items:
                    if it["u"] == uid:
                        if it["f"] == fid and it["cap"] == cap and loc in it["l"]:
                            break  # already indexed exactly like this
                        it["f"], it["cap"] = fid, cap
                        it["l"] = [x for x in it["l"] if x[0] != loc[0]] + [loc]
                        changed = True
                        break
                else:
                    items.append({"u": uid, "f": fid, "cap": cap, "l": [loc]})
                    changed = True
            if changed:
                await put_items(code, items)
        result = result or (code, len(items))
    return result


@lru_cache(maxsize=4096)
def public_caption(cap, code):
    """Caption shown to users: same as channel caption, but without the #code."""
    cap = re.sub(r"\s*━+\s*🔖\s*#" + re.escape(code) + r"\b", "", cap, flags=re.I)
    cap = re.sub(r"\s*#" + re.escape(code) + r"\b", "", cap, flags=re.I)
    return cap.strip()


async def send_item(c, chat_id, it, code):
    # 1) copy from any saved channel location  2) fallback: send by file_id (same bot only)
    cap = public_caption(it["cap"], code)
    for chat, mid in it["l"]:
        if dead_locs.get((chat, mid)):
            continue
        try:
            return await retry(c.copy_message, chat_id, chat, mid, caption=cap,
                               parse_mode=ParseMode.HTML, protect_content=PROTECT)
        except USER_ERRORS:
            raise
        except Exception:
            dead_locs.set((chat, mid), True, 300)
    return await retry(c.send_cached_media, chat_id, it["f"],
                       caption=cap, parse_mode=ParseMode.HTML, protect_content=PROTECT)


async def joined(c, uid):
    if not FORCE_SUB:
        return True
    if member_cache.get(uid):
        return True
    try:
        chat = int(FORCE_SUB) if FORCE_SUB.lstrip("-").isdigit() else FORCE_SUB
        mem = await c.get_chat_member(chat, uid)
        ok = mem.status not in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT)
        if ok:
            member_cache.set(uid, True, 120)
        return ok
    except UserNotParticipant:
        return False
    except Exception:
        return True


async def delete_later(c, chat_id, ids):
    await asyncio.sleep(AUTO_DELETE)
    for i in range(0, len(ids), 100):  # 100 messages per API call instead of 1
        try:
            await retry(c.delete_messages, chat_id, ids[i:i + 100])
        except Exception:
            pass


# ---------- user side ----------
@app.on_message(filters.command("start") & filters.private)
async def start(c, m):
    if len(m.command) < 2:
        return await m.reply("👋 Send me a file link to get your files.")
    code = m.command[1].lower()
    if not re.fullmatch(r"[a-z0-9_-]{1,60}", code):
        return await m.reply("❌ Invalid link.")
    uid = m.from_user.id
    if uid in busy:  # same user spamming the link -> don't multiply the work
        return await m.reply("⏳ Files already aa rahi hain, thoda ruko.")
    busy.add(uid)
    try:
        if not await joined(c, uid):
            retry_url = f"https://t.me/{c.me.username}?start={code}"
            join = FORCE_SUB_LINK or f"https://t.me/{FORCE_SUB.lstrip('@')}"
            return await m.reply("🔒 Join the channel first, then tap Try again.",
                                 reply_markup=Markup([[Btn("Join channel", url=join)],
                                                      [Btn("Try again", url=retry_url)]]))
        try:
            items = await get_items(code)
        except Exception as e:
            logging.error("worker error: %s", e)
            return await m.reply("⚠️ Service error, try again later.")
        if not items:
            return await m.reply("❌ File not found.")
        wait = await retry(m.reply, "⏳ Sending…")
        sent = []
        for it in items:
            try:
                sent.append(await send_item(c, m.chat.id, it, code))
            except USER_ERRORS:
                break  # user blocked the bot / account gone
            except Exception as e:
                logging.warning("send failed (%s): %s", code, e)
        try:
            await retry(wait.delete)
        except Exception:
            pass
        if not sent:
            return await m.reply("❌ Files unavailable. Admin needs to run /restore.")
        bump("dl")
        bump("files", len(sent))
        if AUTO_DELETE:
            note = await retry(m.reply, f"⚠️ Files auto-delete in {AUTO_DELETE // 60 or 1} min. Save them now.")
            t = asyncio.create_task(delete_later(c, m.chat.id, [x.id for x in sent] + [note.id]))
            bg_tasks.add(t)
            t.add_done_callback(bg_tasks.discard)
    finally:
        busy.discard(uid)


# ---------- admin side ----------
EMOJI = {"video": "🎬", "photo": "🖼", "audio": "🎵", "document": "📄",
         "animation": "🎞", "voice": "🎙", "video_note": "📹"}


def human_size(n):
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024 or u == "GB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024


def make_caption(m, title_hint=None):
    """Auto code + stylish caption for a file sent without #code."""
    kind = next(a for a in MEDIA_ATTRS if getattr(m, a, None))
    obj = getattr(m, kind)
    name = getattr(obj, "file_name", None)
    title = title_hint or (re.sub(r"[._-]+", " ", os.path.splitext(name)[0]).strip().title()
                           if name else kind.replace("_", " ").title())
    title = (title or kind.title())[:100]
    code = "".join(secrets.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(8))
    info = []
    if getattr(obj, "file_size", None):
        info.append(f"📦 {human_size(obj.file_size)}")
    d = getattr(obj, "duration", None)
    if d:
        info.append("⏱ " + (f"{d // 3600}:{d % 3600 // 60:02d}:{d % 60:02d}" if d >= 3600
                            else f"{d // 60:02d}:{d % 60:02d}"))
    if kind == "video" and getattr(obj, "height", None):
        info.append(f"🎞 {obj.height}p")
    lines = [f"{EMOJI[kind]} <b>{html.escape(title)}</b>", ""]
    if info:
        lines.append("  •  ".join(info))
    lines += ["━━━━━━━━━━━━━━", f"🔖 #{code}"]
    return code, "\n".join(lines)


@app.on_message(filters.private & media & is_admin)
async def upload(c, m):
    cap = (m.caption or "").strip()
    cm = CODE_RE.search(cap)
    caption = None
    if cm and cm.group(1).startswith("__"):
        return await m.reply("❌ Ye code allowed nahi hai.", quote=True)
    if not cm:  # no #code -> bot makes code + nice caption (your text becomes the title)
        hint = cap.splitlines()[0][:100] if cap else None
        _, caption = make_caption(m, hint)
    saved = []
    for ch in CHANNELS:
        try:
            saved.append(await retry(m.copy, ch, caption=caption, parse_mode=ParseMode.HTML))
        except Exception as e:
            logging.warning("copy to %s failed: %s", ch, e)
            await m.reply(f"⚠️ Channel {ch} mein save nahi hua (bot admin hai?)", quote=True)
    res = None
    if saved:
        try:
            res = await index_messages(saved)  # ONE index write for all channels
        except Exception as e:
            logging.error("index failed: %s", e)
    if not res:
        return await m.reply("❌ Save failed.", quote=True)
    code, n = res
    bump("up")
    await m.reply(f"✅ #{code} ({n} file{'s' if n > 1 else ''})\n🔗 {WORKER_URL}/{code}",
                  quote=True, disable_web_page_preview=True)


@app.on_message(filters.chat(CHANNELS) & media)
async def channel_post(c, m):
    # files posted/forwarded straight into a DB/backup channel with #code get indexed too
    try:
        res = await index_messages([m])
    except Exception as e:
        return logging.error("channel index failed: %s", e)
    if res:
        logging.info("indexed from channel: %s", res)


@app.on_message(filters.command("delete") & filters.private & is_admin)
async def delete_cmd(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: /delete code")
    await del_code(m.command[1].lower().lstrip("#"))
    await m.reply("🗑 Deleted from index.")


@app.on_message(filters.command("export") & filters.private & is_admin)
async def export_cmd(c, m):
    sem = asyncio.Semaphore(20)

    async def one(code):
        async with sem:
            return code, await get_items(code, fresh=True)

    data = dict(await asyncio.gather(*[one(x) for x in await list_codes()]))
    f = io.BytesIO(json.dumps(data, indent=1).encode())
    f.name = "backup.json"
    await m.reply_document(f, caption=f"{len(data)} codes")


@app.on_message(filters.command("restore") & filters.private & is_admin)
async def restore_cmd(c, m):
    """After changing DB_CHANNEL / BACKUP_CHANNEL env: fill every channel with all files."""
    global restoring
    if restoring:
        return await m.reply("♻️ Restore already running.")
    restoring = True
    try:
        status = await m.reply("♻️ Restoring…")
        ok = bad = 0
        for code in await list_codes():  # per-code lock: users & uploads keep working meanwhile
            async with code_lock(code):
                items = copy.deepcopy(await get_items(code, fresh=True))
                for it in items:
                    for target in CHANNELS:
                        if any(x[0] == target for x in it["l"]):
                            continue
                        new_msg = None
                        for chat, mid in it["l"]:
                            try:
                                new_msg = await retry(c.copy_message, target, chat, mid)
                                break
                            except Exception:
                                continue
                        if not new_msg:
                            try:
                                new_msg = await retry(c.send_cached_media, target, it["f"],
                                                       caption=it["cap"], parse_mode=ParseMode.HTML)
                            except Exception as e:
                                bad += 1
                                logging.warning("restore failed %s: %s", code, e)
                                continue
                        it["l"].append([new_msg.chat.id, new_msg.id])
                        ok += 1
                        await asyncio.sleep(1)
                    it["l"] = [x for x in it["l"] if x[0] in CHANNELS] or it["l"]
                await put_items(code, items)
        await status.edit(f"✅ Restored {ok}, failed {bad}")
    finally:
        restoring = False


GQL = """query($acc:String!,$s:Time!,$e:Time!,$d:Date!){viewer{accounts(filter:{accountTag:$acc}){
 w:workersInvocationsAdaptive(limit:100,filter:{datetime_geq:$s,datetime_leq:$e}){sum{requests errors}}
 k:kvOperationsAdaptiveGroups(limit:100,filter:{date_geq:$d,date_leq:$d}){sum{requests} dimensions{actionType}}
}}}"""


def fmt_cf(acc):
    wreq = sum(x["sum"]["requests"] for x in acc.get("w", []))
    kv = {}
    for x in acc.get("k", []):
        a = x["dimensions"]["actionType"]
        kv[a] = kv.get(a, 0) + x["sum"]["requests"]
    return (f"☁️ Cloudflare (aaj, UTC)\n"
            f"🌐 Worker requests: {wreq:,} / 100,000\n"
            f"📖 KV reads: {kv.get('read', 0):,} / 100,000\n"
            f"✍️ KV writes: {kv.get('write', 0):,} / 1,000\n"
            f"🗑 KV deletes: {kv.get('delete', 0):,} / 1,000\n"
            f"📋 KV lists: {kv.get('list', 0):,} / 1,000")


@app.on_message(filters.command("stats") & filters.private & is_admin)
async def stats_cmd(c, m):
    bump("dl", 0)  # rolls the day over if needed
    text = (f"📊 Stats ({stat['day']} UTC)\n\n"
            f"🤖 Bot (restart ke baad se)\n"
            f"⬇️ Downloads: {stat['dl']} ({stat['files']} files)\n"
            f"⬆️ Uploads: {stat['up']}\n\n")
    if CF_ACCOUNT_ID and CF_API_TOKEN:
        now = datetime.now(timezone.utc)
        v = {"acc": CF_ACCOUNT_ID, "d": now.strftime("%Y-%m-%d"),
             "s": now.strftime("%Y-%m-%dT00:00:00Z"), "e": now.strftime("%Y-%m-%dT%H:%M:%SZ")}
        try:
            async with http.post("https://api.cloudflare.com/client/v4/graphql",
                                 headers={"Authorization": f"Bearer {CF_API_TOKEN}"},
                                 json={"query": GQL, "variables": v}) as r:
                data = await r.json()
            if data.get("errors"):
                text += "⚠️ Cloudflare: " + str(data["errors"][0].get("message"))[:200]
            else:
                text += fmt_cf(data["data"]["viewer"]["accounts"][0])
        except Exception as e:
            text += f"⚠️ Cloudflare stats error: {e}"
    else:
        text += "☁️ Exact Cloudflare numbers ke liye CF_ACCOUNT_ID aur CF_API_TOKEN set karo."
    await m.reply(text)


# ======================================================================
#  SCHEDULED POSTS:  SOURCE_CHANNEL (private, many videos) -> TARGET_CHANNEL
#  captioned video + caption-less videos right below it = ONE post (one link)
# ======================================================================
SOURCE_CHANNEL = int(os.environ.get("SOURCE_CHANNEL", "0"))
TARGET_CHANNEL = int(os.environ.get("TARGET_CHANNEL", "0"))
BUTTON_TEXT = os.environ.get("BUTTON_TEXT", "▶️ Watch Now")
POST_LINK_BOT = os.environ.get("POST_LINK_BOT", "0") == "1"   # 1 = t.me/bot?start=code, 0 = WORKER_URL/code
try:
    TZ = ZoneInfo(os.environ.get("TIMEZONE", "Asia/Kolkata"))
except Exception:
    logging.warning("bad TIMEZONE, using UTC")
    TZ = timezone.utc


def _nl(text):
    """'\\n' and '/n' in the env value -> real new line ('/n' inside words/URLs like t.me/news is kept)."""
    return re.sub(r"\\n|(?<![A-Za-z0-9.])/n", "\n", text)


POST_CAPTION = _nl(os.environ.get("POST_CAPTION", "🔥 New Video Uploaded!\\n\\nWatch now 👇"))

SCHED_KEY, LIB_KEY = "__sched", "__lib"   # "__" keys are hidden from /export & /restore
sched = None        # {"last": int, "posted": [first_msg_ids], "queue": [[epoch, tries], ...]}
lib = None          # [{"i": first_id, "c": caption_html, "v": [[msg_id, file_id, unique_id], ...]}]
dirty = set()       # which docs still need saving to the worker
sched_lock = asyncio.Lock()
scan_lock = asyncio.Lock()
made_codes = {}     # group id -> code already written (a retry doesn't write again)

SCHEDULE_HELP = (
    "📅 /schedule examples:\n"
    "• /schedule 10:00 14:00 18:30  → har time pe 1 post\n"
    "• /schedule 6 10:00 2h  → 10:00 se shuru, har 2 ghante me, total 6 posts\n"
    "• /schedule 2026-11-12 10:00 14:00  → us date ke liye\n"
    f"Timezone: {TZ}")


def feature_on():
    return bool(SOURCE_CHANNEL and TARGET_CHANNEL)


def fmt_ts(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%d %b %H:%M")


async def kv_get(key):
    try:
        text = await api("GET", "get", params={"code": key})
    except RuntimeError as e:
        if str(e).startswith("worker 404"):
            return None
        raise
    return json.loads(text) if text.strip() else None


async def kv_put(key, obj):
    await api("POST", "put", params={"code": key}, data=json.dumps(obj))


async def load_sched():
    """Load queue + library from the worker once. Never starts from 'empty' if the load failed."""
    global sched, lib
    if sched is not None and lib is not None:
        return True
    try:
        s, l = await kv_get(SCHED_KEY), await kv_get(LIB_KEY)
    except Exception as e:
        logging.error("could not load schedule data: %s", e)
        return False
    if sched is None:
        sched = s if isinstance(s, dict) else {}
        sched.setdefault("last", 0)
        sched.setdefault("posted", [])
        sched.setdefault("queue", [])
    if lib is None:
        lib = l if isinstance(l, list) else []
    return True


async def persist(*names):
    """Save docs to the worker; on failure they stay 'dirty' and the scheduler retries."""
    dirty.update(names)
    try:
        for name, key, obj in (("lib", LIB_KEY, lib), ("sched", SCHED_KEY, sched)):
            if name in dirty:
                await kv_put(key, obj)
                dirty.discard(name)
        return True
    except Exception as e:
        logging.error("saving schedule data failed (will retry): %s", e)
        return False


def video_of(m):
    v = getattr(m, "video", None)
    if v:
        return v
    d = getattr(m, "document", None)
    if d and (getattr(d, "mime_type", None) or "").startswith("video/"):
        return d


def group_messages(msgs, cur=None, known=frozenset()):
    """msgs in id order. Captioned video starts a post; caption-less videos below it join it."""
    groups, orphans = [], 0
    for m in msgs:
        if getattr(m, "empty", False) or m.id in known:
            continue
        v = video_of(m)
        if not v:
            continue
        entry = [m.id, v.file_id, v.file_unique_id]
        if (m.caption or "").strip():
            cur = {"i": m.id, "c": m.caption.html, "v": [entry]}
            groups.append(cur)
        elif cur is not None:
            cur["v"].append(entry)
        else:
            orphans += 1   # caption-less video with no captioned video above it
    return groups, orphans, cur


async def last_msg_id(c):
    """Newest message id of the source channel: post a dot and delete it right away."""
    t = await retry(c.send_message, SOURCE_CHANNEL, ".", disable_notification=True)
    try:
        await retry(t.delete)
    except Exception:
        pass
    return t.id - 1


def parse_schedule(args):
    """-> list of epoch seconds. Raises ValueError with a readable message."""
    now = datetime.now(TZ)
    times, day, count, interval = [], None, None, None
    for a in args:
        a = a.lower()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", a):
            try:
                day = datetime.strptime(a, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"galat date: {a}")
        elif re.fullmatch(r"\d{1,2}:\d{2}", a):
            h, mi = map(int, a.split(":"))
            if h > 23 or mi > 59:
                raise ValueError(f"galat time: {a}")
            times.append((h, mi))
        elif re.fullmatch(r"\d+[mh]", a):
            interval = int(a[:-1]) * (60 if a[-1] == "m" else 3600)
        elif a.isdigit():
            count = int(a)
        else:
            raise ValueError(f"samajh nahi aaya: {a}")
    if not times:
        raise ValueError("Time do, jaise 18:30")

    def at(h, mi):
        if day:
            dt = day.replace(hour=h, minute=mi, tzinfo=TZ)
            if dt <= now:
                raise ValueError(f"{dt:%d %b %H:%M} ka time nikal chuka hai")
            return dt
        dt = now.replace(hour=h, minute=mi, second=0, microsecond=0)
        return dt if dt > now else dt + timedelta(days=1)

    if count is not None or interval is not None:
        if len(times) != 1 or not count or not interval:
            raise ValueError("Format: /schedule 6 10:00 2h  (count, start time, gap)")
        if interval < 60 or count > 500:
            raise ValueError("Gap kam se kam 1m aur count max 500")
        start = at(*times[0])
        stamps = [start + timedelta(seconds=interval * i) for i in range(count)]
    else:
        stamps = sorted({at(h, mi) for h, mi in times})
    return [int(x.timestamp()) for x in stamps]


def new_code():
    return "".join(secrets.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(8))


async def publish_next(c):
    """Next unposted post -> share link (KV write) -> post in TARGET_CHANNEL. None if nothing left."""
    posted = set(sched["posted"])
    grp = next((g for g in lib if g["i"] not in posted), None)
    if not grp:
        return None
    code = made_codes.get(grp["i"])
    if not code:
        code, seen, items = new_code(), set(), []
        for n, (mid, fid, uid) in enumerate(grp["v"]):
            if uid in seen:
                continue
            seen.add(uid)
            items.append({"u": uid, "f": fid, "cap": grp["c"] if n == 0 else "",
                          "l": [[SOURCE_CHANNEL, mid]]})
        await put_items(code, items)
        made_codes[grp["i"]] = code
    link = (f"https://t.me/{c.me.username}?start={code}" if POST_LINK_BOT
            else f"{WORKER_URL}/{code}")
    # link lives ONLY inside the button (never in the caption text)
    await retry(c.copy_message, TARGET_CHANNEL, SOURCE_CHANNEL, grp["i"], caption=POST_CAPTION,
                parse_mode=ParseMode.HTML, reply_markup=Markup([[Btn(BUTTON_TEXT, url=link)]]))
    sched["posted"].append(grp["i"])
    made_codes.pop(grp["i"], None)
    return code, len(grp["v"]), link


async def notify_admins(c, text):
    for a in ADMINS:
        try:
            await retry(c.send_message, a, text)
        except Exception:
            pass


async def run_due(c):
    while True:
        note = None
        async with sched_lock:
            q = sched["queue"]
            if not q or q[0][0] > time.time():
                return
            ent = q[0]
            try:
                res = await publish_next(c)
            except Exception as e:
                ent[1] += 1
                logging.error("scheduled post failed: %s", e)
                if ent[1] >= 3:
                    q.pop(0)
                    note = f"❌ {fmt_ts(ent[0])} wala post 3 baar fail hua, hata diya.\n{e}\n(Bot TARGET channel me admin hai?)"
                else:
                    ent[0] = int(time.time()) + 300
                    q.sort()
                    note = f"⚠️ Scheduled post fail hua: {e}\n5 min baad dobara try karunga."
            else:
                q.pop(0)
                if res is None:
                    note = "📭 Scheduled time aaya par koi unposted post nahi bacha. /scan se naye posts lao."
            await persist("sched")
        if note:
            await notify_admins(c, note)
        await asyncio.sleep(2)  # gap between catch-up posts


async def scheduler(c):
    while True:
        try:
            if await load_sched():
                if dirty:
                    await persist()
                await run_due(c)
        except Exception as e:
            logging.error("scheduler error: %s", e)
        await asyncio.sleep(5)


async def feature_ready(m):
    if not feature_on():
        await m.reply("⚠️ SOURCE_CHANNEL aur TARGET_CHANNEL env set karo.")
        return False
    if not await load_sched():
        await m.reply("⚠️ Worker se data load nahi hua, thodi der baad try karo.")
        return False
    return True


@app.on_message(filters.command("scan") & filters.private & is_admin)
async def scan_cmd(c, m):
    """Read SOURCE_CHANNEL history by message id and build posts. /scan  |  /scan from  |  /scan from to"""
    if not await feature_ready(m):
        return
    if scan_lock.locked():
        return await m.reply("🔍 Scan already chal raha hai.")
    async with scan_lock:
        try:
            args = [int(x) for x in m.command[1:3]]
        except ValueError:
            return await m.reply("Usage: /scan   ya   /scan from_id   ya   /scan from_id to_id")
        status = await m.reply("🔍 Scanning…")
        try:
            hi = args[1] if len(args) > 1 else await last_msg_id(c)
            lo = max(args[0] if args else sched["last"] + 1, 1)
            if hi < lo:
                return await status.edit("✅ Naya kuch nahi hai (pehle se scanned).")
            if hi - lo > 200000:
                return await status.edit("❌ Range bahut badi hai (max 200000 ids).")
            known = {x[0] for g in lib for x in g["v"]}
            posted = set(sched["posted"])
            before = sum(len(g["v"]) for g in lib)
            # a post cut in half by the previous scan keeps growing (unless already posted)
            cur = lib[-1] if lib and lib[-1]["i"] not in posted and lo > lib[-1]["v"][-1][0] else None
            new, orphans = [], 0
            for s in range(lo, hi + 1, 200):
                msgs = await retry(c.get_messages, SOURCE_CHANNEL, list(range(s, min(s + 200, hi + 1))))
                g, o, cur = group_messages(msgs, cur, known)
                new += g
                orphans += o
        except Exception as e:
            logging.error("scan failed: %s", e)
            return await status.edit(f"❌ Scan fail: {e}\nBot SOURCE channel me admin hai? Manual: /scan 1 5000")
        async with sched_lock:
            lib.extend(new)
            lib.sort(key=lambda g: g["i"])
            sched["last"] = max(sched["last"], hi)
            ok = await persist("lib", "sched")
            vids = sum(len(g["v"]) for g in lib) - before
            posted = set(sched["posted"])
            left = sum(1 for g in lib if g["i"] not in posted)
        await status.edit(f"✅ Scan complete (id {lo}–{hi})\n🆕 Naye posts: {len(new)}\n🎬 Naye videos: {vids}\n"
                          f"⏭ Bina caption ke orphan videos (skip): {orphans}\n"
                          f"📦 Total posts: {len(lib)} | Unposted: {left}"
                          + ("" if ok else "\n⚠️ Worker save fail, auto-retry chalega."))


@app.on_message(filters.command("schedule") & filters.private & is_admin)
async def schedule_cmd(c, m):
    if not await feature_ready(m):
        return
    try:
        stamps = parse_schedule(m.command[1:])
    except ValueError as e:
        return await m.reply(f"❌ {e}\n\n{SCHEDULE_HELP}")
    async with sched_lock:
        sched["queue"] += [[t, 0] for t in stamps]
        sched["queue"].sort()
        ok = await persist("sched")
        posted = set(sched["posted"])
        left = sum(1 for g in lib if g["i"] not in posted)
        total = len(sched["queue"])
    lines = [f"✅ {len(stamps)} post schedule hue ({fmt_ts(stamps[0])} se {fmt_ts(stamps[-1])})"]
    lines += [f"• {fmt_ts(t)}" for t in stamps[:15]] + (["…"] if len(stamps) > 15 else [])
    if total > left:
        lines.append(f"\n⚠️ Queue me {total} hain par unposted posts sirf {left}. /scan se aur lao.")
    if not ok:
        lines.append("⚠️ Worker save fail, auto-retry chalega.")
    await m.reply("\n".join(lines))


@app.on_message(filters.command("posts") & filters.private & is_admin)
async def posts_cmd(c, m):
    if not await feature_ready(m):
        return
    posted = set(sched["posted"])
    done = sum(1 for g in lib if g["i"] in posted)
    q = sched["queue"]
    text = (f"📦 Library: {len(lib)} posts ({sum(len(g['v']) for g in lib)} videos)\n"
            f"✅ Posted: {done}\n🕓 Unposted: {len(lib) - done}\n⏰ Scheduled: {len(q)}")
    if q:
        text += "\n\n" + "\n".join(f"• {fmt_ts(t)}" for t, _ in q[:15]) + ("\n…" if len(q) > 15 else "")
    await m.reply(text)


@app.on_message(filters.command("unschedule") & filters.private & is_admin)
async def unschedule_cmd(c, m):
    if not await feature_ready(m):
        return
    async with sched_lock:
        n = len(sched["queue"])
        sched["queue"] = []
        await persist("sched")
    await m.reply(f"🗑 {n} scheduled post cancel kar diye.")


@app.on_message(filters.command("postnow") & filters.private & is_admin)
async def postnow_cmd(c, m):
    """Post the next unposted post right now (test)."""
    if not await feature_ready(m):
        return
    async with sched_lock:
        try:
            res = await publish_next(c)
        except Exception as e:
            return await m.reply(f"❌ Post fail: {e}\n(Bot TARGET channel me admin hai?)")
        if res is None:
            return await m.reply("📭 Koi unposted post nahi bacha. /scan karo.")
        await persist("sched")
    await m.reply(f"✅ Posted: #{res[0]} ({res[1]} videos)\n🔗 {res[2]}", disable_web_page_preview=True)


async def main():
    global http
    http = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300),
                                 timeout=aiohttp.ClientTimeout(total=15))
    await app.start()
    web_app = web.Application()
    web_app.add_routes([web.get("/", lambda r: web.Response(text="ok"))])
    runner = web.AppRunner(web_app, access_log=None)  # no per-request log spam from health checks
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    try:
        await api("POST", "put", params={"code": "__cfg_bot"}, data=app.me.username)
    except Exception as e:
        logging.error("could not save bot username to worker: %s", e)
    if feature_on():
        t = asyncio.create_task(scheduler(app))
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)
        logging.info("Post scheduler on: %s -> %s", SOURCE_CHANNEL, TARGET_CHANNEL)
    logging.info("Bot @%s running", app.me.username)
    await idle()
    await app.stop()
    await http.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
