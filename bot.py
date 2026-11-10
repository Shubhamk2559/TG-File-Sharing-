from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from contextlib import asynccontextmanager
from functools import lru_cache
import asyncio, copy, hashlib, hmac, html, io, json, logging, os, random, re, secrets, time

try:  # faster event loop (optional) - must be installed BEFORE pyrogram is imported
    import uvloop
    uvloop.install()
except Exception:
    pass

from urllib.parse import parse_qsl
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram import errors as tg_errors
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, UserNotParticipant
from pyrogram.types import CallbackQuery, InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup, WebAppInfo

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

# ---- daily limit + ads (Monetag Rewarded Interstitial in a Telegram Mini App) ----
DAILY_LIMIT = int(os.environ.get("DAILY_LIMIT", "20"))            # videos per user per day (0 = unlimited)
QUOTA_SAVE_EVERY = int(os.environ.get("QUOTA_SAVE_EVERY", "600"))  # seconds between KV saves of the counters
AD_ZONE = os.environ.get("AD_ZONE", "").strip()                    # Monetag zone id
WEBAPP_URL = os.environ.get("WEBAPP_URL", "").rstrip("/")          # public https URL of this bot's web server
AD_MIN_WATCH = int(os.environ.get("AD_MIN_WATCH", "3"))            # min seconds between ad start and finish
AD_TOKEN_TTL = 600                                                 # seconds a "Watch ad" button stays valid
ADMINS_SKIP_ADS = os.environ.get("ADMINS_SKIP_ADS", "0") == "1"
AD_READY = bool(re.sub(r"\D", "", AD_ZONE) and WEBAPP_URL.startswith("https://"))
ads = {"on": AD_READY and os.environ.get("AD_GATE", "1") == "1"}   # can be toggled from the admin panel

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
stat = {"day": "", "dl": 0, "files": 0, "up": 0, "ads": 0, "blocked": 0}


def bump(key, n=1):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if stat["day"] != today:
        stat.update(day=today, dl=0, files=0, up=0, ads=0, blocked=0)
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


# ---------- welcome screen (plain /start for normal users): image + channel join buttons ----------
START_IMAGE = os.environ.get("START_IMAGE", "").strip()   # https image URL or a Telegram file_id
START_TEXT = os.environ.get("START_TEXT", "👋 Welcome!\n\nJoin our channels below, then open any video link to watch.").replace("\\n", "\n")
START_CHANNELS = []   # [(button text, url)]
for _part in os.environ.get("START_CHANNELS", "").split(","):   # "Name|https://t.me/x, Name2|https://t.me/y"
    if "|" in _part:
        _n, _u = _part.split("|", 1)
        if _n.strip() and _u.strip().startswith("http"):
            START_CHANNELS.append((_n.strip(), _u.strip()))


async def send_welcome(m):
    rows = [[Btn(f"📢 {n}", url=u)] for n, u in START_CHANNELS]
    if PAY_READY:
        rows.append([Btn("💎 Increase daily limit", callback_data="p:menu")])
    mk = Markup(rows) if rows else None
    if START_IMAGE:
        try:
            return await retry(m.reply_photo, START_IMAGE, caption=START_TEXT[:1000], reply_markup=mk)
        except Exception as e:
            logging.warning("welcome image failed: %s", e)
    return await m.reply(START_TEXT, reply_markup=mk)


# ---------- user side ----------
@app.on_message(filters.command("start") & filters.private)
async def start(c, m):
    if len(m.command) < 2:
        if m.from_user.id in ADMINS:  # admins get the button panel
            pending.pop(m.from_user.id, None)
            return await m.reply(menu_text(), reply_markup=menu_markup())
        return await send_welcome(m)
    code = m.command[1].lower()
    if not re.fullmatch(r"[a-z0-9_-]{1,60}", code):
        return await m.reply("❌ Invalid link.")
    uid = m.from_user.id
    if uid in busy:  # same user spamming the link -> don't multiply the work
        return await m.reply("⏳ Your files are already being sent. Please wait.")
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
    left = quota_left(uid)
    if left <= 0:  # checked BEFORE the ad, so nobody watches an ad for nothing
        stat["blocked"] += 1
        return await m.reply(f"⛔ Daily limit reached ({limit_for(uid)} videos per day). " + reset_text()
                             + ("\n\n💎 Need more? Increase your limit instantly with UPI." if limit_markup(uid) else ""),
                             reply_markup=limit_markup(uid))
    if ads["on"] and not (ADMINS_SKIP_ADS and uid in ADMINS) and not is_premium(uid):  # paid users: no ads
        text = "🔒 Watch a short ad to unlock your files."
        if len(items) > left:
            text += f"\n\nYou have {left} video(s) left today, so you will receive the first {left}."
        url = f"{WEBAPP_URL}/ad?t={make_token(uid, code)}"
        return await m.reply(text, reply_markup=Markup([[Btn("▶️ Watch ad & unlock", web_app=WebAppInfo(url=url))]]))
    await deliver(c, uid, code, items)


# ======================================================================
#  DAILY LIMIT  +  AD GATE (Monetag Rewarded Interstitial inside a Telegram Mini App)
# ======================================================================
QUOTA_KEY, ADS_CFG_KEY = "__limits", "__cfg_ads"   # "__" keys are hidden from /export & restore
quota = {"day": "", "u": {}}   # uid -> videos received today (resets at midnight in TIMEZONE)
quota_dirty = False
ad_sessions = TTLCache(50000)  # nonce -> monotonic start time of the ad
ad_used = TTLCache(50000)      # nonce -> True (a token unlocks exactly once)
AD_KEY = hashlib.sha256(f"{API_SECRET}:{BOT_TOKEN}".encode()).digest()


def _roll():
    day = datetime.now(TZ).strftime("%Y-%m-%d")
    if quota["day"] != day:
        quota["day"], quota["u"] = day, {}


def quota_left(uid):
    if not DAILY_LIMIT or uid in ADMINS:
        return 10 ** 9
    _roll()
    return max(limit_for(uid) - quota["u"].get(uid, 0), 0)


def quota_use(uid, n):
    global quota_dirty
    if not DAILY_LIMIT or uid in ADMINS or n <= 0:
        return
    _roll()
    quota["u"][uid] = quota["u"].get(uid, 0) + n
    quota_dirty = True


def quota_doc():
    return {"day": quota["day"], "u": {str(k): v for k, v in quota["u"].items()}}


async def quota_saver():
    """One KV write every QUOTA_SAVE_EVERY seconds (only if something changed) - keeps KV writes low."""
    global quota_dirty
    while True:
        await asyncio.sleep(QUOTA_SAVE_EVERY)
        if quota_dirty:
            quota_dirty = False
            try:
                await kv_put(QUOTA_KEY, quota_doc())
            except Exception as e:
                quota_dirty = True
                logging.error("saving daily limits failed (will retry): %s", e)


async def load_quota():
    try:
        d = await kv_get(QUOTA_KEY)
        _roll()
        if isinstance(d, dict) and d.get("day") == quota["day"]:
            quota["u"] = {int(k): int(v) for k, v in d.get("u", {}).items()}
    except Exception as e:
        logging.error("could not load daily limits: %s", e)


def reset_text():
    return f"Your limit resets at midnight ({TZ})."


# ---------- signed one-time tokens (stateless: no storage needed) ----------
def make_token(uid, code):
    payload = f"{uid}.{code}.{int(time.time()) + AD_TOKEN_TTL}.{secrets.token_hex(6)}"
    return payload + "." + hmac.new(AD_KEY, payload.encode(), hashlib.sha256).hexdigest()[:24]


def read_token(tok):
    """-> (uid, code, nonce) or None"""
    try:
        payload, sig = tok.rsplit(".", 1)
        good = hmac.new(AD_KEY, payload.encode(), hashlib.sha256).hexdigest()[:24]
        if not hmac.compare_digest(sig, good):
            return None
        uid, code, exp, nonce = payload.split(".")
        if int(exp) < time.time():
            return None
        return int(uid), code, nonce
    except Exception:
        return None


def verify_init(init_data):
    """Telegram Mini App initData check -> user id, or None."""
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        got = pairs.pop("hash", "")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(hmac.new(key, check.encode(), hashlib.sha256).hexdigest(), got):
            return None
        if time.time() - int(pairs.get("auth_date", "0")) > 3600:
            return None
        return int(json.loads(pairs["user"])["id"])
    except Exception:
        return None


# ---------- delivery (used by the ad gate and by direct links) ----------
async def deliver(c, uid, code, items=None):
    if uid in busy:  # same user spamming -> don't multiply the work
        return
    busy.add(uid)
    try:
        try:
            items = items if items is not None else await get_items(code)
        except Exception as e:
            logging.error("worker error: %s", e)
            return await retry(c.send_message, uid, "⚠️ Service error, try again later.")
        if not items:
            return await retry(c.send_message, uid, "❌ File not found.")
        left = quota_left(uid)
        if left <= 0:
            stat["blocked"] += 1
            return await retry(c.send_message, uid, f"⛔ Daily limit reached ({limit_for(uid)} videos per day). "
                                                    + reset_text()
                               + ("\n\n💎 Need more? Increase your limit instantly with UPI." if limit_markup(uid) else ""),
                               reply_markup=limit_markup(uid))
        trimmed = len(items) > left
        items = items[:left]
        wait = await retry(c.send_message, uid, "⏳ Sending…")
        sent = []
        for it in items:
            try:
                sent.append(await send_item(c, uid, it, code))
            except USER_ERRORS:
                break  # user blocked the bot / account gone
            except Exception as e:
                logging.warning("send failed (%s): %s", code, e)
        try:
            await retry(wait.delete)
        except Exception:
            pass
        if not sent:
            return await retry(c.send_message, uid, "❌ Files unavailable. Please contact the admin.")
        quota_use(uid, len(sent))
        bump("dl")
        bump("files", len(sent))
        notes = []
        if trimmed:
            notes.append(f"ℹ️ Daily limit reached ({limit_for(uid)} videos per day), so only the first "
                         f"{len(sent)} file(s) were sent. " + reset_text())
        elif DAILY_LIMIT and uid not in ADMINS and quota_left(uid) <= 5:
            notes.append(f"📊 {quota_left(uid)} of {limit_for(uid)} videos left today."
                         + (" Send /premium to get more." if PAY_READY else ""))
        if AUTO_DELETE:
            notes.append(f"⚠️ Files auto-delete in {AUTO_DELETE // 60 or 1} min. Save them now.")
        ids = [x.id for x in sent]
        if notes:
            note = await retry(c.send_message, uid, "\n\n".join(notes))
            ids.append(note.id)
        if AUTO_DELETE:
            t = asyncio.create_task(delete_later(c, uid, ids))
            bg_tasks.add(t)
            t.add_done_callback(bg_tasks.discard)
    finally:
        busy.discard(uid)


# ---------- Mini App page + API (served by the bot's own web server) ----------
AD_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Unlock files</title>
<link rel="preconnect" href="https://libtl.com"><link rel="dns-prefetch" href="https://libtl.com">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="https://libtl.com/sdk.js" data-zone="__ZONE__" data-sdk="show___ZONE__"></script>
<style>
:root{--bg:#ffffff;--fg:#111111;--hint:#6b7280;--btn:#2481cc;--btnfg:#ffffff}
@media (prefers-color-scheme:dark){:root{--bg:#17212b;--fg:#f5f5f5;--hint:#9aa5b1;--btn:#2b8be0;--btnfg:#ffffff}}
html,body{height:100%;margin:0}
body{background:var(--bg);color:var(--fg);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
display:flex;align-items:center;justify-content:center;padding:24px;box-sizing:border-box;text-align:center}
.card{max-width:360px;width:100%}
#icon{font-size:56px;margin-bottom:8px}
h1{font-size:22px;margin:0 0 8px}
p{color:var(--hint);font-size:15px;line-height:1.45;margin:0 0 20px}
button{background:var(--btn);color:var(--btnfg);border:0;border-radius:12px;padding:14px 22px;font-size:16px;width:100%}
button:disabled{opacity:.6}
</style></head><body>
<div class="card"><div id="icon">🔒</div><h1 id="title">Unlock your files</h1>
<p id="msg">Watch a short ad and your files will be sent to the chat.</p>
<button id="btn">▶️ Watch ad</button></div>
<script>
const tg=window.Telegram.WebApp;tg.ready();tg.expand();
const ZONE="__ZONE__",T=new URLSearchParams(location.search).get("t")||"";
const $=id=>document.getElementById(id);
function say(i,t,m,btn){$("icon").textContent=i;$("title").textContent=t;$("msg").textContent=m;
$("btn").style.display=btn?"block":"none";if(btn)$("btn").textContent="🔄 Try again";}
async function post(path){
 try{const r=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},
 body:JSON.stringify({t:T,initData:tg.initData})});return await r.json();}catch(e){return{ok:false,error:"Network error. Please try again.",retry:true};}}
let running=false;
async function run(){
 if(running)return;running=true;$("btn").disabled=true;
 try{
  const sp=post("/api/ad/start");   /* runs in parallel: the ad opens immediately */
  let show=window["show_"+ZONE];
  for(let i=0;typeof show!=="function"&&i<40;i++){await new Promise(r=>setTimeout(r,50));show=window["show_"+ZONE];}
  if(typeof show!=="function")throw new Error("sdk");
  const nonce=T.split(".").slice(-2)[0]||"";
  await show({ymid:nonce});
  const s=await sp;
  if(!s.ok){say("⚠️","Cannot start",s.error||"Please open the file link in the bot again.",!!s.retry);return;}
  const d=await post("/api/ad/done");
  if(d.ok){say("✅","Unlocked!","Your files are being sent to the chat.",false);setTimeout(()=>tg.close(),500);}
  else{say("⚠️","Could not unlock",d.error||"Please try again.",!!d.retry);}
 }catch(e){
  say("⚠️","Ad not completed","Watch the full ad. If it does not load, turn off your ad blocker or private DNS, then tap Try again.",true);
 }finally{running=false;$("btn").disabled=false;}
}
$("btn").onclick=run;run();
</script></body></html>"""


ERR_HTML = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;display:flex;align-items:center;justify-content:center;
height:100vh;margin:0;padding:24px;box-sizing:border-box;text-align:center;background:#17212b;color:#f5f5f5}
p{color:#9aa5b1;line-height:1.45}</style></head><body><div><div style="font-size:52px">⚠️</div><h2>Cannot unlock</h2>
<p>__MSG__</p></div><script>try{Telegram.WebApp.ready();Telegram.WebApp.expand();}catch(e){}</script></body></html>"""

AD_PB_SECRET = os.environ.get("AD_POSTBACK_SECRET", "").strip()   # optional strict mode (Monetag postback)
ad_rewarded = TTLCache(50000)                                      # nonce -> True (Monetag confirmed the ad)


async def ad_postback(request):
    """Monetag server-to-server postback: .../api/ad/pb?ymid={ymid}&reward_event_type={reward_event_type}&s=SECRET"""
    q = request.query
    if not AD_PB_SECRET or not hmac.compare_digest(q.get("s", "").encode(), AD_PB_SECRET.encode()):
        return web.Response(status=403, text="forbidden")
    ymid = q.get("ymid", "")
    if ymid and q.get("reward_event_type", "valued") != "not_valued":
        ad_rewarded.set(ymid, True, AD_TOKEN_TTL + 120)
    return web.Response(text="ok")


async def ad_page(request):
    tok = read_token(request.query.get("t", ""))
    err = None
    if not tok:
        err = "This link has expired. Please open the file link in the bot again."
    elif ad_used.get(tok[2]):
        err = "This unlock was already used. Open the file link again."
    elif quota_left(tok[0]) <= 0:
        err = f"Daily limit reached ({limit_for(tok[0])} videos). " + reset_text()
    if err:   # no ad is shown for a request that can never unlock anything
        return web.Response(text=ERR_HTML.replace("__MSG__", html.escape(err)), content_type="text/html",
                            headers={"Cache-Control": "no-store"})
    html_text = AD_HTML.replace("__ZONE__", re.sub(r"\D", "", AD_ZONE))
    return web.Response(text=html_text, content_type="text/html", headers={"Cache-Control": "no-store"})


async def _ad_check(request):
    """-> (uid, code, nonce, None) or (None, None, None, error_json)"""
    try:
        d = await request.json()
    except Exception:
        d = {}
    uid = verify_init(str(d.get("initData", "")))
    tok = read_token(str(d.get("t", "")))
    if not uid or not tok or tok[0] != uid:
        return None, None, None, {"ok": False, "error": "This link has expired. Please open the file link in the bot again."}
    return uid, tok[1], tok[2], None


async def ad_start(request):
    uid, code, nonce, err = await _ad_check(request)
    if err:
        return web.json_response(err)
    if ad_used.get(nonce):
        return web.json_response({"ok": False, "error": "This unlock was already used. Open the file link again."})
    if quota_left(uid) <= 0:
        return web.json_response({"ok": False, "error": f"Daily limit reached ({limit_for(uid)} videos). " + reset_text()})
    ad_sessions.set(nonce, time.monotonic(), AD_TOKEN_TTL)
    return web.json_response({"ok": True, "ymid": nonce})


async def ad_done(request):
    uid, code, nonce, err = await _ad_check(request)
    if err:
        return web.json_response(err)
    t0 = ad_sessions.get(nonce)
    if t0 is None or ad_used.get(nonce):
        return web.json_response({"ok": False, "error": "Session expired. Please try again.", "retry": True})
    if time.monotonic() - t0 < AD_MIN_WATCH:
        return web.json_response({"ok": False, "error": "Please watch the full ad.", "retry": True})
    if AD_PB_SECRET:   # strict mode: wait (max 8s) until Monetag itself confirmed this ad
        for _ in range(16):
            if ad_rewarded.get(nonce):
                break
            await asyncio.sleep(0.5)
        else:
            return web.json_response({"ok": False, "error": "Ad not confirmed yet. Please try again.", "retry": True})
    ad_sessions.pop(nonce)
    ad_used.set(nonce, True, AD_TOKEN_TTL + 120)
    bump("ads")
    t = asyncio.create_task(deliver(app, uid, code))
    bg_tasks.add(t)
    t.add_done_callback(bg_tasks.discard)
    return web.json_response({"ok": True})


# ======================================================================
#  PAID LIMIT UPGRADE - UPI via FamGateway (https://famgateway.in), verified automatically
#  Flow: /premium -> pick plan -> QR + pay page -> payment is verified by webhook AND polling
#        (no screenshot / UTR typing) -> user's daily limit goes up for N days.
# ======================================================================
FAM_API_KEY = os.environ.get("FAMGATEWAY_API_KEY", "").strip()
FAM_BASE = os.environ.get("FAMGATEWAY_URL", "https://famgateway.in").rstrip("/")
PREMIUM_KEY = "__premium"   # "__" key: hidden from /export & restore


def _parse_plans(raw):
    """'29:50:1 99:100:7' -> [(price_inr, videos_per_day, days), ...]"""
    plans = []
    for part in raw.replace(",", " ").split():
        try:
            price, lim, days = part.split(":")
            p = (round(float(price), 2), int(lim), int(days))
            if p[0] >= 1 and p[1] > 0 and p[2] > 0:
                plans.append(p)
        except ValueError:
            logging.warning("bad PLANS entry ignored: %s", part)
    return plans


PLANS = _parse_plans(os.environ.get("PLANS", "29:50:1 99:100:7 299:200:30"))
PAY_READY = bool(FAM_API_KEY and PLANS and DAILY_LIMIT)
premium = {"u": {}, "done": []}   # u: uid -> [videos_per_day, expiry_epoch] | done: credited order ids
orders = {}                       # order_id -> {uid, idx, price, chat, mid, ts}  (pending payments)
fulfilling = set()
order_cooldown = TTLCache(50000)


def spawn(coro):
    t = asyncio.create_task(coro)
    bg_tasks.add(t)
    t.add_done_callback(bg_tasks.discard)


def limit_for(uid):
    """Videos per day for this user (premium plan if active, else DAILY_LIMIT)."""
    e = premium["u"].get(uid)
    if e and e[1] > time.time():
        return max(DAILY_LIMIT, e[0])
    return DAILY_LIMIT


def is_premium(uid):
    e = premium["u"].get(uid)
    return bool(e and e[1] > time.time())


def limit_markup(uid):
    if PAY_READY and uid not in ADMINS:
        return Markup([[Btn("💎 Increase limit", callback_data="p:menu")]])


def fmt_dt(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%d %b %Y %H:%M")


async def load_premium():
    try:
        d = await kv_get(PREMIUM_KEY)
        if isinstance(d, dict):
            now = time.time()
            premium["u"] = {int(k): [int(v[0]), int(v[1])] for k, v in d.get("u", {}).items() if v[1] > now}
            premium["done"] = [str(x) for x in d.get("done", [])][-500:]
    except Exception as e:
        logging.error("could not load premium data: %s", e)


ORDERS_KEY = "__orders"


async def save_orders():
    now = time.time()
    doc = {k: v for k, v in orders.items() if now - v["ts"] < 900 and k not in premium["done"]}
    try:
        await kv_put(ORDERS_KEY, doc)
    except Exception as e:
        logging.error("saving pending orders failed: %s", e)


async def load_orders(c):
    """After a restart: pick up payments that were still open and keep verifying them."""
    try:
        d = await kv_get(ORDERS_KEY)
    except Exception as e:
        return logging.error("could not load pending orders: %s", e)
    for oid, o in (d or {}).items():
        if time.time() - o.get("ts", 0) < 900 and oid not in premium["done"] and 0 <= o.get("idx", -1) < len(PLANS):
            orders[oid] = o
            spawn(watch_order(c, oid, o["ts"] + 330))


async def save_premium():
    now = time.time()
    premium["u"] = {k: v for k, v in premium["u"].items() if v[1] > now}
    doc = {"u": {str(k): v for k, v in premium["u"].items()}, "done": premium["done"][-500:]}
    for i in range(4):
        try:
            return await kv_put(PREMIUM_KEY, doc) or True
        except Exception as e:
            logging.error("saving premium data failed (%s): %s", i + 1, e)
            await asyncio.sleep(2 * (i + 1))
    return False


async def fam(method, path, **kw):
    async with http.request(method, f"{FAM_BASE}/{path}", headers={"X-Api-Key": FAM_API_KEY}, **kw) as r:
        try:
            data = await r.json(content_type=None)
        except Exception:
            data = {}
        return r.status, data if isinstance(data, dict) else {}


def _data(d):
    return d["data"] if isinstance(d.get("data"), dict) else d


async def fam_create(uid, price):
    body = {"amount": price, "customer_name": str(uid)}
    if WEBAPP_URL.startswith("https://"):
        body["webhook_url"] = f"{WEBAPP_URL}/api/fam/webhook"
    st, d = await fam("POST", "api/create-order", json=body)
    data = _data(d)
    if st != 200 or d.get("status") != "success" or not data.get("order_id"):
        raise RuntimeError(f"gateway {st}: {str(d)[:150]}")
    return data


async def fam_status(order_id):
    """Public polling endpoint -> 'pending' | 'success' | 'expired' | 'error'"""
    try:
        async with http.get(f"{FAM_BASE}/api/checkout-status.php", params={"order_id": order_id}) as r:
            return str((await r.json(content_type=None)).get("status", "error")).lower()
    except Exception:
        return "error"


async def fam_verify(order_id, price):
    """Server-to-server check with the API key (authoritative). -> data dict or None"""
    st, d = await fam("GET", "api/verify-order.php", params={"order_id": order_id})
    if st == 401:  # some deployments only read the key from the query string
        st, d = await fam("GET", "api/verify-order.php", params={"order_id": order_id, "api_key": FAM_API_KEY})
    data = _data(d)
    if d.get("status") != "success" or str(data.get("order_id", order_id)) != order_id:
        return None
    raw = data.get("amount", data.get("payable_amount"))
    if raw is not None:
        try:
            if abs(float(raw) - price) > 1:
                logging.warning("amount mismatch for %s: paid %s, plan %s", order_id, raw, price)
                return None
        except ValueError:
            return None
    return data


async def fulfill(c, order_id, webhook=None):
    """Credit the plan exactly once. webhook = signed webhook body (already trusted) or None (-> verify via API)."""
    o = orders.get(order_id)
    if not o or order_id in premium["done"] or order_id in fulfilling:
        return False
    fulfilling.add(order_id)
    try:
        info = None
        if webhook is not None:
            try:
                if abs(float(webhook.get("amount", 0)) - o["price"]) <= 1:
                    info = webhook
                else:
                    logging.warning("webhook amount mismatch for %s: %s", order_id, webhook.get("amount"))
            except (TypeError, ValueError):
                pass
        else:
            info = await fam_verify(order_id, o["price"])
        if not info:
            return False
        uid, (_, lim, days) = o["uid"], PLANS[o["idx"]]
        now, old = time.time(), premium["u"].get(o["uid"])
        active = old and old[1] > now
        premium["u"][uid] = [max(lim, old[0]) if active else lim, int((old[1] if active else now) + days * 86400)]
        premium["done"] = (premium["done"] + [order_id])[-500:]
        orders.pop(order_id, None)
        saved = await save_premium()
        exp = premium["u"][uid][1]
        utr = info.get("utr", "-")
        text = (f"✅ <b>Payment received!</b> (UTR {html.escape(str(utr))})\n\n"
                f"💎 Your limit: <b>{premium['u'][uid][0]} videos/day</b> until {fmt_dt(exp)}.\n"
                f"📊 Left today: {quota_left(uid)}\n\nNow open your file link again.")
        try:
            if o.get("mid"):
                await retry(c.delete_messages, o["chat"], o["mid"])
        except Exception:
            pass
        try:
            await retry(c.send_message, uid, text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logging.warning("could not notify %s: %s", uid, e)
        await notify_admins(c, f"💰 ₹{o['price']:g} received from user {uid} ({order_id}, UTR {utr}) -> "
                               f"{premium['u'][uid][0]}/day till {fmt_dt(exp)}"
                            + ("" if saved else "\n⚠️ Could not save to the worker! It is active in RAM only."))
        return True
    finally:
        fulfilling.discard(order_id)


async def watch_order(c, order_id, deadline):
    """Polls the gateway until the order is paid or expired -> automatic verification."""
    while time.time() < deadline and order_id in orders:
        await asyncio.sleep(4)
        st = await fam_status(order_id)
        if st == "success":
            if await fulfill(c, order_id) or order_id in premium["done"]:
                return
        elif st == "expired":
            break
    if order_id in orders and order_id not in premium["done"]:   # last chance: paid right at the end?
        if await fam_status(order_id) == "success" and await fulfill(c, order_id):
            return
    o = orders.pop(order_id, None) if order_id not in premium["done"] else None
    if o:
        try:
            if o.get("mid"):
                await retry(c.delete_messages, o["chat"], o["mid"])
            await retry(c.send_message, o["chat"], "⌛ Payment window expired. If you paid, it will still be "
                        "credited automatically; otherwise tap /premium to try again.")
        except Exception:
            pass


def premium_text(uid):
    lines = ["💎 <b>Increase your daily limit</b>", "",
             f"Free limit: {DAILY_LIMIT} videos/day"]
    e = premium["u"].get(uid)
    if e and e[1] > time.time():
        lines.append(f"✅ Your plan: {e[0]} videos/day until {fmt_dt(e[1])}")
    lines += ["", "Pay with any UPI app (GPay, PhonePe, Paytm, BHIM...). Payment is verified "
              "<b>automatically</b> in a few seconds.", "", "Choose a plan:"]
    return "\n".join(lines)


def plans_markup():
    return Markup([[Btn(f"₹{p:g} • {lim} videos/day • {days} day{'s' if days > 1 else ''}",
                        callback_data=f"p:buy:{i}")] for i, (p, lim, days) in enumerate(PLANS)])


@app.on_message(filters.command(["premium", "buy", "plans"]) & filters.private)
async def premium_cmd(c, m):
    if not PAY_READY:
        return await m.reply("⚠️ Payments are not available right now.")
    await m.reply(premium_text(m.from_user.id), reply_markup=plans_markup(), parse_mode=ParseMode.HTML)


@app.on_callback_query(filters.regex(r"^p:"))
async def pay_cb(c, q):
    uid, parts = q.from_user.id, q.data.split(":")
    act = parts[1] if len(parts) > 1 else ""

    async def say(text, alert=False):
        try:
            await q.answer(text, show_alert=alert)
        except Exception:
            pass

    if not PAY_READY:
        return await say("Payments are not available right now.", True)

    if act == "menu":
        await say("")
        return await retry(q.message.reply, premium_text(uid), reply_markup=plans_markup(),
                           parse_mode=ParseMode.HTML)

    if act == "buy" and len(parts) == 3 and parts[2].isdigit() and int(parts[2]) < len(PLANS):
        idx = int(parts[2])
        if order_cooldown.get(uid):
            return await say("⏳ Please wait a few seconds.")
        order_cooldown.set(uid, True, 8)
        if any(o["uid"] == uid and time.time() - o["ts"] < 300 for o in orders.values()):
            return await say("⏳ You already have a pending payment. Pay it, or wait 5 minutes for it to expire.", True)
        price, lim, days = PLANS[idx]
        await say("Creating payment…")
        try:
            d = await fam_create(uid, price)
        except Exception as e:
            logging.error("create order failed: %s", e)
            return await retry(q.message.reply, "⚠️ Could not create the payment right now. Please try again in a minute.")
        oid = d["order_id"]
        pay = d.get("payable_amount") or d.get("amount") or f"{price:.2f}"
        caption = (f"💎 <b>{lim} videos/day for {days} day{'s' if days > 1 else ''}</b>\n\n"
                   f"💰 Pay exactly: <b>₹{html.escape(str(pay))}</b>\n"
                   f"🏦 UPI ID: <code>{html.escape(str(d.get('upi_id', '')))}</code>\n\n"
                   "1️⃣ Scan this QR with any UPI app (or tap “Open payment page”)\n"
                   "2️⃣ Pay the <b>exact amount</b> shown\n"
                   "3️⃣ Your limit is increased automatically ✅\n\n"
                   f"⏱ Valid for 5 minutes" + (f" (till {html.escape(str(d['expires_at_ist']))} IST)" if d.get("expires_at_ist") else ""))
        rows = [[Btn("🔄 I have paid - check", callback_data=f"p:chk:{oid}")]]
        if d.get("checkout_url"):
            rows.insert(0, [Btn("💳 Open payment page", url=d["checkout_url"])])
        mk = Markup(rows)
        orders[oid] = {"uid": uid, "idx": idx, "price": price, "chat": uid, "mid": None, "ts": time.time()}
        sent = None
        try:
            if d.get("qr_url"):
                sent = await retry(q.message.reply_photo, d["qr_url"], caption=caption,
                                   parse_mode=ParseMode.HTML, reply_markup=mk)
        except Exception as e:
            logging.warning("QR photo failed: %s", e)
        if sent is None:
            sent = await retry(q.message.reply, caption, parse_mode=ParseMode.HTML, reply_markup=mk)
        orders[oid]["mid"] = sent.id
        await save_orders()
        spawn(watch_order(c, oid, time.time() + 330))
        return

    if act == "chk" and len(parts) == 3:
        oid = parts[2]
        o = orders.get(oid)
        if oid in premium["done"]:
            return await say("✅ Already credited. Your limit is increased.", True)
        if not o or o["uid"] != uid:
            return await say("This payment has expired. Tap /premium to start again.", True)
        if await fam_status(oid) == "success" and await fulfill(c, oid):
            return await say("✅ Payment received!")
        return await say("Payment not received yet. If you just paid, wait 10-20 seconds; it is credited automatically.", True)

    await say("")


async def fam_webhook(request):
    raw = await request.read()
    sig = request.headers.get("X-FamGateway-Signature", "").strip().lower().encode()
    good = hmac.new(FAM_API_KEY.encode(), raw, hashlib.sha256).hexdigest().encode()
    if not FAM_API_KEY or not hmac.compare_digest(sig, good):
        return web.Response(status=401, text="invalid signature")
    try:
        ev = json.loads(raw)
    except Exception:
        return web.Response(status=400, text="bad json")
    oid = str(ev.get("order_id", ""))
    if ev.get("event", "payment.success") == "payment.success" and str(ev.get("status", "success")) == "success" and oid:
        if oid in orders:
            spawn(fulfill(app, oid, webhook=ev))
        elif oid not in premium["done"]:
            logging.warning("webhook for unknown order %s (bot restarted mid-payment?)", oid)
            spawn(notify_admins(app, f"⚠️ Payment webhook for unknown order {oid} (₹{ev.get('amount')}, "
                                     f"UTR {ev.get('utr')}). Credit it manually if the user complains."))
    return web.json_response({"status": "ok"})


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
        return await m.reply("❌ This code is not allowed.", quote=True)
    if not cm:  # no #code -> bot makes code + nice caption (your text becomes the title)
        hint = cap.splitlines()[0][:100] if cap else None
        _, caption = make_caption(m, hint)
    saved = []
    for ch in CHANNELS:
        try:
            saved.append(await retry(m.copy, ch, caption=caption, parse_mode=ParseMode.HTML))
        except Exception as e:
            logging.warning("copy to %s failed: %s", ch, e)
            await m.reply(f"⚠️ Could not save to channel {ch}. Is the bot an admin there?", quote=True)
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
                  quote=True, disable_web_page_preview=True,
                  reply_markup=Markup([[Btn("🛠 Menu", callback_data="a:menunew")]]))


@app.on_message(filters.chat(CHANNELS) & media)
async def channel_post(c, m):
    # files posted/forwarded straight into a DB/backup channel with #code get indexed too
    try:
        res = await index_messages([m])
    except Exception as e:
        return logging.error("channel index failed: %s", e)
    if res:
        logging.info("indexed from channel: %s", res)


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
    return (f"☁️ Cloudflare (today, UTC)\n"
            f"🌐 Worker requests: {wreq:,} / 100,000\n"
            f"📖 KV reads: {kv.get('read', 0):,} / 100,000\n"
            f"✍️ KV writes: {kv.get('write', 0):,} / 1,000\n"
            f"🗑 KV deletes: {kv.get('delete', 0):,} / 1,000\n"
            f"📋 KV lists: {kv.get('list', 0):,} / 1,000")



# ======================================================================
#  SCHEDULED POSTS (multi-channel)
#  Every TARGET channel has its OWN memory (library + queue + daily plan) and its own
#  SOURCE channels. Sources/targets are added & removed from the bot panel (no env, no redeploy);
#  everything is saved in the Cloudflare worker, so it survives restarts.
#  captioned video + caption-less videos right below it = ONE post (one link)
# ======================================================================
# optional, ONLY used once to import an older single-channel setup:
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
    """'\\n' (and '/n' not followed by a letter) in the env value -> real new line."""
    return re.sub(r"\\n|/n(?![A-Za-z])", "\n", text)


POST_CAPTION = _nl(os.environ.get("POST_CAPTION", "🔥 New Video Uploaded!\\n\\nWatch now 👇"))

CFG_KEY, LIB_KEY, SCHED_KEY = "__chans", "__lib", "__sched"   # "__" keys are hidden from /export & /restore
cfg = None          # {"ch": {tid: {"t": title, "src": {sid: title}}}}   (tid/sid = channel id without the "-")
data = {}           # tid -> {"lib": [...], "sched": {...}}
#   lib   = [{"s": sid, "i": first_msg_id, "c": caption_html, "v": [[msg_id, file_id, unique_id], ...], "k": code?}]
#   sched = {"last": {sid: last_scanned_id}, "posted": ["sid:id", ...], "queue": [[epoch, tries, "r"?], ...],
#            "cycle": int, "rules": [{"n","start","gap"}, ...] (daily batches), "rday": "YYYY-MM-DD"|None, "lastk": "sid:id"}
dirty = set()       # "cfg" | "lib:<tid>" | "sched:<tid>" docs that still need saving to the worker
sched_lock = asyncio.Lock()
scan_lock = asyncio.Lock()
made_codes = {}     # (tid, group key) -> code progress (a retry doesn't write again)

SCHEDULE_HELP = (
    "📅 Send your times, for example:\n"
    "• 10:00 14:00 18:30  → one post at each time\n"
    "• 6 10:00 2h  → 6 posts, starting at 10:00, every 2 hours\n"
    "• 2026-11-12 10:00 14:00  → same, but on that date\n"
    f"Timezone: {TZ}")
PLAN_HELP = ("⌨️ Send one batch: count, start time, gap (in one line)\n"
             "Example: 20 09:00 10m  → every day 20 posts, first at 09:00, then every 10 minutes\n"
             "Send it again for a second batch (it is added, not replaced).\n"
             f"Timezone: {TZ}")


def fmt_ts(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%d %b %H:%M")


def fmt_gap(s):
    return f"{s // 3600}h" if s % 3600 == 0 else (f"{s // 60}m" if s % 60 == 0 else f"{s}s")


def rule_text(rule):
    return f"{rule['n']} posts · first at {rule['start']} · every {fmt_gap(rule['gap'])}"


def rules_text(rules):
    if not rules:
        return "off"
    return " | ".join(rule_text(r) for r in rules) + f"  (= {sum(r['n'] for r in rules)} posts/day)"


def gk(g):
    return f"{g['s']}:{g['i']}"


def cid(x):
    """tid/sid string -> real chat id"""
    return int("-" + x)


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


def lib_key(tid):
    return f"__lib_{tid}"


def sched_key(tid):
    return f"__sched_{tid}"


def _fresh_sched():
    return {"last": {}, "posted": [], "queue": [], "cycle": 0, "rules": [], "rday": None}


async def persist(*names):
    """Save docs to the worker; on failure they stay 'dirty' and the scheduler retries."""
    dirty.update(names)
    try:
        for n in list(dirty):
            kind, _, tid = n.partition(":")
            if kind == "cfg":
                await kv_put(CFG_KEY, cfg)
            elif tid in data:
                await kv_put(lib_key(tid) if kind == "lib" else sched_key(tid), data[tid][kind])
            dirty.discard(n)   # (a channel that was removed meanwhile is simply dropped)
        return True
    except Exception as e:
        logging.error("saving schedule data failed (will retry): %s", e)
        return False


async def load_all():
    """Load the channel list once. Never starts from 'empty' if the load failed."""
    global cfg
    if cfg is not None:
        return True
    legacy = None
    try:
        c0 = await kv_get(CFG_KEY)
        if not isinstance(c0, dict) and SOURCE_CHANNEL and TARGET_CHANNEL:
            legacy = (await kv_get(LIB_KEY), await kv_get(SCHED_KEY))
    except Exception as e:
        logging.error("could not load channel data: %s", e)
        return False
    if isinstance(c0, dict):
        c0.setdefault("ch", {})
        cfg = c0
        return True
    cfg = {"ch": {}}
    if legacy:   # one-time import of the old single SOURCE/TARGET setup (its posts, queue and history stay)
        tid, sid = str(abs(TARGET_CHANNEL)), str(abs(SOURCE_CHANNEL))
        l, s = legacy
        l = l if isinstance(l, list) else []
        s = s if isinstance(s, dict) else {}
        for g in l:
            g["s"] = sid
        sc = _fresh_sched()
        sc["last"] = {sid: s.get("last", 0) if isinstance(s.get("last"), int) else 0}
        sc["posted"] = [f"{sid}:{i}" for i in s.get("posted", [])]
        sc["queue"] = s.get("queue", [])
        data[tid] = {"lib": l, "sched": sc}
        cfg["ch"][tid] = {"t": f"Channel {tid}", "src": {sid: f"Source {sid}"}}
        await persist("cfg", f"lib:{tid}", f"sched:{tid}")
        logging.info("imported old SOURCE/TARGET setup into the channel list")
    return True


async def ch_data(tid):
    """Library + schedule of one target channel (loaded on first use)."""
    if tid in data:
        return data[tid]
    try:
        l, s = await kv_get(lib_key(tid)), await kv_get(sched_key(tid))
    except Exception as e:
        logging.error("could not load data of %s: %s", tid, e)
        return None
    if tid in data:
        return data[tid]
    sc = _fresh_sched()
    if isinstance(s, dict):
        sc.update(s)
    if sc.get("rule"):   # older single plan -> list of batches
        sc["rules"] = list(sc.get("rules") or []) + [sc["rule"]]
    sc.pop("rule", None)
    if not isinstance(sc.get("rules"), list):
        sc["rules"] = []
    data[tid] = {"lib": l if isinstance(l, list) else [], "sched": sc}
    return data[tid]


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


async def last_msg_id(c, source):
    """Newest message id of a source channel: post a dot and delete it right away."""
    t = await retry(c.send_message, source, ".", disable_notification=True)
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
                raise ValueError(f"Invalid date: {a}")
        elif re.fullmatch(r"\d{1,2}:\d{2}", a):
            h, mi = map(int, a.split(":"))
            if h > 23 or mi > 59:
                raise ValueError(f"Invalid time: {a}")
            times.append((h, mi))
        elif re.fullmatch(r"\d+[mh]", a):
            interval = int(a[:-1]) * (60 if a[-1] == "m" else 3600)
        elif a.isdigit():
            count = int(a)
        else:
            raise ValueError(f"Could not understand: {a}")
    if not times:
        raise ValueError("Please give a time, for example 18:30")

    def at(h, mi):
        if day:
            dt = day.replace(hour=h, minute=mi, tzinfo=TZ)
            if dt <= now:
                raise ValueError(f"{dt:%d %b %H:%M} has already passed")
            return dt
        dt = now.replace(hour=h, minute=mi, second=0, microsecond=0)
        return dt if dt > now else dt + timedelta(days=1)

    if count is not None or interval is not None:
        if len(times) != 1 or not count or not interval:
            raise ValueError("Format: 6 10:00 2h  (count, start time, gap)")
        if interval < 60 or count > 500:
            raise ValueError("Gap must be at least 1m and count at most 500")
        start = at(*times[0])
        stamps = [start + timedelta(seconds=interval * i) for i in range(count)]
    else:
        stamps = sorted({at(h, mi) for h, mi in times})
    return [int(x.timestamp()) for x in stamps]


def parse_plan(args):
    """['5', '10:00', '2h'] -> {"n": 5, "start": "10:00", "gap": 7200}. Raises ValueError."""
    n = start = gap = None
    for a in args:
        a = a.lower()
        if re.fullmatch(r"\d{1,2}:\d{2}", a):
            h, mi = map(int, a.split(":"))
            if h > 23 or mi > 59:
                raise ValueError(f"Invalid time: {a}")
            start = f"{h:02d}:{mi:02d}"
        elif re.fullmatch(r"\d+[mh]", a):
            gap = int(a[:-1]) * (60 if a[-1] == "m" else 3600)
        elif a.isdigit():
            n = int(a)
        else:
            raise ValueError(f"Could not understand: {a}")
    if not (n and start and gap):
        raise ValueError("I need all three: count, start time and gap")
    if not 1 <= n <= 100:
        raise ValueError("Count must be between 1 and 100")
    if gap < 60 or (n - 1) * gap >= 86400:
        raise ValueError("Gap must be at least 1m, and all posts must fit inside 24 hours")
    return {"n": n, "start": start, "gap": gap}


def rule_stamps(rule, day):
    h, mi = map(int, rule["start"].split(":"))
    base = day.replace(hour=h, minute=mi, second=0, microsecond=0)
    return [int((base + timedelta(seconds=rule["gap"] * i)).timestamp()) for i in range(rule["n"])]


def ensure_daily(tid):
    """Once per day: put today's slots of the daily plan into the queue. Call with sched_lock held.
    Slots that are already more than 30 min late are skipped (e.g. plan set in the afternoon)."""
    sc = data[tid]["sched"]
    rules = sc.get("rules")
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    if not rules or sc.get("rday") == today:
        return False
    limit = time.time() - 1800
    stamps = sorted({t for r in rules for t in rule_stamps(r, datetime.now(TZ))})
    sc["queue"] += [[t, 0, "r"] for t in stamps if t > limit]
    sc["queue"].sort()
    sc["rday"] = today
    return True


def new_code():
    return "".join(secrets.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(8))


async def publish_next(c, tid):
    """Pick the next post of this channel's memory, post it. Returns (code, n_videos, link) or None if the
    memory is empty. First round goes in order; once every post has been used the round restarts SILENTLY
    in random order (no notice anywhere, same link reused). Safe to retry after a failure."""
    d = data[tid]
    lib, sc = d["lib"], d["sched"]
    if not lib:
        return None
    posted = set(sc["posted"])
    pool = [g for g in lib if gk(g) not in posted]
    if not pool:   # everything has been posted -> start a new shuffled round
        sc["posted"], sc["cycle"] = [], sc.get("cycle", 0) + 1
        posted, pool = set(), list(lib)
        d.pop("pick", None)
    grp = next((g for g in pool if gk(g) == d.get("pick")), None)
    if grp is None:
        if sc.get("cycle", 0) == 0:
            grp = pool[0]
        else:   # random, but never the same post twice in a row
            grp = random.choice([g for g in pool if gk(g) != sc.get("lastk")] or pool)
        d["pick"] = gk(grp)
    source, target = cid(grp["s"]), cid(tid)
    code = grp.get("k")
    if code:   # already has a link (repost): reuse it unless it was deleted from the index
        if not await get_items(code, fresh=True):
            grp.pop("k", None)
            code = None
    if code:
        n = len(grp["v"][1:] or grp["v"])
    else:
        mk = (tid, gk(grp))
        st = made_codes.get(mk)
        if st is None:
            vids = grp["v"][1:] or grp["v"]  # link = caption-less videos only (lone captioned video: fallback)
            seen, uniq = set(), []
            for v in vids:
                if v[2] not in seen:
                    seen.add(v[2])
                    uniq.append(v)
            st = made_codes[mk] = {"code": new_code(), "n": len(uniq), "saved": [], "indexed": False,
                                   "todo": [(v[0], ch) for v in uniq for ch in CHANNELS]}
        code, n = st["code"], st["n"]
        while st["todo"]:  # copy into the database channel(s); progress is kept if something fails midway
            mid, ch = st["todo"][0]
            st["saved"].append(await retry(c.copy_message, ch, source, mid, caption=f"🔖 #{code}"))
            st["todo"].pop(0)
        if not st["indexed"]:
            if not await index_messages(st["saved"]):  # ONE KV write for the whole post
                raise RuntimeError("could not save the code index")
            st["indexed"], st["saved"] = True, []
    link = (f"https://t.me/{c.me.username}?start={code}" if POST_LINK_BOT
            else f"{WORKER_URL}/{code}")
    # link lives ONLY inside the button (never in the caption text)
    await retry(c.copy_message, target, source, grp["i"], caption=POST_CAPTION,
                parse_mode=ParseMode.HTML, reply_markup=Markup([[Btn(BUTTON_TEXT, url=link)]]))
    if grp.get("k") != code:
        grp["k"] = code
        dirty.add(f"lib:{tid}")
    sc["posted"].append(gk(grp))
    sc["lastk"] = gk(grp)
    made_codes.pop((tid, gk(grp)), None)
    d.pop("pick", None)
    return code, n, link


async def notify_admins(c, text):
    for a in ADMINS:
        try:
            await retry(c.send_message, a, text)
        except Exception:
            pass


async def run_due(c, tid):
    title = cfg["ch"][tid]["t"]
    while True:
        note = None
        async with sched_lock:
            if tid not in cfg["ch"] or tid not in data:
                return
            q = data[tid]["sched"]["queue"]
            if not q or q[0][0] > time.time():
                return
            ent = q[0]
            try:
                res = await publish_next(c, tid)
            except Exception as e:
                ent[1] += 1
                logging.error("scheduled post failed (%s): %s", title, e)
                if ent[1] >= 3:
                    q.pop(0)
                    note = f"❌ [{title}] The post scheduled for {fmt_ts(ent[0])} failed 3 times and was removed.\n{e}\n(Is the bot an admin in the TARGET and SOURCE channel?)"
                else:
                    ent[0] = int(time.time()) + 300
                    q.sort()
                    note = f"⚠️ [{title}] Scheduled post failed: {e}\nRetrying in 5 minutes."
            else:
                q.pop(0)
                if res is None:
                    note = f"📭 [{title}] A scheduled time arrived but its memory is empty. Add a source and scan it."
            await persist(f"sched:{tid}")
        if note:
            await notify_admins(c, note)
        await asyncio.sleep(2)  # gap between catch-up posts


async def scheduler(c):
    while True:
        try:
            if await load_all():
                if dirty:
                    await persist()
                for tid in list(cfg["ch"]):
                    if not await ch_data(tid):
                        continue
                    async with sched_lock:
                        if ensure_daily(tid):
                            await persist(f"sched:{tid}")
                    await run_due(c, tid)
        except Exception as e:
            logging.error("scheduler error: %s", e)
        await asyncio.sleep(5)


# ======================================================================
#  ADMIN PANEL - everything is done with buttons (no slash commands needed)
# ======================================================================
pending = {}   # admin id -> {"a": action, "text": waiting for typed input?, "ts": time, ...}
PENDING_TTL = 600


def _awaiting(_, __, m):
    st = pending.get(m.from_user.id) if m.from_user else None
    if not st:
        return False
    if time.time() - st["ts"] > PENDING_TTL:
        pending.pop(m.from_user.id, None)
        return False
    return bool(st["text"])


awaiting_input = filters.create(_awaiting)


def _adding(_, __, m):
    """admin is adding a channel: accepts a forwarded post OR typed text"""
    st = pending.get(m.from_user.id) if m.from_user else None
    return bool(st and st["a"] in ("addsrc", "addtgt") and time.time() - st["ts"] <= PENDING_TTL)


adding_input = filters.create(_adding)


def set_state(uid, action, text=False, **kw):
    pending[uid] = {"a": action, "text": text, "ts": time.time(), **kw}


def kb(*rows):
    return Markup([[Btn(t, callback_data=d) for t, d in row] for row in rows])


BACK = kb([("⬅️ Menu", "a:menu")])
CANCEL = kb([("✖️ Cancel", "a:menu")])


def menu_text():
    lines = ["🛠 <b>Admin Panel</b>", ""]
    lines.append(f"📢 Ads: {'ON' if ads['on'] else 'OFF'}" + ("" if AD_READY else " (set AD_ZONE and an https WEBAPP_URL)"))
    lines.append(f"⏱ Daily limit: {DAILY_LIMIT} videos per user" if DAILY_LIMIT else "⏱ Daily limit: off")
    lines.append(f"💎 UPI payments: {'ON (' + str(len(PLANS)) + ' plans, ' + str(len(premium['u'])) + ' active)' if PAY_READY else 'OFF (set FAMGATEWAY_API_KEY)'}")
    if cfg is not None:
        lines.append(f"📡 Post channels: {len(cfg['ch'])}")
    lines += ["", "Send any file to this chat to get a share link.", "Use the buttons below for everything else."]
    return "\n".join(lines)


GAPS = [("10 min", "10m"), ("15 min", "15m"), ("30 min", "30m"), ("1 hour", "1h"), ("2 hours", "2h"),
        ("3 hours", "3h"), ("6 hours", "6h"), ("12 hours", "12h")]
GAP_LABEL = {v: k for k, v in GAPS}
COUNTS = [5, 10, 15, 20, 30, 40]
QUICK_TIMES = ["08:00", "10:00", "12:00", "18:00"]


def menu_markup():
    rows = [[("📊 Stats", "a:stats"), ("📤 Export", "a:export")],
            [("🗑 Delete Code", "a:del"), ("♻️ Restore", "a:restore")],
            [(f"📢 Ads: {'ON ✅' if ads['on'] else 'OFF ⛔'}", "a:ads")],
            [("📡 Channels (Source → Post)", "a:chs")]]
    return kb(*rows)


def plural(n, word="file"):
    return f"{n} {word}{'' if n == 1 else 's'}"


async def show(x, text, markup=None):
    """Button press -> edit the panel message. Typed message -> send a new one."""
    try:
        if isinstance(x, CallbackQuery):
            return await retry(x.message.edit_text, text, reply_markup=markup,
                               disable_web_page_preview=True)
        return await retry(x.reply, text, reply_markup=markup,
                           disable_web_page_preview=True, quote=True)
    except MessageNotModified:
        return x.message


async def upd(msg, text, markup=None):
    try:
        return await retry(msg.edit_text, text, reply_markup=markup, disable_web_page_preview=True)
    except MessageNotModified:
        return msg


async def feature_ready(x, tid=None):
    if not await load_all():
        await show(x, "⚠️ Could not load data from the worker. Please try again in a moment.", BACK)
        return False
    if tid is not None:
        if tid not in cfg["ch"]:
            await show(x, "⚠️ That channel no longer exists.", kb([("📡 Channels", "a:chs")]))
            return False
        if not await ch_data(tid):
            await show(x, "⚠️ Could not load this channel's data. Please try again in a moment.", BACK)
            return False
    return True


# ---------- stats ----------
async def build_stats():
    bump("dl", 0)  # rolls the day over if needed
    text = (f"📊 Stats ({stat['day']} UTC)\n\n"
            f"🤖 Bot (since last restart)\n"
            f"⬇️ Downloads: {stat['dl']} ({stat['files']} files)\n"
            f"⬆️ Uploads: {stat['up']}\n"
            f"📢 Ads completed: {stat['ads']}\n"
            f"⛔ Daily-limit blocks: {stat['blocked']}\n\n")
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
        text += "☁️ Set CF_ACCOUNT_ID and CF_API_TOKEN to see exact Cloudflare numbers."
    return text


# ---------- export / restore ----------
async def do_export(c, x):
    await show(x, "📤 Preparing backup…")
    sem = asyncio.Semaphore(20)

    async def one(code):
        async with sem:
            return code, await get_items(code, fresh=True)

    try:
        data = dict(await asyncio.gather(*[one(k) for k in await list_codes()]))
        f = io.BytesIO(json.dumps(data, indent=1).encode())
        f.name = "backup.json"
        chat_id = x.message.chat.id if isinstance(x, CallbackQuery) else x.chat.id
        await retry(c.send_document, chat_id, f, caption=f"{len(data)} codes")
        await show(x, menu_text(), menu_markup())
    except Exception as e:
        logging.error("export failed: %s", e)
        await show(x, f"❌ Export failed: {e}", BACK)


async def run_restore(c, status):
    """After changing DB_CHANNEL / BACKUP_CHANNEL env: fill every channel with all files."""
    global restoring
    if restoring:
        return await upd(status, "♻️ A restore is already running.", BACK)
    restoring = True
    try:
        await upd(status, "♻️ Restoring… this can take a while.")
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
        await upd(status, f"✅ Restored {ok}, failed {bad}", BACK)
    except Exception as e:
        logging.error("restore failed: %s", e)
        await upd(status, f"❌ Restore failed: {e}", BACK)
    finally:
        restoring = False


# ---------- channels: add / remove from the bot ----------
async def chat_from_input(c, m):
    """Forwarded channel post, or typed id / @username -> (chat_id, title). Raises ValueError."""
    fc = getattr(m, "forward_from_chat", None)
    if not fc:
        fo = getattr(m, "forward_origin", None)
        fc = getattr(fo, "chat", None) or getattr(fo, "sender_chat", None)
    if fc:
        return fc.id, fc.title or str(fc.id)
    text = (m.text or "").strip()
    if re.fullmatch(r"-?\d+", text):
        n = int(text)
        ref = n if n < 0 else int(("-" if text.startswith("100") else "-100") + text.lstrip("-"))
    elif re.fullmatch(r"@?[A-Za-z]\w{4,}", text):
        ref = "@" + text.lstrip("@")
    else:
        raise ValueError("Forward a post from the channel, or send its ID (-100…) or @username.")
    try:
        chat = await retry(c.get_chat, ref)
    except Exception:
        raise ValueError("I can't find that channel. Add the bot as admin there first, then try again.")
    return chat.id, chat.title or str(chat.id)


async def require_admin(c, chat_id):
    try:
        me = await retry(c.get_chat_member, chat_id, "me")
    except Exception:
        raise ValueError("The bot can't access that channel. Add it as admin first, then try again.")
    if me.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
        raise ValueError("The bot is not an admin in that channel. Make it admin (post + delete rights), then try again.")


def chan_text(tid):
    ch, d = cfg["ch"][tid], data[tid]
    lib, sc = d["lib"], d["sched"]
    posted = set(sc["posted"])
    left = sum(1 for g in lib if gk(g) not in posted)
    q = sc["queue"]
    t = (f"📡 <b>{html.escape(ch['t'])}</b>\n\n"
         f"📥 Sources: {len(ch['src'])}\n"
         f"📦 Memory: {len(lib)} posts ({sum(len(g['v']) for g in lib)} videos)\n"
         f"🕓 Left in this round: {left}  (rounds finished: {sc.get('cycle', 0)})\n"
         f"🔁 Daily plan: {rules_text(sc.get('rules'))}\n"
         f"⏰ Queued: {len(q)}")
    if q:
        t += "\n" + "\n".join(f"• {fmt_ts(e[0])}" for e in q[:6]) + ("\n…" if len(q) > 6 else "")
    return t


def chan_markup(tid):
    return kb([("📡 Sources", f"a:srcs:{tid}"), ("🔍 Scan", f"a:scan:{tid}")],
              [("📅 Daily Plan", f"a:plan:{tid}"), ("🕐 One-time times", f"a:sch_t:{tid}")],
              [("⚡ Post Now", f"a:now:{tid}"), ("🔄 Refresh", f"a:ch:{tid}")],
              [("❌ Clear Queue", f"a:clr:{tid}"), ("🗑 Remove channel", f"a:rmt:{tid}")],
              [("⬅️ Channels", "a:chs")])


def src_markup(tid):
    rows = [[(f"❌ {t[:28]}", f"a:rms:{tid}:{sid}")] for sid, t in cfg["ch"][tid]["src"].items()]
    return kb(*rows, [("➕ Add source channel", f"a:adds:{tid}")], [("⬅️ Back", f"a:ch:{tid}")])


def src_text(tid):
    ch = cfg["ch"][tid]
    body = ("\n".join(f"• {html.escape(t)}" for t in ch["src"].values())
            if ch["src"] else "No source channels yet.")
    return (f"📥 <b>Sources of {html.escape(ch['t'])}</b>\n\n{body}\n\n"
            "Posts scanned from these channels are saved in this channel's own memory.\n"
            "Tap ❌ to remove a source.")


async def add_channel_input(c, m):
    uid = m.from_user.id
    st = pending[uid]
    text = (m.text or "").strip()
    if text.startswith("/"):
        pending.pop(uid, None)
        return await show(m, menu_text(), menu_markup())
    try:
        chat_id, title = await chat_from_input(c, m)
        await require_admin(c, chat_id)
    except ValueError as e:
        return await show(m, f"❌ {e}", CANCEL)
    if not await feature_ready(m):
        return
    new = str(abs(chat_id))
    if st["a"] == "addtgt":
        if new in cfg["ch"]:
            pending.pop(uid, None)
            return await show(m, "ℹ️ That channel is already added.", kb([(" Open", f"a:ch:{new}")]))
        async with sched_lock:
            cfg["ch"][new] = {"t": title, "src": {}}
            data[new] = {"lib": [], "sched": _fresh_sched()}
            ok = await persist("cfg", f"lib:{new}", f"sched:{new}")
        pending.pop(uid, None)
        await show(m, f"✅ <b>{html.escape(title)}</b> added.\nNow add its source channel(s)." +
                   ("" if ok else "\n⚠️ Saving failed, it will retry automatically."),
                   kb([("➕ Add source channel", f"a:adds:{new}")], [("📡 Open channel", f"a:ch:{new}")]))
    else:
        tid = st["tid"]
        if tid not in cfg["ch"]:
            pending.pop(uid, None)
            return await show(m, "⚠️ That channel no longer exists.", kb([("📡 Channels", "a:chs")]))
        if new == tid:
            return await show(m, "❌ A channel can't be its own source. Send another channel.", CANCEL)
        async with sched_lock:
            cfg["ch"][tid]["src"][new] = title
            ok = await persist("cfg")
        pending.pop(uid, None)
        await show(m, f"✅ Source <b>{html.escape(title)}</b> added.\nTap 🔍 Scan to read its videos into the memory." +
                   ("" if ok else "\n⚠️ Saving failed, it will retry automatically."),
                   kb([("🔍 Scan now", f"a:scan:{tid}")], [("📥 Sources", f"a:srcs:{tid}")]))


@app.on_message(filters.private & is_admin & adding_input, group=-1)
async def add_channel_handler(c, m):
    try:
        await add_channel_input(c, m)
    except Exception as e:
        logging.error("add channel failed: %s", e)
        await show(m, f"❌ Failed: {e}", CANCEL)
    m.stop_propagation()   # a forwarded video must NOT be treated as a file upload


# ---------- scan ----------
async def run_scan(c, x, tid, only=None, rng=None):
    """Read the source channels' history by message id and add the posts to THIS channel's memory.
    rng = [from] | [from, to] (custom range, one source)"""
    if not await feature_ready(x, tid):
        return
    back = kb([("⬅️ Back", f"a:ch:{tid}")])
    ch, d = cfg["ch"][tid], data[tid]
    srcs = [only] if only else list(ch["src"])
    if not srcs or any(s not in ch["src"] for s in srcs):
        return await show(x, "📥 No source channel yet. Add one first.", kb([("📥 Sources", f"a:srcs:{tid}")], [("⬅️ Back", f"a:ch:{tid}")]))
    if scan_lock.locked():
        return await show(x, "🔍 A scan is already running.", back)
    async with scan_lock:
        status = await show(x, "🔍 Scanning…")
        lib, sc = d["lib"], d["sched"]
        new, orphans, done, ext_all = [], 0, [], []
        try:
            for sid in srcs:
                source = cid(sid)
                hi = rng[1] if rng and len(rng) > 1 else await last_msg_id(c, source)
                lo = max(rng[0] if rng else sc["last"].get(sid, 0) + 1, 1)
                if hi < lo:
                    continue
                if hi - lo > 200000:
                    return await upd(status, "❌ Range is too large (max 200000 ids).", back)
                known = {v[0] for g in lib if g["s"] == sid for v in g["v"]}
                posted = set(sc["posted"])
                mine = [g for g in lib if g["s"] == sid]
                # a post cut in half by the previous scan keeps growing (unless already posted)
                ext = mine[-1] if mine and gk(mine[-1]) not in posted and lo > mine[-1]["v"][-1][0] else None
                n0, cur = (len(ext["v"]) if ext else 0), ext
                for s in range(lo, hi + 1, 200):
                    msgs = await retry(c.get_messages, source, list(range(s, min(s + 200, hi + 1))))
                    g, o, cur = group_messages(msgs, cur, known)
                    for x_ in g:
                        x_["s"] = sid
                    new += g
                    orphans += o
                if ext and len(ext["v"]) != n0:
                    ext.pop("k", None)   # its link must be rebuilt because it got more videos
                done.append((sid, hi))
        except Exception as e:
            logging.error("scan failed: %s", e)
            return await upd(status, f"❌ Scan failed: {e}\nIs the bot an admin in the SOURCE channel? "
                                     "You can also try a custom range.", back)
        async with sched_lock:
            before = sum(len(g["v"]) for g in lib)
            lib.extend(new)
            lib.sort(key=lambda g: (g["s"], g["i"]))
            for sid, hi in done:
                sc["last"][sid] = max(sc["last"].get(sid, 0), hi)
            ok = await persist(f"lib:{tid}", f"sched:{tid}")
            vids = sum(len(g["v"]) for g in lib) - before
            posted = set(sc["posted"])
            left = sum(1 for g in lib if gk(g) not in posted)
        if not done:
            return await upd(status, "✅ Nothing new (already scanned).", back)
        await upd(status, f"✅ Scan complete ({plural(len(done), 'source')})\n🆕 New posts: {len(new)}\n🎬 New videos: {vids}\n"
                          f"⏭ Skipped videos without a captioned video above them: {orphans}\n"
                          f"📦 Memory: {len(lib)} posts | Left in this round: {left}"
                  + ("" if ok else "\n⚠️ Saving to the worker failed, it will retry automatically."),
                  back)


# ---------- schedule ----------
async def apply_schedule(x, tid, stamps):
    async with sched_lock:
        sc, lib = data[tid]["sched"], data[tid]["lib"]
        sc["queue"] += [[t, 0] for t in stamps]
        sc["queue"].sort()
        ok = await persist(f"sched:{tid}")
        posted = set(sc["posted"])
        left = sum(1 for g in lib if gk(g) not in posted)
        total = len(sc["queue"])
    lines = [f"✅ {plural(len(stamps), 'post')} scheduled ({fmt_ts(stamps[0])} to {fmt_ts(stamps[-1])})"]
    lines += [f"• {fmt_ts(t)}" for t in stamps[:15]] + (["…"] if len(stamps) > 15 else [])
    if not lib:
        lines.append("\n⚠️ This channel's memory is empty. Add a source and scan it.")
    elif total > left and data[tid]["sched"].get("cycle", 0) == 0:
        lines.append(f"\nℹ️ {total} in the queue but {left} unused posts. After they are used the memory restarts in shuffled order.")
    if not ok:
        lines.append("⚠️ Saving to the worker failed, it will retry automatically.")
    await show(x, "\n".join(lines), kb([("⬅️ Back", f"a:ch:{tid}")]))


async def apply_rules(x, tid, rules):
    """Save the daily batches of a channel ([] = turn off) and rebuild today's slots."""
    async with sched_lock:
        sc = data[tid]["sched"]
        sc["rules"] = rules
        sc["queue"] = [e for e in sc["queue"] if len(e) < 3]   # drop the old plan's pending slots
        sc["rday"] = None
        if rules:
            ensure_daily(tid)
        ok = await persist(f"sched:{tid}")
        today = [e[0] for e in sc["queue"] if len(e) >= 3]
    back = kb([("📅 Daily plan", f"a:plan:{tid}"), ("⬅️ Channel", f"a:ch:{tid}")])
    if not rules:
        return await show(x, "🚫 Daily plan turned off.", back)
    lines = [f"✅ Daily plan: {rules_text(rules)}", f"Timezone: {TZ}", ""]
    if today:
        lines.append("Today: " + ", ".join(datetime.fromtimestamp(t, TZ).strftime("%H:%M") for t in today))
    else:
        nxt = min(rule_stamps(r, datetime.now(TZ) + timedelta(days=1))[0] for r in rules)
        lines.append(f"Today's slots have already passed. First post: {fmt_ts(nxt)}")
    lines.append("It repeats every day. When all posts are used, the memory restarts in shuffled order.")
    if not data[tid]["lib"]:
        lines.append("\n⚠️ Memory is empty. Add a source and scan it.")
    if not ok:
        lines.append("⚠️ Saving to the worker failed, it will retry automatically.")
    await show(x, "\n".join(lines), back)


async def do_postnow(c, x, tid):
    """Post the next post of this channel right now (test)."""
    if not await feature_ready(x, tid):
        return
    back = kb([("⬅️ Back", f"a:ch:{tid}")])
    await show(x, "⏳ Posting…")
    async with sched_lock:
        try:
            res = await publish_next(c, tid)
        except Exception as e:
            return await show(x, f"❌ Post failed: {e}\n(Is the bot an admin in the TARGET and SOURCE channel?)", back)
        if res is None:
            return await show(x, "📭 Memory is empty. Add a source channel and scan it first.",
                              kb([("📥 Sources", f"a:srcs:{tid}")], [("⬅️ Back", f"a:ch:{tid}")]))
        await persist(f"sched:{tid}")
    await show(x, f"✅ Posted: #{res[0]} ({plural(res[1], 'video')})\n🔗 {res[2]}", back)


async def schedule_from_args(x, tid, args):
    try:
        stamps = parse_schedule(args)
    except ValueError as e:
        return await show(x, f"❌ {e}\n\n{SCHEDULE_HELP}", CANCEL)
    pending.pop(x.from_user.id, None)
    await apply_schedule(x, tid, stamps)


# ---------- button presses ----------
@app.on_callback_query(filters.regex(r"^a:") & is_admin)
async def panel(c, q):
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    parts = q.data[2:].split(":")
    act = parts[0]

    if act in ("menu", "cancel"):
        pending.pop(uid, None)
        return await show(q, menu_text(), menu_markup())
    if act == "menunew":
        pending.pop(uid, None)
        return await retry(q.message.reply, menu_text(), reply_markup=menu_markup())

    if act == "ads":
        if not AD_READY:
            return await show(q, "⚠️ Set the AD_ZONE and WEBAPP_URL (https) env variables first.", BACK)
        ads["on"] = not ads["on"]
        try:
            await kv_put(ADS_CFG_KEY, {"on": ads["on"]})
        except Exception as e:
            logging.error("saving ads setting failed: %s", e)
        return await show(q, menu_text(), menu_markup())

    if act == "stats":
        return await show(q, await build_stats(), kb([("🔄 Refresh", "a:stats"), ("⬅️ Menu", "a:menu")]))
    if act == "export":
        return await do_export(c, q)

    if act == "del":
        set_state(uid, "del", text=True)
        return await show(q, "🗑 Send the code you want to delete (for example: abc123xy).", CANCEL)
    if act == "delok":
        st = pending.pop(uid, None)
        if not st or st["a"] != "delok":
            return await show(q, menu_text(), menu_markup())
        try:
            await del_code(st["code"])
        except Exception as e:
            return await show(q, f"❌ Delete failed: {e}", BACK)
        return await show(q, f"🗑 #{st['code']} deleted from the index.", BACK)

    if act == "restore":
        return await show(q, "♻️ Restore copies every saved file into the current DB / backup channels.\n"
                             "This can take a long time. Continue?",
                          kb([("✅ Yes, restore", "a:restoreok"), ("✖️ Cancel", "a:menu")]))
    if act == "restoreok":
        if restoring:
            return await show(q, "♻️ A restore is already running.", BACK)
        t = asyncio.create_task(run_restore(c, q.message))
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)
        return

    # ---------- channels ----------
    if act == "chs":
        pending.pop(uid, None)
        if not await feature_ready(q):
            return
        rows = [[(f"📡 {t['t'][:30]}", f"a:ch:{tid}")] for tid, t in cfg["ch"].items()]
        return await show(q, "📡 <b>Post channels</b>\n\nEach channel has its own memory, its own source channels "
                             "and its own schedule." + ("" if rows else "\n\nNo channel yet. Add one 👇"),
                          kb(*rows, [("➕ Add post channel", "a:addt")], [("⬅️ Menu", "a:menu")]))
    if act == "addt":
        set_state(uid, "addtgt")
        return await show(q, "➕ <b>Add a post channel</b> (where posts will be published)\n\n"
                             "1. Add the bot as admin there (post rights)\n"
                             "2. Forward any post from that channel here, or send its ID (-100…) / @username", CANCEL)

    tid = parts[1] if len(parts) > 1 else None
    if tid is not None:
        if not await feature_ready(q, tid):
            return
        back = kb([("⬅️ Back", f"a:ch:{tid}")])

        if act == "ch":
            pending.pop(uid, None)
            return await show(q, chan_text(tid), chan_markup(tid))

        if act == "srcs":
            pending.pop(uid, None)
            return await show(q, src_text(tid), src_markup(tid))
        if act == "adds":
            set_state(uid, "addsrc", tid=tid)
            return await show(q, "➕ <b>Add a source channel</b>\n\n1. Add the bot as admin there\n"
                                 "2. Forward any post from the source channel here, or send its ID (-100…) / @username",
                              kb([("✖️ Cancel", f"a:srcs:{tid}")]))
        if act == "rms":
            sid = parts[2]
            title = cfg["ch"][tid]["src"].get(sid)
            if title is None:
                return await show(q, src_text(tid), src_markup(tid))
            n = sum(1 for g in data[tid]["lib"] if g["s"] == sid)
            return await show(q, f"❌ Remove source <b>{html.escape(title)}</b>?\n\n"
                                 f"Its {plural(n, 'post')} will also be removed from this channel's memory. "
                                 "Posts already published stay as they are.",
                              kb([("✅ Yes, remove", f"a:rmsok:{tid}:{sid}"), ("✖️ No", f"a:srcs:{tid}")]))
        if act == "rmsok":
            sid = parts[2]
            async with sched_lock:
                d = data[tid]
                cfg["ch"][tid]["src"].pop(sid, None)
                d["lib"][:] = [g for g in d["lib"] if g["s"] != sid]
                d["sched"]["last"].pop(sid, None)
                d["sched"]["posted"] = [k for k in d["sched"]["posted"] if not k.startswith(sid + ":")]
                d.pop("pick", None)
                for k in [k for k in made_codes if k[0] == tid and k[1].startswith(sid + ":")]:
                    made_codes.pop(k, None)
                ok = await persist("cfg", f"lib:{tid}", f"sched:{tid}")
            return await show(q, "🗑 Source removed." + ("" if ok else "\n⚠️ Saving failed, it will retry automatically."),
                              kb([("📥 Sources", f"a:srcs:{tid}")]))

        if act == "scan":
            rows = [[(f"🎯 Custom range: {t[:20]}", f"a:scanrng:{tid}:{sid}")] for sid, t in cfg["ch"][tid]["src"].items()]
            return await show(q, "🔍 Scan the source channels for new videos.\n\n"
                                 "“Scan new” continues from the last scanned message of every source.",
                              kb([("🔍 Scan new videos", f"a:scannew:{tid}")], *rows, [("⬅️ Back", f"a:ch:{tid}")]))
        if act == "scannew":
            return await run_scan(c, q, tid)
        if act == "scanrng":
            sid = parts[2]
            set_state(uid, "scanrng", text=True, tid=tid, sid=sid)
            return await show(q, "🎯 Send the message ID range:\n• 1 5000  → from id 1 to id 5000\n"
                                 "• 300  → from id 300 to the newest message", CANCEL)

        if act == "now":
            return await do_postnow(c, q, tid)

        if act == "sch_t":
            set_state(uid, "sch_t", text=True, tid=tid)
            return await show(q, SCHEDULE_HELP, CANCEL)

        if act == "plan":
            rules = data[tid]["sched"].get("rules", [])
            rows = [[(f"❌ {rule_text(r)}"[:60], f"a:pl_rm:{tid}:{i}")] for i, r in enumerate(rules)]
            rows.append([("➕ Add a batch", f"a:pl_n:{tid}")])
            if rules:
                rows.append([("🚫 Turn off all", f"a:pl_off:{tid}")])
            return await show(q, f"📅 <b>Daily plan</b> of {html.escape(cfg['ch'][tid]['t'])}\n\nNow: {rules_text(rules)}\n"
                                 f"Timezone: {TZ}\n\nA batch = N posts starting at a time you choose, one every X minutes/hours. "
                                 "You can add several batches (for example 20 at 09:00 and 20 at 20:00). "
                                 "Tap a batch to remove it.", kb(*rows, [("⬅️ Back", f"a:ch:{tid}")]))
        if act == "pl_n":
            return await show(q, "🔢 How many posts per day?",
                              kb([(str(n), f"a:pl_g:{tid}:{n}") for n in COUNTS[:3]],
                                 [(str(n), f"a:pl_g:{tid}:{n}") for n in COUNTS[3:]],
                                 [("⌨️ Type everything in one line", f"a:pl_x:{tid}")],
                                 [("⬅️ Back", f"a:plan:{tid}")]))
        if act == "pl_g":
            n = parts[2]
            return await show(q, f"⏱ {n} posts per day. Gap between posts?",
                              kb(*[[(label, f"a:pl_t:{tid}:{n}:{v}") for label, v in GAPS[i:i + 3]] for i in range(0, len(GAPS), 3)],
                                 [("⬅️ Back", f"a:pl_n:{tid}")]))
        if act == "pl_t":
            n, gap = parts[2], parts[3]
            return await show(q, f"🕐 {n} posts, every {GAP_LABEL.get(gap, gap)}. At what time should the first post of the day go out?\n"
                                 f"Timezone: {TZ}",
                              kb([(t, f"a:pl_ok:{tid}:{n}:{gap}:{t.replace(':', '')}") for t in QUICK_TIMES],
                                 [("⌨️ Type start time", f"a:pl_s:{tid}:{n}:{gap}")],
                                 [("⬅️ Back", f"a:pl_g:{tid}:{n}")]))
        if act == "pl_ok":
            n, gap, hhmm = parts[2], parts[3], parts[4]
            try:
                rule = parse_plan([n, f"{hhmm[:2]}:{hhmm[2:]}", gap])
            except ValueError as e:
                return await show(q, f"❌ {e}", back)
            return await apply_rules(q, tid, data[tid]["sched"].get("rules", []) + [rule])
        if act == "pl_rm":
            rules = list(data[tid]["sched"].get("rules", []))
            if parts[2].isdigit() and int(parts[2]) < len(rules):
                rules.pop(int(parts[2]))
            return await apply_rules(q, tid, rules)
        if act == "pl_s":
            set_state(uid, "pl_s", text=True, tid=tid, n=parts[2], gap=parts[3])
            return await show(q, f"⌨️ Send the start time as HH:MM (24-hour), for example 18:30.\nTimezone: {TZ}", CANCEL)
        if act == "pl_x":
            set_state(uid, "pl_x", text=True, tid=tid)
            return await show(q, PLAN_HELP, CANCEL)
        if act == "pl_off":
            return await apply_rules(q, tid, [])

        if act == "clr":
            n = len(data[tid]["sched"]["queue"])
            if not n:
                return await show(q, "📭 No scheduled posts to cancel.", back)
            return await show(q, f"❌ Cancel all {plural(n, 'scheduled post')}?\n(The daily plan stays on and continues tomorrow.)",
                              kb([("✅ Yes, cancel them", f"a:clrok:{tid}"), ("✖️ No", f"a:ch:{tid}")]))
        if act == "clrok":
            async with sched_lock:
                sc = data[tid]["sched"]
                n = len(sc["queue"])
                sc["queue"] = []
                await persist(f"sched:{tid}")
            return await show(q, f"🗑 {plural(n, 'scheduled post')} cancelled.", back)

        if act == "rmt":
            return await show(q, f"🗑 Remove <b>{html.escape(cfg['ch'][tid]['t'])}</b> from the bot?\n\n"
                                 "Its memory, sources and schedule are deleted. The channel itself and the "
                                 "share links stay untouched.",
                              kb([("✅ Yes, remove", f"a:rmtok:{tid}"), ("✖️ No", f"a:ch:{tid}")]))
        if act == "rmtok":
            async with sched_lock:
                cfg["ch"].pop(tid, None)
                data.pop(tid, None)
                for k in [k for k in made_codes if k[0] == tid]:
                    made_codes.pop(k, None)
                dirty.difference_update({f"lib:{tid}", f"sched:{tid}"})
                ok = await persist("cfg")
            for key in (lib_key(tid), sched_key(tid)):
                try:
                    await api("POST", "delete", params={"code": key})
                except Exception as e:
                    logging.warning("could not delete %s: %s", key, e)
            return await show(q, "🗑 Channel removed." + ("" if ok else "\n⚠️ Saving failed, it will retry automatically."),
                              kb([("📡 Channels", "a:chs")]))

    await show(q, menu_text(), menu_markup())


# ---------- typed answers (only when a button asked for them) ----------
@app.on_message(filters.private & filters.text & is_admin & awaiting_input)
async def typed_input(c, m):
    uid = m.from_user.id
    st = pending[uid]
    text = m.text.strip()
    if text.startswith("/"):  # old habit: slash text just opens the panel
        pending.pop(uid, None)
        return await show(m, menu_text(), menu_markup())
    act = st["a"]

    if act == "del":
        code = text.lower().lstrip("#")
        if not re.fullmatch(r"[a-z0-9_-]{1,60}", code) or code.startswith("__"):
            return await show(m, "❌ Invalid code. Send the code again.", CANCEL)
        try:
            items = await get_items(code, fresh=True)
        except Exception as e:
            return await show(m, f"⚠️ Service error: {e}", BACK)
        if not items:
            return await show(m, "❌ Code not found. Send another code.", CANCEL)
        set_state(uid, "delok", code=code)
        return await show(m, f"🗑 Delete #{code} ({plural(len(items))}) from the index?",
                          kb([("✅ Yes, delete", "a:delok"), ("✖️ Cancel", "a:menu")]))

    # everything below belongs to one channel
    tid = st.get("tid")
    if not tid or not await feature_ready(m, tid):
        return

    if act == "scanrng":
        try:
            args = [int(x) for x in text.split()[:2]]
        except ValueError:
            args = []
        if not args:
            return await show(m, "❌ Send numbers only, for example: 1 5000", CANCEL)
        pending.pop(uid, None)
        return await run_scan(c, m, tid, only=st["sid"], rng=args)

    if act == "sch_t":
        return await schedule_from_args(m, tid, text.split())

    if act in ("pl_s", "pl_x"):
        try:
            rule = parse_plan([st["n"], text, st["gap"]] if act == "pl_s" else text.split())
        except ValueError as e:
            return await show(m, f"❌ {e}\n\n{PLAN_HELP}", CANCEL)
        pending.pop(uid, None)
        return await apply_rules(m, tid, data[tid]["sched"].get("rules", []) + [rule])


@app.on_message(filters.private & filters.text & is_admin)
async def admin_fallback(c, m):
    await show(m, menu_text(), menu_markup())


async def main():
    global http
    http = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300),
                                 timeout=aiohttp.ClientTimeout(total=15))
    await app.start()
    try:  # remove the old slash-command menu
        await app.delete_bot_commands()
    except Exception:
        pass
    web_app = web.Application()
    web_app.add_routes([web.get("/", lambda r: web.Response(text="ok")),
                        web.get("/ad", ad_page),
                        web.post("/api/ad/start", ad_start),
                        web.post("/api/ad/done", ad_done),
                        web.post("/api/fam/webhook", fam_webhook),
                        web.get("/api/ad/pb", ad_postback)])
    runner = web.AppRunner(web_app, access_log=None)  # no per-request log spam from health checks
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    try:
        await api("POST", "put", params={"code": "__cfg_bot"}, data=app.me.username)
    except Exception as e:
        logging.error("could not save bot username to worker: %s", e)
    if AD_READY:
        try:
            cfg = await kv_get(ADS_CFG_KEY)
            if isinstance(cfg, dict) and "on" in cfg:
                ads["on"] = bool(cfg["on"])
        except Exception as e:
            logging.error("could not load ads setting: %s", e)
    elif AD_ZONE or WEBAPP_URL:
        logging.warning("Ads are OFF: set both AD_ZONE and an https WEBAPP_URL")
    if PAY_READY:
        await load_premium()
        await load_orders(app)
        logging.info("UPI payments ON (FamGateway): %d plans, webhook %s", len(PLANS),
                     f"{WEBAPP_URL}/api/fam/webhook" if WEBAPP_URL.startswith("https://") else "off (polling only)")
    elif FAM_API_KEY:
        logging.warning("Payments are OFF: need PLANS and DAILY_LIMIT > 0")
    if DAILY_LIMIT:
        await load_quota()
        t = asyncio.create_task(quota_saver())
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)
    logging.info("Ads: %s | daily limit: %s", "ON" if ads["on"] else "OFF", DAILY_LIMIT or "off")
    t = asyncio.create_task(scheduler(app))
    bg_tasks.add(t)
    t.add_done_callback(bg_tasks.discard)
    logging.info("Post scheduler on (channels are managed from the bot panel)")
    logging.info("Bot @%s running", app.me.username)
    await idle()
    if quota_dirty:  # save the counters before shutting down
        try:
            await kv_put(QUOTA_KEY, quota_doc())
        except Exception:
            pass
    await app.stop()
    await http.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
