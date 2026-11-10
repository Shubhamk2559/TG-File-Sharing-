from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from contextlib import asynccontextmanager
from functools import lru_cache
import asyncio, copy, hashlib, hmac, html, io, json, logging, os, re, secrets, time

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
AD_MIN_WATCH = int(os.environ.get("AD_MIN_WATCH", "5"))            # min seconds between ad start and finish
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


# ---------- user side ----------
@app.on_message(filters.command("start") & filters.private)
async def start(c, m):
    if len(m.command) < 2:
        if m.from_user.id in ADMINS:  # admins get the button panel
            pending.pop(m.from_user.id, None)
            return await m.reply(menu_text(), reply_markup=menu_markup())
        return await m.reply("👋 Send me a file link to get your files.")
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
        return await m.reply(f"⛔ Daily limit reached ({DAILY_LIMIT} videos per day). " + reset_text())
    if ads["on"] and not (ADMINS_SKIP_ADS and uid in ADMINS):
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
    return max(DAILY_LIMIT - quota["u"].get(uid, 0), 0)


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
            return await retry(c.send_message, uid, f"⛔ Daily limit reached ({DAILY_LIMIT} videos per day). "
                                                    + reset_text())
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
            notes.append(f"ℹ️ Daily limit reached ({DAILY_LIMIT} videos per day), so only the first "
                         f"{len(sent)} file(s) were sent. " + reset_text())
        elif DAILY_LIMIT and uid not in ADMINS and quota_left(uid) <= 5:
            notes.append(f"📊 {quota_left(uid)} of {DAILY_LIMIT} videos left today.")
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
  const s=await post("/api/ad/start");
  if(!s.ok){say("⚠️","Cannot start",s.error||"Please open the file link in the bot again.",!!s.retry);return;}
  const show=window["show_"+ZONE];
  if(typeof show!=="function")throw new Error("sdk");
  await show({ymid:String(s.ymid||"")});
  const d=await post("/api/ad/done");
  if(d.ok){say("✅","Unlocked!","Your files are being sent to the chat.",false);setTimeout(()=>tg.close(),1500);}
  else{say("⚠️","Could not unlock",d.error||"Please try again.",!!d.retry);}
 }catch(e){
  say("⚠️","Ad not completed","Watch the full ad. If it does not load, turn off your ad blocker or private DNS, then tap Try again.",true);
 }finally{running=false;$("btn").disabled=false;}
}
$("btn").onclick=run;window.addEventListener("load",()=>setTimeout(run,300));
</script></body></html>"""


async def ad_page(request):
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
        return web.json_response({"ok": False, "error": f"Daily limit reached ({DAILY_LIMIT} videos). " + reset_text()})
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
    ad_sessions.pop(nonce)
    ad_used.set(nonce, True, AD_TOKEN_TTL + 120)
    bump("ads")
    t = asyncio.create_task(deliver(app, uid, code))
    bg_tasks.add(t)
    t.add_done_callback(bg_tasks.discard)
    return web.json_response({"ok": True})


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
    """'\\n' (and '/n' not followed by a letter) in the env value -> real new line."""
    return re.sub(r"\\n|/n(?![A-Za-z])", "\n", text)


POST_CAPTION = _nl(os.environ.get("POST_CAPTION", "🔥 New Video Uploaded!\\n\\nWatch now 👇"))

SCHED_KEY, LIB_KEY = "__sched", "__lib"   # "__" keys are hidden from /export & /restore
sched = None        # {"last": int, "posted": [first_msg_ids], "queue": [[epoch, tries], ...]}
lib = None          # [{"i": first_id, "c": caption_html, "v": [[msg_id, file_id, unique_id], ...]}]
dirty = set()       # which docs still need saving to the worker
sched_lock = asyncio.Lock()
scan_lock = asyncio.Lock()
made_codes = {}     # group id -> code already written (a retry doesn't write again)

SCHEDULE_HELP = (
    "📅 Send your times, for example:\n"
    "• 10:00 14:00 18:30  → one post at each time\n"
    "• 6 10:00 2h  → 6 posts, starting at 10:00, every 2 hours\n"
    "• 2026-11-12 10:00 14:00  → same, but on that date\n"
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


def new_code():
    return "".join(secrets.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(8))


async def publish_next(c):
    """Next unposted post: save its CAPTION-LESS videos into DB_CHANNEL (+backup) under a new code,
    index the code, then post the captioned video in TARGET_CHANNEL with the link button.
    Returns (code, n_videos, link), or None if nothing is left. Safe to retry after a failure."""
    posted = set(sched["posted"])
    grp = next((g for g in lib if g["i"] not in posted), None)
    if not grp:
        return None
    st = made_codes.get(grp["i"])
    if st is None:
        vids = grp["v"][1:] or grp["v"]  # link = caption-less videos only (lone captioned video: fallback)
        seen, uniq = set(), []
        for v in vids:
            if v[2] not in seen:
                seen.add(v[2])
                uniq.append(v)
        st = made_codes[grp["i"]] = {"code": new_code(), "n": len(uniq), "saved": [], "indexed": False,
                                     "todo": [(v[0], ch) for v in uniq for ch in CHANNELS]}
    code = st["code"]
    while st["todo"]:  # copy into the database channel(s); progress is kept if something fails midway
        mid, ch = st["todo"][0]
        st["saved"].append(await retry(c.copy_message, ch, SOURCE_CHANNEL, mid, caption=f"🔖 #{code}"))
        st["todo"].pop(0)
    if not st["indexed"]:
        if not await index_messages(st["saved"]):  # ONE KV write for the whole post
            raise RuntimeError("could not save the code index")
        st["indexed"], st["saved"] = True, []
    link = (f"https://t.me/{c.me.username}?start={code}" if POST_LINK_BOT
            else f"{WORKER_URL}/{code}")
    # link lives ONLY inside the button (never in the caption text)
    await retry(c.copy_message, TARGET_CHANNEL, SOURCE_CHANNEL, grp["i"], caption=POST_CAPTION,
                parse_mode=ParseMode.HTML, reply_markup=Markup([[Btn(BUTTON_TEXT, url=link)]]))
    sched["posted"].append(grp["i"])
    made_codes.pop(grp["i"], None)
    return code, st["n"], link


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
                    note = f"❌ The post scheduled for {fmt_ts(ent[0])} failed 3 times and was removed.\n{e}\n(Is the bot an admin in the TARGET channel?)"
                else:
                    ent[0] = int(time.time()) + 300
                    q.sort()
                    note = f"⚠️ Scheduled post failed: {e}\nRetrying in 5 minutes."
            else:
                q.pop(0)
                if res is None:
                    note = "📭 A scheduled time arrived but no unposted posts are left. Open the panel and scan for new posts."
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
    lines += ["", "Send any file to this chat to get a share link.", "Use the buttons below for everything else."]
    return "\n".join(lines)
GAPS = [("30 min", "30m"), ("1 hour", "1h"), ("2 hours", "2h"),
        ("3 hours", "3h"), ("6 hours", "6h"), ("12 hours", "12h")]
GAP_LABEL = {v: k for k, v in GAPS}
COUNTS = [5, 10, 20, 30, 50, 100]


def menu_markup():
    rows = [[("📊 Stats", "a:stats"), ("📤 Export", "a:export")],
            [("🗑 Delete Code", "a:del"), ("♻️ Restore", "a:restore")],
            [(f"📢 Ads: {'ON ✅' if ads['on'] else 'OFF ⛔'}", "a:ads")]]
    if feature_on():
        rows += [[("🔍 Scan Source", "a:scan"), ("📋 Posts", "a:posts")],
                 [("📅 Schedule", "a:sch"), ("⚡ Post Now", "a:now")],
                 [("❌ Clear Schedule", "a:clr")]]
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


async def feature_ready(x):
    if not feature_on():
        await show(x, "⚠️ Set the SOURCE_CHANNEL and TARGET_CHANNEL env variables first.", BACK)
        return False
    if not await load_sched():
        await show(x, "⚠️ Could not load data from the worker. Please try again in a moment.", BACK)
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


# ---------- scheduled posts ----------
async def run_scan(c, x, args):
    """Read SOURCE_CHANNEL history by message id and build posts. args = [] | [from] | [from, to]"""
    if not await feature_ready(x):
        return
    if scan_lock.locked():
        return await show(x, "🔍 A scan is already running.", BACK)
    async with scan_lock:
        status = await show(x, "🔍 Scanning…")
        try:
            hi = args[1] if len(args) > 1 else await last_msg_id(c)
            lo = max(args[0] if args else sched["last"] + 1, 1)
            if hi < lo:
                return await upd(status, "✅ Nothing new (already scanned).", BACK)
            if hi - lo > 200000:
                return await upd(status, "❌ Range is too large (max 200000 ids).", BACK)
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
            return await upd(status, f"❌ Scan failed: {e}\nIs the bot an admin in the SOURCE channel? "
                                     "You can also try a custom range.", BACK)
        async with sched_lock:
            lib.extend(new)
            lib.sort(key=lambda g: g["i"])
            sched["last"] = max(sched["last"], hi)
            ok = await persist("lib", "sched")
            vids = sum(len(g["v"]) for g in lib) - before
            posted = set(sched["posted"])
            left = sum(1 for g in lib if g["i"] not in posted)
        await upd(status, f"✅ Scan complete (id {lo}–{hi})\n🆕 New posts: {len(new)}\n🎬 New videos: {vids}\n"
                          f"⏭ Skipped videos without a captioned video above them: {orphans}\n"
                          f"📦 Total posts: {len(lib)} | Unposted: {left}"
                          + ("" if ok else "\n⚠️ Saving to the worker failed, it will retry automatically."),
                  BACK)


async def apply_schedule(x, stamps):
    async with sched_lock:
        sched["queue"] += [[t, 0] for t in stamps]
        sched["queue"].sort()
        ok = await persist("sched")
        posted = set(sched["posted"])
        left = sum(1 for g in lib if g["i"] not in posted)
        total = len(sched["queue"])
    lines = [f"✅ {plural(len(stamps), 'post')} scheduled ({fmt_ts(stamps[0])} to {fmt_ts(stamps[-1])})"]
    lines += [f"• {fmt_ts(t)}" for t in stamps[:15]] + (["…"] if len(stamps) > 15 else [])
    if total > left:
        lines.append(f"\n⚠️ {total} in the queue but only {left} unposted posts. Scan for more.")
    if not ok:
        lines.append("⚠️ Saving to the worker failed, it will retry automatically.")
    await show(x, "\n".join(lines), BACK)


async def do_posts(x):
    if not await feature_ready(x):
        return
    posted = set(sched["posted"])
    done = sum(1 for g in lib if g["i"] in posted)
    q = sched["queue"]
    text = (f"📦 Library: {len(lib)} posts ({sum(len(g['v']) for g in lib)} videos)\n"
            f"✅ Posted: {done}\n🕓 Unposted: {len(lib) - done}\n⏰ Scheduled: {len(q)}")
    if q:
        text += "\n\n" + "\n".join(f"• {fmt_ts(t)}" for t, _ in q[:15]) + ("\n…" if len(q) > 15 else "")
    await show(x, text, kb([("🔍 Scan Source", "a:scan"), ("📅 Schedule", "a:sch")],
                           [("⬅️ Menu", "a:menu")]))


async def do_postnow(c, x):
    """Post the next unposted post right now (test)."""
    if not await feature_ready(x):
        return
    await show(x, "⏳ Posting…")
    async with sched_lock:
        try:
            res = await publish_next(c)
        except Exception as e:
            return await show(x, f"❌ Post failed: {e}\n(Is the bot an admin in the TARGET channel?)", BACK)
        if res is None:
            return await show(x, "📭 No unposted posts left. Scan the source channel first.",
                              kb([("🔍 Scan Source", "a:scan")], [("⬅️ Menu", "a:menu")]))
        await persist("sched")
    await show(x, f"✅ Posted: #{res[0]} ({plural(res[1], 'video')})\n🔗 {res[2]}", BACK)


def start_time(when):
    now = datetime.now(TZ)
    t = now + timedelta(minutes=5) if when == "5m" else (now + timedelta(hours=1)).replace(minute=0)
    return f"{t:%H:%M}"


async def schedule_from_args(x, args):
    try:
        stamps = parse_schedule(args)
    except ValueError as e:
        return await show(x, f"❌ {e}\n\n{SCHEDULE_HELP}", CANCEL)
    pending.pop(x.from_user.id, None)
    await apply_schedule(x, stamps)


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

    if act == "scan":
        if not await feature_ready(q):
            return
        return await show(q, "🔍 Scan the source channel for new videos.\n\n"
                             "“New videos” continues from the last scanned message.",
                          kb([("🔍 Scan new videos", "a:scannew")],
                             [("🎯 Custom range", "a:scanrng")],
                             [("⬅️ Menu", "a:menu")]))
    if act == "scannew":
        return await run_scan(c, q, [])
    if act == "scanrng":
        set_state(uid, "scanrng", text=True)
        return await show(q, "🎯 Send the message ID range:\n• 1 5000  → from id 1 to id 5000\n"
                             "• 300  → from id 300 to the newest message", CANCEL)

    if act == "posts":
        return await do_posts(q)
    if act == "now":
        return await do_postnow(c, q)

    if act == "sch":
        if not await feature_ready(q):
            return
        return await show(q, f"📅 Schedule posts\nTimezone: {TZ}",
                          kb([("🔁 Repeat at an interval", "a:sch_i")],
                             [("⌨️ Type custom times", "a:sch_t")],
                             [("⬅️ Menu", "a:menu")]))
    if act == "sch_t":
        set_state(uid, "sch_t", text=True)
        return await show(q, SCHEDULE_HELP, CANCEL)
    if act == "sch_i":
        rows = [GAPS[i:i + 3] for i in range(0, len(GAPS), 3)]
        return await show(q, "🔁 How often should a post go out?",
                          kb(*[[(label, f"a:sch_g:{v}") for label, v in r] for r in rows],
                             [("⬅️ Back", "a:sch")]))
    if act == "sch_g":
        gap = parts[1]
        rows = [COUNTS[i:i + 3] for i in range(0, len(COUNTS), 3)]
        return await show(q, f"🔢 Every {GAP_LABEL.get(gap, gap)}. How many posts in total?",
                          kb(*[[(str(n), f"a:sch_c:{gap}:{n}") for n in r] for r in rows],
                             [("⬅️ Back", "a:sch_i")]))
    if act == "sch_c":
        gap, n = parts[1], parts[2]
        return await show(q, f"⏰ {n} posts, every {GAP_LABEL.get(gap, gap)}. When should the first one go out?",
                          kb([("In 5 minutes", f"a:sch_s:{gap}:{n}:5m"),
                              ("Next full hour", f"a:sch_s:{gap}:{n}:hr")],
                             [("⌨️ Type start time", f"a:sch_x:{gap}:{n}")],
                             [("⬅️ Back", f"a:sch_g:{gap}")]))
    if act == "sch_s":
        gap, n, when = parts[1], parts[2], parts[3]
        return await schedule_from_args(q, [n, start_time(when), gap])
    if act == "sch_x":
        set_state(uid, "sch_x", text=True, gap=parts[1], n=parts[2])
        return await show(q, f"⌨️ Send the start time as HH:MM (24-hour), for example 18:30.\nTimezone: {TZ}", CANCEL)

    if act == "clr":
        if not await feature_ready(q):
            return
        n = len(sched["queue"])
        if not n:
            return await show(q, "📭 No scheduled posts to cancel.", BACK)
        return await show(q, f"❌ Cancel all {plural(n, 'scheduled post')}?",
                          kb([("✅ Yes, cancel them", "a:clrok"), ("✖️ No", "a:menu")]))
    if act == "clrok":
        if not await feature_ready(q):
            return
        async with sched_lock:
            n = len(sched["queue"])
            sched["queue"] = []
            await persist("sched")
        return await show(q, f"🗑 {plural(n, 'scheduled post')} cancelled.", BACK)

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

    if act == "scanrng":
        try:
            args = [int(x) for x in text.split()[:2]]
        except ValueError:
            args = []
        if not args:
            return await show(m, "❌ Send numbers only, for example: 1 5000", CANCEL)
        pending.pop(uid, None)
        return await run_scan(c, m, args)

    if act == "sch_t":
        return await schedule_from_args(m, text.split())

    if act == "sch_x":
        return await schedule_from_args(m, [st["n"], text, st["gap"]])


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
                        web.post("/api/ad/done", ad_done)])
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
    if DAILY_LIMIT:
        await load_quota()
        t = asyncio.create_task(quota_saver())
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)
    logging.info("Ads: %s | daily limit: %s", "ON" if ads["on"] else "OFF", DAILY_LIMIT or "off")
    if feature_on():
        t = asyncio.create_task(scheduler(app))
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)
        logging.info("Post scheduler on: %s -> %s", SOURCE_CHANNEL, TARGET_CHANNEL)
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
