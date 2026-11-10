from datetime import datetime, timezone
import asyncio, html, io, json, logging, os, re, secrets, string
import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.types import InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup

logging.basicConfig(level=logging.INFO)

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

app = Client("filebot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, workers=16)
http: aiohttp.ClientSession = None
lock = asyncio.Lock()

CODE_RE = re.compile(r"#([A-Za-z0-9_-]{1,60})")
MEDIA_ATTRS = ("document", "video", "audio", "photo", "animation", "voice", "video_note")
is_admin = filters.create(lambda _, __, m: m.from_user and m.from_user.id in ADMINS)
media = (filters.document | filters.video | filters.audio | filters.photo
         | filters.animation | filters.voice | filters.video_note)


# ---------- Cloudflare Worker storage ----------
async def api(method, path, **kw):
    async with http.request(method, f"{WORKER_URL}/api/{path}",
                            headers={"x-secret": API_SECRET}, **kw) as r:
        text = await r.text()
        if r.status != 200:
            raise RuntimeError(f"worker {r.status}: {text[:100]}")
        return text


async def get_items(code):
    return json.loads(await api("GET", "get", params={"code": code}))


async def put_items(code, items):
    await api("POST", "put", params={"code": code}, data=json.dumps(items))


async def del_code(code):
    await api("POST", "delete", params={"code": code})


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
    while True:
        try:
            return await fn(*a, **k)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)


def media_of(m):
    for a in MEDIA_ATTRS:
        o = getattr(m, a, None)
        if o:
            return o.file_id, o.file_unique_id


async def index_message(m):
    """Save message under its #code. Same file (unique id) -> one item with many locations."""
    cap = m.caption.html if m.caption else ""
    cm, mi = CODE_RE.search(m.caption or ""), media_of(m)
    if not cm or not mi:
        return None
    code = cm.group(1).lower()
    if code.startswith("__"):
        return None
    fid, uid = mi
    loc = [m.chat.id, m.id]
    async with lock:
        items = await get_items(code)
        for it in items:
            if it["u"] == uid:
                it["f"], it["cap"] = fid, cap
                it["l"] = [x for x in it["l"] if x[0] != loc[0]] + [loc]
                break
        else:
            items.append({"u": uid, "f": fid, "cap": cap, "l": [loc]})
        await put_items(code, items)
    return code, len(items)


def public_caption(cap, code):
    """Caption shown to users: same as channel caption, but without the #code."""
    cap = re.sub(r"\s*━+\s*🔖\s*#" + re.escape(code) + r"\b", "", cap, flags=re.I)
    cap = re.sub(r"\s*#" + re.escape(code) + r"\b", "", cap, flags=re.I)
    return cap.strip()


async def send_item(c, chat_id, it, code):
    # 1) copy from any saved channel location  2) fallback: send by file_id (same bot only)
    cap = public_caption(it["cap"], code)
    for chat, mid in it["l"]:
        try:
            return await retry(c.copy_message, chat_id, chat, mid, caption=cap,
                               parse_mode=ParseMode.HTML, protect_content=PROTECT)
        except Exception:
            continue
    return await retry(c.send_cached_media, chat_id, it["f"],
                       caption=cap, parse_mode=ParseMode.HTML, protect_content=PROTECT)


async def joined(c, uid):
    if not FORCE_SUB:
        return True
    try:
        chat = int(FORCE_SUB) if FORCE_SUB.lstrip("-").isdigit() else FORCE_SUB
        mem = await c.get_chat_member(chat, uid)
        return mem.status not in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT)
    except UserNotParticipant:
        return False
    except Exception:
        return True


async def delete_later(msgs):
    await asyncio.sleep(AUTO_DELETE)
    for m in msgs:
        try:
            await m.delete()
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
    if not await joined(c, m.from_user.id):
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
    wait = await m.reply("⏳ Sending…")
    sent = []
    for it in items:
        try:
            sent.append(await send_item(c, m.chat.id, it, code))
        except Exception as e:
            logging.warning("send failed (%s): %s", code, e)
    await wait.delete()
    if not sent:
        return await m.reply("❌ Files unavailable. Admin needs to run /restore.")
    bump("dl")
    bump("files", len(sent))
    if AUTO_DELETE:
        note = await m.reply(f"⚠️ Files auto-delete in {AUTO_DELETE // 60 or 1} min. Save them now.")
        asyncio.create_task(delete_later(sent + [note]))


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
    res = None
    for ch in CHANNELS:
        try:
            saved = await retry(m.copy, ch, caption=caption, parse_mode=ParseMode.HTML)
            res = await index_message(saved) or res
        except Exception as e:
            logging.warning("copy to %s failed: %s", ch, e)
            await m.reply(f"⚠️ Channel {ch} mein save nahi hua (bot admin hai?)", quote=True)
    if not res:
        return await m.reply("❌ Save failed.", quote=True)
    code, n = res
    bump("up")
    await m.reply(f"✅ #{code} ({n} file{'s' if n > 1 else ''})\n🔗 {WORKER_URL}/{code}",
                  quote=True, disable_web_page_preview=True)


@app.on_message(filters.chat(CHANNELS) & media)
async def channel_post(c, m):
    # files posted/forwarded straight into a DB/backup channel with #code get indexed too
    res = await index_message(m)
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
    data = {code: await get_items(code) for code in await list_codes()}
    f = io.BytesIO(json.dumps(data, indent=1).encode())
    f.name = "backup.json"
    await m.reply_document(f, caption=f"{len(data)} codes")


@app.on_message(filters.command("restore") & filters.private & is_admin)
async def restore_cmd(c, m):
    """After changing DB_CHANNEL / BACKUP_CHANNEL env: fill every channel with all files."""
    status = await m.reply("♻️ Restoring…")
    ok = bad = 0
    async with lock:
        for code in await list_codes():
            items = await get_items(code)
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


async def main():
    global http
    http = aiohttp.ClientSession()
    await app.start()
    web_app = web.Application()
    web_app.add_routes([web.get("/", lambda r: web.Response(text="ok"))])
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    try:
        await api("POST", "put", params={"code": "__cfg_bot"}, data=app.me.username)
    except Exception as e:
        logging.error("could not save bot username to worker: %s", e)
    logging.info("Bot @%s running", app.me.username)
    await idle()
    await app.stop()
    await http.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
