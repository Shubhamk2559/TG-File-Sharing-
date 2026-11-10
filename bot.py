import asyncio, io, json, logging, os, re
import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.types import InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup

logging.basicConfig(level=logging.INFO)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
DB_CHANNEL = int(os.environ["DB_CHANNEL"])
ADMINS = {int(x) for x in os.environ.get("ADMINS", "").split() if x}
WORKER_URL = os.environ["WORKER_URL"].rstrip("/")
API_SECRET = os.environ["API_SECRET"]
FORCE_SUB = os.environ.get("FORCE_SUB", "")
FORCE_SUB_LINK = os.environ.get("FORCE_SUB_LINK", "")
AUTO_DELETE = int(os.environ.get("AUTO_DELETE", "0"))
PROTECT = os.environ.get("PROTECT_CONTENT", "0") == "1"
PORT = int(os.environ.get("PORT", "8000"))

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
    return json.loads(await api("GET", "list"))


# ---------- helpers ----------
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
    """Save message under its #code. Same file (unique id) is updated in place."""
    cap = m.caption or ""
    cm, mi = CODE_RE.search(cap), media_of(m)
    if not cm or not mi:
        return None
    code = cm.group(1).lower()
    fid, uid = mi
    entry = {"u": uid, "f": fid, "c": m.chat.id, "m": m.id, "cap": cap}
    async with lock:
        items = await get_items(code)
        for i, it in enumerate(items):
            if it["u"] == uid:
                items[i] = entry
                break
        else:
            items.append(entry)
        await put_items(code, items)
    return code, len(items)


async def send_item(c, chat_id, it):
    try:  # 1) copy from DB channel  2) fallback: send by saved file_id
        return await retry(c.copy_message, chat_id, it["c"], it["m"], protect_content=PROTECT)
    except Exception:
        return await retry(c.send_cached_media, chat_id, it["f"],
                           caption=it["cap"], protect_content=PROTECT)


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
            sent.append(await send_item(c, m.chat.id, it))
        except Exception as e:
            logging.warning("send failed (%s): %s", code, e)
    await wait.delete()
    if not sent:
        return await m.reply("❌ Files unavailable. Admin needs to run /restore.")
    if AUTO_DELETE:
        note = await m.reply(f"⚠️ Files auto-delete in {AUTO_DELETE // 60 or 1} min. Save them now.")
        asyncio.create_task(delete_later(sent + [note]))


# ---------- admin side ----------
@app.on_message(filters.private & media & is_admin)
async def upload(c, m):
    if not CODE_RE.search(m.caption or ""):
        return await m.reply("✏️ Caption mein #code likho, jaise #movie1", quote=True)
    saved = await m.copy(DB_CHANNEL)
    res = await index_message(saved)
    code, n = res
    await m.reply(f"✅ #{code} ({n} file{'s' if n > 1 else ''})\n🔗 {WORKER_URL}/{code}",
                  quote=True, disable_web_page_preview=True)


@app.on_message(filters.chat(DB_CHANNEL) & media)
async def channel_post(c, m):
    # files posted/forwarded straight into the DB channel with #code get indexed too
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
    """After creating a new DB channel (set DB_CHANNEL env, redeploy): re-upload everything."""
    status = await m.reply("♻️ Restoring…")
    ok = bad = 0
    async with lock:
        for code in await list_codes():
            items = await get_items(code)
            for it in items:
                if it["c"] == DB_CHANNEL:
                    continue
                try:
                    s = await retry(c.send_cached_media, DB_CHANNEL, it["f"], caption=it["cap"])
                    it["c"], it["m"] = s.chat.id, s.id
                    ok += 1
                except Exception as e:
                    bad += 1
                    logging.warning("restore failed %s: %s", code, e)
                await asyncio.sleep(1)
            await put_items(code, items)
    await status.edit(f"✅ Restored {ok}, failed {bad}")


async def main():
    global http
    http = aiohttp.ClientSession()
    await app.start()
    web_app = web.Application()
    web_app.add_routes([web.get("/", lambda r: web.Response(text="ok"))])
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info("Bot @%s running", app.me.username)
    await idle()
    await app.stop()
    await http.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
