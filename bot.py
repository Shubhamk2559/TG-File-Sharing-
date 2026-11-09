import asyncio, base64, logging, os
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.types import InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup

logging.basicConfig(level=logging.INFO)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
DB_CHANNEL = int(os.environ["DB_CHANNEL"])            # private channel, bot = admin
ADMINS = {int(x) for x in os.environ.get("ADMINS", "").split() if x}
FORCE_SUB = os.environ.get("FORCE_SUB", "")           # channel id or @username (optional)
FORCE_SUB_LINK = os.environ.get("FORCE_SUB_LINK", "")  # join link for button
AUTO_DELETE = int(os.environ.get("AUTO_DELETE", "0"))  # seconds, 0 = off
PROTECT = os.environ.get("PROTECT_CONTENT", "0") == "1"
SALT = int(os.environ.get("SALT", "7919"))
PORT = int(os.environ.get("PORT", "8000"))

app = Client("filebot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, workers=16)
batches: dict[int, list[int]] = {}


def enc(a: int, b: int) -> str:
    return base64.urlsafe_b64encode(f"{a * SALT}-{b * SALT}".encode()).decode().rstrip("=")


def dec(s: str):
    try:
        raw = base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()
        a, b = (int(x) // SALT for x in raw.split("-"))
        return (a, b) if 0 < a <= b and b - a < 200 else None
    except Exception:
        return None


is_admin = filters.create(lambda _, __, m: m.from_user and m.from_user.id in ADMINS)
media = (filters.document | filters.video | filters.audio | filters.photo
         | filters.animation | filters.voice | filters.video_note)


async def joined(c, uid: int) -> bool:
    if not FORCE_SUB:
        return True
    try:
        chat = int(FORCE_SUB) if FORCE_SUB.lstrip("-").isdigit() else FORCE_SUB
        mem = await c.get_chat_member(chat, uid)
        return mem.status not in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT)
    except UserNotParticipant:
        return False
    except Exception:
        return True  # don't block users if the check itself fails


async def delete_later(msgs):
    await asyncio.sleep(AUTO_DELETE)
    for m in msgs:
        try:
            await m.delete()
        except Exception:
            pass


@app.on_message(filters.command("start") & filters.private)
async def start(c, m):
    if len(m.command) < 2:
        return await m.reply("👋 Send me a file link to get your files.")
    ids = dec(m.command[1])
    if not ids:
        return await m.reply("❌ Invalid or expired link.")
    if not await joined(c, m.from_user.id):
        retry = f"https://t.me/{c.me.username}?start={m.command[1]}"
        return await m.reply("🔒 Join the channel first, then tap Try again.",
                             reply_markup=Markup([[Btn("Join channel", url=FORCE_SUB_LINK or f"https://t.me/{FORCE_SUB.lstrip('@')}")],
                                                  [Btn("Try again", url=retry)]]))
    wait = await m.reply("⏳ Sending…")
    msgs = await c.get_messages(DB_CHANNEL, list(range(ids[0], ids[1] + 1)))
    sent = []
    for src in msgs:
        if src.empty:
            continue
        while True:
            try:
                sent.append(await src.copy(m.chat.id, protect_content=PROTECT))
                break
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
            except Exception as e:
                logging.warning("copy failed: %s", e)
                break
    await wait.delete()
    if not sent:
        return await m.reply("❌ File not found.")
    if AUTO_DELETE:
        note = await m.reply(f"⚠️ Files auto-delete in {AUTO_DELETE // 60 or 1} min. Save them now.")
        asyncio.create_task(delete_later(sent + [note]))


@app.on_message(filters.command("batch") & filters.private & is_admin)
async def batch_start(c, m):
    batches[m.from_user.id] = []
    await m.reply("📦 Batch started. Send files, then /done (or /cancel).")


@app.on_message(filters.command("cancel") & filters.private & is_admin)
async def batch_cancel(c, m):
    batches.pop(m.from_user.id, None)
    await m.reply("Cancelled.")


@app.on_message(filters.command("done") & filters.private & is_admin)
async def batch_done(c, m):
    ids = batches.pop(m.from_user.id, [])
    if not ids:
        return await m.reply("Batch is empty.")
    await m.reply(f"✅ {len(ids)} files\nhttps://t.me/{c.me.username}?start={enc(min(ids), max(ids))}",
                  disable_web_page_preview=True)


@app.on_message(filters.private & media & is_admin)
async def upload(c, m):
    saved = await m.copy(DB_CHANNEL)
    if m.from_user.id in batches:
        batches[m.from_user.id].append(saved.id)
        return await m.reply(f"➕ Added ({len(batches[m.from_user.id])})", quote=True)
    await m.reply(f"🔗 https://t.me/{c.me.username}?start={enc(saved.id, saved.id)}",
                  quote=True, disable_web_page_preview=True)


async def main():
    await app.start()
    web_app = web.Application()
    web_app.add_routes([web.get("/", lambda r: web.Response(text="ok"))])
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info("Bot @%s running", app.me.username)
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
