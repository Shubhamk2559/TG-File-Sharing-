// Cloudflare Worker: permanent links + index storage (KV binding name: FILES)
// Variables needed: BOT_USERNAME (e.g. wtfhappybot, without @), API_SECRET (secret)
const J = { "content-type": "application/json" };

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const path = decodeURIComponent(url.pathname.slice(1));

    if (path.startsWith("api/")) {
      if (req.headers.get("x-secret") !== env.API_SECRET)
        return new Response("forbidden", { status: 403 });
      const code = (url.searchParams.get("code") || "").toLowerCase();
      switch (path) {
        case "api/get":
          return new Response((await env.FILES.get(code)) || "[]", { headers: J });
        case "api/put":
          await env.FILES.put(code, await req.text());
          return new Response("ok");
        case "api/delete":
          await env.FILES.delete(code);
          return new Response("ok");
        case "api/list": {
          const keys = [];
          let cursor;
          do {
            const r = await env.FILES.list({ cursor });
            keys.push(...r.keys.map((k) => k.name));
            cursor = r.list_complete ? undefined : r.cursor;
          } while (cursor);
          return new Response(JSON.stringify(keys), { headers: J });
        }
      }
      return new Response("not found", { status: 404 });
    }

    if (/^[A-Za-z0-9_-]{1,60}$/.test(path))
      return Response.redirect(
        `https://t.me/${env.BOT_USERNAME}?start=${path.toLowerCase()}`, 302);

    return new Response("OK", { status: 200 });
  },
};
