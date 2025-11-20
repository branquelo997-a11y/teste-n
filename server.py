#!/usr/bin/env python3
import os
import aiohttp
import asyncio
import random
import logging
import urllib.parse
from flask import Flask, jsonify
import threading

logging.basicConfig(level=logging.INFO, format='[ASYNC] %(message)s')
app = Flask(__name__)

# ==============================
# CONFIG
# ==============================

GAME_ID = os.environ.get("GAME_ID", "109983668079237")
BASE_URL = f"https://games.roblox.com/v1/games/{GAME_ID}/servers/Public?limit=100"

MAIN_API_URL = os.environ.get("MAIN_API_URL", "")

MAX_PAGES = int(os.environ.get("MAX_PAGES", "80"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "50"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "5"))
SEND_INTERVAL = int(os.environ.get("SEND_INTERVAL", "25"))

MIN_PLAYERS = int(os.environ.get("MIN_PLAYERS", "0"))
MAX_PLAYERS = int(os.environ.get("MAX_PLAYERS", "9999"))

# ==============================
# PROXIES
# ==============================

def normalize_proxy(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return None

    if raw.startswith("http://") or raw.startswith("https://"):
        return raw

    parts = raw.split(":")
    if len(parts) >= 4:
        host, port, user = parts[0], parts[1], parts[2]
        pwd = ":".join(parts[3:])
        return f"http://{urllib.parse.quote(user)}:{urllib.parse.quote(pwd)}@{host}:{port}"

    return raw

raw_proxies = os.environ.get("PROXIES", "")
PROXIES = [normalize_proxy(p) for p in raw_proxies.split(",") if p.strip()]

if not PROXIES:
    logging.warning("[WARN] Nenhuma proxy configurada — usando conexão direta.")
else:
    logging.info(f"[INIT] {len(PROXIES)} proxies carregadas.")

# ==============================
# ASYNC FETCH
# ==============================

async def fetch_page(session, cursor):
    """Baixa uma única página usando proxy aleatória."""
    proxy = random.choice(PROXIES) if PROXIES else None

    url = BASE_URL + (f"&cursor={cursor}" if cursor else "")

    try:
        async with session.get(url, proxy=proxy, timeout=REQUEST_TIMEOUT) as r:
            if r.status == 429:
                return [], None

            data = await r.json()
            return data.get("data", []), data.get("nextPageCursor")

    except Exception:
        return [], None

# ==============================
# RANDOM ASYNC PAGE SCAN
# ==============================

async def collect_servers_async():
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=None)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        servers = []

        # Página inicial para obter o cursor base
        base_servers, cursor = await fetch_page(session, None)
        servers.extend(base_servers)

        if not cursor:
            return servers

        cursors = [cursor]

        tasks = []

        for _ in range(MAX_PAGES):
            cur = random.choice(cursors)
            tasks.append(fetch_page(session, cur))

            if len(tasks) >= CONCURRENCY:
                results = await asyncio.gather(*tasks)
                tasks.clear()

                for s, next_c in results:
                    servers.extend(s)
                    if next_c:
                        cursors.append(next_c)

        if tasks:
            results = await asyncio.gather(*tasks)
            for s, next_c in results:
                servers.extend(s)

        return servers

# ==============================
# LOOP PRINCIPAL
# ==============================

async def async_loop():
    while True:
        logging.info("🔍 Coletando servidores (async)...")

        servers = await collect_servers_async()

        if not servers:
            logging.info("Nenhum servidor encontrado.")
            await asyncio.sleep(SEND_INTERVAL)
            continue

        job_ids = [
            s["id"]
            for s in servers
            if MIN_PLAYERS <= s.get("playing", 0) <= MAX_PLAYERS
        ]

        logging.info(f"📊 Total bruto: {len(servers)} | Após filtro: {len(job_ids)}")

        if not job_ids:
            await asyncio.sleep(SEND_INTERVAL)
            continue

        # Envia para MAIN
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(MAIN_API_URL, json={"servers": job_ids}) as r:
                    if r.status == 200:
                        logging.info(f"✅ Enviados {len(job_ids)} servidores.")
                    else:
                        logging.warning(f"MAIN retornou {r.status}")
        except Exception as e:
            logging.error(f"Erro ao enviar para MAIN: {e}")

        await asyncio.sleep(SEND_INTERVAL)

# ==============================
# WRAPPER PARA RODAR ASYNC COM FLASK
# ==============================

def start_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_loop())

threading.Thread(target=start_async_loop, daemon=True).start()

# ==============================
# ENDPOINT
# ==============================

@app.route("/")
def home():
    return jsonify({
        "status": "ultra async collector online",
        "proxies": len(PROXIES),
        "game": GAME_ID,
        "concurrency": CONCURRENCY,
        "max_pages": MAX_PAGES,
        "filters": {
            "min_players": MIN_PLAYERS,
            "max_players": MAX_PLAYERS
        }
    })

# ==============================
# RUN
# ==============================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    logging.info(f"ASYNC API rodando na porta {port}")
    app.run(host="0.0.0.0", port=port)
