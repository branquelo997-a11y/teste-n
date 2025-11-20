#!/usr/bin/env python3
import os
import requests
import threading
import time
import logging
import random
import urllib.parse
from flask import Flask, jsonify

logging.basicConfig(level=logging.INFO, format='[MINI] %(message)s')
app = Flask(__name__)

# ==============================
# CONFIG
# ==============================

GAME_ID = os.environ.get("GAME_ID", "109983668079237")
BASE_URL = f"https://games.roblox.com/v1/games/{GAME_ID}/servers/Public?sortOrder=Asc&limit=100"
MAIN_API_URL = os.environ.get("MAIN_API_URL", "https://main-jobid-production.up.railway.app/add-pool")

SEND_INTERVAL = int(os.environ.get("SEND_INTERVAL", "10"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "10"))
SEND_MIN_SERVERS = int(os.environ.get("SEND_MIN_SERVERS", "1"))

# 🔥 FAIXA DE PÁGINAS PARA CADA INSTÂNCIA
SKIP_PAGES = int(os.environ.get("SKIP_PAGES", "0"))
MAX_PAGES_PER_CYCLE = int(os.environ.get("MAX_PAGES_PER_CYCLE", "50"))

# FILTRO DE PLAYERS
MIN_PLAYERS = int(os.environ.get("MIN_PLAYERS", "0"))
MAX_PLAYERS = int(os.environ.get("MAX_PLAYERS", "999"))

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
        host = parts[0]
        port = parts[1]
        user = parts[2]
        pwd = ":".join(parts[3:])
        user_enc = urllib.parse.quote(user, safe="")
        pwd_enc = urllib.parse.quote(pwd, safe="")
        return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    return raw

raw_proxies = os.environ.get("PROXIES", "")
PROXIES = [normalize_proxy(p) for p in raw_proxies.split(",") if p.strip()]

if not PROXIES:
    logging.warning("[WARN] Nenhuma proxy configurada — requisições diretas.")
else:
    logging.info(f"[INIT] {len(PROXIES)} proxies carregadas.")


# ==============================
# FETCH SERVERS (FAIXA CONTROLADA)
# ==============================

def fetch_all_roblox_servers(retries=3):
    all_servers = []
    cursor = None
    page_count = 0
    proxy_index = 0

    skip_done = False

    while True:
        proxy = random.choice(PROXIES) if PROXIES else None
        proxies = {"http": proxy, "https": proxy} if proxy else None

        try:
            url = BASE_URL + (f"&cursor={cursor}" if cursor else "")
            page_count += 1

            # ⛔ AINDA PULANDO AS PRIMEIRAS SKIP_PAGES
            if page_count <= SKIP_PAGES:
                logging.info(f"[SKIP] Página {page_count}/{SKIP_PAGES} via {proxy or 'sem proxy'}")
            else:
                logging.info(f"[FETCH] Página real {page_count - SKIP_PAGES} via {proxy or 'sem proxy'}")

            r = requests.get(url, proxies=proxies, timeout=REQUEST_TIMEOUT)

            if r.status_code == 429:
                logging.warning("[429] Too Many Requests — trocando proxy...")
                time.sleep(1)
                continue

            r.raise_for_status()
            data = r.json()
            cursor = data.get("nextPageCursor")
            servers = data.get("data", [])

            if page_count > SKIP_PAGES:
                all_servers.extend(servers)
                logging.info(f"[ADD] +{len(servers)} servidores (Total: {len(all_servers)})")

            # PARAR QUANDO CHEGAR NA FAIXA DEFINIDA
            if cursor is None:
                logging.info("[END] Última página encontrada. Parou.")
                break

            if page_count - SKIP_PAGES >= MAX_PAGES_PER_CYCLE:
                logging.info("[END] Limite de páginas dessa instância atingido.")
                break

            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            logging.warning(f"[ERRO] Proxy {proxy or 'sem proxy'} falhou: {e}")
            time.sleep(1)
            proxy_index += 1
            if proxy_index >= (len(PROXIES) or 1) * retries:
                break

    return all_servers


# ==============================
# LOOP PRINCIPAL
# ==============================

def fetch_and_send():
    while True:
        servers = fetch_all_roblox_servers()

        if not servers:
            logging.warning("⚠️ Nenhum servidor encontrado.")
            time.sleep(SEND_INTERVAL)
            continue

        job_ids = [
            s["id"]
            for s in servers
            if "id" in s and MIN_PLAYERS <= s.get("playing", 0) <= MAX_PLAYERS
        ]

        logging.info(f"[FILTER] {len(job_ids)} servers após filtro")

        payload = {"servers": job_ids}

        try:
            resp = requests.post(MAIN_API_URL, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                logging.info(f"✅ Enviados {len(job_ids)} IDs")
            else:
                logging.warning(f"MAIN retornou {resp.status_code}: {resp.text}")

        except Exception as e:
            logging.exception(f"❌ Erro ao enviar para MAIN: {e}")

        time.sleep(SEND_INTERVAL)


threading.Thread(target=fetch_and_send, daemon=True).start()


# ==============================
# ENDPOINT
# ==============================

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "mini API running",
        "skip_pages": SKIP_PAGES,
        "max_pages_per_cycle": MAX_PAGES_PER_CYCLE
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    logging.info(f"API rodando na porta {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
