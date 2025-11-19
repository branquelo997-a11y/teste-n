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

SEND_INTERVAL = int(os.environ.get("SEND_INTERVAL", "30"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "10"))
SEND_MIN_SERVERS = int(os.environ.get("SEND_MIN_SERVERS", "1"))
MAX_PAGES_PER_CYCLE = int(os.environ.get("MAX_PAGES_PER_CYCLE", "10"))

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
        host, port, user = parts[0], parts[1], parts[2]
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
# CURSOR RANDOM SYSTEM
# ==============================

CURSOR_CACHE = set()
CURSOR_CACHE_MAX = 5000  # limite para evitar explosão de memória

def fetch_page(cursor=None):
    """Busca UMA página. Se cursor=None, pega a primeira."""
    proxy = random.choice(PROXIES) if PROXIES else None
    proxies = {"http": proxy, "https": proxy} if proxy else None

    try:
        url = BASE_URL + (f"&cursor={cursor}" if cursor else "")
        r = requests.get(url, proxies=proxies, timeout=REQUEST_TIMEOUT)

        if r.status_code == 429:
            logging.warning("[429] Too Many Requests, trocando proxy...")
            return None, None

        r.raise_for_status()
        data = r.json()

        servers = data.get("data", [])
        next_cursor = data.get("nextPageCursor")

        # adicionar cursor no cache
        if next_cursor and len(CURSOR_CACHE) < CURSOR_CACHE_MAX:
            CURSOR_CACHE.add(next_cursor)

        return servers, next_cursor

    except Exception as e:
        logging.warning(f"[ERRO] Falha ao buscar página cursor={cursor}: {e}")
        return None, None


def fetch_all_roblox_servers():
    """Mistura busca sequencial e busca aleatória."""
    all_servers = []
    page_count = 0

    # PRIMEIRA VEZ: SEM CURSORES = MODO SEQUENCIAL
    if not CURSOR_CACHE:
        logging.info("[INIT] Cache vazio — carregando páginas sequenciais...")
        cursor = None

        while page_count < MAX_PAGES_PER_CYCLE:
            servers, cursor = fetch_page(cursor)
            if not servers:
                break

            all_servers.extend(servers)
            page_count += 1

            logging.info(f"[SEQ] Página {page_count} carregada (+{len(servers)} servers | total={len(all_servers)})")

            if not cursor:
                break

            time.sleep(0.3)

        return all_servers

    # CACHE CHEIO → MODO ALEATÓRIO
    logging.info("[RANDOM] Selecionando páginas aleatórias...")

    random_cursors = random.sample(list(CURSOR_CACHE), min(MAX_PAGES_PER_CYCLE, len(CURSOR_CACHE)))

    for cur in random_cursors:
        servers, _ = fetch_page(cur)
        if servers:
            all_servers.extend(servers)
        time.sleep(0.2)

    logging.info(f"[RANDOM] Total coletado: {len(all_servers)} servers")
    return all_servers

# ==============================
# LOOP PRINCIPAL
# ==============================

def fetch_and_send():
    while True:
        servers = fetch_all_roblox_servers()

        if not servers:
            logging.warning("⚠️ Nenhum servidor coletado. Aguardando...")
            time.sleep(SEND_INTERVAL)
            continue

        # FILTRO DE PLAYERS
        job_ids = [
            s["id"]
            for s in servers
            if "id" in s and MIN_PLAYERS <= s.get("playing", 0) <= MAX_PLAYERS
        ]

        logging.info(f"[FILTER] {len(job_ids)} servers válidos após filtro ({MIN_PLAYERS}-{MAX_PLAYERS} players)")

        if len(job_ids) < SEND_MIN_SERVERS:
            logging.info(f"[SKIP] Apenas {len(job_ids)} servers válidos (mínimo: {SEND_MIN_SERVERS})")
            time.sleep(SEND_INTERVAL)
            continue

        payload = {"servers": job_ids}

        # ENVIO PARA A MAIN API
        try:
            resp = requests.post(MAIN_API_URL, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                added = resp.json().get("added")
                logging.info(f"✅ Enviados {len(job_ids)} jobIds → adicionados: {added}")
            else:
                logging.warning(f"⚠️ Erro MAIN_API: {resp.status_code} → {resp.text}")

        except Exception as e:
            logging.exception(f"❌ Erro ao enviar para MAIN: {e}")

        time.sleep(SEND_INTERVAL)

# INICIA LOOP EM THREAD
threading.Thread(target=fetch_and_send, daemon=True).start()

# ==============================
# ENDPOINT
# ==============================

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "Random JobID Harvester Running",
        "proxy_count": len(PROXIES),
        "cached_cursors": len(CURSOR_CACHE),
        "game_id": GAME_ID,
        "target_api": MAIN_API_URL,
        "min_players": MIN_PLAYERS,
        "max_players": MAX_PLAYERS,
        "pages_per_cycle": MAX_PAGES_PER_CYCLE
    })

# ==============================
# RUN
# ==============================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    logging.info(f"Mini API rodando na porta {port} | MIN={MIN_PLAYERS} MAX={MAX_PLAYERS}")
    app.run(host="0.0.0.0", port=port, debug=False)
