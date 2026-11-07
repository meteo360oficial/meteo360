glm_pipeline.py raios/glm_pipeline.py · 244 linhas
#!/usr/bin/env python3
"""
Pipeline de relâmpagos GLM (GOES-East, NOAA) -> data/raios.json
 
Roda no GitHub Actions a cada 5 minutos. Baixa os arquivos GLM L2 LCFA
(flashes de relâmpago) dos últimos WINDOW_MIN minutos no bucket público
da NOAA (S3, acesso anônimo), filtra para uma caixa que cobre o Brasil
e os vizinhos, e grava um JSON pequeno que o site consome.
 
Dependências: python 3.9+, numpy, netCDF4 (pip install numpy netCDF4).
Não usa boto3: a listagem do bucket público é feita por HTTP.
 
Variáveis: a caixa e o bucket podem ser trocados por variáveis de ambiente.
Campos do NetCDF usados (confirmar na primeira execução real, ver README):
  flash_lat, flash_lon, flash_energy, flash_quality_flag,
  flash_time_offset_of_first_event (opcional)
"""
 
import json
import os
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
 
# ---------- configuração ----------
BUCKET = os.environ.get("GLM_BUCKET", "noaa-goes19")   # GOES-19 = GOES-East (desde 2025)
PRODUCT = "GLM-L2-LCFA"
WINDOW_MIN = int(os.environ.get("GLM_WINDOW_MIN", "15"))
# caixa: sul, oeste, norte, leste (cobre Brasil + Argentina/Uruguai/Paraguai/Bolívia)
BBOX = {"south": -36.0, "west": -78.0, "north": 9.0, "east": -30.0}
MAX_FLASHES = 150000
HIST_MIN = int(os.environ.get("GLM_HIST_MIN", "60"))  # histórico mantido no JSON (para voltar no tempo)
OUT_PATH = os.environ.get("RAIOS_OUT", "data/raios.json")
UA = "meteo360-raios-pipeline/1.0"
 
FNAME_RE = re.compile(r"_s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)")
 
 
# ---------- lógica pura (testável sem rede) ----------
 
def parse_start_time(filename):
    """Extrai o início da varredura do nome do arquivo GLM (UTC, datetime aware)."""
    m = FNAME_RE.search(filename)
    if not m:
        return None
    year, doy, hh, mm, ss, tenth = (int(x) for x in m.groups())
    base = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy - 1)
    return base.replace(hour=hh, minute=mm, second=ss) + timedelta(milliseconds=tenth * 100)
 
 
def hours_in_window(now, window_min):
    """Lista as horas UTC (datetime) que cobrem a janela [now-window, now]."""
    start = now - timedelta(minutes=window_min)
    cur = start.replace(minute=0, second=0, microsecond=0)
    out = []
    while cur <= now:
        out.append(cur)
        cur += timedelta(hours=1)
    return out
 
 
def keep_file_times(names_with_times, window_start, window_end):
    """Mantém só os arquivos cujo início cai na janela (com 1 varredura de folga)."""
    slack = timedelta(minutes=1)
    return [
        (n, t) for n, t in names_with_times
        if t is not None and window_start - slack <= t <= window_end
    ]
 
 
def filter_flashes(lat, lon, energy, qflag, bbox):
    """Máscara booleana: dentro da caixa e com qualidade boa (flag 0, se existir)."""
    import numpy as np
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    m = (
        (lat >= bbox["south"]) & (lat <= bbox["north"])
        & (lon >= bbox["west"]) & (lon <= bbox["east"])
        & np.isfinite(lat) & np.isfinite(lon)
    )
    if qflag is not None:
        m &= np.asarray(qflag) == 0
    return m
 
 
def merge_history(old_flashes, new_flashes, now, hist_min):
    """Junta o histórico antigo com os novos, tira duplicados e descarta o que passou de hist_min."""
    cutoff = now.timestamp() - hist_min * 60
    seen, out = set(), []
    for f in list(old_flashes) + list(new_flashes):
        if f[2] < cutoff:
            continue
        key = (round(f[0], 3), round(f[1], 3), int(f[2]))
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    return out
 
 
def load_previous(path):
    """Lê o JSON anterior (do checkout do repositório), se existir e for válido."""
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh).get("flashes", [])
    except Exception:
        return []
 
 
def build_payload(flashes, now, window_min, bbox, files_used, errors):
    """Monta o JSON final. flashes = lista de [lat, lon, t_epoch_s, energia]."""
    flashes = sorted(flashes, key=lambda f: f[2])
    truncated = len(flashes) > MAX_FLASHES
    if truncated:
        flashes = flashes[-MAX_FLASHES:]  # mantém os mais recentes
    return {
        "generated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": f"NOAA GOES-East GLM L2 LCFA ({BUCKET})",
        "window_min": window_min,
        "bbox": bbox,
        "count": len(flashes),
        "truncated": truncated,
        "files": files_used,
        "errors": errors[:5],
        "fields": ["lat", "lon", "t", "energy"],
        "flashes": [
            [round(f[0], 3), round(f[1], 3), int(f[2]), round(float(f[3]), 0)]
            for f in flashes
        ],
    }
 
 
# ---------- rede e leitura ----------
 
def http_get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()
 
 
def list_prefix(prefix):
    """Lista chaves de um prefixo no bucket público (S3 ListObjectsV2, paginado)."""
    keys, token = [], None
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    while True:
        q = f"?list-type=2&prefix={prefix}"
        if token:
            q += f"&continuation-token={urllib.request.quote(token)}"
        xml = http_get(f"https://{BUCKET}.s3.amazonaws.com/{q}")
        root = ET.fromstring(xml)
        for c in root.iter(f"{ns}Contents"):
            keys.append(c.find(f"{ns}Key").text)
        trunc = root.find(f"{ns}IsTruncated")
        if trunc is None or trunc.text != "true":
            break
        token = root.find(f"{ns}NextContinuationToken").text
    return keys
 
 
def find_files(now, window_min):
    start = now - timedelta(minutes=window_min)
    cands = []
    for h in hours_in_window(now, window_min):
        prefix = f"{PRODUCT}/{h:%Y}/{h:%j}/{h:%H}/"
        for key in list_prefix(prefix):
            name = key.rsplit("/", 1)[-1]
            cands.append((key, parse_start_time(name)))
    keep = keep_file_times(cands, start, now)
    return sorted(keep, key=lambda kt: kt[1])
 
 
def read_granule(path):
    """Lê um arquivo LCFA. Retorna (lat, lon, t_epoch, energy, qflag)."""
    import netCDF4
    import numpy as np
    with netCDF4.Dataset(path) as ds:
        lat = ds.variables["flash_lat"][:]
        lon = ds.variables["flash_lon"][:]
        energy = ds.variables["flash_energy"][:] if "flash_energy" in ds.variables else np.zeros(len(lat))
        qflag = ds.variables["flash_quality_flag"][:] if "flash_quality_flag" in ds.variables else None
        base = None
        # tempo: início da varredura (nome do arquivo) + offset do primeiro evento, se existir
        if "flash_time_offset_of_first_event" in ds.variables:
            off = np.asarray(ds.variables["flash_time_offset_of_first_event"][:], dtype=float)
        else:
            off = np.zeros(len(lat))
        return lat, lon, energy, qflag, off
 
 
def main():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(minutes=WINDOW_MIN)
    errors, used, flashes = [], [], []
 
    try:
        files = find_files(now, WINDOW_MIN)
    except Exception as e:  # rede caiu: mantém o último JSON bom
        print(f"ERRO listando bucket: {e}", file=sys.stderr)
        return 1
 
    if not files:
        errors.append("nenhum arquivo GLM na janela")
 
    for key, t0 in files:
        local = os.path.join("/tmp", key.rsplit("/", 1)[-1])
        try:
            with open(local, "wb") as fh:
                fh.write(http_get(f"https://{BUCKET}.s3.amazonaws.com/{key}", timeout=120))
            lat, lon, energy, qflag, off = read_granule(local)
            import numpy as np
            m = filter_flashes(lat, lon, energy, qflag, BBOX)
            base_s = t0.timestamp()
            t = base_s + np.asarray(off)[m]
            for la, lo, t_s, en in zip(np.asarray(lat)[m], np.asarray(lon)[m], t, np.asarray(energy)[m]):
                flashes.append([float(la), float(lo), float(t_s), float(en)])
            used.append(key.rsplit("/", 1)[-1])
        except Exception as e:
            errors.append(f"{key.rsplit('/', 1)[-1]}: {e}")
            print(f"ERRO em {key}: {e}", file=sys.stderr)
        finally:
            if os.path.exists(local):
                os.remove(local)
 
    # se nada foi lido por erro (não por falta de raios), não sobrescreve o último bom
    if files and not used:
        print("Nenhum arquivo lido; mantendo o JSON anterior.", file=sys.stderr)
        return 1
 
    previous = load_previous(OUT_PATH)
    # o JSON anterior vem como [lat, lon, t, energia]; o novo também
    merged = merge_history(previous, flashes, now, HIST_MIN)
    payload = build_payload(merged, now, HIST_MIN, BBOX, used, errors)
    os.makedirs(os.path.dirname(OUT_PATH) or ".", exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"OK: {payload['count']} raios em {len(used)} arquivo(s) -> {OUT_PATH}")
    return 0
 
 
if __name__ == "__main__":
    sys.exit(main())
 
