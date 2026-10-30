#!/usr/bin/env python3
"""Ingesta de ofertas y noticias en ficheros JSONL mensuales.

Uso:
  python ingest.py ofertas  inbox/ofertas.json     # lista JSON de ofertas extraídas del mail
  python ingest.py noticias inbox/noticias.json    # lista JSON de noticias extraídas del mail
  python ingest.py estado linkedin:4484715007 aplicada --nota "CV v3"

El script hace la parte determinista: limpia URLs, calcula ids, valida contra
schemas/, descarta duplicados (contra todo el histórico y dentro del lote) y
añade al fichero <tipo>/<YYYY-MM>.jsonl según fecha_email.
Solo usa la librería estándar.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
TZ = ZoneInfo("Europe/Madrid")
ESTADOS = {"vista", "interesante", "aplicada", "entrevista", "oferta", "rechazada", "descartada"}
TRACKING_PARAMS = re.compile(r"^(utm_|ref|refId|trackingId|trk|fbclid|gclid|source$|ust$|sa$|usg$)", re.I)

JSON_TYPES = {"string": str, "boolean": bool, "integer": int, "array": list, "object": dict, "null": type(None)}


# ---------- utilidades ----------

def clean_url(url: str) -> str:
    """Quita la redirección de Gmail (google.com/url?q=...) y parámetros de tracking."""
    url = url.strip().strip("<>()[]")
    parts = urlsplit(url)
    if parts.netloc.endswith("google.com") and parts.path == "/url":
        q = dict(parse_qsl(parts.query)).get("q")
        if q:
            url = unquote(q)
            parts = urlsplit(url)
    scheme = "https" if parts.scheme in ("http", "https", "") else parts.scheme
    netloc = parts.netloc.lower().removeprefix("www.") if "linkedin" in parts.netloc else parts.netloc.lower()
    query = urlencode([(k, v) for k, v in parse_qsl(parts.query) if not TRACKING_PARAMS.match(k)])
    path = parts.path
    if "linkedin.com" in netloc:
        netloc = "www.linkedin.com"
        m = re.search(r"/jobs/view/(?:[^/]*?-)?(\d+)", path)
        if m:
            path, query = f"/jobs/view/{m.group(1)}/", ""
    return urlunsplit((scheme, netloc, path, query, ""))


def short_hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:16]


def domain(url: str) -> str:
    return urlsplit(url).netloc.lower().removeprefix("www.")


def now_iso() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


def validate(record: dict, schema: dict, path: str = "") -> list[str]:
    """Validador mínimo: required, tipos y additionalProperties=false."""
    errors = []
    for key in schema.get("required", []):
        if record.get(key) in (None, ""):
            errors.append(f"{path}{key}: obligatorio")
    props = schema.get("properties", {})
    if schema.get("additionalProperties") is False:
        errors += [f"{path}{k}: campo no permitido" for k in record if k not in props]
    for key, value in record.items():
        spec = props.get(key)
        if not spec:
            continue
        types = spec["type"] if isinstance(spec["type"], list) else [spec["type"]]
        ok = any(isinstance(value, JSON_TYPES[t]) and not (t == "integer" and isinstance(value, bool)) for t in types)
        if not ok:
            errors.append(f"{path}{key}: se esperaba {types}, llegó {type(value).__name__}")
        elif isinstance(value, dict) and "properties" in spec:
            errors += validate(value, spec, f"{path}{key}.")
    if errors == [] and "fecha_email" in record and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", record["fecha_email"]):
        errors.append(f"{path}fecha_email: formato YYYY-MM-DD")
    return errors


def existing_ids(folder: Path) -> set[str]:
    ids = set()
    for f in folder.glob("*.jsonl"):
        for line in f.read_text(encoding="utf-8").splitlines():
            if line.strip():
                ids.add(json.loads(line)["id"])
    return ids


# ---------- normalización por tipo ----------

def normalize_oferta(raw: dict, ts: str) -> dict:
    rec = {k: (v.strip() if isinstance(v, str) else v) for k, v in raw.items()}
    rec["url"] = clean_url(rec["url"])
    m = re.search(r"linkedin\.com/jobs/view/(\d+)", rec["url"])
    if m:
        rec.setdefault("fuente", "linkedin")
        rec["job_id"] = m.group(1)
        rec["id"] = f"linkedin:{m.group(1)}"
    else:
        rec.setdefault("fuente", domain(rec["url"]).split(".")[-2] if "." in domain(rec["url"]) else "desconocida")
        rec.setdefault("job_id", None)
        rec["id"] = f"url:{short_hash(rec['url'])}"
    rec.setdefault("solicitud_sencilla", False)
    rec.setdefault("email_id", None)
    rec["fecha_ingesta"] = ts
    return rec


def normalize_noticia(raw: dict, ts: str) -> dict:
    rec = {k: (v.strip() if isinstance(v, str) else v) for k, v in raw.items()}
    rec["url"] = clean_url(rec["url"])
    rec["id"] = short_hash(rec["url"])
    rec["dominio"] = domain(rec["url"])
    fuentes = []
    for u in rec.get("fuentes") or []:
        cu = clean_url(u)
        if cu not in fuentes:
            fuentes.append(cu)
    rec["fuentes"] = fuentes
    post = rec.get("post")
    if post:
        post["hashtags"] = [h.lstrip("#").strip() for h in post.get("hashtags", []) if h.strip("# ")]
    rec.setdefault("post", None)
    rec.setdefault("email_id", None)
    rec["fecha_ingesta"] = ts
    return rec


NORMALIZERS = {"ofertas": normalize_oferta, "noticias": normalize_noticia}
SCHEMAS = {"ofertas": "oferta.schema.json", "noticias": "noticia.schema.json"}


# ---------- comandos ----------

def cmd_ingest(tipo: str, input_path: Path) -> int:
    schema = json.loads((ROOT / "schemas" / SCHEMAS[tipo]).read_text(encoding="utf-8"))
    folder = ROOT / tipo
    folder.mkdir(exist_ok=True)
    raws = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(raws, list):
        print("El fichero de entrada debe ser una lista JSON", file=sys.stderr)
        return 2

    seen = existing_ids(folder)
    ts = now_iso()
    nuevos, duplicados, invalidos = {}, 0, []
    for i, raw in enumerate(raws):
        try:
            rec = NORMALIZERS[tipo](raw, ts)
        except (KeyError, AttributeError) as e:
            invalidos.append(f"#{i}: falta {e}")
            continue
        errs = validate(rec, schema)
        if errs:
            invalidos.append(f"#{i} ({rec.get('url', '?')}): " + "; ".join(errs))
            continue
        if rec["id"] in seen:
            duplicados += 1
            continue
        seen.add(rec["id"])
        nuevos.setdefault(rec["fecha_email"][:7], []).append(rec)

    for month, recs in sorted(nuevos.items()):
        with (folder / f"{month}.jsonl").open("a", encoding="utf-8") as f:
            for rec in recs:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    total = sum(len(v) for v in nuevos.values())
    print(f"{tipo}: {total} nuevas, {duplicados} duplicadas, {len(invalidos)} inválidas")
    for msg in invalidos:
        print(f"  INVÁLIDA {msg}", file=sys.stderr)
    return 1 if invalidos else 0


def cmd_estado(oferta_id: str, estado: str, nota: str | None) -> int:
    if estado not in ESTADOS:
        print(f"Estado no válido. Usa uno de: {', '.join(sorted(ESTADOS))}", file=sys.stderr)
        return 2
    if oferta_id not in existing_ids(ROOT / "ofertas"):
        print(f"No existe ninguna oferta con id {oferta_id}", file=sys.stderr)
        return 2
    folder = ROOT / "seguimiento"
    folder.mkdir(exist_ok=True)
    evento = {"id": oferta_id, "estado": estado, "fecha": now_iso(), "nota": nota}
    with (folder / "candidaturas.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(evento, ensure_ascii=False) + "\n")
    print(f"{oferta_id} → {estado}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for tipo in NORMALIZERS:
        s = sub.add_parser(tipo)
        s.add_argument("input", type=Path)
    s = sub.add_parser("estado")
    s.add_argument("id")
    s.add_argument("estado")
    s.add_argument("--nota")
    a = p.parse_args()
    if a.cmd == "estado":
        return cmd_estado(a.id, a.estado, a.nota)
    return cmd_ingest(a.cmd, a.input)


if __name__ == "__main__":
    sys.exit(main())
