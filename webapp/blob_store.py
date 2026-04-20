"""
Vercel Blob storage – slanke client zonder extra dependencies.
Wordt gebruikt om KB-bestanden persistent op te slaan naast het
alleen-lezen Vercel-bestandssysteem.

Vereist de omgevingsvariabele BLOB_READ_WRITE_TOKEN (wordt automatisch
ingesteld als je een Blob-store aan het Vercel-project koppelt).
Zonder het token valt alles stil terug op lokale bestanden.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

# Vercel Blob REST API base URL
_BASE = "https://blob.vercel-storage.com"
# Prefix voor alle KB-bestanden in de Blob-store
KB_PREFIX = "kb/"


# ── helpers ─────────────────────────────────────────────────────────────────

def _token() -> str:
    return os.environ.get("BLOB_READ_WRITE_TOKEN", "")


def available() -> bool:
    """True als de Blob-integratie geconfigureerd is."""
    return bool(_token())


def _request(method: str, url: str, data: bytes | None = None,
             headers: dict | None = None) -> dict | None:
    extra = headers or {}
    extra.setdefault("Authorization", f"Bearer {_token()}")
    req = urllib.request.Request(url, data=data, method=method, headers=extra)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except (urllib.error.URLError, json.JSONDecodeError, OSError):
        return None


# ── upload ───────────────────────────────────────────────────────────────────

def upload(rel_path: str, content: str) -> str | None:
    """
    Sla een KB-bestand op in Vercel Blob.

    rel_path  – relatief pad binnen de KB, bijv. '01_Wetgeving/eu/reach.md'
    Geeft de publieke download-URL terug, of None bij een fout.
    """
    if not available():
        return None
    pathname = KB_PREFIX + rel_path.replace("\\", "/")
    url = f"{_BASE}/{pathname}"
    result = _request(
        "PUT",
        url,
        data=content.encode("utf-8"),
        headers={
            "Authorization": f"Bearer {_token()}",
            "Content-Type": "text/markdown; charset=utf-8",
            "x-content-type": "text/markdown",
        },
    )
    return (result or {}).get("url")


# ── list ─────────────────────────────────────────────────────────────────────

def list_files() -> list[dict]:
    """
    Geeft alle opgeslagen KB-bestanden terug als lijst van dicts met
    ten minste de sleutels 'pathname' en 'url'.
    """
    if not available():
        return []
    url = f"{_BASE}/?prefix={KB_PREFIX}&limit=1000"
    result = _request("GET", url)
    return (result or {}).get("blobs", [])


# ── sync ─────────────────────────────────────────────────────────────────────

def sync_to_dir(target_dir: Path) -> int:
    """
    Download alle KB-bestanden uit Blob naar target_dir.
    Bestaande lokale bestanden worden overschreven.
    Geeft het aantal gesynchroniseerde bestanden terug.
    """
    blobs = list_files()
    count = 0
    for blob in blobs:
        pathname: str = blob.get("pathname", "")
        download_url: str = blob.get("url", "")
        if not pathname.startswith(KB_PREFIX) or not download_url:
            continue
        # Relatief pad binnen de KB (strip het 'kb/' prefix)
        rel = pathname[len(KB_PREFIX):]
        dest = target_dir / rel
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            req = urllib.request.Request(download_url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                dest.write_bytes(resp.read())
            count += 1
        except (urllib.error.URLError, OSError):
            pass
    return count
