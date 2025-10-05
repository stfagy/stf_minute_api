# main.py — API fusionnée (Vidéos + Fretboard)

import os
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# ==== Imports "fretboard" (depuis ton code existant) ====
from music_utils import (   # ← garde music_utils.py à côté de main.py
    get_fretboard_notes, get_note_name, NOTES_SHARP
)

# ── Config DB (reprend ta logique Render) ────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL manquant (ajoute-le dans Render > Environment).")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=5,
    kwargs={"autocommit": True, "row_factory": dict_row},
)

# ── App & CORS ──────────────────────────────────────────────────────────────
app = FastAPI(title="MUdM API (fusion)", version="1.0")

ALLOWED_ORIGINS = [
    "https://stfagy.github.io",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,   # mets True si tu utilises des cookies
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Schémas de sortie (partie vidéos) ───────────────────────────────────────
class VideoOut(BaseModel):
    id: str
    nom: str
    url: str
    thumbnail: Optional[str] = ""
    created_at: Optional[datetime] = None
    difficulties: List[str] = []
    pdfs: List[Dict[str, Any]] = []

class PageOut(BaseModel):
    total: int
    items: List[VideoOut]

# ── SQL helpers (partie vidéos) ─────────────────────────────────────────────
BASE_SELECT = """
SELECT
  v.id, v.nom, v.url, v.thumbnail, v.created_at,
  COALESCE(array_agg(DISTINCT p.difficulte) FILTER (WHERE p.id IS NOT NULL), '{}') AS difficulties,
  COALESCE(
    json_agg(
      DISTINCT jsonb_build_object(
        'nom', p.nom,
        'path', p.path,
        'gpx', p.gpx,
        'couleur_uniforme', p.couleur_uniforme,
        'difficulte', p.difficulte
      )
    ) FILTER (WHERE p.id IS NOT NULL),
    '[]'
  ) AS pdfs
FROM video v
LEFT JOIN pdf_video pv ON v.id = pv.id_video
LEFT JOIN pdf p ON pv.id_pdf = p.id
"""

def _where(q: str, diffs: List[str]) -> Tuple[str, list]:
    clauses, params = [], []
    if q:
        clauses.append("v.nom ILIKE %s")
        params.append(f"%{q}%")
    if diffs:
        placeholders = ",".join(["%s"] * len(diffs))
        clauses.append(f"""
            EXISTS (
              SELECT 1
              FROM pdf_video pv2
              JOIN pdf p2 ON pv2.id_pdf = p2.id
              WHERE pv2.id_video = v.id AND p2.difficulte IN ({placeholders})
            )
        """)
        params.extend(diffs)
    return ("WHERE " + " AND ".join(clauses), params) if clauses else ("", params)

# ── Endpoints communs ───────────────────────────────────────────────────────
@app.get("/health", tags=["core"])
def health():
    # Ping DB pour healthcheck robuste
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1;")
        cur.fetchone()
    return {"ok": True}

# ── Endpoints Vidéos ────────────────────────────────────────────────────────
@app.get("/difficulties", tags=["videos"])
def list_difficulties():
    sql = "SELECT COALESCE(p.difficulte,'') AS d, COUNT(*) AS n FROM pdf p GROUP BY d ORDER BY d;"
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [{"difficulte": r["d"], "count": r["n"]} for r in rows]

@app.get("/videos", response_model=PageOut, tags=["videos"])
def list_videos(
    q: str = Query("", description="Recherche plein texte sur v.nom"),
    diff: List[str] = Query([], description="Filtre sur une ou plusieurs difficultés"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("desc", pattern="^(asc|desc)$"),
):
    where_sql, params = _where(q.strip(), diff)
    order_sql = "DESC" if order == "desc" else "ASC"

    total_sql = "SELECT COUNT(*) FROM video v " + where_sql
    page_sql = (
        BASE_SELECT +
        where_sql +
        f" GROUP BY v.id ORDER BY v.created_at {order_sql} NULLS LAST LIMIT %s OFFSET %s"
    )

    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(total_sql, params)
        total = cur.fetchone()["count"]
        cur.execute(page_sql, [*params, limit, offset])
        rows = cur.fetchall()

    items = [VideoOut(**r) for r in rows]
    return PageOut(total=total, items=items)

# ── Endpoint Fretboard (depuis ton second backend) ──────────────────────────
@app.get("/fretboard", tags=["fretboard"])
def fretboard(
    root: str = "C",
    pattern: List[int] = Query(..., description="Ex: gamme: 0,2,4,5,7,9,11"),
    tuning: List[str] = Query(["G","C","E","A"], description="Ex: ukulélé standard"),
    frets: int = 12
):
    try:
        # validation simple des entrées
        if root not in NOTES_SHARP:
            raise ValueError("Note racine invalide")
        patt = [int(p) for p in pattern]
        board = get_fretboard_notes(root, patt, tuning, frets)

        root_idx = NOTES_SHARP.index(root)
        scale_notes = [get_note_name((root_idx + p) % 12, root, patt) for p in patt]

        return {"board": board, "scale_notes": scale_notes}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
