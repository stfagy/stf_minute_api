import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# ── Config ───────────────────────────────────────────────────────────────────
# Sur Render, mets DATABASE_URL en variable d'environnement (Internal Database URL)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL manquant (ajoute-le dans Render > Environment).")

# Pool Postgres (psycopg v3)
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=5,
    kwargs={"autocommit": True, "row_factory": dict_row},
)

app = FastAPI(title="MUdM Videos API", version="1.0")

# Autorise ton site (GitHub Pages) + local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://stfagy.github.io",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ── Schémas de sortie ───────────────────────────────────────────────────────
class VideoOut(BaseModel):
    id: str
    nom: str
    url: str
    thumbnail: Optional[str] = ""
    created_at: Optional[datetime] = None   # ← au lieu de Optional[str]
    difficulties: List[str] = []
    pdfs: List[Dict[str, Any]] = []

class PageOut(BaseModel):
    total: int
    items: List[VideoOut]

# ── SQL helpers ─────────────────────────────────────────────────────────────
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

def _where(q: str, diffs: List[str]) -> (str, list):
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


# ── Endpoints ────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1;")
        cur.fetchone()
    return {"ok": True}

@app.get("/difficulties")
def list_difficulties():
    sql = "SELECT COALESCE(p.difficulte,'') AS d, COUNT(*) AS n FROM pdf p GROUP BY d ORDER BY d;"
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [{"difficulte": r["d"], "count": r["n"]} for r in rows]

@app.get("/videos", response_model=PageOut)
def list_videos(
    q: str = Query("", description="Recherche plein texte sur v.nom"),
    diff: List[str] = Query([], description="Filtre sur une ou plusieurs difficultés"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("desc", pattern="^(asc|desc)$"),
):
    # ICI: ne pas faire diff.strip() (diff est une liste)
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
