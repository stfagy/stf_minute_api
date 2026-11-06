"""API musicale autonome pour Cloudflare Workers, sans PostgreSQL."""
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from workers import asgi
from music_utils import NOTES_SHARP, get_fretboard_notes, get_note_name

app = FastAPI(title="MUdM Fretboard Worker")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://stfagy.github.io"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/fretboard")
def fretboard(
    root: str = "C",
    pattern: list[int] = Query(...),
    tuning: list[str] = Query(["G", "C", "E", "A"]),
    frets: int = 12,
):
    if root not in NOTES_SHARP or any(note not in NOTES_SHARP for note in tuning):
        raise HTTPException(status_code=400, detail="Note invalide")
    if not pattern or len(pattern) > 12 or any(not 0 <= n <= 11 for n in pattern):
        raise HTTPException(status_code=400, detail="Motif invalide")
    if not 1 <= len(tuning) <= 12 or not 0 <= frets <= 36:
        raise HTTPException(status_code=400, detail="Accordage ou nombre de frettes invalide")
    try:
        board = get_fretboard_notes(root, pattern, tuning, frets)
        idx = NOTES_SHARP.index(root)
        return {"board": board, "scale_notes": [
            get_note_name((idx + n) % 12, root, pattern) for n in pattern
        ]}
    except (ValueError, IndexError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

Default = asgi.entrypoint(app)
