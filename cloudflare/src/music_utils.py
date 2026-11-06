# backend/music_utils.py

NOTES_SHARP = ['C','C#','D','D#','E','F','F#','G','G#','A','A#','B']
NOTES_FLAT  = ['C','Db','D','Eb','E','F','Gb','G','Ab','A','Bb','B']

INTERVALS = {
    0:  ("R",  "#000000"),
    1:  ("b2", "#e7c502"),
    2:  ("M2", "#e7c502"),
    3:  ("b3", "#ff0400"),
    4:  ("M3", "#ff0400"),
    5:  ("P4", "#00CFFF"),
    6:  ("b5", "#43ff0c"),
    7:  ("P5", "#43ff0c"),
    8:  ("b6", "#9a29fd"),
    9:  ("M6", "#9a29fd"),
    10: ("b7", "#268cf8"),
    11: ("M7", "#268cf8"),
}

def note_to_index(note: str) -> int:
    return NOTES_SHARP.index(note)

def prefer_sharps(root: str) -> bool:
    """Retourne True si la tonalité préfère les dièses."""
    return root in ['C','G','D','A','E','B','F#','C#']

def get_note_name(idx: int, root: str, pattern: list[int]) -> str:
    """
    Renvoie le nom enharmonique correct pour la note idx
    dans la gamme (root, pattern).
    """
    # --- Raccourci pour pentatoniques (5 notes) ---
    if len(pattern) == 5:
        return NOTES_SHARP[idx]

    # Sinon, logique diatonique à 7 degrés
    root_idx = note_to_index(root)
    interval = (idx - root_idx) % 12

    try:
        degree   = pattern.index(interval)
        letters  = ['C','D','E','F','G','A','B']
        start    = letters.index(root[0])
        expected = letters[(start + degree) % 7]
    except ValueError:
        expected = None

    sharp = NOTES_SHARP[idx]
    flat  = NOTES_FLAT[idx]

    if expected:
        for cand in (sharp, flat):
            if cand[0] == expected:
                return cand
        # si aucune ne matche, forcer # ou b
        alt = '#' if prefer_sharps(root) else 'b'
        return expected + alt

    # fallback
    return sharp if prefer_sharps(root) else flat


def get_fretboard_notes(root: str, pattern: list[int], tuning: list[str], frets: int):
    root_idx    = note_to_index(root)
    pattern_mod = [(root_idx + i) % 12 for i in pattern]

    board = []
    for open_note in reversed(tuning):
        o_idx = note_to_index(open_note)
        string_notes = []
        for fret in range(frets + 1):
            idx = (o_idx + fret) % 12
            if idx in pattern_mod:
                interval = (idx - root_idx) % 12
                lbl, col = INTERVALS[interval]
                name = get_note_name(idx, root, pattern)
                string_notes.append({
                    "fret":     fret,
                    "note":     name,
                    "interval": lbl,
                    "color":    col,
                    "index":    idx
                })
        board.append(string_notes)
    return board
