"""Tests de non-régression du moteur musical utilisé par Cloudflare."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from music_utils import NOTES_SHARP, get_fretboard_notes, get_note_name

class FretboardTests(unittest.TestCase):
    def test_accord_majeur_guitare(self):
        pattern = [0, 4, 7]
        board = get_fretboard_notes("C", pattern, ["E","A","D","G","B","E"], 12)
        self.assertEqual(len(board), 6)
        self.assertTrue(all(0 <= n["fret"] <= 12 for string in board for n in string))
        self.assertTrue(all(n["interval"] in {"R", "M3", "P5"} for string in board for n in string))

    def test_gamme_ukulele(self):
        pattern = [0,2,4,5,7,9,11]
        board = get_fretboard_notes("C", pattern, ["G","C","E","A"], 15)
        self.assertEqual(len(board), 4)
        self.assertEqual([get_note_name((NOTES_SHARP.index("C")+n)%12,"C",pattern) for n in pattern],
                         ["C","D","E","F","G","A","B"])

    def test_accordage_et_notes(self):
        pattern = [0,3,7]
        board = get_fretboard_notes("A", pattern, ["E","A","D","G"], 20)
        self.assertEqual(len(board), 4)
        self.assertGreater(sum(len(s) for s in board), 10)

if __name__ == "__main__":
    unittest.main()
