import re
import unittest

from app.config import BASE_DIR
from app.wording import counted, plural

# "arquivo(s)", "imagem(ns)", "condição(ões)": the count must pick the form.
_GUESSED_PLURAL = re.compile(r"\w\((?:s|es|ns|is|ões)\)")


class WordingTest(unittest.TestCase):
    def test_count_picks_the_form_and_zero_is_plural(self):
        self.assertEqual(counted(1, "imagem nova", "imagens novas"), "1 imagem nova")
        self.assertEqual(counted(2, "imagem nova", "imagens novas"), "2 imagens novas")
        self.assertEqual(counted(0, "arquivo", "arquivos"), "0 arquivos")
        self.assertEqual(plural(1, "selecionada", "selecionadas"), "selecionada")

    def test_no_message_guesses_the_plural(self):
        offenders = [
            f"{path.relative_to(BASE_DIR)}:{number}"
            for path in (BASE_DIR / "app").rglob("*")
            if path.suffix in {".py", ".html", ".js"}
            for number, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            )
            if _GUESSED_PLURAL.search(line)
        ]
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
