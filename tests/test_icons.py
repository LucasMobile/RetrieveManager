import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "app" / "templates"


class IconSystemTest(unittest.TestCase):
    def test_every_template_icon_has_a_vector_definition(self):
        icon_source = (TEMPLATES / "icons.html").read_text(encoding="utf-8")
        defined = set(re.findall(r'name == "([^"]+)"', icon_source))
        used: set[str] = set()
        for template in TEMPLATES.glob("*.html"):
            if template.name == "icons.html":
                continue
            source = template.read_text(encoding="utf-8")
            used.update(re.findall(r'icon\("([^"]+)"', source))

        self.assertFalse(used - defined, f"Ícones sem definição: {used - defined}")

    def test_icons_use_the_crisp_vector_contract(self):
        icon_source = (TEMPLATES / "icons.html").read_text(encoding="utf-8")
        css_source = (ROOT / "app" / "static" / "app.css").read_text(encoding="utf-8")

        self.assertIn('width="24" height="24" viewBox="0 0 24 24"', icon_source)
        self.assertIn('stroke-width="1.75"', icon_source)
        self.assertIn('shape-rendering="geometricPrecision"', icon_source)
        self.assertNotIn("vector-effect: non-scaling-stroke", css_source)


if __name__ == "__main__":
    unittest.main()
