"""Singular or plural wording for counts shown to people.

Messages never use "(s)": the count picks the form, and the caller passes the
whole phrase so adjectives and participles agree with it.
"""


def plural(count: int, singular: str, plural_form: str) -> str:
    """``singular`` for exactly one, ``plural_form`` otherwise (0 included)."""
    return singular if count == 1 else plural_form


def counted(count: int, singular: str, plural_form: str) -> str:
    """``counted(2, "imagem nova", "imagens novas")`` → "2 imagens novas"."""
    return f"{count} {plural(count, singular, plural_form)}"
