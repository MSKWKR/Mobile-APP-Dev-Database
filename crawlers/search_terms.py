import itertools
import string

# ──────────────────────────────────────────────
# Character sets
# ──────────────────────────────────────────────

LATIN    = string.ascii_lowercase + string.digits
CYRILLIC = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
HIRAGANA = "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん"
KOREAN   = "가나다라마바사아자차카타파하"

# Map country codes to extra character sets to include in their sweep.
# Latin + digits are always included.
COUNTRY_CHARSETS: dict[str, str] = {
    "jp": HIRAGANA,
    "cn": HIRAGANA,   # swap for CJK unified if needed
    "ru": CYRILLIC,
    "ua": CYRILLIC,
    "kr": KOREAN,
}

# ──────────────────────────────────────────────
# Curated verticals (Tier 3)
# ──────────────────────────────────────────────

VERTICALS: list[str] = [
    "photo editor", "video editor", "music player", "podcast",
    "vpn", "password manager", "meditation", "workout", "recipe",
    "budget", "invoice", "flashcard", "bible", "quran",
    "manga", "comic", "radio", "scanner", "translator",
    "parental control", "baby", "pregnancy", "pet",
    "real estate", "stocks", "crypto", "dating", "chat",
    "video call", "whiteboard",
]

# ──────────────────────────────────────────────
# Generator
# ──────────────────────────────────────────────

def generate_search_terms(country: str = "") -> list[str]:
    """
    Returns search terms in priority order for a given country:
      1. Single chars  — broadest buckets
      2. Bigrams       — fills gaps where top-200 saturates
      3. Verticals     — semantic boost for known app clusters

    Pass a country code (e.g. "jp", "ru") to include the appropriate
    non-Latin character set in the single/bigram sweep.
    """
    chars = LATIN + COUNTRY_CHARSETS.get(country.lower(), "")

    terms: list[str] = []
    terms.extend(chars)
    terms.extend("".join(p) for p in itertools.product(chars, repeat=2))
    terms.extend(VERTICALS)

    return terms