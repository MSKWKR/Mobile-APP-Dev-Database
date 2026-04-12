import itertools
import json
import string

with open("listings/languages.json", "r") as f:
    ALL_LANGS: list[str] = json.load(f)

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

# Map country codes to their primary language code (ISO 639-1).
# Used by crawlers that support lang= in search (e.g. Google Play).
# Countries not listed here default to "en".
COUNTRY_LANGS: dict[str, str] = {
    "jp": "ja",
    "cn": "zh",
    "kr": "ko",
    "ru": "ru",
    "ua": "uk",
    "de": "de",
    "fr": "fr",
    "es": "es",
    "it": "it",
    "pt": "pt",
    "br": "pt",
    "nl": "nl",
    "pl": "pl",
    "se": "sv",
    "no": "no",
    "dk": "da",
    "fi": "fi",
    "tr": "tr",
    "ar": "ar",
    "sa": "ar",
    "eg": "ar",
    "th": "th",
    "vn": "vi",
    "id": "id",
    "my": "ms",
    "in": "hi",
}


def get_country_lang(country: str) -> str:
    """Return the primary language code for a given country, defaulting to 'en'."""
    return COUNTRY_LANGS.get(country.lower(), "en")

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