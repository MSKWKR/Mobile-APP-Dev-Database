import itertools
import json
import random
import string
from typing import Generator

with open("listings/languages.json", "r") as f:
    ALL_LANGS: list[str] = json.load(f)

# ──────────────────────────────────────────────
# Character sets
# ──────────────────────────────────────────────

LATIN    = string.ascii_lowercase + string.digits
CYRILLIC = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
HIRAGANA = "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん"

# High-frequency CJK characters for Chinese — no need for thousands,
# these cover the vast majority of Chinese app name prefixes
CJK = "的一是不了人我在有他这为之大来以个中上们到说国和地也子时道出而要于就下得可你年生"

# Extended Hangul — original 14 initials + all eo-endings for better coverage
KOREAN = "가나다라마바사아자차카타파하거너더러머버서어저처커터퍼허기니디리미비시이지치키티피히"

# Map country codes to extra character sets to include in their sweep.
# Latin + digits are always included.
COUNTRY_CHARSETS: dict[str, str] = {
    "jp": HIRAGANA,
    "cn": CJK,      # fixed: was incorrectly set to HIRAGANA
    "ru": CYRILLIC,
    "ua": CYRILLIC,
    "kr": KOREAN,
}

# Map country codes to their primary language code (ISO 639-1).
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
# Curated term lists
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

NUMERIC_TOKENS: list[str] = [
    "100", "200", "300", "360", "365", "500", "600", "700", "800", "900",
    "999", "1000", "2024", "2025",
    "1010", "1024", "2048", "4096",
    "007", "247", "911", "101",
]

MIXED_TOKENS: list[str] = [
    "mp3", "mp4", "pdf", "gif", "png", "jpg", "mkv", "aac",
    "4k", "8k", "hd", "3d", "ar", "vr", "ai", "ml",
    "fm", "tv", "dvr", "iptv", "m3u",
    "ocr", "crm", "erp", "pos", "b2b",
    "bmi", "ecg", "hrv",
    "nfc", "qr", "bt5", "ssh", "ftp",
    "pro", "lite", "mini", "plus",
]

# Prefix bias — Apple/Google search favors prefix matches.
# Prepending common short prefixes surfaces apps that bigrams alone miss.
COMMON_PREFIXES: list[str] = ["a", "i", "my", "go", "get", "best", "top", "free", "new", "pro"]

# ──────────────────────────────────────────────
# Generator
# ──────────────────────────────────────────────

def generate_search_terms(country: str = "") -> Generator[str, None, None]:
    """
    Yields search terms in tiered priority order for a given country.
    Uses a generator (not a list) to avoid memory explosion with large charsets.

    Tiers:
      1. Single chars        — broadest buckets, shuffled to avoid bias
      2. Bigrams             — fills gaps where top-N saturates, shuffled
      3. Trigram sample      — top-10 chars only to avoid combinatorial explosion
      4. Space variants      — "a b" hits different ranking buckets than "ab"
      5. Prefix expansions   — common short prefixes + each char
      6. Numeric tokens      — 3–4 digit numbers common in app names
      7. Mixed tokens        — short alphanumeric shorthands (mp3, 4k, pdf)
      8. Verticals           — curated multi-word semantic clusters
    """
    chars = list(LATIN + COUNTRY_CHARSETS.get(country.lower(), ""))

    seen: set[str] = set()

    def _yield(term: str):
        if term not in seen:
            seen.add(term)
            return term
        return None

    # Tier 1 — single chars (shuffled)
    singles = chars[:]
    random.shuffle(singles)
    for c in singles:
        t = _yield(c)
        if t:
            yield t

    # Tier 2 — bigrams (shuffled)
    bigrams = [a + b for a in chars for b in chars]
    random.shuffle(bigrams)
    for b in bigrams:
        t = _yield(b)
        if t:
            yield t

    # Tier 3 — trigram sample (top-10 chars only to cap combinatorial explosion)
    # 10^3 = 1000 trigrams, manageable
    sample_chars = chars[:10]
    trigrams = [a + b + c for a in sample_chars for b in sample_chars for c in sample_chars]
    random.shuffle(trigrams)
    for tri in trigrams:
        t = _yield(tri)
        if t:
            yield t

    # Tier 4 — space variants ("a b" hits different ranking buckets than "ab")
    space_variants = [f"{a} {b}" for a in chars for b in chars]
    random.shuffle(space_variants)
    for sv in space_variants:
        t = _yield(sv)
        if t:
            yield t

    # Tier 5 — prefix expansions
    for prefix in COMMON_PREFIXES:
        expansions = [prefix + c for c in chars]
        random.shuffle(expansions)
        for e in expansions:
            t = _yield(e)
            if t:
                yield t

    # Tier 6 — numeric tokens
    for n in NUMERIC_TOKENS:
        t = _yield(n)
        if t:
            yield t

    # Tier 7 — mixed tokens
    for m in MIXED_TOKENS:
        t = _yield(m)
        if t:
            yield t

    # Tier 8 — verticals
    for v in VERTICALS:
        t = _yield(v)
        if t:
            yield t