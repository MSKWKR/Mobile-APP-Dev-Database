import itertools
import json
import random
import string
from typing import Generator

with open("listings/languages.json", "r") as f:
    ALL_LANGS: list[str] = json.load(f)

# ──────────────────────────────────────────────
# Character sets — major Unicode script blocks
# ──────────────────────────────────────────────

LATIN      = string.ascii_lowercase + string.digits

# Extended Latin — covers French, German, Spanish, Portuguese, Italian,
# Dutch, Polish, Turkish, Vietnamese, Romanian, Czech, and most European languages
LATIN_EXT  = "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿœšžß"

CYRILLIC   = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
GREEK      = "αβγδεζηθικλμνξοπρστυφχψω"
ARABIC     = "ابتثجحخدذرزسشصضطظعغفقكلمنهوي"
HEBREW     = "אבגדהוזחטיכלמנסעפצקרשת"
DEVANAGARI = "अआइईउऊएऐओऔकखगघचछजझटठडढणतथदधनपफबभमयरलवशषसह"
THAI       = "กขคงจฉชซญดตถทนบปผฝพฟภมยรลวสหอฮ"
HIRAGANA   = "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん"
KATAKANA   = "アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン"
CJK        = "的一是不了人我在有他这为之大来以个中上们到说国和地也子时道出而要于就下得可你年生"
KOREAN     = "가나다라마바사아자차카타파하거너더러머버서어저처커터퍼허기니디리미비시이지치키티피히"
GEORGIAN   = "აბგდევზთიკლმნოპჟრსტუფქღყშჩცძწჭხჯჰ"
ARMENIAN   = "աբգդեզէըթժիլխծկհձղճմյնշոչπջռسвтрцў"
BENGALI    = "অআইউএওকখগঘচছজঝটঠডঢতথদধনপফবভমযরলশষসহ"
TAMIL      = "அஆஇஈஉஊஎஏஐஒஓஔகசஞடணதநபமயரலவழளறன"
TELUGU     = "అఆఇఈఉఊఎఏఐఒఓఔకఖగఘచఛజఝటఠడఢతథదధనపఫబభమయరలవశషసహ"
GUJARATI   = "અઆઇઈઉઊએઐઓઔકખગઘચછજઝટઠડઢતથदधनपफबभमयरलवशषसह"
PUNJABI    = "ਅਆਇਈਉਊਏਐਓਔਕਖਗਘਚਛਜਝਟਠਡਢਤਥਦਧਨਪਫਬਭਮਯਰਲਵਸਹ"
MYANMAR    = "ကခဂဃငစဆဇဈညတထဒဓနပဖဗဘမယရလဝသဟဠအ"
KHMER      = "កខគឃងចឆជឈញដឋឌឍណតថទធនបផពភមយរលវសហឡអ"
ETHIOPIC   = "አቡቢባቤብቦሀሁሂሃሄህሆለሉሊላሌልሎሐሑሒሓሔሕሖመሙሚማሜምሞ"
MONGOLIAN  = "ᠠᠡᠢᠣᠤᠥᠦᠧᠨᠩᠪᠫᠬᠭᠮᠯᠰᠱᠲᠳᠴᠵᠶᠷᠸᠹᠺᠻᠼᠽᠾᠿ"

# Map country → extra chars (LATIN + digits always included)
COUNTRY_CHARSETS: dict[str, str] = {
    # East Asia
    "jp": HIRAGANA + KATAKANA,
    "cn": CJK,
    "tw": CJK,
    "hk": CJK,
    "kr": KOREAN,
    # Cyrillic
    "ru": CYRILLIC,
    "ua": CYRILLIC,
    "by": CYRILLIC,
    "kz": CYRILLIC,
    "bg": CYRILLIC,
    "rs": CYRILLIC,
    # South / Southeast Asia
    "in": DEVANAGARI + BENGALI + TAMIL + TELUGU + GUJARATI + PUNJABI,
    "bd": BENGALI,
    "th": THAI,
    "mm": MYANMAR,
    "kh": KHMER,
    # Middle East
    "sa": ARABIC,
    "eg": ARABIC,
    "ae": ARABIC,
    "il": HEBREW,
    # Europe
    "gr": GREEK,
    "ge": GEORGIAN,
    "am": ARMENIAN,
    "mn": MONGOLIAN,
    # Extended Latin
    "fr": LATIN_EXT,
    "de": LATIN_EXT,
    "es": LATIN_EXT,
    "it": LATIN_EXT,
    "pt": LATIN_EXT,
    "br": LATIN_EXT,
    "pl": LATIN_EXT,
    "nl": LATIN_EXT,
    "se": LATIN_EXT,
    "no": LATIN_EXT,
    "dk": LATIN_EXT,
    "fi": LATIN_EXT,
    "tr": LATIN_EXT,
    "ro": LATIN_EXT,
    "cz": LATIN_EXT,
    "hu": LATIN_EXT,
    "vn": LATIN_EXT,
    # Africa
    "et": ETHIOPIC,
}

COUNTRY_LANGS: dict[str, str] = {
    "jp": "ja", "cn": "zh", "kr": "ko", "ru": "ru", "ua": "uk",
    "de": "de", "fr": "fr", "es": "es", "it": "it", "pt": "pt",
    "br": "pt", "nl": "nl", "pl": "pl", "se": "sv", "no": "no",
    "dk": "da", "fi": "fi", "tr": "tr", "ar": "ar", "sa": "ar",
    "eg": "ar", "th": "th", "vn": "vi", "id": "id", "my": "ms",
    "in": "hi", "tw": "zh", "hk": "zh", "by": "be", "kz": "kk",
    "bg": "bg", "rs": "sr", "gr": "el", "ge": "ka", "am": "hy",
    "mn": "mn", "bd": "bn", "mm": "my", "kh": "km", "ae": "ar",
    "il": "he", "ro": "ro", "cz": "cs", "hu": "hu", "et": "am",
}


def get_country_lang(country: str) -> str:
    return COUNTRY_LANGS.get(country.lower(), "en")


# ──────────────────────────────────────────────
# Curated term lists
# ──────────────────────────────────────────────

VERTICALS: list[str] = [
    # Photo & Video
    "photo editor", "video editor", "camera", "selfie", "filter",
    "collage", "slideshow", "photo book", "video maker", "screen recorder",
    "gif maker", "wallpaper", "photo album", "portrait", "background remover",
    # Music & Audio
    "music player", "podcast", "radio", "karaoke", "guitar tuner",
    "drum machine", "beat maker", "metronome", "ringtone", "voice recorder",
    "audio editor", "equalizer", "music downloader", "lyrics", "soundboard",
    # Health & Fitness
    "workout", "meditation", "yoga", "running", "step counter",
    "calorie counter", "diet", "sleep tracker", "water tracker", "bmi calculator",
    "period tracker", "pregnancy", "baby", "weight loss", "home workout",
    # Finance
    "budget", "invoice", "expense tracker", "accounting", "tax",
    "crypto", "stocks", "forex", "banking", "payment",
    "money transfer", "receipt scanner", "net worth", "savings", "loan calculator",
    # Productivity
    "to do list", "note taking", "calendar", "reminder", "habit tracker",
    "time tracker", "pomodoro", "password manager", "file manager", "pdf reader",
    "scanner", "whiteboard", "mind map", "flashcard", "focus timer",
    # Education
    "language learning", "translator", "dictionary", "math", "algebra",
    "typing tutor", "coding", "kids learning", "spelling", "reading",
    "science", "history quiz", "sat prep", "gre prep", "driving test",
    # Social & Communication
    "dating", "chat", "video call", "messaging", "voice chat",
    "anonymous chat", "meet friends", "social network", "live stream", "group chat",
    # Entertainment
    "manga", "comic", "novel", "ebook", "audiobook",
    "bible", "quran", "horoscope", "tarot", "puzzle",
    "trivia", "chess", "sudoku", "crossword", "word game",
    # Travel & Navigation
    "navigation", "maps", "gps", "flight tracker", "hotel booking",
    "travel planner", "currency converter", "visa", "weather", "compass",
    # Shopping & Lifestyle
    "shopping", "coupon", "barcode scanner", "recipe", "meal planner",
    "grocery list", "food delivery", "restaurant", "wine", "coffee",
    # Tools & Utilities
    "vpn", "wifi analyzer", "battery saver", "cleaner", "antivirus",
    "qr code", "flashlight", "ruler", "unit converter", "clock",
    "alarm", "stopwatch", "calculator", "speedtest", "remote control",
    # Business
    "crm", "project management", "team chat", "video conference", "e-signature",
    "inventory", "pos", "payroll", "hr", "job search",
    # Kids
    "kids game", "alphabet", "coloring", "nursery rhyme", "toddler",
    "parental control", "homework helper", "story book", "drawing for kids",
    # Real Estate & Home
    "real estate", "home design", "interior design", "floor plan", "mortgage calculator",
    # Sports
    "football", "basketball", "baseball", "soccer", "tennis",
    "golf", "cricket", "cycling", "swimming", "fantasy sports",
    # Pets
    "pet", "dog training", "cat", "animal sounds", "vet finder",
]

NUMERIC_TOKENS: list[str] = [
    "100", "200", "300", "360", "365", "500", "600", "700", "800", "900",
    "999", "1000", "2024", "2025", "1010", "1024", "2048", "4096",
    "007", "247", "911", "101", "24", "30", "60", "90",
]

MIXED_TOKENS: list[str] = [
    "mp3", "mp4", "pdf", "gif", "png", "jpg", "mkv", "aac",
    "4k", "8k", "hd", "3d", "ar", "vr", "ai", "ml",
    "fm", "tv", "dvr", "iptv", "m3u",
    "ocr", "crm", "erp", "pos", "b2b",
    "bmi", "ecg", "hrv",
    "nfc", "qr", "bt5", "ssh", "ftp",
    "pro", "lite", "mini", "plus", "max", "ultra",
]

COMMON_PREFIXES: list[str] = [
    "a", "i", "my", "go", "get", "best", "top", "free", "new", "pro",
    "smart", "quick", "easy", "fast", "super", "mega", "ultra", "air",
    "pocket", "mini", "daily", "real", "simple", "just",
]

QUALIFIERS: list[str] = [
    "free", "pro", "best", "top", "easy", "simple", "smart",
    "offline", "ai", "hd", "lite", "plus", "new", "premium",
]


# ──────────────────────────────────────────────
# Generator
# ──────────────────────────────────────────────

def generate_search_terms(country: str = "") -> Generator[str, None, None]:
    """
    Yields search terms in tiered priority order for a given country.

    Tiers:
      1.  Single chars         — broadest buckets, shuffled to avoid bias
      2.  Bigrams              — fills gaps where top-N saturates
      3.  Trigram sample       — top-10 chars only to cap combinatorial explosion
      4.  Space variants       — "a b" hits different ranking buckets than "ab"
      5.  Prefix expansions    — common short prefixes + each char
      6.  Numeric tokens       — 3-4 digit numbers common in app names
      7.  Mixed tokens         — short alphanumeric shorthands (mp3, 4k, pdf)
      8.  Verticals            — curated multi-word semantic clusters
      9.  Vertical × qualifier — "free photo editor", "ai translator"
      10. Vertical × prefix    — "pro photo editor", "smart budget"
      11. Vertical × mixed     — "photo editor ai", "vpn 4k"
      12. Vertical × numeric   — "workout 30", "bible 365"
    """
    chars = list(LATIN + COUNTRY_CHARSETS.get(country.lower(), ""))
    seen: set[str] = set()

    def _yield(term: str):
        t = term.strip()
        if t and t not in seen:
            seen.add(t)
            return t
        return None

    # Tier 1 — single chars
    singles = chars[:]
    random.shuffle(singles)
    for c in singles:
        t = _yield(c)
        if t: yield t

    # Tier 2 — bigrams
    bigrams = [a + b for a in chars for b in chars]
    random.shuffle(bigrams)
    for b in bigrams:
        t = _yield(b)
        if t: yield t

    # Tier 3 — trigram sample (top-10 chars only)
    sample_chars = chars[:10]
    trigrams = [a + b + c for a in sample_chars for b in sample_chars for c in sample_chars]
    random.shuffle(trigrams)
    for tri in trigrams:
        t = _yield(tri)
        if t: yield t

    # Tier 4 — space variants
    space_variants = [f"{a} {b}" for a in chars for b in chars]
    random.shuffle(space_variants)
    for sv in space_variants:
        t = _yield(sv)
        if t: yield t

    # Tier 5 — prefix expansions
    for prefix in COMMON_PREFIXES:
        expansions = [prefix + c for c in chars]
        random.shuffle(expansions)
        for e in expansions:
            t = _yield(e)
            if t: yield t

    # Tier 6 — numeric tokens
    for n in NUMERIC_TOKENS:
        t = _yield(n)
        if t: yield t

    # Tier 7 — mixed tokens
    for m in MIXED_TOKENS:
        t = _yield(m)
        if t: yield t

    # Tier 8 — verticals
    shuffled_verticals = VERTICALS[:]
    random.shuffle(shuffled_verticals)
    for v in shuffled_verticals:
        t = _yield(v)
        if t: yield t

    # Tier 9 — vertical × qualifier
    pairs = list(itertools.product(QUALIFIERS, shuffled_verticals))
    random.shuffle(pairs)
    for qualifier, vertical in pairs:
        t = _yield(f"{qualifier} {vertical}")
        if t: yield t

    # Tier 10 — vertical × prefix
    pairs = list(itertools.product(COMMON_PREFIXES, shuffled_verticals))
    random.shuffle(pairs)
    for prefix, vertical in pairs:
        t = _yield(f"{prefix} {vertical}")
        if t: yield t

    # Tier 11 — vertical × mixed token
    pairs = list(itertools.product(shuffled_verticals, MIXED_TOKENS))
    random.shuffle(pairs)
    for vertical, mixed in pairs:
        t = _yield(f"{vertical} {mixed}")
        if t: yield t

    # Tier 12 — vertical × numeric
    pairs = list(itertools.product(shuffled_verticals, NUMERIC_TOKENS))
    random.shuffle(pairs)
    for vertical, num in pairs:
        t = _yield(f"{vertical} {num}")
        if t: yield t