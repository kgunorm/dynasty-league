"""Step 2: Field normalization — adds _norm_* columns."""
import re
import pandas as pd

# Common street abbreviation expansions for address normalization
_ABBREVS = {
    r"\bst\b": "street",
    r"\bave\b": "avenue",
    r"\bblvd\b": "boulevard",
    r"\bdr\b": "drive",
    r"\brd\b": "road",
    r"\bln\b": "lane",
    r"\bct\b": "court",
    r"\bpl\b": "place",
    r"\bn\b": "north",
    r"\bs\b": "south",
    r"\be\b": "east",
    r"\bw\b": "west",
    r"\bapt\b": "apartment",
    r"\bste\b": "suite",
}


def _norm_name(val) -> str:
    if not val or pd.isna(val):
        return ""
    # Lowercase, strip punctuation except spaces, token-sort
    val = str(val).lower()
    val = re.sub(r"[^\w\s]", "", val)
    tokens = sorted(val.split())
    return " ".join(tokens)


def _norm_email(val) -> str:
    if not val or pd.isna(val):
        return ""
    return str(val).strip().lower()


def _norm_phone(val) -> str:
    """Strip to digits only (E.164-ish). Keep last 10 digits for US numbers."""
    if not val or pd.isna(val):
        return ""
    digits = re.sub(r"\D", "", str(val))
    # Drop country code if 11 digits and starts with 1
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def _norm_address(val) -> str:
    if not val or pd.isna(val):
        return ""
    val = str(val).lower().strip()
    for pattern, replacement in _ABBREVS.items():
        val = re.sub(pattern, replacement, val)
    val = re.sub(r"\s+", " ", val)
    return val


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_norm_name"] = df["full_name"].apply(_norm_name)
    df["_norm_email"] = df["email"].apply(_norm_email)
    df["_norm_phone"] = df["phone"].apply(_norm_phone)
    df["_norm_address"] = df["address"].apply(_norm_address)
    return df
