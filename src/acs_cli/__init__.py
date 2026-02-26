import re


def clean_county_name(name: str) -> str:
    """Normalize a county name: strip state/county suffixes, spell out 'Saint'."""
    name = name.replace(", Michigan", "")
    name = re.sub(r"\s+County$", "", name)
    name = re.sub(r"\bSt\.\s+", "Saint ", name)
    return name
