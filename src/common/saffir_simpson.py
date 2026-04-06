"""
Saffir-Simpson Hurricane Wind Scale — shared utility.

Canonical source for wind→category conversion used across the codebase.
Replaces duplicate implementations in catalog.py, hurdat2_parser.py,
and vector_overlays.py.
"""


def wind_to_category(wind_kt: int) -> int:
    """Convert maximum sustained wind (knots) to Saffir-Simpson category (0–5).

    Category 0 represents Tropical Storm / Tropical Depression strength.
    """
    if wind_kt >= 137:
        return 5
    if wind_kt >= 113:
        return 4
    if wind_kt >= 96:
        return 3
    if wind_kt >= 83:
        return 2
    if wind_kt >= 64:
        return 1
    return 0


def wind_to_category_str(wind_kt: int) -> str:
    """Convert max sustained wind (knots) to category string for display.

    Returns e.g. 'CAT5', 'CAT1', 'TS' (tropical storm / depression).
    """
    cat = wind_to_category(wind_kt)
    return f"CAT{cat}" if cat > 0 else "TS"
