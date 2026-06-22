"""Shared listing-source definitions for marketplace-based models."""

MARKETPLACE_SOURCES = ("99acres", "magicbricks")
MARKETPLACE_SOURCE_SET = frozenset(MARKETPLACE_SOURCES)
COMBINED_MARKETPLACE_SOURCE = "marketplaces"
PRICE_HISTORY_MARKETPLACE_SOURCES = MARKETPLACE_SOURCE_SET | {
    COMBINED_MARKETPLACE_SOURCE,
}


def is_marketplace_source(value: object) -> bool:
    return str(value or "").strip().lower() in MARKETPLACE_SOURCE_SET
