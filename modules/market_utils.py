
def normalize_title(title):
    """
    Normalizes a title string for robust comparison.
    1. Lowercase
    2. Strip whitespace
    3. Remove alphanumeric noise (basic)
    """
    if not title: return ""
    return title.strip().lower()
