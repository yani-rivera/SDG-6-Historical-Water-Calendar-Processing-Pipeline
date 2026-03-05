import re
import unicodedata


def canonical_raw_group(text):
    """
    Standardize the raw group text, converting it to uppercase and applying normalization.
    This version handles invisible characters and forces uppercase.
    
    Args:
    - text (str): The raw text to clean.
    
    Returns:
    - str: The cleaned and case-normalized text in uppercase.
    """
    if not isinstance(text, str):
        return text

    # Step 1: Remove any non-printable characters (like invisible or special characters)
    #text = ''.join(char for char in text if char.isprintable())

    # Step 2: Convert the text to uppercase
    text = text.upper()

    # Step 3: Remove line breaks and replace with a single space
    text = text.replace("\n", " ")  # Replace line breaks with a single space

    # Step 4: Strip outer whitespace
    text = text.strip()

    # Step 5: Collapse multiple spaces into one
    text = re.sub(r"\s+", " ", text)

    # Step 6: Standardize comma spacing (ensure exactly one space after a comma)
    text = re.sub(r"\s*,\s*", ", ", text)

    # Step 7: Normalize common patterns (e.g., replace '&' with 'y')
    text = text.replace("&", " Y ")

    # Step 8: Clean text inside parentheses (remove extra spaces inside)
    text = re.sub(r"\((.*?)\)", lambda m: f"({re.sub(r'\s+', ' ', m.group(1)).strip()})", text)

    # Step 9: Remove unwanted characters outside parentheses (keeping letters, numbers, spaces, and commas)
    text = re.sub(r"[^A-Z0-9\s\(\),ÑÁÉÍÓÚÜ]", " ", text)

    # Step 10: Final normalization of spaces to ensure consistency
    text = re.sub(r"\s+", " ", text).strip()

    return text

 

def norm(s: str) -> str:
    """Stable normalization preserving parentheses and their content."""
    if not isinstance(s, str):
        return ""

    # Step 1: Uppercase
    t = s.upper().strip()

    # Step 2: Standardize connectors
    t = t.replace("&", " Y ")

    # Step 3: Remove punctuation but KEEP Spanish letters
    # Keep letters (including accented), numbers, spaces, parentheses
    t = re.sub(r"[^\w\s\(\)]", " ", t, flags=re.UNICODE)

    # Step 4: Normalize spacing
    t = re.sub(r"\s+", " ", t).strip()

# Create an alias for the function




def clean_neighborhood(s: str) -> str:
    """Stable Spanish-aware normalization preserving accents and parentheses."""

    if not isinstance(s, str):
        return ""

    # Step 1: Uppercase
    t = s.upper().strip()

    # Step 2: Standardize connectors
    t = t.replace("&", " Y ")

    # Step 3: Remove punctuation but KEEP Spanish letters
    # Keep letters (including accented), numbers, spaces, parentheses
    t = re.sub(r"[^\w\s\(\)]", " ", t, flags=re.UNICODE)

    # Step 4: Normalize spacing
    t = re.sub(r"\s+", " ", t).strip()

    return t