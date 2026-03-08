import re
from dataclasses import dataclass


@dataclass
class NormalizedItem:
    full_name: str
    clean_name: str
    gun: str
    skin: str
    quality: str
    is_stattrack: bool
    category: str


def normalize_item_name(name: str) -> NormalizedItem:

    quality_pattern = r'\s*\(([^()]+)\)$'
    match = re.search(quality_pattern, name)

    if match:
        quality = match.group(1)
        clean_name = re.sub(quality_pattern, '', name).strip()
    else:
        quality = "Not Painted"
        clean_name = name.strip()

    is_stattrack = "StatTrak™" in clean_name

    category = "Weapon"
    if "★" in clean_name:
        category = "Knife/Gloves"
    elif "Sticker |" in clean_name:
        category = "Sticker"
    elif "Agent |" in clean_name:
        category = "Agent"
    elif "Patch |" in clean_name:
        category = "Patch"
    elif "Music Kit |" in clean_name:
        category = "Music Kit"
    elif "Graffiti |" in clean_name:
        category = "Graffiti"
    elif "Charm |" in clean_name:
        category = "Charm"
    elif "Souvenir Charm |" in clean_name:
        category = "Charm"

    name_for_split = clean_name.replace("StatTrak™ ", "")
    if "|" in name_for_split:
        parts = name_for_split.split(" | ")
        gun = parts[0].strip()
        skin = " | ".join(parts[1:]).strip()
    else:
        gun = name_for_split.strip()
        skin = ""

    return NormalizedItem(
        full_name=name,
        clean_name=clean_name,
        gun=gun,
        skin=skin,
        quality=quality,
        is_stattrack=is_stattrack,
        category=category
    )