from __future__ import annotations

import re

DEMO_FIRST_NAMES_ES_CAT: tuple[str, ...] = (
    "Jose",
    "Antonio",
    "Manuel",
    "Francisco",
    "David",
    "Javier",
    "Juan",
    "Carlos",
    "Daniel",
    "Miguel",
    "Maria",
    "Carmen",
    "Ana",
    "Laura",
    "Isabel",
    "Marta",
    "Elena",
    "Rosa",
    "Silvia",
    "Lucia",
    "Jordi",
    "Pere",
    "Joan",
    "Montserrat",
    "Nuria",
    "Merce",
    "Adria",
    "Aina",
    "Albert",
    "Aleix",
    "Alba",
    "Alex",
    "Amparo",
    "Andrea",
    "Arnau",
    "Berta",
    "Carla",
    "Celia",
    "Claudia",
    "Cristina",
    "Dolors",
    "Eloi",
    "Emma",
    "Eric",
    "Eva",
    "Felix",
    "Gemma",
    "Hector",
    "Irene",
    "Ivan",
    "Laia",
    "Lluis",
    "Lola",
    "Marc",
    "Mireia",
    "Noelia",
    "Oriol",
    "Paula",
    "Raul",
    "Ruben",
    "Sergi",
    "Sonia",
    "Teresa",
    "Victor",
)

DEMO_LAST_NAMES_ES_CAT: tuple[str, ...] = (
    "Garcia",
    "Martinez",
    "Lopez",
    "Sanchez",
    "Perez",
    "Gonzalez",
    "Rodriguez",
    "Fernandez",
    "Alvarez",
    "Ruiz",
    "Moreno",
    "Romero",
    "Navarro",
    "Torres",
    "Dominguez",
    "Vidal",
    "Riera",
    "Pons",
    "Puig",
    "Soler",
    "Mora",
    "Serra",
    "Casals",
    "Costa",
    "Ferrer",
    "Prat",
    "Ribas",
    "Campos",
    "Ibanez",
    "Serrano",
    "Ortega",
    "Mendez",
)

GENERIC_NAME_TOKENS_BLOCKLIST: tuple[str, ...] = (
    "DEMO",
    "PERSONA EXTRA",
    "TITULAR DEMO",
)

_GENERIC_NUMERIC_PATTERN = re.compile(r"\d{2,}")


def is_generic_demo_name(first_name: str, last_name: str) -> bool:
    full_name = f"{(first_name or '').strip()} {(last_name or '').strip()}".strip()
    full_name_upper = full_name.upper()
    for token in GENERIC_NAME_TOKENS_BLOCKLIST:
        if token in full_name_upper:
            return True
    return bool(_GENERIC_NUMERIC_PATTERN.search(full_name))


def generate_demo_names(total: int, offset: int = 0) -> list[tuple[str, str]]:
    if total < 0:
        raise ValueError("total must be >= 0")

    generated: list[tuple[str, str]] = []
    first_len = len(DEMO_FIRST_NAMES_ES_CAT)
    last_len = len(DEMO_LAST_NAMES_ES_CAT)

    for idx in range(total):
        first_name = DEMO_FIRST_NAMES_ES_CAT[(idx + offset) % first_len]
        last_name = (
            f"{DEMO_LAST_NAMES_ES_CAT[(idx + offset) % last_len]} "
            f"{DEMO_LAST_NAMES_ES_CAT[(idx + offset + 7) % last_len]}"
        )
        if is_generic_demo_name(first_name, last_name):
            raise ValueError(f"Generated generic DEMO name is not allowed: {first_name} {last_name}")
        generated.append((first_name, last_name))
    return generated
