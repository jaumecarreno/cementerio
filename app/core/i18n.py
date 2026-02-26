from __future__ import annotations

from flask import session

SUPPORTED_LANGS = {"es", "ca"}

I18N: dict[str, dict[str, str]] = {
    "menu.home": {"es": "Inicio", "ca": "Inici"},
    "menu.funeral_services": {"es": "Servicios funerarios", "ca": "Serveis funeraris"},
    "menu.crematorium": {"es": "Crematorio", "ca": "Crematori"},
    "menu.cemetery": {"es": "Cementerio", "ca": "Cementiri"},
    "menu.billing": {"es": "Facturación", "ca": "Facturació"},
    "menu.inventory": {"es": "Inventario", "ca": "Inventari"},
    "menu.reporting": {"es": "Reporting", "ca": "Informes"},
    "menu.settings": {"es": "Configuración", "ca": "Configuració"},
    "cem.panel": {"es": "Panel", "ca": "Panell"},
    "cem.graves": {"es": "Sepulturas", "ca": "Sepultures"},
    "cem.fees": {"es": "Tasas", "ca": "Taxes"},
    "cem.cases": {"es": "Expedientes", "ca": "Expedients"},
    "cem.ownership": {"es": "Titularidad", "ca": "Titularitat"},
    "cem.engraving": {"es": "Lápidas/inscripciones", "ca": "Làpides/inscripcions"},
    "cem.rights": {"es": "Derechos funerarios", "ca": "Drets funeraris"},
    "action.search_grave": {"es": "Buscar sepultura", "ca": "Cercar sepultura"},
    "action.collect_fees": {"es": "Cobrar tasas", "ca": "Cobrar taxes"},
    "action.mass_create": {"es": "Alta masiva", "ca": "Alta massiva"},
    "state.lliure": {"es": "Lliure", "ca": "Lliure"},
    "state.disponible": {"es": "Disponible", "ca": "Disponible"},
    "state.ocupada": {"es": "Ocupada", "ca": "Ocupada"},
    "state.inactiva": {"es": "Inactiva", "ca": "Inactiva"},
    "state.propia": {"es": "Pròpia", "ca": "Pròpia"},
}


def get_locale() -> str:
    lang = session.get("lang", "es")
    if lang not in SUPPORTED_LANGS:
        return "es"
    return lang


def translate(key: str) -> str:
    lang = get_locale()
    return I18N.get(key, {}).get(lang, key)
