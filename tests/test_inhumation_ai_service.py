from __future__ import annotations

from io import BytesIO

import pytest
from werkzeug.datastructures import FileStorage

from app.cemetery.inhumation_ai_service import (
    InhumationAIUnprocessableError,
    _normalize_for_form,
    _parse_fields,
    extract_inhumation_document,
)


def test_parse_and_normalize_extracts_key_inhumation_fields():
    sample_text = """
CERTIFICO la defuncion de D./Dna. MARIA LOPEZ RUIZ en Medicina y Cirugia, colegiado/a en Barcelona y con ejercicio profesional en Hospital Central, con el numero 12345
Nombre del fallecido/a: JUAN
1º Apellido del fallecido/a: PEREZ
2º Apellido del fallecido/a: GOMEZ
DNI: 12345678Z
Fecha de nacimiento: 07 04 1942
Sexo: varon
Hora y fecha de la defuncion hora:minutos 13:27 Dia Mes Ano 04 03 2026
La defuncion ha ocurrido como consecuencia directa o indirecta de accidente de trafico
"""
    extracted, raw_confidence = _parse_fields(sample_text)
    normalized, normalized_confidence = _normalize_for_form(extracted, raw_confidence)

    assert extracted["nombre_difunto"] == "JUAN"
    assert extracted["apellido1"] == "PEREZ"
    assert extracted["apellido2"] == "GOMEZ"
    assert extracted["documento_numero"] == "12345678Z"
    assert extracted["documento_tipo"] == "DNI"
    assert extracted["medico_nombre"] == "MARIA LOPEZ RUIZ"
    assert extracted["medico_numero"] == "12345"
    assert extracted["sexo"] == "M"
    assert extracted["consecuencia_defuncion"] == "ACCIDENTE_TRAFICO"

    assert normalized["first_name"] == "JUAN"
    assert normalized["last_name"] == "PEREZ"
    assert normalized["second_last_name"] == "GOMEZ"
    assert normalized["document_type"] == "DNI"
    assert normalized["document_number"] == "12345678Z"
    assert normalized["birth_day"] == "7"
    assert normalized["birth_month"] == "4"
    assert normalized["birth_year"] == "1942"
    assert normalized["death_hour"] == "13"
    assert normalized["death_minute"] == "27"
    assert normalized["death_day"] == "4"
    assert normalized["death_month"] == "3"
    assert normalized["death_year"] == "2026"
    assert normalized["doctor_name"] == "MARIA LOPEZ RUIZ"
    assert normalized["doctor_registration_number"] == "12345"

    assert normalized_confidence["first_name"] >= 0.65
    assert normalized_confidence["death_year"] >= 0.65


def test_normalize_does_not_include_low_confidence_values():
    normalized, confidence = _normalize_for_form(
        {"nombre_difunto": "JUAN", "documento_numero": "12345678Z"},
        {"nombre_difunto": 0.5, "documento_numero": 0.64},
    )
    assert normalized == {}
    assert confidence == {}


def test_extract_document_raises_unprocessable_when_no_text_can_be_read(app):
    with app.app_context():
        file_obj = FileStorage(
            stream=BytesIO(b"not-an-image"),
            filename="scan.png",
            content_type="image/png",
        )
        with pytest.raises(InhumationAIUnprocessableError) as exc_info:
            extract_inhumation_document(file_obj)

    err = exc_info.value
    assert "No se ha podido" in str(err)
    assert isinstance(err.warnings, list)
    assert any("OPENAI_API_KEY" in warning for warning in err.warnings)
