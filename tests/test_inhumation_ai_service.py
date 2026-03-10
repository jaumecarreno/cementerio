from __future__ import annotations

from io import BytesIO

import pytest
from werkzeug.datastructures import FileStorage

from app.cemetery.inhumation_ai_service import (
    InhumationAIUnprocessableError,
    _load_blank_template_static_lines,
    _normalize_for_form,
    _parse_fields_with_meta,
    _parse_fields,
    _subtract_static_template,
    _text_to_static_lines,
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

    assert normalized_confidence["first_name"] >= 0.8
    assert normalized_confidence["death_year"] >= 0.8


def test_normalize_does_not_include_low_confidence_values():
    normalized, confidence = _normalize_for_form(
        {"nombre_difunto": "JUAN", "documento_numero": "12345678Z"},
        {"nombre_difunto": 0.79, "documento_numero": 0.79},
    )
    assert normalized == {}
    assert confidence == {}


def test_parse_fields_discards_label_like_candidates_in_strict_mode():
    sample_text = """
Nombre del fallecido/a: Nombre del fallecido/a
1o Apellido del fallecido/a: Primer apellido
2o Apellido del fallecido/a: Segundo apellido
Causa inmediata: Causa inmediata
"""
    extracted, _confidence, warnings = _parse_fields_with_meta(
        sample_text,
        static_lines=set(),
        strict=True,
    )

    assert "nombre_difunto" not in extracted
    assert "apellido1" not in extracted
    assert "apellido2" not in extracted
    assert any("parece etiqueta" in warning for warning in warnings)


def test_parse_fields_extracts_clean_doctor_blocks():
    sample_text = """
CERTIFICO la defuncion de D./Dna. LAURA SANTOS GIL en Medicina y Cirugia,
colegiado en Barcelona, con el numero 778899,
y con ejercicio profesional en Hospital General de Barcelona
"""
    extracted, _confidence, _warnings = _parse_fields_with_meta(
        sample_text,
        static_lines=set(),
        strict=True,
    )

    assert extracted["medico_nombre"] == "LAURA SANTOS GIL"
    assert extracted["medico_colegiado_en"] == "Barcelona"
    assert extracted["medico_numero"] == "778899"
    assert extracted["medico_ejercicio"] == "Hospital General de Barcelona"


def test_parse_fields_cleans_cause_titles():
    sample_text = """
Causa inmediata: /Causa inmediata Shock septico
Causas antecedentes: Causas antecedentes Neumonia bilateral
Causa inicial o fundamental: Causa inicial o fundamental Insuficiencia respiratoria
"""
    extracted, _confidence, _warnings = _parse_fields_with_meta(
        sample_text,
        static_lines=set(),
        strict=True,
    )

    assert extracted["causa_inmediata"] == "Shock septico"
    assert extracted["causa_antecedente"] == "Neumonia bilateral"
    assert extracted["causa_fundamental"] == "Insuficiencia respiratoria"


def test_parse_fields_extracts_dni_labels_multiline():
    sample_text = """
APELLIDOS / SURNAMES
GARCIA LOPEZ
NOMBRE / NAME
JUAN CARLOS
DNI: 12345678Z
"""
    extracted, _confidence, warnings = _parse_fields_with_meta(
        sample_text,
        static_lines=set(),
        strict=True,
    )

    assert extracted["nombre_difunto"] == "JUAN CARLOS"
    assert extracted["apellido1"] == "GARCIA"
    assert extracted["apellido2"] == "LOPEZ"
    assert extracted["documento_numero"] == "12345678Z"
    assert not any("no se han podido aislar nombre/apellidos" in w.lower() for w in warnings)


def test_parse_fields_extracts_name_from_mrz():
    sample_text = """
IDESPAAAAAAAAAAAAAAAAAAAAAAAAA
GARCIA<LOPEZ<<JUAN<CARLOS<<<<<<<<<<<
"""
    extracted, _confidence, _warnings = _parse_fields_with_meta(
        sample_text,
        static_lines=set(),
        strict=True,
    )

    assert extracted["nombre_difunto"] == "JUAN CARLOS"
    assert extracted["apellido1"] == "GARCIA"
    assert extracted["apellido2"] == "LOPEZ"


def test_parse_fields_warns_when_dni_has_no_holder_name():
    sample_text = "DNI: 12345678Z"
    extracted, _confidence, warnings = _parse_fields_with_meta(
        sample_text,
        static_lines=set(),
        strict=True,
    )

    assert extracted["documento_numero"] == "12345678Z"
    assert any("documento de identidad detectado" in warning.lower() for warning in warnings)


def test_parse_fields_extracts_real_catalan_dni_layout():
    sample_text = """
REINO DE ESPAÑA
DOCUMENTO NACIONAL DE IDENTIDAD

DNI 45646530V

APELLIDOS / COGNOMS
CARREÑO
ZORRILLA

NOMBRE / NOM
JAUME

SEXO / SEXE
M

NACIMIENTO / NAIXEMENT
13 02 1984

EMISION / EMISSIO
25 11 2024

VALIDEZ / VALIDEZA
25 11 2034
"""
    extracted, _confidence, warnings = _parse_fields_with_meta(
        sample_text,
        static_lines=set(),
        strict=True,
    )

    assert extracted["documento_numero"] == "45646530V"
    assert extracted["documento_tipo"] == "DNI"
    assert extracted["apellido1"] == "CARREÑO"
    assert extracted["apellido2"] == "ZORRILLA"
    assert extracted["nombre_difunto"] == "JAUME"
    assert extracted["sexo"] == "M"
    assert extracted["fecha_nacimiento"]["day"] == "13"
    assert extracted["fecha_nacimiento"]["month"] == "2"
    assert extracted["fecha_nacimiento"]["year"] == "1984"
    assert not any("no se han podido aislar nombre/apellidos" in w.lower() for w in warnings)


def test_parse_and_normalize_extracts_billing_certificate_fields():
    sample_text = """
CERTIFICADO DE TITULARIDAD DE CUENTA
Titular de la cuenta: JAUME CARRENO ZORRILLA
DNI/NIF del titular: 45646530V
IBAN: ES12 2100 0418 4502 0005 1332
Entidad bancaria: Banco Santander, S.A.
"""
    extracted, raw_confidence = _parse_fields(sample_text)
    normalized, normalized_confidence = _normalize_for_form(extracted, raw_confidence)

    assert extracted["titular_cuenta_nombre"] == "JAUME CARRENO ZORRILLA"
    assert extracted["titular_cuenta_documento"] == "45646530V"
    assert extracted["iban_cuenta"] == "ES1221000418450200051332"
    assert extracted["banco_nombre"] == "Banco Santander, S.A"

    assert normalized["billing_account_holder_name"] == "JAUME CARRENO ZORRILLA"
    assert normalized["billing_account_holder_document_number"] == "45646530V"
    assert normalized["billing_iban"] == "ES1221000418450200051332"
    assert normalized["billing_bank_name"] == "Banco Santander, S.A"
    assert normalized_confidence["billing_iban"] >= 0.9


def test_parse_billing_ignores_bic_and_keeps_bank_name():
    sample_text = """
CERTIFICADO DE TITULARIDAD DE CUENTA
Titular de la cuenta: JAUME CARRENO ZORRILLA
DNI/NIF del titular: 45646530V
IBAN: ES79 1465 9999 9999 9999 9999
BIC/SWIFT: INGDESMMXXX
Entidad bancaria: ING Direct
"""
    extracted, raw_confidence = _parse_fields(sample_text)
    normalized, _normalized_confidence = _normalize_for_form(extracted, raw_confidence)

    assert extracted["banco_nombre"] == "ING Direct"
    assert normalized["billing_bank_name"] == "ING Direct"
    assert extracted["iban_cuenta"] == "ES7914659999999999999999"


def test_subtract_static_template_removes_fixed_lines_and_keeps_values():
    blank_template_text = """
NOMBRE DEL FALLECIDO/A
PRIMER APELLIDO DEL FALLECIDO/A
CAUSA INMEDIATA
"""
    static_lines = _text_to_static_lines(blank_template_text)
    filled_text = """
NOMBRE DEL FALLECIDO/A
Nombre del fallecido/a: JUAN
PRIMER APELLIDO DEL FALLECIDO/A
1o Apellido del fallecido/a: PEREZ
CAUSA INMEDIATA
Causa inmediata: Infarto agudo de miocardio
"""

    dynamic_text = _subtract_static_template(filled_text, static_lines)

    assert "NOMBRE DEL FALLECIDO/A" not in dynamic_text
    assert "PRIMER APELLIDO DEL FALLECIDO/A" not in dynamic_text
    assert "CAUSA INMEDIATA" not in dynamic_text
    assert "Nombre del fallecido/a: JUAN" in dynamic_text
    assert "1o Apellido del fallecido/a: PEREZ" in dynamic_text
    assert "Causa inmediata: Infarto agudo de miocardio" in dynamic_text


def test_blank_template_invalid_path_returns_warning_without_crash(app):
    with app.app_context():
        app.config["INHUMATION_AI_BLANK_TEMPLATE_PATH"] = "C:/no/existe/plantilla.pdf"
        static_lines, warnings = _load_blank_template_static_lines()

    assert static_lines == set()
    assert any("plantilla base" in warning.lower() for warning in warnings)


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
