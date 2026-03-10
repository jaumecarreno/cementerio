from __future__ import annotations

import base64
import difflib
import re
import unicodedata
import uuid
from pathlib import Path
from threading import Lock
from typing import Any

from flask import current_app
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png"}
_MIN_LOCAL_TEXT_CHARS = 120
_DEFAULT_MIN_CONFIDENCE = 0.80

_TEMPLATE_CACHE: dict[str, dict[str, Any]] = {}
_TEMPLATE_CACHE_LOCK = Lock()

_LABEL_KEYWORDS = {
    "nombre del fallecido",
    "primer apellido",
    "segundo apellido",
    "fecha de nacimiento",
    "hora y fecha de la defuncion",
    "lugar en el que ocurrio la defuncion",
    "documento de identidad",
    "causa inmediata",
    "causas antecedentes",
    "causa inicial o fundamental",
    "colegiado en",
    "ejercicio profesional",
    "certifico la defuncion",
    "intervalo",
}


class InhumationAIInputError(ValueError):
    """Raised when upload payload is invalid."""


class InhumationAIUnprocessableError(ValueError):
    """Raised when document cannot be processed with enough quality."""

    def __init__(
        self,
        message: str,
        *,
        raw_text: str = "",
        fields_extracted: dict[str, Any] | None = None,
        normalized_data: dict[str, Any] | None = None,
        confidence: dict[str, float] | None = None,
        warnings: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.raw_text = raw_text
        self.fields_extracted = fields_extracted or {}
        self.normalized_data = normalized_data or {}
        self.confidence = confidence or {}
        self.warnings = warnings or []


def extract_inhumation_document(file_obj: FileStorage) -> dict[str, Any]:
    max_upload_mb = _max_upload_mb()
    max_upload_bytes = max_upload_mb * 1024 * 1024
    filename, extension = _validated_upload_name(file_obj)
    file_size = _file_size(file_obj)
    if file_size <= 0:
        raise InhumationAIInputError("El fichero est\u00e1 vac\u00edo.")
    if file_size > max_upload_bytes:
        raise InhumationAIInputError(
            f"El fichero supera el l\u00edmite de {max_upload_mb} MB."
        )

    root = (
        Path(current_app.instance_path)
        / "storage"
        / "cemetery"
        / "tmp"
        / "inhumation_ai"
        / uuid.uuid4().hex
    )
    root.mkdir(parents=True, exist_ok=True)
    absolute = root / filename

    warnings: list[str] = []
    raw_text = ""
    try:
        file_obj.save(absolute)
        if extension == ".pdf":
            raw_text = _extract_pdf_text_local(absolute)

        if len(raw_text.strip()) < _MIN_LOCAL_TEXT_CHARS:
            ocr_text, ocr_warnings = _extract_text_with_openai(absolute, extension)
            warnings.extend(ocr_warnings)
            if ocr_text.strip():
                raw_text = ocr_text

        if not raw_text.strip():
            warnings.append("No se ha podido extraer texto legible del documento.")
            raise InhumationAIUnprocessableError(
                "No se ha podido leer el documento.",
                raw_text="",
                warnings=warnings,
            )

        static_lines, template_warnings = _load_blank_template_static_lines()
        warnings.extend(template_warnings)
        dynamic_text = _subtract_static_template(raw_text, static_lines)

        fields_extracted, field_confidence, parse_warnings = _parse_fields_with_meta(
            dynamic_text or raw_text,
            static_lines=static_lines,
            strict=True,
        )
        warnings.extend(parse_warnings)

        (
            normalized_data,
            normalized_confidence,
            normalize_warnings,
        ) = _normalize_for_form_with_meta(
            fields_extracted,
            field_confidence,
            static_lines=static_lines,
            min_confidence=_min_confidence(),
            strict=True,
        )
        warnings.extend(normalize_warnings)

        if not normalized_data and dynamic_text.strip() and dynamic_text != raw_text:
            warnings.append(
                "No se han encontrado datos fiables tras comparar con la plantilla base; se revisa el OCR completo."
            )
            (
                fields_extracted,
                field_confidence,
                parse_warnings,
            ) = _parse_fields_with_meta(
                raw_text,
                static_lines=static_lines,
                strict=True,
            )
            warnings.extend(parse_warnings)
            (
                normalized_data,
                normalized_confidence,
                normalize_warnings,
            ) = _normalize_for_form_with_meta(
                fields_extracted,
                field_confidence,
                static_lines=static_lines,
                min_confidence=_min_confidence(),
                strict=True,
            )
            warnings.extend(normalize_warnings)

        if not normalized_data:
            warnings.append(
                "No se han detectado datos fiables para autocompletar el formulario."
            )
            raise InhumationAIUnprocessableError(
                "No se han encontrado datos autocompletables.",
                raw_text=raw_text,
                fields_extracted=fields_extracted,
                normalized_data={},
                confidence={},
                warnings=_dedupe_warnings(warnings),
            )

        if len(normalized_data) < 3:
            warnings.append(
                "Se han identificado pocos campos. Revise manualmente el documento."
            )

        warnings = _dedupe_warnings(warnings)
        needs_review = bool(
            warnings
            or any(score < 0.9 for score in normalized_confidence.values())
            or len(normalized_data) < 6
        )

        return {
            "success": True,
            "raw_text": raw_text,
            "fields_extracted": fields_extracted,
            "normalized_data": normalized_data,
            "confidence": normalized_confidence,
            "needs_review": needs_review,
            "warnings": warnings,
        }
    finally:
        _cleanup_temp(root)


def _max_upload_mb() -> int:
    configured = current_app.config.get("INHUMATION_AI_MAX_UPLOAD_MB", 15)
    try:
        value = int(str(configured).strip())
    except Exception:
        value = 15
    return max(value, 1)


def _min_confidence() -> float:
    configured = current_app.config.get(
        "INHUMATION_AI_MIN_CONFIDENCE", _DEFAULT_MIN_CONFIDENCE
    )
    try:
        value = float(str(configured).strip())
    except Exception:
        value = _DEFAULT_MIN_CONFIDENCE
    return min(max(value, 0.5), 0.99)


def _validated_upload_name(file_obj: FileStorage | None) -> tuple[str, str]:
    if not file_obj or not file_obj.filename:
        raise InhumationAIInputError("Debes seleccionar un fichero.")
    filename = secure_filename(file_obj.filename) or "documento.bin"
    extension = Path(filename).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
        raise InhumationAIInputError(
            f"Formato de fichero no permitido. Usa: {allowed}."
        )
    return filename, extension


def _file_size(file_obj: FileStorage) -> int:
    stream = file_obj.stream
    if not stream:
        return 0
    try:
        pos = stream.tell()
        stream.seek(0, 2)
        size = int(stream.tell() or 0)
        stream.seek(pos)
        return size
    except Exception:
        return int(getattr(file_obj, "content_length", 0) or 0)


def _extract_pdf_text_local(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        return ""
    try:
        reader = PdfReader(str(path))
    except Exception:
        return ""

    chunks: list[str] = []
    for page in reader.pages:
        try:
            page_text = page.extract_text() or ""
        except Exception:
            page_text = ""
        if page_text.strip():
            chunks.append(page_text)
    return "\n".join(chunks).strip()


def _extract_text_with_openai(path: Path, extension: str) -> tuple[str, list[str]]:
    warnings: list[str] = []
    api_key = (current_app.config.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        warnings.append(
            "OCR por IA no disponible: falta configurar OPENAI_API_KEY."
        )
        return "", warnings

    try:
        from openai import OpenAI
    except Exception:
        warnings.append("OCR por IA no disponible: dependencia OpenAI no instalada.")
        return "", warnings

    model = (
        current_app.config.get("INHUMATION_AI_MODEL") or "gpt-4.1-mini"
    ).strip() or "gpt-4.1-mini"
    timeout_seconds = _openai_timeout_seconds()
    max_output_tokens = _openai_max_output_tokens()
    mime_map = {
        ".pdf": "application/pdf",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
    }
    mime = mime_map.get(extension, "application/octet-stream")
    try:
        payload = base64.b64encode(path.read_bytes()).decode("ascii")
    except Exception:
        warnings.append("No se ha podido leer el fichero temporal para OCR.")
        return "", warnings

    user_content: list[dict[str, str]] = [
        {
            "type": "input_text",
            "text": (
                "Extrae TODO el texto legible de este certificado de defuncion. "
                "Devuelve solo texto plano, sin markdown y sin explicaciones."
            ),
        }
    ]
    if extension == ".pdf":
        user_content.append(
            {
                "type": "input_file",
                "filename": path.name,
                "file_data": f"data:{mime};base64,{payload}",
            }
        )
    else:
        user_content.append(
            {
                "type": "input_image",
                "image_url": f"data:{mime};base64,{payload}",
            }
        )

    try:
        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=model,
            input=[{"role": "user", "content": user_content}],
            temperature=0,
            max_output_tokens=max_output_tokens,
            timeout=timeout_seconds,
        )
    except Exception:
        warnings.append(
            "OCR por IA no disponible temporalmente (timeout/red/proveedor). Se usa solo extraccion local."
        )
        return "", warnings

    text = _read_openai_response_text(response)
    if not text.strip():
        warnings.append("OCR por IA no devolvio texto legible.")
    return text.strip(), warnings


def _read_openai_response_text(response: Any) -> str:
    text = getattr(response, "output_text", None)
    if isinstance(text, str) and text.strip():
        return text

    parts: list[str] = []
    output = getattr(response, "output", None) or []
    for item in output:
        contents = getattr(item, "content", None) or []
        for chunk in contents:
            chunk_text = getattr(chunk, "text", None)
            if isinstance(chunk_text, str) and chunk_text.strip():
                parts.append(chunk_text)
                continue
            if isinstance(chunk, dict):
                maybe = chunk.get("text")
                if isinstance(maybe, str) and maybe.strip():
                    parts.append(maybe)
    return "\n".join(parts).strip()


def _cleanup_temp(root: Path) -> None:
    try:
        if root.exists():
            for item in root.iterdir():
                if item.is_file():
                    item.unlink(missing_ok=True)
            root.rmdir()
    except Exception:
        current_app.logger.warning(
            "No se pudo limpiar temporal de OCR en %s", root.as_posix()
        )


def _load_blank_template_static_lines() -> tuple[set[str], list[str]]:
    warnings: list[str] = []
    raw_path = (current_app.config.get("INHUMATION_AI_BLANK_TEMPLATE_PATH") or "").strip()
    if not raw_path:
        return set(), warnings

    path = Path(raw_path)
    if not path.is_absolute():
        warnings.append(
            "La plantilla base debe ser una ruta absoluta (INHUMATION_AI_BLANK_TEMPLATE_PATH)."
        )
        return set(), warnings
    if not path.exists() or not path.is_file():
        warnings.append(
            "No se ha encontrado la plantilla base configurada para comparar campos fijos."
        )
        return set(), warnings

    try:
        mtime = path.stat().st_mtime
    except Exception:
        warnings.append("No se ha podido leer metadata de la plantilla base.")
        return set(), warnings

    cache_key = path.as_posix()
    with _TEMPLATE_CACHE_LOCK:
        cached = _TEMPLATE_CACHE.get(cache_key)
        if cached and float(cached.get("mtime", 0.0)) == float(mtime):
            return set(cached.get("static_lines", set())), warnings

    template_text = _extract_pdf_text_local(path)
    if not template_text.strip():
        warnings.append(
            "La plantilla base esta configurada, pero no se ha podido extraer su texto."
        )
        return set(), warnings

    static_lines = _text_to_static_lines(template_text)
    with _TEMPLATE_CACHE_LOCK:
        _TEMPLATE_CACHE[cache_key] = {
            "mtime": float(mtime),
            "static_lines": set(static_lines),
        }
    return static_lines, warnings


def _text_to_static_lines(text: str) -> set[str]:
    lines: set[str] = set()
    for raw_line in (text or "").splitlines():
        normalized = _normalize_for_search(raw_line).strip("-/:;,. ")
        if len(normalized) < 4:
            continue
        lines.add(normalized)
    return lines


def _subtract_static_template(text: str, static_lines: set[str]) -> str:
    if not text.strip() or not static_lines:
        return text

    kept: list[str] = []
    for raw_line in text.splitlines():
        normalized = _normalize_for_search(raw_line).strip("-/:;,. ")
        if not normalized:
            continue
        if _line_has_explicit_value(raw_line):
            kept.append(raw_line)
            continue
        if _is_static_line(normalized, static_lines):
            continue
        kept.append(raw_line)
    return "\n".join(kept).strip()


def _line_has_explicit_value(raw_line: str) -> bool:
    if ":" not in raw_line:
        return False
    left, right = raw_line.split(":", 1)
    left_norm = _normalize_token(left)
    right_norm = _normalize_token(right)
    if not left_norm or not right_norm:
        return False
    if _looks_like_only_label(right_norm):
        return False
    return len(right_norm) >= 2


def _is_static_line(normalized_line: str, static_lines: set[str]) -> bool:
    if normalized_line in static_lines:
        return True

    if len(normalized_line) < 6:
        return False

    for static_line in static_lines:
        if abs(len(static_line) - len(normalized_line)) > 20:
            continue
        ratio = difflib.SequenceMatcher(None, normalized_line, static_line).ratio()
        if ratio >= 0.94:
            return True
    return False


def _openai_timeout_seconds() -> float:
    configured = current_app.config.get("INHUMATION_AI_OPENAI_TIMEOUT_SEC", 45)
    try:
        value = float(str(configured).strip())
    except Exception:
        value = 45.0
    return max(value, 5.0)


def _openai_max_output_tokens() -> int:
    configured = current_app.config.get("INHUMATION_AI_OPENAI_MAX_OUTPUT_TOKENS", 2500)
    try:
        value = int(str(configured).strip())
    except Exception:
        value = 2500
    return max(value, 200)


def _parse_fields(text: str) -> tuple[dict[str, Any], dict[str, float]]:
    clean_text = _clean_text(text)
    normalized_text = _normalize_for_search(clean_text)
    extracted: dict[str, Any] = {}
    confidence: dict[str, float] = {}

    doctor_name = _extract_doctor_name(clean_text)
    if doctor_name:
        extracted["medico_nombre"] = doctor_name
        confidence["medico_nombre"] = 0.86

    doctor_registered = _extract_near_label(
        clean_text,
        [r"colegiad[oa](?:/a)?\s+en"],
        max_chars=70,
    )
    if doctor_registered:
        extracted["medico_colegiado_en"] = doctor_registered
        confidence["medico_colegiado_en"] = 0.82

    doctor_number = _extract_doctor_number(clean_text)
    if doctor_number:
        extracted["medico_numero"] = doctor_number
        confidence["medico_numero"] = 0.9

    doctor_practice = _extract_near_label(
        clean_text,
        [r"ejercicio\s+profesional\s+en", r"exercici\s+professional\s+a"],
        max_chars=80,
    )
    if doctor_practice:
        extracted["medico_ejercicio"] = doctor_practice
        confidence["medico_ejercicio"] = 0.8

    full_name = _extract_full_name(clean_text)
    if full_name:
        extracted["nombre_completo_difunto"] = full_name
        confidence["nombre_completo_difunto"] = 0.78
        first, last, second = _split_spanish_name(full_name)
        if first:
            extracted["nombre_difunto"] = first
            confidence["nombre_difunto"] = 0.78
        if last:
            extracted["apellido1"] = last
            confidence["apellido1"] = 0.74
        if second:
            extracted["apellido2"] = second
            confidence["apellido2"] = 0.74

    direct_first = _extract_near_label(
        clean_text,
        [r"nombre\s+del\s+fallecid[oa]/a", r"nom\s+del\s+difunt/a"],
        max_chars=90,
    )
    if direct_first and len(direct_first.split()) <= 4:
        extracted["nombre_difunto"] = direct_first
        confidence["nombre_difunto"] = 0.81

    direct_last = _extract_near_label(
        clean_text,
        [r"1[.oºr]*\s+apellido\s+del\s+fallecid[oa]/a", r"1r\s+cognom"],
        max_chars=70,
    )
    if direct_last:
        extracted["apellido1"] = direct_last
        confidence["apellido1"] = 0.81

    direct_second_last = _extract_near_label(
        clean_text,
        [r"2[.oºn]*\s+apellido\s+del\s+fallecid[oa]/a", r"2n\s+cognom"],
        max_chars=70,
    )
    if direct_second_last:
        extracted["apellido2"] = direct_second_last
        confidence["apellido2"] = 0.81

    document_type, document_number, doc_conf = _extract_document(normalized_text)
    if document_number:
        extracted["documento_numero"] = document_number
        confidence["documento_numero"] = doc_conf
    if document_type:
        extracted["documento_tipo"] = document_type
        confidence["documento_tipo"] = doc_conf

    birth_date = _extract_date_after_labels(
        normalized_text,
        [
            "fecha de nacimiento",
            "data de naixement",
        ],
    )
    if birth_date:
        extracted["fecha_nacimiento"] = birth_date
        confidence["fecha_nacimiento"] = 0.87

    death_date = _extract_date_after_labels(
        normalized_text,
        [
            "fecha de la defuncion",
            "hora y fecha de la defuncion",
            "hora i data de la defuncio",
        ],
    )
    if death_date:
        extracted["fecha_defuncion"] = death_date
        confidence["fecha_defuncion"] = 0.88

    death_hour = _extract_time_after_labels(
        normalized_text,
        ["hora de la defuncion", "hora y fecha de la defuncion", "hora i data de la defuncio"],
    )
    if death_hour:
        extracted["hora_defuncion"] = death_hour
        confidence["hora_defuncion"] = 0.86

    sex = _extract_sex(normalized_text)
    if sex:
        extracted["sexo"] = sex
        confidence["sexo"] = 0.75

    death_place = _extract_death_place(normalized_text)
    if death_place:
        extracted["lugar_defuncion"] = death_place
        confidence["lugar_defuncion"] = 0.7

    immediate_cause = _extract_near_label(
        clean_text,
        [r"causa\s+inmediata"],
        max_chars=110,
    )
    if immediate_cause and not _looks_like_only_label(immediate_cause):
        extracted["causa_inmediata"] = immediate_cause
        confidence["causa_inmediata"] = 0.7

    antecedent_cause = _extract_near_label(
        clean_text,
        [r"causas?\s+antecedentes"],
        max_chars=110,
    )
    if antecedent_cause and not _looks_like_only_label(antecedent_cause):
        extracted["causa_antecedente"] = antecedent_cause
        confidence["causa_antecedente"] = 0.7

    root_cause = _extract_near_label(
        clean_text,
        [r"causa\s+inicial\s+o\s+fundamental"],
        max_chars=110,
    )
    if root_cause and not _looks_like_only_label(root_cause):
        extracted["causa_fundamental"] = root_cause
        confidence["causa_fundamental"] = 0.7

    death_consequence = _extract_death_consequence(normalized_text)
    if death_consequence:
        extracted["consecuencia_defuncion"] = death_consequence
        confidence["consecuencia_defuncion"] = 0.72

    return extracted, confidence


def _normalize_for_form(
    fields_extracted: dict[str, Any], field_confidence: dict[str, float]
) -> tuple[dict[str, Any], dict[str, float]]:
    normalized: dict[str, Any] = {}
    confidence: dict[str, float] = {}

    def set_value(form_name: str, value: Any, score: float) -> None:
        if value in (None, "", [], {}):
            return
        if isinstance(score, (int, float)) and float(score) < _AUTOFILL_CONFIDENCE_MIN:
            return
        normalized[form_name] = value
        confidence[form_name] = round(float(score), 2)

    set_value(
        "first_name",
        fields_extracted.get("nombre_difunto"),
        field_confidence.get("nombre_difunto", 0.0),
    )
    set_value(
        "last_name",
        fields_extracted.get("apellido1"),
        field_confidence.get("apellido1", 0.0),
    )
    set_value(
        "second_last_name",
        fields_extracted.get("apellido2"),
        field_confidence.get("apellido2", 0.0),
    )
    set_value(
        "document_type",
        fields_extracted.get("documento_tipo"),
        field_confidence.get("documento_tipo", 0.0),
    )
    set_value(
        "document_number",
        fields_extracted.get("documento_numero"),
        field_confidence.get("documento_numero", 0.0),
    )
    set_value(
        "sex",
        fields_extracted.get("sexo"),
        field_confidence.get("sexo", 0.0),
    )
    set_value(
        "death_place",
        fields_extracted.get("lugar_defuncion"),
        field_confidence.get("lugar_defuncion", 0.0),
    )
    set_value(
        "death_consequence",
        fields_extracted.get("consecuencia_defuncion"),
        field_confidence.get("consecuencia_defuncion", 0.0),
    )
    set_value(
        "doctor_name",
        fields_extracted.get("medico_nombre"),
        field_confidence.get("medico_nombre", 0.0),
    )
    set_value(
        "doctor_registered_in",
        fields_extracted.get("medico_colegiado_en"),
        field_confidence.get("medico_colegiado_en", 0.0),
    )
    set_value(
        "doctor_registration_number",
        fields_extracted.get("medico_numero"),
        field_confidence.get("medico_numero", 0.0),
    )
    set_value(
        "doctor_professional_practice",
        fields_extracted.get("medico_ejercicio"),
        field_confidence.get("medico_ejercicio", 0.0),
    )
    set_value(
        "immediate_cause_reason",
        fields_extracted.get("causa_inmediata"),
        field_confidence.get("causa_inmediata", 0.0),
    )
    set_value(
        "antecedent_cause_reason",
        fields_extracted.get("causa_antecedente"),
        field_confidence.get("causa_antecedente", 0.0),
    )
    set_value(
        "root_cause_reason",
        fields_extracted.get("causa_fundamental"),
        field_confidence.get("causa_fundamental", 0.0),
    )

    birth = fields_extracted.get("fecha_nacimiento")
    if isinstance(birth, dict):
        score = field_confidence.get("fecha_nacimiento", 0.0)
        set_value("birth_day", birth.get("day"), score)
        set_value("birth_month", birth.get("month"), score)
        set_value("birth_year", birth.get("year"), score)

    death = fields_extracted.get("fecha_defuncion")
    if isinstance(death, dict):
        score = field_confidence.get("fecha_defuncion", 0.0)
        set_value("death_day", death.get("day"), score)
        set_value("death_month", death.get("month"), score)
        set_value("death_year", death.get("year"), score)

    death_time = fields_extracted.get("hora_defuncion")
    if isinstance(death_time, dict):
        score = field_confidence.get("hora_defuncion", 0.0)
        set_value("death_hour", death_time.get("hour"), score)
        set_value("death_minute", death_time.get("minute"), score)

    return normalized, confidence


def _clean_text(text: str) -> str:
    raw = (text or "").replace("\r", "\n")
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw.strip()


def _normalize_for_search(text: str) -> str:
    stripped = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in stripped if unicodedata.category(ch) != "Mn")
    stripped = stripped.lower()
    stripped = re.sub(r"[ \t]+", " ", stripped)
    return stripped


def _normalize_token(text: str) -> str:
    value = unicodedata.normalize("NFD", text or "")
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", value).strip().lower()


def _extract_doctor_name(text: str) -> str:
    patterns = [
        r"certifico\s+la\s+defuncion\s+de\s*(?:d\.?\s*/?\s*d[ñn]a\.?|don|do[ñn]a|sr\.?\s*/?\s*sra\.?)?\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ'\-\.\s]{2,90}?)\s+en\s+medicina",
        r"certificat\s+medic.*?de\s+defuncio.*?de\s*(?:d\.?\s*/?\s*d[ñn]a\.?|sr\.?\s*/?\s*sra\.?)?\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ'\-\.\s]{2,90}?)\s+en\s+medicina",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        candidate = _clean_value(match.group(1))
        if candidate:
            return candidate
    return ""


def _extract_doctor_number(text: str) -> str:
    patterns = [
        r"(?:con\s+el\s+n[uú]mero|amb\s+el\s+n[uú]mero)\s*[:\-]?\s*([A-Z0-9\-\/]{3,20})",
        r"n[uú]mero\s+de\s+colegiad[oa]\s*[:\-]?\s*([A-Z0-9\-\/]{3,20})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        return _clean_value(match.group(1))
    return ""


def _extract_full_name(text: str) -> str:
    patterns = [
        r"nombre\s+del\s+fallecid[oa]/a\s*[:\-]?\s*([^\n]{4,100})",
        r"nom\s+del\s+difunt/a\s*[:\-]?\s*([^\n]{4,100})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        candidate = _clean_value(match.group(1))
        candidate_norm = _normalize_token(candidate)
        if _looks_like_only_label(candidate):
            continue
        if _looks_like_form_sentence(candidate_norm):
            continue
        if any(
            token in candidate_norm
            for token in ("fallecido", "fallecida", "apellido", "nombre del")
        ):
            continue
        if candidate:
            return candidate
    return ""


def _extract_near_label(text: str, patterns: list[str], max_chars: int = 80) -> str:
    for pattern in patterns:
        regex = re.compile(pattern + rf"\s*[:\-]?\s*([^\n]{{1,{max_chars}}})", re.IGNORECASE)
        match = regex.search(text)
        if not match:
            continue
        candidate = _clean_value(match.group(1))
        if candidate:
            return candidate
    return ""


def _split_spanish_name(full_name: str) -> tuple[str, str, str]:
    tokens = [part for part in full_name.split() if part]
    if len(tokens) < 3:
        return full_name, "", ""
    if len(tokens) == 3:
        return tokens[0], tokens[1], tokens[2]
    return " ".join(tokens[:-2]), tokens[-2], tokens[-1]


def _extract_document(normalized_text: str) -> tuple[str, str, float]:
    dni_nie_match = re.search(r"\b([xyz]\d{7}[a-z]|\d{8}[a-z])\b", normalized_text, re.IGNORECASE)
    if dni_nie_match:
        value = dni_nie_match.group(1).upper()
        doc_type = "NIE" if value[0] in {"X", "Y", "Z"} else "DNI"
        return doc_type, value, 0.92

    passport_match = re.search(r"\b([a-z0-9]{6,12})\b", normalized_text, re.IGNORECASE)
    if passport_match:
        value = passport_match.group(1).upper()
        if re.search(r"pasaporte|passaport", normalized_text):
            return "PASAPORTE", value, 0.68
    return "", "", 0.0


def _extract_date_after_labels(
    normalized_text: str, labels: list[str]
) -> dict[str, str] | None:
    for label in labels:
        label_match = re.search(re.escape(label), normalized_text, re.IGNORECASE)
        if not label_match:
            continue
        segment = normalized_text[label_match.end() : label_match.end() + 140]
        date_matches = list(
            re.finditer(r"(?=(\d{1,2})\D+(\d{1,2})\D+(\d{2,4}))", segment)
        )
        best: tuple[str, str, str] | None = None
        best_score = -1
        for item in date_matches:
            day, month, year_raw = item.groups()
            year_value = year_raw if len(year_raw) == 4 else _expand_year(year_raw)
            if not _valid_date_parts(day, month, year_value):
                continue
            score = 0
            if len(year_raw) == 4:
                score += 2
            if int(year_value) >= 1900:
                score += 1
            if score > best_score:
                best = (day, month, year_value)
                best_score = score
        if not best:
            continue
        day, month, year_value = best
        return {
            "day": str(int(day)),
            "month": str(int(month)),
            "year": str(int(year_value)),
        }
    return None


def _extract_time_after_labels(
    normalized_text: str, labels: list[str]
) -> dict[str, str] | None:
    for label in labels:
        pattern = re.compile(
            re.escape(label) + r"[^0-9]{0,25}([01]?\d|2[0-3])\D+([0-5]\d)",
            re.IGNORECASE,
        )
        match = pattern.search(normalized_text)
        if not match:
            continue
        hour, minute = match.groups()
        return {"hour": str(int(hour)), "minute": str(int(minute))}

    direct = re.search(r"\bhora\s*[:.]?\s*([01]?\d|2[0-3])\D+([0-5]\d)\b", normalized_text)
    if direct:
        hour, minute = direct.groups()
        return {"hour": str(int(hour)), "minute": str(int(minute))}
    return None


def _extract_sex(normalized_text: str) -> str:
    if re.search(r"sexo[^a-z0-9]{0,8}(varon|hombre|masculino)", normalized_text):
        return "M"
    if re.search(r"sexo[^a-z0-9]{0,8}(mujer|femenino)", normalized_text):
        return "F"
    if re.search(r"(x|✓|✔)\s*(varon|hombre|masculino)", normalized_text):
        return "M"
    if re.search(r"(x|✓|✔)\s*(mujer|femenino)", normalized_text):
        return "F"
    return ""


def _extract_death_place(normalized_text: str) -> str:
    marked_map = {
        "DOMICILIO_PARTICULAR": r"(x|✓|✔)\s*domicilio\s+particular",
        "CENTRO_HOSPITALARIO": r"(x|✓|✔)\s*centro\s+hospitalario",
        "RESIDENCIA_SOCIOSANITARIA": r"(x|✓|✔)\s*residencia\s+socio(?:\s|-)?sanitaria",
        "LUGAR_TRABAJO": r"(x|✓|✔)\s*lugar\s+de\s+trabajo",
        "OTRO": r"(x|✓|✔)\s*otro\s+lugar",
    }
    for code, pattern in marked_map.items():
        if re.search(pattern, normalized_text):
            return code
    return ""


def _extract_death_consequence(normalized_text: str) -> str:
    if re.search(r"(x|✓|✔)\s*accidente\s+de\s+trafico", normalized_text):
        return "ACCIDENTE_TRAFICO"
    if re.search(r"(x|✓|✔)\s*accidente\s+laboral", normalized_text):
        return "ACCIDENTE_LABORAL"
    if re.search(r"consecuencia[^.\n]{0,80}accidente\s+de\s+trafico", normalized_text):
        return "ACCIDENTE_TRAFICO"
    if re.search(r"consecuencia[^.\n]{0,80}accidente\s+laboral", normalized_text):
        return "ACCIDENTE_LABORAL"
    return ""


def _looks_like_only_label(value: str) -> bool:
    token = _normalize_token(value)
    if not token:
        return True
    generic = {
        "debido a",
        "causa",
        "causa inmediata",
        "causas antecedentes",
        "causa inicial o fundamental",
        "intervalo h d m a",
    }
    return token in generic


def _expand_year(year_2_or_4: str) -> str:
    if len(year_2_or_4) == 4:
        return year_2_or_4
    year = int(year_2_or_4)
    return str(1900 + year if year > 30 else 2000 + year)


def _valid_date_parts(day: str, month: str, year: str) -> bool:
    try:
        d = int(day)
        m = int(month)
        y = int(year)
    except Exception:
        return False
    if d < 1 or d > 31:
        return False
    if m < 1 or m > 12:
        return False
    if y < 1800 or y > 2200:
        return False
    return True


def _clean_value(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    cleaned = cleaned.strip(",:;.-")
    return cleaned


def _extract_doctor_registered_in(text: str) -> str:
    patterns = [
        r"colegiad[oa](?:/a)?\s+en\s*[:,]?\s*(.*?)\s*(?:,?\s+y\s+con\s+ejercicio|,?\s+i\s+amb\s+exercici|,?\s*con\s+el\s+n[uú]mero|,?\s*amb\s+el\s+n[uú]mero|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return _clean_value(match.group(1))
    return ""


def _extract_doctor_professional_practice(text: str) -> str:
    patterns = [
        r"(?:y\s+con\s+ejercicio\s+profesional\s+en|i\s+amb\s+exercici\s+professional\s+a)\s*[:,]?\s*(.*?)\s*(?:,?\s*con\s+el\s+n[uú]mero|,?\s*amb\s+el\s+n[uú]mero|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return _clean_value(match.group(1))
    return ""


def _extract_doctor_name(text: str) -> str:
    patterns = [
        r"certifico\s+la\s+defuncion\s+de\s*(.*?)\s+en\s+medicina",
        r"certificat\s+medic.*?de\s+defuncio.*?de\s*(.*?)\s+en\s+medicina",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            candidate = _clean_value(match.group(1))
            candidate = re.sub(
                r"^(?:d\.?\s*/?\s*d(?:na|n)\.?|d\.?\s*/?\s*d[ñn]a\.?|don|do[ñn]a|sr\.?\s*/?\s*sra\.?)\s*",
                "",
                candidate,
                flags=re.IGNORECASE,
            )
            candidate = re.sub(r"^[\W_]+", "", candidate)
            candidate = _clean_value(candidate)
            if candidate:
                return candidate
    return ""


def _extract_doctor_number(text: str) -> str:
    patterns = [
        r"(?:con\s+el\s+numero|amb\s+el\s+numero)\s*[:\-]?\s*([A-Z0-9\-\/]{3,20})",
        r"numero\s+de\s+colegiad[oa]\s*[:\-]?\s*([A-Z0-9\-\/]{3,20})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return _clean_value(match.group(1))
    return ""


def _extract_cause_value(text: str, label_patterns: list[str]) -> str:
    value = _extract_near_label(text, label_patterns, max_chars=130)
    if not value:
        return ""
    cleaned = re.sub(
        r"^[/\-\s]*(?:causa(?:s)?\s+inmediata|causas?\s+antecedentes|causa\s+inicial\s+o\s+fundamental)\s*",
        "",
        value,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(intervalo|horas?|dias?|meses?|anos?)\b.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return _clean_value(cleaned)


def _looks_like_only_label(value: str) -> bool:
    token = _normalize_token(value)
    if not token:
        return True
    generic = {
        "debido a",
        "causa",
        "causa inmediata",
        "causas antecedentes",
        "causa inicial o fundamental",
        "intervalo h d m a",
        "hora minutos",
        "dia mes ano",
        "nombre del fallecido/a",
        "primer apellido",
        "segundo apellido",
    }
    if token in generic:
        return True
    if token.startswith("causa ") and len(token.split()) <= 4:
        return True
    if token.startswith("fecha de"):
        return True
    return False


def _parse_fields(text: str) -> tuple[dict[str, Any], dict[str, float]]:
    fields, confidence, _warnings = _parse_fields_with_meta(
        text,
        static_lines=set(),
        strict=False,
    )
    return fields, confidence


def _parse_fields_with_meta(
    text: str,
    *,
    static_lines: set[str],
    strict: bool,
) -> tuple[dict[str, Any], dict[str, float], list[str]]:
    clean_text = _clean_text(text)
    normalized_text = _normalize_for_search(clean_text)
    extracted: dict[str, Any] = {}
    confidence: dict[str, float] = {}
    warnings: list[str] = []

    _set_candidate(
        extracted,
        confidence,
        warnings,
        "medico_nombre",
        _extract_doctor_name(clean_text),
        0.9,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "medico_colegiado_en",
        _extract_doctor_registered_in(clean_text),
        0.88,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "medico_numero",
        _extract_doctor_number(clean_text),
        0.92,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "medico_ejercicio",
        _extract_doctor_professional_practice(clean_text),
        0.86,
        static_lines=static_lines,
        strict=strict,
    )

    full_name = _extract_full_name(clean_text)
    if full_name:
        first, last, second = _split_spanish_name(full_name)
        _set_candidate(
            extracted,
            confidence,
            warnings,
            "nombre_difunto",
            first,
            0.8,
            static_lines=static_lines,
            strict=strict,
        )
        _set_candidate(
            extracted,
            confidence,
            warnings,
            "apellido1",
            last,
            0.78,
            static_lines=static_lines,
            strict=strict,
        )
        _set_candidate(
            extracted,
            confidence,
            warnings,
            "apellido2",
            second,
            0.78,
            static_lines=static_lines,
            strict=strict,
        )

    _set_candidate(
        extracted,
        confidence,
        warnings,
        "nombre_difunto",
        _extract_near_label(
            clean_text,
            [r"nombre\s+del\s+fallecid[oa]/a", r"nom\s+del\s+difunt/a"],
            max_chars=90,
        ),
        0.82,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "apellido1",
        _extract_near_label(
            clean_text,
            [
                r"1(?:\s*[.ºo0]\s*)?apellido\s+del\s+fallecid[oa]/a",
                r"1r\s+cognom",
            ],
            max_chars=70,
        ),
        0.82,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "apellido2",
        _extract_near_label(
            clean_text,
            [
                r"2(?:\s*[.ºo0]\s*)?apellido\s+del\s+fallecid[oa]/a",
                r"2n\s+cognom",
            ],
            max_chars=70,
        ),
        0.82,
        static_lines=static_lines,
        strict=strict,
    )

    document_type, document_number, doc_conf = _extract_document(normalized_text)
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "documento_tipo",
        document_type,
        doc_conf,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "documento_numero",
        document_number,
        doc_conf,
        static_lines=static_lines,
        strict=strict,
    )

    birth_date = _extract_date_after_labels(
        normalized_text,
        ["fecha de nacimiento", "data de naixement"],
    )
    if birth_date:
        extracted["fecha_nacimiento"] = birth_date
        confidence["fecha_nacimiento"] = 0.9

    death_date = _extract_date_after_labels(
        normalized_text,
        [
            "fecha de la defuncion",
            "hora y fecha de la defuncion",
            "hora i data de la defuncio",
        ],
    )
    if death_date:
        extracted["fecha_defuncion"] = death_date
        confidence["fecha_defuncion"] = 0.9

    death_hour = _extract_time_after_labels(
        normalized_text,
        [
            "hora de la defuncion",
            "hora y fecha de la defuncion",
            "hora i data de la defuncio",
        ],
    )
    if death_hour:
        extracted["hora_defuncion"] = death_hour
        confidence["hora_defuncion"] = 0.88

    _set_candidate(
        extracted,
        confidence,
        warnings,
        "sexo",
        _extract_sex(normalized_text),
        0.8,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "lugar_defuncion",
        _extract_death_place(normalized_text),
        0.78,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "consecuencia_defuncion",
        _extract_death_consequence(normalized_text),
        0.78,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "causa_inmediata",
        _extract_cause_value(clean_text, [r"causa\s+inmediata"]),
        0.8,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "causa_antecedente",
        _extract_cause_value(clean_text, [r"causas?\s+antecedentes"]),
        0.8,
        static_lines=static_lines,
        strict=strict,
    )
    _set_candidate(
        extracted,
        confidence,
        warnings,
        "causa_fundamental",
        _extract_cause_value(clean_text, [r"causa\s+inicial\s+o\s+fundamental"]),
        0.8,
        static_lines=static_lines,
        strict=strict,
    )

    return extracted, confidence, _dedupe_warnings(warnings)


def _normalize_for_form(
    fields_extracted: dict[str, Any], field_confidence: dict[str, float]
) -> tuple[dict[str, Any], dict[str, float]]:
    normalized, confidence, _warnings = _normalize_for_form_with_meta(
        fields_extracted,
        field_confidence,
        static_lines=set(),
        min_confidence=_DEFAULT_MIN_CONFIDENCE,
        strict=False,
    )
    return normalized, confidence


def _normalize_for_form_with_meta(
    fields_extracted: dict[str, Any],
    field_confidence: dict[str, float],
    *,
    static_lines: set[str],
    min_confidence: float,
    strict: bool,
) -> tuple[dict[str, Any], dict[str, float], list[str]]:
    normalized: dict[str, Any] = {}
    confidence: dict[str, float] = {}
    warnings: list[str] = []

    def set_value(form_name: str, semantic_key: str) -> None:
        value = fields_extracted.get(semantic_key)
        score = float(field_confidence.get(semantic_key, 0.0) or 0.0)
        if value in (None, "", [], {}):
            return
        if score < min_confidence:
            return
        if isinstance(value, str):
            allowed, reason = _candidate_allowed(
                semantic_key,
                value,
                static_lines=static_lines,
                strict=strict,
            )
            if not allowed:
                warnings.append(
                    f"Valor descartado para {semantic_key}: {reason}."
                )
                return
        normalized[form_name] = value
        confidence[form_name] = round(score, 2)

    mapping = [
        ("first_name", "nombre_difunto"),
        ("last_name", "apellido1"),
        ("second_last_name", "apellido2"),
        ("document_type", "documento_tipo"),
        ("document_number", "documento_numero"),
        ("sex", "sexo"),
        ("death_place", "lugar_defuncion"),
        ("death_consequence", "consecuencia_defuncion"),
        ("doctor_name", "medico_nombre"),
        ("doctor_registered_in", "medico_colegiado_en"),
        ("doctor_registration_number", "medico_numero"),
        ("doctor_professional_practice", "medico_ejercicio"),
        ("immediate_cause_reason", "causa_inmediata"),
        ("antecedent_cause_reason", "causa_antecedente"),
        ("root_cause_reason", "causa_fundamental"),
    ]
    for form_name, semantic_key in mapping:
        set_value(form_name, semantic_key)

    birth = fields_extracted.get("fecha_nacimiento")
    if isinstance(birth, dict):
        score = float(field_confidence.get("fecha_nacimiento", 0.0) or 0.0)
        if score >= min_confidence:
            for key in ("day", "month", "year"):
                val = birth.get(key)
                if val not in (None, ""):
                    normalized[f"birth_{key}"] = val
                    confidence[f"birth_{key}"] = round(score, 2)

    death = fields_extracted.get("fecha_defuncion")
    if isinstance(death, dict):
        score = float(field_confidence.get("fecha_defuncion", 0.0) or 0.0)
        if score >= min_confidence:
            for key in ("day", "month", "year"):
                val = death.get(key)
                if val not in (None, ""):
                    normalized[f"death_{key}"] = val
                    confidence[f"death_{key}"] = round(score, 2)

    death_time = fields_extracted.get("hora_defuncion")
    if isinstance(death_time, dict):
        score = float(field_confidence.get("hora_defuncion", 0.0) or 0.0)
        if score >= min_confidence:
            hour = death_time.get("hour")
            minute = death_time.get("minute")
            if hour not in (None, ""):
                normalized["death_hour"] = hour
                confidence["death_hour"] = round(score, 2)
            if minute not in (None, ""):
                normalized["death_minute"] = minute
                confidence["death_minute"] = round(score, 2)

    return normalized, confidence, _dedupe_warnings(warnings)


def _set_candidate(
    extracted: dict[str, Any],
    confidence: dict[str, float],
    warnings: list[str],
    key: str,
    value: str,
    score: float,
    *,
    static_lines: set[str],
    strict: bool,
) -> None:
    candidate = _clean_value(value)
    if not candidate:
        return
    allowed, reason = _candidate_allowed(
        key,
        candidate,
        static_lines=static_lines,
        strict=strict,
    )
    if not allowed:
        warnings.append(f"Valor descartado para {key}: {reason}.")
        return
    extracted[key] = candidate
    confidence[key] = float(score)


def _candidate_allowed(
    key: str,
    value: str,
    *,
    static_lines: set[str],
    strict: bool,
) -> tuple[bool, str]:
    normalized = _normalize_token(value)
    if not normalized:
        return False, "valor vacio"
    if _looks_like_only_label(normalized):
        return False, "parece etiqueta de formulario"
    if _looks_like_form_sentence(normalized):
        return False, "parece titulo/cabecera del formulario"
    if static_lines and _is_static_phrase(normalized, static_lines):
        return False, "coincide con texto fijo de la plantilla"
    if strict and key.startswith("causa_") and len(normalized) <= 2:
        return False, "valor de causa demasiado corto"
    return True, ""


def _is_static_phrase(normalized_value: str, static_lines: set[str]) -> bool:
    if normalized_value in static_lines:
        return True
    if len(normalized_value) < 6:
        return False
    for static_line in static_lines:
        if abs(len(static_line) - len(normalized_value)) > 20:
            continue
        ratio = difflib.SequenceMatcher(None, normalized_value, static_line).ratio()
        if ratio >= 0.93:
            return True
    return False


def _looks_like_form_sentence(value: str) -> bool:
    for keyword in _LABEL_KEYWORDS:
        if keyword in value:
            return True
    words = value.split()
    if len(words) >= 5:
        generic_words = {"dia", "mes", "ano", "hora", "minutos", "intervalo"}
        generic_hits = sum(1 for word in words if word in generic_words)
        if generic_hits >= 2:
            return True
    return False


def _dedupe_warnings(warnings: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for warning in warnings:
        item = str(warning or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
