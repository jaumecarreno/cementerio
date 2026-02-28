# Pasada de consistencia de textos UI

## 1) Inventario de cadenas hardcodeadas en `app/templates/`

Se realizó un inventario de cadenas visibles no internacionalizadas en `app/templates/`, priorizando `app/templates/cemetery/`:

- **Títulos/página**: `Resumen`, `Actividad reciente`, `Recibo de cobro`, `Alta masiva...`, `Lápidas e inscripciones`.
- **Etiquetas de formularios**: `Contrato`, `Sepultura ID`, `Fecha prevista`, `Documento`, `Cantidad`, `Forma de pago`.
- **Botones y CTAs**: `Crear`, `Guardar`, `Actualizar estado`, `Registrar entrada/salida`, `Imprimir`, `Abrir`.
- **Estados vacíos**: `Sin resultados`, `Sin partes`, `No hay personas...`.

Plantillas con mayor concentración de hardcode previo:

- `app/templates/cemetery/personas.html`
- `app/templates/cemetery/person_form.html`
- `app/templates/cemetery/mass_create.html`
- `app/templates/cemetery/_mass_preview.html`
- `app/templates/cemetery/ownership_case_detail.html`
- `app/templates/cemetery/_ownership_cases_table.html`
- `app/templates/cemetery/expediente_detail.html`
- `app/templates/cemetery/receipt.html`
- `app/templates/cemetery/detail.html`
- `app/templates/cemetery/lapidas.html`

## 2) Normalización ortográfica y por idioma (es/ca)

Se corrigieron textos y acentuación en UI y se centralizaron variantes ES/CA en i18n:

- `Telefono` → `Teléfono`.
- `Direccion` → `Dirección`.
- `Resolucion` → `Resolución`.
- `Lapidas` / `inscripcion` → `Lápidas` / `inscripción`.
- `Concesion ... anos` → `Concesión ... años`.

Además, se unificaron términos de navegación y etiquetas con claves compartidas para español/catalán.

## 3) Textos frecuentes movidos a i18n

Se añadieron claves nuevas en `app/core/i18n.py` y se aplicaron en templates:

- Comunes (`common.summary`, `common.update_status`, `common.work_orders`, etc.).
- Campos (`field.contract`, `field.grave`, `field.expected_date`, `field.code`, etc.).
- Vistas (`mass_create.*`, `people.*`, `ownership.*`, `receipt.*`, `panel.recent_activity`).

## 4) Glosario aplicado

Glosario corto de términos funcionales:

- **Derecho funerario** / **Dret funerari**
- **Titularidad** / **Titularitat**
- **Expediente** / **Expedient**

Criterio de uso: estos términos se usan de forma consistente en títulos, secciones, tablas y acciones relevantes.

## 5) Títulos y botones: verbos claros y consistentes

Se homogenizó el estilo de acciones:

- Verbos base: **Crear**, **Guardar**, **Abrir**, **Actualizar**, **Registrar**, **Imprimir**.
- CTAs ambiguos o abreviados se reemplazaron por acciones explícitas cuando aplicaba.
