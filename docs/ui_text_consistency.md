# Pasada de consistencia de textos UI

## 1) Inventario de cadenas hardcodeadas en `app/templates/`

Se revisaron las vistas y se encontraron grupos de cadenas repetidas hardcodeadas:

- Navegación y títulos: `Cementerio > ...`, `Buscar`, `Listado`, `Resumen`.
- Acciones: `Abrir`, `Crear`, `Guardar`, `Filtrar`, `Limpiar`, `Subir`, `Descargar`, `Rechazar`.
- Campos recurrentes: `Tipo`, `Estado`, `Bloque`, `Número`, `Fila`, `Columna`, `Difunto`, `Declarante`, `Notas`.
- Estados vacíos: `Sin resultados`, `Sin casos`, `Sin expedientes`.

Plantillas con mayor concentración:

- `app/templates/cemetery/search.html`
- `app/templates/cemetery/_search_results.html`
- `app/templates/cemetery/fees_search.html`
- `app/templates/cemetery/_fees_search_results.html`
- `app/templates/cemetery/expedientes.html`
- `app/templates/cemetery/expediente_detail.html`
- `app/templates/cemetery/ownership_cases.html`
- `app/templates/cemetery/_ownership_cases_table.html`
- `app/templates/cemetery/ownership_case_detail.html`

## 2) Normalización ortográfica y por idioma (es/ca)

Se movieron textos frecuentes a i18n y se corrigieron términos visibles desde los catálogos:

- `Derechos funerarios` → `Derecho funerario` (es) y `Dret funerari` (ca).
- Unificación de acciones cortas (`Abrir`, `Guardar`, `Filtrar`, etc.).
- Etiquetas de campos normalizadas en español/catalán desde claves compartidas.

## 3) Glosario funcional aplicado

- **Derecho funerario** / **Dret funerari**
- **Titularidad** / **Titularitat**
- **Expediente** / **Expedient**

Uso esperado: emplear siempre estos términos en títulos, botones y tablas de vistas de cementerio.
