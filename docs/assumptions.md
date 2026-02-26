# Assumptions

## ASSUMPTION-001 Mockups incompletos en v2
- `spec/mockups_v2` no contiene `page-3`.
- Acción: búsqueda de sepulturas se toma de `spec/mockups/page-3.png`.

## ASSUMPTION-002 Bilingüe mínimo
- ES/CAT se implementa con diccionario local de claves y cambio por sesión.
- Queda fuera un motor i18n completo con catálogos externos.

## ASSUMPTION-003 Expedientes y derechos funerarios
- Se implementan entidades y navegación preparada.
- Flujos completos de expedientes/transmisiones/expropiaciones quedan fuera del MVP funcional.

## ASSUMPTION-004 Cambio manual de estado
- `OCUPADA` no se permite manualmente.
- Se fuerza al crear contrato conforme a Spec 9.4.2.

## ASSUMPTION-005 Recibo MVP
- El recibo se entrega como vista HTML imprimible.
- Formato fiscal definitivo queda para iteración posterior.

## ASSUMPTION-006 .doc/.docx/.pdf
- Referencia principal funcional: `spec/GSF_v1.0.14.doc`.
- `.docx` y `.pdf` se usan solo como apoyo de lectura.

## ASSUMPTION-007 Alcance de regla antigüedad
- La validación de prefijo contiguo se aplica sobre tiquets no facturados seleccionables.
- Las facturas impagadas se muestran separadas y no entran en esa selección.

## ASSUMPTION-008 Alta masiva y numeración
- La numeración interna de alta masiva se calcula secuencialmente por rango de fila/columna en el formulario.
- Si una ubicación ya existe, se omite sin abortar el lote.
