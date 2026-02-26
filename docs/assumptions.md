# Assumptions

## ASSUMPTION-001 Mockups incompletos en v2
- `spec/mockups_v2` no contiene `page-3`.
- Accion: busqueda de sepulturas tomada de `spec/mockups/page-3.png`.

## ASSUMPTION-002 Bilingue minimo
- ES/CAT implementado con diccionario local y sesion.
- Queda fuera un motor i18n completo.

## ASSUMPTION-003 USO_INMEDIATO en UI
- El enum persistido sigue siendo `USO_INMEDIATO`.
- En UI se etiqueta como `LLOGUER` para alineacion funcional.

## ASSUMPTION-004 Legacy 99 anos
- Nuevas concesiones >50 solo se permiten con `legacy_99_years=true`.
- No se cambian datos legacy existentes.

## ASSUMPTION-005 Estado FACTURADO
- `TicketEstado.FACTURADO` se mantiene por compatibilidad historica.
- Nuevos flujos de mantenimiento operan con `PENDIENTE` y `COBRADO`.

## ASSUMPTION-006 Criterio de caja
- Para tasas de mantenimiento, la factura se emite en el momento del cobro.
- El boton de facturar previo se deshabilita funcionalmente.

## ASSUMPTION-007 Titulo PDF
- El titulo del derecho funerario se genera on-demand.
- No se almacena binario del PDF en base de datos.

## ASSUMPTION-008 Descuento pensionista
- El porcentaje se configura por organizacion (`organization.pensionista_discount_pct`).
- Para anos previos a `pensionista_desde`, el descuento se aplica solo si el usuario lo marca.

## ASSUMPTION-009 Alta de contrato
- Solo se permite contratar sobre sepulturas en estado `DISPONIBLE`.
- `OCUPADA` se sigue asignando por evento al crear contrato.
