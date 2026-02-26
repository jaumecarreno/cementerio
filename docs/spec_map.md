# Spec Map - Modulo Cementerio MVP real

Source of truth:
- `spec/GSF_v1.0.14.doc`
- `spec/mockups/page-*.png`
- `spec/mockups_v2/page-*.png`

## Mapeo seccion -> implementacion

| Spec | Funcionalidad | Pantalla / Ruta | Notas |
|---|---|---|---|
| 9.4.1 Alta de sepultures | Alta masiva por bloque, estado inicial `LLIURE` | `/cementerio/sepulturas/alta-masiva` | Incluye validacion + previsualizacion + creacion |
| 9.4.2 Canvi d'estat de les sepultures | Cambio de estado manual (excepto `OCUPADA`) | `/cementerio/sepulturas/<id>` | `OCUPADA` se asigna al crear contrato |
| 9.4.3 / 9.4.4 / 9.4.5 | Tabs de ficha (Titulares/Beneficiarios/Movimientos) | `/cementerio/sepulturas/<id>` | Filtrado de movimientos y trazabilidad |
| 9.1.7.1 Contractacio (Concessio) | Alta contrato CONCESION | `POST /cementerio/sepulturas/<id>/derecho/contratar` | Limite 50 anos (legacy 99 con flag) |
| 9.1.7.2 Contractacio us immediat (lloguer) | Alta contrato USO_INMEDIATO (Lloguer) | `POST /cementerio/sepulturas/<id>/derecho/contratar` | Limite 25 anos |
| 9.1.7.3 / 9.1.7.4 | Ampliacion / Prorroga | Tab Derecho funerario | Acciones visibles en disabled para MVP |
| 9.1.4 Generacio del titol | Titulo de derecho funerario en PDF on-demand | `GET /cementerio/contratos/<id>/titulo.pdf` | Registra movimiento tipo `CONTRATO` |
| 9.1.6 Nomenament de beneficiari | Alta rapida de beneficiario desde cobro | `POST /cementerio/contratos/<id>/beneficiario/nombrar` | CTA cuando no hay beneficiario |
| 5.2.5.2.2 Facturacio taxes manteniment | Generacion anual de tiquets por concesion | CLI `tickets-generate-year` + `POST /cementerio/admin/tickets/generar` | Idempotente, solo concesiones activas a 1 de enero |
| 9.1.3 Cobrament de taxes + 5.3.4 | Cobro en criterio de caja, facturas impagadas separadas y regla de antiguedad | `/cementerio/tasas/cobro?sepultura_id=<id>` + `POST /cementerio/tasas/cobro/cobrar` | Factura se crea en el momento de cobro |

## Mockups aplicados

- `mockups_v2/page-2.png` -> diseno `Panel`.
- `mockups/page-3.png` -> diseno `Buscar sepulturas` (faltante en v2).
- `mockups_v2/page-4.png` -> diseno `Ficha sepultura`.
- `mockups_v2/page-5.png` -> diseno `Cobro tasas`.
- `mockups/page-6.png` -> diseno `Alta masiva`.
- `mockups_v2/page-1.png` -> menu interno de Cementerio.
