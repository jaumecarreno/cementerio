# Spec Map - Módulo Cementerio MVP

Source of truth:
- `spec/GSF_v1.0.14.doc`
- `spec/mockups/page-*.png`
- `spec/mockups_v2/page-*.png`

## Mapeo sección -> implementación

| Spec | Funcionalidad | Pantalla / Ruta | Notas |
|---|---|---|---|
| 9.4.1 Alta de sepultures | Alta masiva por bloque, estado inicial `LLIURE` | `/cementerio/sepulturas/alta-masiva` | Incluye validación + previsualización + creación |
| 9.4.2 Canvi d'estat de les sepultures | Cambio de estado manual (excepto `OCUPADA`) | `/cementerio/sepulturas/<id>` | `OCUPADA` se asigna al crear contrato |
| 9.4.3 Consulta de titulars | Consulta de titulares históricos/activos | Tab `Titulares` en `/cementerio/sepulturas/<id>` | Solo consulta/edición mínima en MVP |
| 9.4.4 Consulta de beneficiaris | Consulta de beneficiarios | Tab `Beneficiarios` en `/cementerio/sepulturas/<id>` | Solo consulta en MVP |
| 9.4.5 Consulta de moviments | Lista filtrable de movimientos | Tab `Movimientos` en `/cementerio/sepulturas/<id>` | Incluye tipos principales |
| 9.1.3 Cobrament de taxes | Cobro de tasas con regla antigüedad | `/cementerio/tasas/cobro?sepultura_id=<id>` | Separa tiquets no facturados vs facturas impagadas |
| 5.3.4 Cobrament taxes manteniment sepultures | Flujo buscar -> comprobar pendientes -> facturar/cobrar | Buscar + Ficha + Cobro | Pensionista no retroactivo y aviso de beneficiario |
| 9.1.7.1 / 9.1.7.2 | Tipos de derecho funerario + límites 50/25 | Modelo `DerechoFunerarioContrato` | Validación de duración |
| 9.1.5 / 9.1.6 | Titularidad/beneficiarios y transmisiones | Navegación preparada (MVP parcial) | Flujo completo fuera de alcance MVP |

## Mockups aplicados

- `mockups_v2/page-2.png` -> diseño `Panel`.
- `mockups/page-3.png` -> diseño `Buscar sepulturas` (faltante en v2).
- `mockups_v2/page-4.png` -> diseño `Ficha sepultura`.
- `mockups_v2/page-5.png` -> diseño `Cobro tasas`.
- `mockups/page-6.png` -> diseño `Alta masiva`.
- `mockups_v2/page-1.png` -> menú interno de Cementerio.
