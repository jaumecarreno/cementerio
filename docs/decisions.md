# Decisions (ADR breve)

## ADR-001 Stack backend/frontend
- Decisión: `Flask + SQLAlchemy + Alembic + Flask-Login + Jinja2 + HTMX`.
- Motivo: entrega rápida server-rendered con baja complejidad para backoffice.

## ADR-002 Multi-tenant MVP
- Decisión: 1 organización activa por usuario (membership única usada en sesión).
- Motivo: aislamiento fuerte y mínimo riesgo de mezcla de datos.
- Impacto: todas las consultas de negocio filtran por `org_id`.

## ADR-003 Modelo de persona/difunto
- Decisión: entidad `Person` reutilizable + relación `SepulturaDifunto`.
- Motivo: titular, beneficiario, difunto y declarante comparten estructura básica.

## ADR-004 Contratos y límites legales
- Decisión: `DerechoFunerarioContrato` con enum `CONCESION`/`USO_INMEDIATO`.
- Regla: duración máxima 50 años concesión, 25 años uso inmediato.
- Motivo: alinear con capítulo 9 y reglas de negocio del módulo.

## ADR-005 Estados de sepultura exactos
- Decisión: enum exacto `LLIURE`, `DISPONIBLE`, `OCUPADA`, `INACTIVA`, `PROPIA`.
- Regla: `OCUPADA` no se asigna manualmente; se provoca por contrato.

## ADR-006 Cobro de tasas
- Decisión: separación explícita de pendientes:
  - Tiquets no facturados (`TicketEstado.PENDIENTE`).
  - Facturas impagadas (`InvoiceEstado.IMPAGADA`).
- Regla: selección por prefijo contiguo desde año pendiente más antiguo.
- Regla pensionista: descuento solo desde `pensionista_desde` (no retroactivo).

## ADR-007 UI bilingüe ES/CAT
- Decisión: i18n mínima basada en diccionario y sesión (`/auth/lang`).
- Motivo: cumplir requisito de interfaz bilingüe MVP sin sobreingeniería.
