# Decisions (ADR breve)

## ADR-008 Source of truth de titularidad
- Decision: reemplazar `titularidad` por `ownership_record` como fuente unica de titular actual + historico.
- Motivo: modelar cierre/apertura de titular en transmisiones con regla de "1 titular actual por contrato".

## ADR-009 Pensionista en ownership, no en contrato
- Decision: la condicion pensionista vive en `OwnershipRecord` (`is_pensioner`, `pensioner_since_date`).
- Motivo:
  - el pensionista es una propiedad del titular, no del contrato.
  - conserva historico cuando cambia titular.
  - permite recalculo de tasas por titular vigente en una fecha.

## ADR-010 Expediente de transmision separado del contrato
- Decision: `OwnershipTransferCase` + `OwnershipTransferParty` + `CaseDocument` + `Publication`.
- Motivo: separar estado administrativo/documental de la entidad contractual.

## ADR-011 Auditoria dual
- Decision: registrar eventos en `MovimientoSepultura` y `ContractEvent`.
- Motivo:
  - `MovimientoSepultura` mantiene visibilidad operativa en ficha de sepultura.
  - `ContractEvent` ofrece traza administrativa del expediente.

## ADR-012 Numeracion administrativa
- Decision: numeracion anual por org:
  - casos: `TR-AAAA-####`
  - resoluciones: `RES-AAAA-####`
- Motivo: legibilidad y trazabilidad administrativa sin tabla extra de secuencias.

## ADR-013 Beneficiario al cierre
- Decision: si existe beneficiario activo al cerrar, exigir decision explicita `KEEP` o `REPLACE`.
- Motivo: evitar borrado implicito y forzar confirmacion operativa.
