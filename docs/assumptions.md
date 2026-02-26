# Assumptions - Titularidad/Transmisiones

## ASSUMPTION-010 Checklist documental MVP
La spec define requisitos por tipo de transmision, pero no normaliza catalogo tecnico de `doc_type`.
Se implementa un checklist fijo reversible por tipo:

- `MORTIS_CAUSA_TESTAMENTO`:
  - required: `CERT_DEFUNCION`, `TITULO_SEPULTURA`, `SOLICITUD_CAMBIO_TITULARIDAD`, `CERT_ULTIMAS_VOLUNTADES`, `TESTAMENTO_O_ACEPTACION_HERENCIA`
  - opcional: `CESION_DERECHOS`, `SOLICITUD_BENEFICIARIO`, `DNI_NUEVO_BENEFICIARIO`
- `MORTIS_CAUSA_SIN_TESTAMENTO`:
  - required: `CERT_DEFUNCION`, `TITULO_SEPULTURA`, `SOLICITUD_CAMBIO_TITULARIDAD`, `CERT_ULTIMAS_VOLUNTADES`
  - opcional: `LIBRO_FAMILIA_O_TESTIGOS`, `CESION_DERECHOS`, `SOLICITUD_BENEFICIARIO`, `DNI_NUEVO_BENEFICIARIO`
- `INTER_VIVOS`:
  - required: `SOLICITUD_CAMBIO_TITULARIDAD`, `TITULO_SEPULTURA`, `DNI_TITULAR_ACTUAL`, `DNI_NUEVO_TITULAR`
  - opcional: `SOLICITUD_BENEFICIARIO`, `DNI_NUEVO_BENEFICIARIO`
- `PROVISIONAL`:
  - required: `SOLICITUD_CAMBIO_TITULARIDAD`, `ACEPTACION_SMSFT`, `PUBLICACION_BOP`, `PUBLICACION_DIARIO`
  - opcional: `SOLICITUD_BENEFICIARIO`, `DNI_NUEVO_BENEFICIARIO`

## ASSUMPTION-011 Inter-vivos sin validacion de parentesco automatica
La spec limita a familiares de hasta 2o grado, pero no existe modelo de parentesco.
En MVP no se valida parentesco automaticamente ni documentalmente.

## ASSUMPTION-012 Provisional sin bloqueo operativo
Se guarda vigencia (`provisional_until`) y publicaciones.
No se implementan bloqueos de inhumacion/exhumacion en esta PR.

## ASSUMPTION-013 Resolucion PDF simple
El PDF de resolucion es un documento simple on-demand con contenido minimo.
No incorpora plantilla oficial firmada ni firma digital en MVP.

## ASSUMPTION-014 Storage local
Documentos de caso y resoluciones se guardan localmente en:
`instance/storage/cemetery/ownership_cases/<org_id>/<case_id>/...`

## ASSUMPTION-015 Reapertura funcional de rechazados
Un caso `REJECTED` puede volver a `DOCS_PENDING` para retramitacion.
