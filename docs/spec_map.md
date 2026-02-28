# Spec Map - Titularidad, Transmisiones y Beneficiarios

Source of truth:
- `spec/GSF-reducido a cementerio.pdf` (principal)
- `spec/cementerio_extract.md` (digest operativo)
- `spec/GSF_v1.0.14.docx` (respaldo)

## Mapeo seccion -> implementacion

| Spec | Funcionalidad | Ruta / Servicio | Modelo |
|---|---|---|---|
| 9.1.5 | Transmisiones de titularidad | `GET/POST /cementerio/titularidad/casos` | `OwnershipTransferCase` |
| 9.1.5.1 | Mortis-causa con testamento | Alta de caso `type=MORTIS_CAUSA_TESTAMENTO` | `OwnershipTransferCase`, `CaseDocument` |
| 9.1.5.2 | Inter-vivos | Alta de caso `type=INTER_VIVOS` | `OwnershipTransferCase`, `OwnershipTransferParty` |
| 9.1.5.3 | Mortis-causa con beneficiario | Cierre de caso + decision beneficiario `KEEP/REPLACE` | `OwnershipRecord`, `Beneficiario` |
| 9.1.5.4 | Transmision provisional | Publicaciones + vigencia 10 anos | `Publication`, `OwnershipRecord.is_provisional` |
| 9.1.6 | Nomenamiento de beneficiario | `POST /cementerio/contratos/<id>/beneficiario/nombrar` + cierre de caso | `Beneficiario` |
| 9.4.4 | Consulta de beneficiarios | Ficha de sepultura tab `Titularidad` + tab `Beneficiarios` | `Beneficiario` |
| 9.4.5 | Consulta de movimientos | Ficha de sepultura tab `Movimientos` | `MovimientoSepultura` |
| 9.1.4 | Documento final | `GET /cementerio/titularidad/casos/<id>/resolucion.pdf` | `OwnershipTransferCase.resolution_pdf_path` |

## Reglas implementadas

- Un unico titular actual por contrato (`ownership_record.end_date IS NULL`) mediante indice unico parcial.
- Un unico beneficiario activo por contrato (`beneficiario.activo_hasta IS NULL`) mediante indice unico parcial.
- Una sepultura puede tener 0..n difuntos asociados mediante `sepultura_difunto`.
- Cierre de caso aprobado aplica titularidad en transaccion:
  - cierre de titular anterior
  - alta de nuevo titular
  - movimiento `CAMBIO_TITULARIDAD`
  - `ContractEvent` espejo
- Cierre bloqueado si faltan documentos obligatorios en `VERIFIED`.
- En caso provisional, cierre bloqueado sin publicaciones en `BOP` y otro canal.

## Modelo funcional reorganizado (titular, beneficiario y difuntos)

### 1) Titular (quien contrata la concesion)
- El **titular** es la persona que figura como responsable del derecho funerario.
- Se modela por contrato en `OwnershipRecord` (activo + historico).
- Regla operativa: por contrato solo puede existir **un titular activo**.

### 2) Beneficiario (cambio rapido por fallecimiento del titular)
- El **beneficiario** es una designacion vinculada al contrato para simplificar el cambio de titularidad cuando fallece el titular.
- Se modela en `Beneficiario` y puede mantenerse o sustituirse durante el cierre de un caso de transmision.
- Regla operativa: por contrato solo puede existir **un beneficiario activo**.

### 3) Difuntos asociados a la sepultura
- Los **difuntos no forman parte de la titularidad**: se gestionan aparte, ligados a la sepultura fisica.
- Se modelan en `SepulturaDifunto`.
- Regla operativa: una sepultura (nicho, columbario, panteon, etc.) puede tener **varios difuntos**.

### 4) Resumen de relaciones
- `Sepultura` -> `DerechoFunerarioContrato` (contrato activo/historico).
- `DerechoFunerarioContrato` -> `OwnershipRecord` (titular activo/historico).
- `DerechoFunerarioContrato` -> `Beneficiario` (beneficiario activo/historico).
- `Sepultura` -> `SepulturaDifunto` (uno o varios difuntos).

Esta separacion evita mezclar conceptos administrativos (titular/beneficiario) con conceptos de ocupacion fisica (difuntos inhumados).

## Endpoints de titularidad (MVP)

- `GET /cementerio/titularidad/casos`
- `POST /cementerio/titularidad/casos`
- `GET /cementerio/titularidad/casos/<id>`
- `POST /cementerio/titularidad/casos/<id>/status`
- `POST /cementerio/titularidad/casos/<id>/approve`
- `POST /cementerio/titularidad/casos/<id>/reject`
- `POST /cementerio/titularidad/casos/<id>/close`
- `POST /cementerio/titularidad/casos/<id>/parties`
- `POST /cementerio/titularidad/casos/<id>/publications`
- `POST /cementerio/titularidad/casos/<id>/documents/<doc_id>/upload`
- `POST /cementerio/titularidad/casos/<id>/documents/<doc_id>/verify`
- `GET /cementerio/titularidad/casos/<id>/resolucion.pdf`
