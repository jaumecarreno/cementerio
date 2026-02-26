# Cementerio Extract (GSF v1.0.14)

Digest operativo para desarrollo. Fuente: `spec/GSF_v1.0.14.pdf` (secciones indicadas).

## 9.1.3 Cobrament de taxes
- No requiere documentacion especifica.
- Flujo base:
1. Seleccionar la sepultura para cobrar tasas.
2. Si la sepultura no tiene beneficiario, el sistema debe avisar para intentar tramitar su nombramiento.
3. Mostrar articulos desde la ultima fecha de pago hasta hoy con precio y total.
4. Mostrar tambien facturas impagadas asociadas al contrato de la sepultura.

## 9.1.4 Generacio del titol (duplicat o no)
- Antes de generar el titulo se pregunta si es duplicado.
- Si es duplicado:
1. Se usa cuando el titulo se ha extraviado.
2. El nuevo titulo debe marcarse como `DUPLICAT`.
3. Debe quedar constancia de que se genero duplicado y su codigo, para validar solo el ultimo duplicado.
- Si no es duplicado:
1. Se usa por deterioro del titulo o cambio de decision en una transmision reciente.
2. No es una gestion prioritaria.

## 9.1.5 Transmissions de titularitat

### 9.1.5.1 Mortis causa (con y sin testamento)
- Causa: fallecimiento del titular actual.
- Con testamento:
1. Los herederos son los indicados en el testamento.
2. Si hay varios, deben ceder derechos a una sola persona (nuevo titular).
3. El titular puede nombrar beneficiario.
4. Requisitos: certificado de defuncion, titulo de sepultura, solicitud de cambio, certificado de ultimas voluntades, testamento/aceptacion de herencia, cesiones si aplica, y si se nombra beneficiario: solicitud + DNI titular + DNI nuevo beneficiario.
- Sin testamento:
1. Herederos segun Codigo Civil.
2. Si hay varios, deben ceder derechos a una sola persona (nuevo titular).
3. El titular puede nombrar beneficiario.
4. Requisitos: certificado de defuncion, titulo, solicitud de cambio, libro de familia o declaracion jurada de 2 testigos, certificado de ultimas voluntades (sin testamento), cesiones si aplica, y si se nombra beneficiario: solicitud + DNI titular + DNI nuevo beneficiario.

### 9.1.5.2 Inter-vivos
- Causa: cesion de derechos por titular vivo.
- Limitacion funcional indicada por SMSFT: familiares hasta 2o grado.
- Requisitos: solicitud cambio titularidad, acreditacion de relacion (libro familia/declaracion de testigos), titulo de sepultura, DNI titular actual, DNI nuevo titular.
- Puede incluir nombramiento de beneficiario (solicitud + DNI titular + DNI nuevo beneficiario).

### 9.1.5.4 Provisional (10 anos + publicaciones)
- Se aplica cuando no es posible mortis-causa o inter-vivos (muerte no justificable, documentacion insuficiente, ausencias con derechos).
- Condiciones:
1. Titularidad provisional durante 10 anos.
2. Durante esos 10 anos no se permiten movimientos de difuntos/restos inhumados previos al cambio.
3. Si se permiten nuevas inhumaciones y exhumaciones de nuevas inhumaciones.
4. No es proceso de un solo dia: requiere publicaciones.
- Requisitos: solicitud de cambio + aceptacion de SMSFT.
- Gestiones administrativas obligatorias SMSFT:
1. Publicacion en BOP.
2. Publicacion en al menos un diario de mayor tirada de la ciudad.

## 9.1.6 Nomenament de beneficiari
- Proceso individual permitido.
- Requisitos:
1. Solicitud de nombramiento de beneficiario.
2. Copia DNI del titular.
3. Copia DNI del nuevo beneficiario.

## 9.1.7 Contractacio del dret funerari (concesio/lloguer, 50/25)
- Se contrata el derecho de uso de sepultura (bien publico) por tiempo limitado.
- Modalidades principales:
1. `Concessio` (derecho funerario).
2. `Lloguer` / uso inmediato.
- Diferencias clave:
1. Duracion del contrato.
2. Sepulturas orientadas a cada modalidad (dato orientativo).
3. En uso inmediato/lloguer no hay cobro anual de tasas.
- Limites referenciados:
1. Concesion: normalmente 25-50 anos; tope legal operativo 50 (con legacy historico hasta 99).
2. Lloguer/uso inmediato: normalmente 2-25 anos; tope 25.
- Flujo operativo (ambas modalidades):
1. Seleccion de sepultura.
2. Informar datos del titular.
3. Informar beneficiario (opcional).
4. Generar nuevo titulo.
5. Generar facturas asociadas.

## 9.4.3 / 9.4.4 / 9.4.5 Consultas desde sepultura
- 9.4.3 Titulares:
1. Desde sepultura se consultan titulares activos e inactivos.
2. Se pueden consultar/editar sus datos.
3. No se pueden anadir o eliminar titulares desde sepultura.
- 9.4.4 Beneficiarios:
1. Desde sepultura se consultan beneficiarios activos e inactivos.
2. Se pueden consultar/editar sus datos.
3. No se pueden anadir o eliminar beneficiarios desde sepultura.
- 9.4.5 Movimientos:
1. Desde sepultura solo se consultan movimientos.
2. No se pueden anadir ni eliminar movimientos.
3. Se puede filtrar por tipo, fechas, titulares, etc.

## Ejemplos de referencia

### 5.3.4 Cobro de tasas
Escenario: paga un hijo del titular (no beneficiario), titular pensionista desde ano anterior y contrato sin beneficiario.
- Pasos:
1. Buscar sepultura (ubicacion, titular, difunto).
2. Preguntas de control: beneficiario y pensionista.
3. Si no hay beneficiario, intentar captar nombramiento.
4. Mostrar pendientes: tickets no facturados y facturas impagadas por separado.
5. Cobrar con regla de anos pendientes: incluir anos anteriores y aplicar pensionista solo donde corresponda.
6. Emitir factura y recibo registrando usuario y momento.

### 5.3.5 Cambio titular mortis-causa con testamento
Escenario: una persona no beneficiaria solicita cambio de titular por defuncion con testamento.
- Pasos:
1. Verificar documentacion (defuncion, testamento, ultimas voluntades, titulo, DNI nuevo titular).
2. Buscar sepultura.
3. Comprobar pagos pendientes y exigir pago previo para continuar.
4. Facturar/cobrar pendientes a nombre del titular actual.
5. Preguntas de control (beneficiario, pensionista) para actualizar datos.
6. Alta del nuevo titular e impresion de documentos de cambio (titulo y anotacion libro).
7. Facturar/cobrar tramite de cambio e impresion del nuevo titulo a nombre del nuevo titular.
