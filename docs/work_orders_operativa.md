# Sistema propuesto de Órdenes de Trabajo (OT) para operativa integral de cementerio

## 1) Objetivo

Diseñar un sistema de OT que sirva para:

1. **Lo ya implementado** (expedientes + OT manuales).
2. **La operativa habitual completa** de un cementerio (inhumaciones, exhumaciones, traslados, mantenimiento, incidencias, campañas, etc.).
3. **Casos no previstos** hoy mediante un modelo configurable por plantillas, reglas y eventos.

La idea clave: una OT no es solo una tarea suelta, sino una unidad trazable con prioridad, dependencias, SLA, recursos, costes y evidencia.

---

## 2) Qué tenemos hoy (base real del sistema)

En el estado actual ya existe una base funcional:

- Entidad `Expediente` (tipo, estado, sepultura, difunto, declarante, fecha prevista, notas).
- Entidad `OrdenTrabajo` ligada opcionalmente a expediente.
- Flujo básico de OT: crear y completar.
- Trazabilidad de movimientos de sepultura al crear/cerrar OT.
- Restricción operativa en titularidad provisional para ciertos expedientes (`EXHUMACION`, `RESCATE`).

Esto permite trabajar, pero todavía sin un motor avanzado de planificación/ejecución.

---

## 3) Visión de funcionamiento: “Expediente orquestado por OT”

### 3.1 Principio general

Cada proceso de negocio relevante (ej. inhumación) genera o usa un **expediente** y ese expediente se ejecuta mediante un **plan de OT**.

- **Expediente = contexto administrativo/jurídico**.
- **OT = trabajo operativo real en campo/oficina**.

### 3.2 Resultado esperado

- El operador no “inventa” cada paso: el sistema propone el flujo.
- Se reducen olvidos (permisos, verificación documental, preparación de nicho, cierre administrativo, etc.).
- Se mantiene flexibilidad para añadir OT nuevas sin tocar código crítico.

---

## 4) Modelo de datos recomendado (extensión)

## 4.1 Orden de trabajo (OT)
Campos recomendados adicionales:

- `code` (ej. `OT-2026-000123`)
- `category` (`OPERATIVA`, `MANTENIMIENTO`, `INCIDENCIA`, `ADMINISTRATIVA`, `SANIDAD`, etc.)
- `subtype` (`INHUMACION_APERTURA`, `EXHUMACION_RESTOS`, `LIMPIEZA_ZONA`, ...)
- `priority` (`BAJA`, `MEDIA`, `ALTA`, `URGENTE`)
- `status` detallado (ver 4.4)
- `due_at`, `planned_start_at`, `planned_end_at`
- `assigned_team_id` / `assigned_user_id`
- `location_ref` (sector, bloque, calle, nivel)
- `requires_confirmation` (doble validación)
- `checklist_schema_version`
- `cost_center`, `estimated_cost`, `actual_cost`
- `parent_ot_id` (subtareas)
- `trigger_source` (`MANUAL`, `EVENTO`, `PLANTILLA`, `API`)

## 4.2 Plantillas de OT (muy importante para flexibilidad)

Entidad `work_order_template`:

- `template_code`
- `name`
- `event_type` (qué evento la dispara)
- `applicability_rules` (reglas en JSON o DSL)
- `default_priority`
- `required_roles` (brigada, marmolista, administración, etc.)
- `default_checklist`
- `sla_hours`
- `auto_create` (sí/no)
- `auto_assign_policy` (por carga, por zona, por guardia)

Con esto puedes añadir procesos nuevos sin rediseñar todo.

## 4.3 Dependencias y paquetes de trabajo

- `ot_dependency`: “OT B depende de A”.
- `ot_bundle`: grupo de OT para un proceso completo (ej. “Proceso INHUMACION”).

## 4.4 Estados operativos recomendados

Más finos que solo `PENDIENTE/COMPLETADA`:

1. `BORRADOR`
2. `PENDIENTE_PLANIFICACION`
3. `PLANIFICADA`
4. `ASIGNADA`
5. `EN_CURSO`
6. `BLOQUEADA`
7. `EN_VALIDACION`
8. `COMPLETADA`
9. `CANCELADA`
10. `NO_EJECUTABLE` (falta requisito externo)

---

## 5) Motor de orquestación por eventos (automático o casi)

## 5.1 Eventos que deben disparar OT

- `EXPEDIENTE_CREADO`
- `EXPEDIENTE_ESTADO_CAMBIADO`
- `INHUMACION_PROGRAMADA`
- `EXHUMACION_AUTORIZADA`
- `TRASLADO_CONFIRMADO`
- `CONTRATO_CREADO/RENOVADO`
- `TITULARIDAD_APROBADA`
- `INCIDENCIA_REPORTADA`
- `INSPECCION_VENCIDA`
- `STOCK_BAJO_LAPIDAS`

## 5.2 Cómo funciona

1. Ocurre evento.
2. Se evalúan reglas de plantillas aplicables.
3. Se crean OT sugeridas/automáticas.
4. Se encadenan dependencias.
5. Se notifican equipos.
6. Se vigila SLA y bloqueos.

Esto resuelve tu necesidad: inhumación/exhumación lanza automáticamente trabajo real asociado.

---

## 6) Flujos detallados propuestos

## 6.1 Inhumación (ejemplo completo)

### Disparador
- Alta de expediente tipo `INHUMACION` o cambio a `EN_TRAMITE` con documentación válida.

### OT generadas automáticamente (bundle)
1. Verificación documental final (administración).
2. Confirmación de disponibilidad y estado físico de sepultura.
3. Preparación de unidad (apertura, seguridad, señalización).
4. Coordinación ceremonial/horaria.
5. Ejecución inhumación en campo.
6. Cierre de unidad y acondicionamiento.
7. Registro de ocupación y actualización de datos.
8. Comunicación a familia + documentos.
9. Facturación/cierre administrativo (si aplica).

### Reglas
- No ejecutar OT 5 si OT 1-4 no están completadas.
- Si hay incidencia en OT 5, crear OT de contingencia (`INCIDENCIA_INHUMACION`).
- Si se retrasa la hora, recalcular prioridades y avisos.

## 6.2 Exhumación

### Disparador
- Expediente `EXHUMACION` autorizado.

### OT automáticas
1. Revalidación legal (especialmente titularidad/restricciones).
2. Preparación EPIs y recursos.
3. Ejecución exhumación.
4. Destino de restos (traslado/depósito/reducción según expediente).
5. Limpieza y cierre.
6. Actualización de trazabilidad y libro de movimientos.

### Regla sensible
- Si titularidad provisional con restos previos bloqueados, OT se marca `NO_EJECUTABLE` y se genera tarea administrativa para resolución.

## 6.3 Traslado

- OT de coordinación origen, OT de recepción destino, OT de transporte custodiado, OT de cierre dual de trazabilidad.

## 6.4 Mantenimiento preventivo

- Planes periódicos por zona/activo:
  - Viales, drenaje, alumbrado, cerramientos, riego.
- Motor recurrente genera OTs semanales/mensuales con SLA.

## 6.5 Incidencias

- Entrada por personal interno o atención ciudadana.
- Triage automático por severidad:
  - Riesgo seguridad = `URGENTE`.
  - Estético = `MEDIA/BAJA`.
- Escalado automático si vence SLA.

---

## 7) Checklists y evidencias (clave de calidad)

Cada tipo de OT debe tener checklist parametrizable:

- Items obligatorios/opcionales.
- Adjuntos requeridos (foto antes/después, firma, documento).
- Validación por rol supervisor cuando aplique.

Sin checklist completo, no se puede pasar a `COMPLETADA` (o requiere excepción auditada).

---

## 8) Planificación y asignación de recursos

## 8.1 Asignación
- Por cuadrilla y especialidad.
- Por proximidad/zona.
- Por capacidad (WIP máximo).

## 8.2 Agenda operativa
- Vista calendario + tablero Kanban de OT.
- Bloqueos por solape en misma sepultura/ubicación.

## 8.3 Materiales
- Reserva automática de stock al planificar OT.
- Si no hay stock: OT en `BLOQUEADA` + OT de aprovisionamiento.

---

## 9) Cumplimiento, auditoría y trazabilidad

- Bitácora completa: quién crea/asigna/modifica/ejecuta/cierra.
- Línea temporal unificada expediente + OT + movimientos + documentos.
- Motivo obligatorio en cancelaciones/reaperturas.
- Versionado de checklist/plantilla para auditorías futuras.

---

## 10) Qué probablemente falta hoy (gap analysis)

A partir de la base existente, los huecos típicos a cubrir son:

1. **Estados OT avanzados y dependencias** (hoy flujo simple).
2. **Plantillas y reglas de auto-creación** por evento.
3. **SLA y escalados automáticos**.
4. **Asignación inteligente de equipos**.
5. **Checklists estructurados + evidencia obligatoria**.
6. **Integración fuerte con stock/materiales y logística**.
7. **Motor recurrente de mantenimiento preventivo**.
8. **Gestión formal de incidencias ciudadanas**.
9. **Panel operativo en tiempo real** (carga, atrasos, bloqueos).
10. **Métricas KPI** (cumplimiento SLA, tiempo medio, retrabajos, etc.).

---

## 11) Arquitectura funcional flexible (para OT no previstas)

Para poder aceptar “cosas nuevas” sin romper el sistema:

1. **Catálogo abierto de tipos/subtipos OT** editable por administración.
2. **Plantillas versionadas** con campos dinámicos.
3. **Reglas declarativas** (`if evento + condiciones => crear bundle`).
4. **Campos extendidos JSON** para requerimientos especiales.
5. **Motor de validaciones por política** (por rol, por tipo, por fase).
6. **Webhooks/API** para disparar OT desde sistemas externos.

Así, cuando aparezca un trámite nuevo (sanitario, judicial, municipal), se configura más que se programa.

---

## 12) Propuesta de roadmap por fases

### Fase 1 (rápida, alto impacto)
- Estados OT extendidos.
- Dependencias básicas.
- Bundle automático para INHUMACION y EXHUMACION.
- Checklist mínimo por tipo de OT.

### Fase 2
- Motor de plantillas/reglas declarativas.
- SLA + notificaciones + escalado.
- Agenda/kanban operativo y asignación por equipo.

### Fase 3
- Integración completa con stock/materiales.
- Mantenimiento preventivo recurrente.
- Portal de incidencias y autoservicio.

### Fase 4
- Optimización avanzada (capacidad, rutas, predicción de carga).
- Cuadro de mando de KPIs y mejora continua.

---

## 13) Recomendaciones prácticas inmediatas

1. Definir **2 bundles automáticos** de referencia (`INHUMACION_STD`, `EXHUMACION_STD`).
2. Añadir estado `BLOQUEADA` y `EN_VALIDACION` a OT.
3. Exigir al menos 1 evidencia en cierre de OT crítica.
4. Implementar tabla de `ot_dependency` simple.
5. Añadir reglas de negocio “hard-stop” (no continuar sin requisito legal).

Con solo esto, ya tendrías un sistema mucho más robusto y cercano a la operativa real.
