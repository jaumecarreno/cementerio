# SaaS Cementerio MVP (GSF v1.0.14)

MVP vertical multi-tenant del modulo Cementerio, implementado con:
- Python 3.12
- Flask (app factory)
- SQLAlchemy + Alembic
- Flask-Login
- Jinja2 + HTMX
- PostgreSQL

## Source of truth (spec)
- `spec/GSF-reducido a cementerio.pdf` (referencia principal)
- `spec/cementerio_extract.md` (digest operativo)
- `spec/GSF_v1.0.14.docx` (respaldo)

## Funcionalidad MVP entregada
- Panel: `/cementerio/panel`
- Buscar sepulturas: `/cementerio/sepulturas/buscar`
- Ficha de sepultura con tabs: `/cementerio/sepulturas/<id>`
- Cobro de tasas: `/cementerio/tasas/cobro?sepultura_id=<id>`
- Alta masiva: `/cementerio/sepulturas/alta-masiva`
- Contratacion derecho funerario: `POST /cementerio/sepulturas/<id>/derecho/contratar`
- Titulo de derecho funerario PDF: `GET /cementerio/contratos/<id>/titulo.pdf`
- Generacion anual de tiquets: `flask --app app:create_app tickets-generate-year --year <YYYY>`

Incluye:
- Estados exactos de sepultura: `LLIURE`, `DISPONIBLE`, `OCUPADA`, `INACTIVA`, `PROPIA`
- Tipos de contrato: `CONCESION`, `USO_INMEDIATO` (UI: LLOGUER)
- Limites legales: 50/25 + legacy concesion 99 con flag
- Regla de cobro por antiguedad (prefijo contiguo desde ano mas antiguo)
- Diferenciacion de pendientes:
  - tiquets no facturados (`PENDIENTE`)
  - facturas impagadas (`IMPAGADA`)
- Regla pensionista configurable por organizacion
- Criterio de caja: factura de tasas en el momento del cobro
- Aislamiento multi-tenant por `org_id`

## Estructura
- `app/core`: config, auth, db, tenancy, modelos, permisos
- `app/cemetery`: blueprint, rutas, servicios
- `app/templates`: vistas server-rendered + HTMX
- `migrations`: Alembic
- `docs/spec_map.md`, `docs/decisions.md`, `docs/assumptions.md`

## Arranque local (sin Docker)
1. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```
2. Configurar entorno:
   ```bash
   copy .env.example .env
   ```
3. Ejecutar migraciones:
   ```bash
   flask --app app:create_app db upgrade
   ```
4. Seed demo:
   ```bash
   flask --app app:create_app seed-demo
   ```
5. Generar tiquets anuales (opcional):
   ```bash
   flask --app app:create_app tickets-generate-year --year 2026 --org-code SMSFT
   ```
6. Levantar servidor:
   ```bash
   flask --app app:create_app run
   ```

## Arranque con Docker
1. Levantar servicios:
   ```bash
   docker compose up --build
   ```
2. Ejecutar migraciones en web:
   ```bash
   docker compose exec web flask --app app:create_app db upgrade
   ```
3. Seed demo:
   ```bash
   docker compose exec web flask --app app:create_app seed-demo
   ```

## Credenciales demo
- Admin: `admin@smsft.local` / `admin123`
- Operario: `operario@smsft.local` / `operario123`

## Tests
```bash
pytest -q
```

Cobertura funcional de tests:
- Reglas de estado de sepultura
- Limites de contratos 50/25 y legacy 99
- Generacion idempotente de tiquets anuales (solo concesion)
- Regla de antiguedad en cobro
- Descuento pensionista en cobro de anos previos
- Cobro crea `Invoice(PAGADA)` + `Payment(user_id)`
- Aislamiento tenant
- Flujo vertical busqueda -> ficha -> cobro
- Alta masiva en estado `LLIURE`
