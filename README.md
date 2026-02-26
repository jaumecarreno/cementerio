# SaaS Cementerio MVP (GSF v1.0.14)

MVP vertical multi-tenant del módulo Cementerio, implementado sobre:
- Python 3.12
- Flask (app factory)
- SQLAlchemy + Alembic
- Flask-Login
- Jinja2 + HTMX
- PostgreSQL (docker-compose)

## Source of truth (spec)
- `spec/GSF_v1.0.14.doc` (referencia principal)
- `spec/mockups/page-*.png`
- `spec/mockups_v2/page-*.png`

## Funcionalidad MVP entregada
- Panel: `/cementerio/panel`
- Buscar sepulturas: `/cementerio/sepulturas/buscar`
- Ficha de sepultura con tabs: `/cementerio/sepulturas/<id>`
- Cobro de tasas: `/cementerio/tasas/cobro?sepultura_id=<id>`
- Alta masiva: `/cementerio/sepulturas/alta-masiva`

Incluye:
- Estados exactos de sepultura: `LLIURE`, `DISPONIBLE`, `OCUPADA`, `INACTIVA`, `PROPIA`
- Regla de cobro por antigüedad (prefijo contiguo desde año más antiguo)
- Diferenciación de pendientes:
  - tiquets no facturados
  - facturas impagadas
- Regla pensionista no retroactiva
- Aislamiento multi-tenant por `org_id`

## Estructura
- `app/core`: config, auth, db, tenancy, modelos
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
5. Levantar servidor:
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
- Regla de antigüedad en cobro
- Descuento pensionista no retroactivo
- Bloqueo de cobro para sepultura `PROPIA`
- Límite legal contratos 50/25
- Aislamiento tenant
- Flujo vertical búsqueda -> ficha -> cobro
- Alta masiva en estado `LLIURE`
