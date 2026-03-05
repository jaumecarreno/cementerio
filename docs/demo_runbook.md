# Demo runbook (pre-reunion)

## 1) Reset controlado
1. Iniciar sesion con usuario `operativo` (o `admin`).
2. Abrir `/demo`.
3. Ejecutar **Reset a cero** y confirmar.
4. Verificar mensaje de exito en pantalla.

## 2) Cargar escenario verosimil
1. En la misma pantalla `/demo`, ejecutar **Cargar escenario inicial**.
2. Esperar confirmacion de carga de sepulturas, contratos, expedientes, casos y facturacion.
3. Comprobar rapidamente:
   - `/cementerio/titularidad` (casos y titularidades).
   - `/cementerio/facturacion` (facturas y cobros).
   - `/cementerio/ot` (ordenes operativas).

## 3) Login de presentacion
- `comercial@smsft.local` / `comercial123`
  - Modo lectura guiada (sin acciones de carga/reset ni alta operativa).
- `operativo@smsft.local` / `operativo123`
  - Acciones controladas para preparar y operar la demo.

## 4) Smoke antes de reunion
Ejecutar:

```bash
pytest -q tests/test_demo_ready.py -k "smoke or presentation_users or avoids_demo_tokens"
```

Si falla, reintentar:
1. `flask --app app:create_app db upgrade`
2. Reset + carga inicial desde `/demo`
3. Repetir smoke

## 5) Rutas de respaldo en vivo
- Home panel: `/cementerio/panel`
- Busqueda sepulturas: `/cementerio/sepulturas/buscar`
- Titularidad/expedientes: `/cementerio/titularidad`
- Facturacion: `/cementerio/facturacion`
- Ordenes de trabajo: `/cementerio/ot`
- Pantalla de control demo: `/demo`
