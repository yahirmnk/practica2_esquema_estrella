# Práctica 2: Esquema Estrella en PostgreSQL con Pandas (ETL)

Transforma transacciones operacionales simuladas (OLTP) en un **esquema
estrella** para un Data Warehouse de Ventas y lo carga en **PostgreSQL**:

- **Dimensiones:** `dim_cliente`, `dim_producto`, `dim_tiempo`, `dim_canal` (adicional)
- **Hechos:** `fact_ventas` con llaves subrogadas (SK) y métricas

## Estructura

| Archivo / carpeta | Descripción |
|---|---|
| `etl_esquema_estrella.py` | Script ETL: extracción, diagnóstico, dimensiones, hechos y carga |
| `config.example.yaml` | Plantilla de conexión (sin contraseña) |
| `config.yaml` | Conexión real con contraseña: **excluido de Git** |
| `sql/01_ddl_esquema_estrella.sql` | `CREATE SCHEMA` y `CREATE TABLE` con PK, FK, CHECK e índices |
| `sql/02_dml_carga.sql` | `INSERT` de todas las tablas |
| `sql/03_consultas_olap.sql` | Consultas analíticas (ROLLUP, fin de semana, canal, día) |
| `salidas/` | CSV de cada dimensión y de la tabla de hechos |

## Instalación

```bash
python -m venv venv
venv\Scripts\Activate.ps1          # Windows (PowerShell)
pip install -r requirements.txt
copy config.example.yaml config.yaml   # y escribe tu contraseña de PostgreSQL
```

## Ejecución

```bash
python etl_esquema_estrella.py           # ETL completo + carga a PostgreSQL
python etl_esquema_estrella.py --sin-bd  # solo genera CSV y scripts SQL
```

El script crea la base `dw_ventas` (si no existe), el esquema `dw`, las tablas
y carga los datos. Se puede ejecutar varias veces: las tablas se recrean.
Para usar una instancia PostgreSQL en AWS EC2 solo cambia `host` en `config.yaml`.

## Modelo

```
              dim_cliente          dim_producto
                    \                  /
   dim_tiempo ── fact_ventas ── dim_canal
```

| Tabla | Llave | Atributos |
|---|---|---|
| `dim_cliente` | `id_cliente_sk` | nombre, ciudad |
| `dim_producto` | `id_producto_sk` | nombre, categoría, precio |
| `dim_tiempo` | `id_tiempo_sk` (AAAAMMDD) | año, trimestre, mes, nombre_mes, día, semana, día de la semana, **es_fin_semana** |
| `dim_canal` | `id_canal_sk` | canal (Tienda física, Sitio web, App móvil), tipo (Presencial/Digital) |
| `fact_ventas` | `id_transaccion` | 4 SK, hora, cantidad, precio, monto_total, **descuento, iva, monto_final, monto_acumulado, monto_acumulado_dia** |

## Ajustes respecto al código base

1. **Corrección con catálogos maestros:** en el código base, producto,
   categoría y precio (y cliente y ciudad) se sortean por separado, lo que
   genera 27 "productos" y 9 "clientes" (p. ej. 52 ventas de un mouse a
   $15,000). Se asignan desde un catálogo para obtener 3 productos y 3 clientes.
2. **Dimensión adicional `dim_canal`** y atributos de calendario en `dim_tiempo`.
3. **Métricas:** descuento por volumen (3 u. = 5 %, 4 u. = 10 %), IVA 16 %,
   monto final y acumulados.
4. **Validaciones:** `merge(validate="many_to_one")`, conteo de filas, SK sin
   nulos y orden por `id_transaccion` tras los cruces.
5. **Carga en PostgreSQL** con SQLAlchemy y verificación de conteos e
   integridad referencial.
