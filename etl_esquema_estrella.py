"""
Práctica 2: Generación y Estructuración de un Esquema Estrella en PostgreSQL / Pandas.

Proceso ETL (Extract - Transform - Load):
    1. EXTRACT   Simula la ingesta de un sistema transaccional (OLTP) de ventas
                 (código base, semilla 101) y diagnostica su calidad.
    2. TRANSFORM Corrige inconsistencias con catálogos maestros y construye el
                 esquema estrella:
                   - Dim_Cliente, Dim_Producto, Dim_Tiempo (+ atributos calculados)
                   - Dim_Canal (dimensión adicional)
                   - Fact_Ventas con llaves subrogadas (SK) y métricas
                     (subtotal, descuento, IVA, monto final y acumulados).
    3. LOAD      Genera los scripts SQL (DDL, DML y consultas OLAP) y carga las
                 tablas en PostgreSQL con SQLAlchemy + psycopg2.

Las credenciales de PostgreSQL se leen de config.yaml (excluido de Git).
Copia config.example.yaml como config.yaml y escribe tu contraseña.

Uso:
    python etl_esquema_estrella.py           # ETL completo con carga a PostgreSQL
    python etl_esquema_estrella.py --sin-bd  # solo transforma y genera los .sql/.csv
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError, ProgrammingError

CARPETA = Path(__file__).parent
CARPETA_SQL = CARPETA / "sql"
CARPETA_SALIDAS = CARPETA / "salidas"
ARCHIVO_CONFIG = CARPETA / "config.yaml"

SEMILLA = 101
N_TRANSACCIONES = 500
TASA_IVA = 0.16

# Catálogos maestros: en un OLTP real, el cliente y el producto viven en sus
# propias tablas, así que cada uno tiene una sola ciudad / categoría / precio.
CATALOGO_CLIENTES = {
    "Juan Pérez": "Puebla",
    "María Gómez": "CDMX",
    "Carlos Ruiz": "Guadalajara",
}
CATALOGO_PRODUCTOS = {
    "Laptop Dell": ("Cómputo", 15000.0),
    "Mouse Logi": ("Accesorios", 350.0),
    "Monitor LG": ("Pantallas", 4500.0),
}
CATALOGO_CANALES = {          # dimensión adicional
    "Tienda física": "Presencial",
    "Sitio web": "Digital",
    "App móvil": "Digital",
}
# Regla de negocio de descuento por volumen (unidades -> porcentaje)
DESCUENTO_POR_CANTIDAD = {1: 0.00, 2: 0.00, 3: 0.05, 4: 0.10}

DIAS_SEMANA = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
         "Septiembre", "Octubre", "Noviembre", "Diciembre"]

pd.set_option("display.width", 160)
pd.set_option("display.max_columns", 20)


# ======================================================================
# 1. EXTRACT: simulación del sistema OLTP
# ======================================================================
def extraer_oltp(n=N_TRANSACCIONES, semilla=SEMILLA):
    """Código base: simula las transacciones de un sistema operacional."""
    np.random.seed(semilla)
    df = pd.DataFrame({
        "id_transaccion": range(1000, 1000 + n),
        "fecha_hora": pd.date_range(start="2026-01-01", periods=n, freq="h"),
        "cliente_nombre": np.random.choice(["Juan Pérez", "María Gómez", "Carlos Ruiz"], n),
        "cliente_ciudad": np.random.choice(["Puebla", "CDMX", "Guadalajara"], n),
        "producto_nombre": np.random.choice(["Laptop Dell", "Mouse Logi", "Monitor LG"], n),
        "categoria_prod": np.random.choice(["Cómputo", "Accesorios", "Pantallas"], n),
        "precio_unitario": np.random.choice([15000.0, 350.0, 4500.0], n),
        "cantidad": np.random.randint(1, 5, n),
    })
    # Extensión: canal de venta (se genera DESPUÉS de las columnas base para
    # no alterar la secuencia aleatoria del código original)
    df["canal_venta"] = np.random.choice(list(CATALOGO_CANALES), n, p=[0.5, 0.3, 0.2])
    return df


def diagnosticar_oltp(df):
    """Detecta inconsistencias en los datos crudos antes de modelar."""
    print(f"Registros: {len(df)} | Columnas: {df.shape[1]}")
    print(f"Rango de fechas: {df['fecha_hora'].min()} -> {df['fecha_hora'].max()} "
          f"({df['fecha_hora'].dt.date.nunique()} días)")
    print(f"Nulos totales: {int(df.isna().sum().sum())} | "
          f"id_transaccion duplicados: {int(df['id_transaccion'].duplicated().sum())} | "
          f"cantidades <= 0: {int((df['cantidad'] <= 0).sum())}")

    n_cli = df[["cliente_nombre", "cliente_ciudad"]].drop_duplicates().shape[0]
    n_prod = df[["producto_nombre", "categoria_prod", "precio_unitario"]].drop_duplicates().shape[0]
    print(f"Combinaciones únicas cliente+ciudad: {n_cli} (clientes reales: {df['cliente_nombre'].nunique()})")
    print(f"Combinaciones únicas producto+categoría+precio: {n_prod} "
          f"(productos reales: {df['producto_nombre'].nunique()})")

    incongruentes = df[df["precio_unitario"] != df["producto_nombre"].map(lambda p: CATALOGO_PRODUCTOS[p][1])]
    print(f"Transacciones con precio que no corresponde al producto: {len(incongruentes)} "
          f"({len(incongruentes) / len(df):.0%})")
    ejemplo = df[(df["producto_nombre"] == "Mouse Logi") & (df["precio_unitario"] == 15000.0)]
    print(f"Ejemplo: ventas de 'Mouse Logi' a $15,000.00 -> {len(ejemplo)}")
    return n_cli, n_prod


def corregir_con_catalogos(df):
    """Asigna ciudad, categoría y precio a partir de los catálogos maestros."""
    df = df.copy()
    df["cliente_ciudad"] = df["cliente_nombre"].map(CATALOGO_CLIENTES)
    df["categoria_prod"] = df["producto_nombre"].map(lambda p: CATALOGO_PRODUCTOS[p][0])
    df["precio_unitario"] = df["producto_nombre"].map(lambda p: CATALOGO_PRODUCTOS[p][1])
    return df


# ======================================================================
# 2. TRANSFORM: dimensiones y tabla de hechos
# ======================================================================
def construir_dim_cliente(df):
    dim = df[["cliente_nombre", "cliente_ciudad"]].drop_duplicates() \
        .sort_values("cliente_nombre").reset_index(drop=True)
    dim.insert(0, "id_cliente_sk", dim.index + 1)          # Surrogate Key
    return dim


def construir_dim_producto(df):
    dim = df[["producto_nombre", "categoria_prod", "precio_unitario"]].drop_duplicates() \
        .sort_values("producto_nombre").reset_index(drop=True)
    dim.insert(0, "id_producto_sk", dim.index + 1)
    return dim


def construir_dim_canal(df):
    dim = pd.DataFrame({"canal_venta": sorted(df["canal_venta"].unique())})
    dim["tipo_canal"] = dim["canal_venta"].map(CATALOGO_CANALES)
    dim.insert(0, "id_canal_sk", dim.index + 1)
    return dim


def construir_dim_tiempo(df):
    """Dimensión de fecha con SK inteligente (AAAAMMDD) y atributos calculados."""
    fechas = pd.to_datetime(pd.Series(sorted(df["fecha_hora"].dt.date.unique())))
    dim = pd.DataFrame({"fecha_completa": fechas})
    dim.insert(0, "id_tiempo_sk", dim["fecha_completa"].dt.strftime("%Y%m%d").astype(int))
    dim["anio"] = dim["fecha_completa"].dt.year
    dim["mes"] = dim["fecha_completa"].dt.month
    dim["dia"] = dim["fecha_completa"].dt.day
    # --- Atributos adicionales ---
    dim["trimestre"] = dim["fecha_completa"].dt.quarter
    dim["nombre_mes"] = dim["mes"].map(lambda m: MESES[m - 1])
    dim["semana_anio"] = dim["fecha_completa"].dt.isocalendar().week.astype(int)
    dim["dia_semana_num"] = dim["fecha_completa"].dt.dayofweek + 1        # 1 = lunes
    dim["dia_semana"] = dim["dia_semana_num"].map(lambda d: DIAS_SEMANA[d - 1])
    dim["es_fin_semana"] = dim["dia_semana_num"] >= 6
    return dim


def construir_fact_ventas(df, dim_cliente, dim_producto, dim_tiempo, dim_canal):
    """Cruza las transacciones con cada dimensión para obtener las SK y calcula las métricas."""
    n_inicial = len(df)
    fact = df.merge(dim_cliente, on=["cliente_nombre", "cliente_ciudad"], how="left", validate="many_to_one") \
             .merge(dim_producto, on=["producto_nombre", "categoria_prod", "precio_unitario"],
                    how="left", validate="many_to_one") \
             .merge(dim_canal[["id_canal_sk", "canal_venta"]], on="canal_venta", how="left", validate="many_to_one")

    fact["fecha_completa"] = pd.to_datetime(fact["fecha_hora"].dt.date)
    fact = fact.merge(dim_tiempo[["id_tiempo_sk", "fecha_completa"]], on="fecha_completa",
                      how="left", validate="many_to_one")

    # Validaciones de integridad del cruce
    assert len(fact) == n_inicial, "El merge duplicó o perdió filas"
    sk = ["id_cliente_sk", "id_producto_sk", "id_tiempo_sk", "id_canal_sk"]
    assert fact[sk].notna().all().all(), "Hay transacciones sin llave de dimensión"

    # El merge no conserva el orden original: se restablece por id_transaccion
    fact = fact.sort_values("id_transaccion").reset_index(drop=True)

    # --- Métricas ---
    fact["hora"] = fact["fecha_hora"].dt.hour                                  # atributo degenerado
    fact["monto_total"] = fact["cantidad"] * fact["precio_unitario"]           # métrica del código base (subtotal)
    fact["descuento"] = (fact["monto_total"] * fact["cantidad"].map(DESCUENTO_POR_CANTIDAD)).round(2)
    fact["iva"] = ((fact["monto_total"] - fact["descuento"]) * TASA_IVA).round(2)
    fact["monto_final"] = (fact["monto_total"] - fact["descuento"] + fact["iva"]).round(2)
    # Métricas acumuladas (orden cronológico)
    fact["monto_acumulado"] = fact["monto_final"].cumsum().round(2)
    fact["monto_acumulado_dia"] = fact.groupby("id_tiempo_sk")["monto_final"].cumsum().round(2)

    columnas = ["id_transaccion", "id_cliente_sk", "id_producto_sk", "id_tiempo_sk", "id_canal_sk",
                "hora", "cantidad", "precio_unitario", "monto_total", "descuento", "iva",
                "monto_final", "monto_acumulado", "monto_acumulado_dia"]
    return fact[columnas].astype({c: int for c in sk})


# ======================================================================
# 3. LOAD: scripts SQL y carga a PostgreSQL
# ======================================================================
def ddl(esquema):
    """Sentencias CREATE para el esquema estrella."""
    return f"""-- DDL: Esquema estrella del Data Warehouse de Ventas
CREATE SCHEMA IF NOT EXISTS {esquema};

DROP TABLE IF EXISTS {esquema}.fact_ventas;
DROP TABLE IF EXISTS {esquema}.dim_cliente;
DROP TABLE IF EXISTS {esquema}.dim_producto;
DROP TABLE IF EXISTS {esquema}.dim_tiempo;
DROP TABLE IF EXISTS {esquema}.dim_canal;

CREATE TABLE {esquema}.dim_cliente (
    id_cliente_sk   INTEGER PRIMARY KEY,
    cliente_nombre  VARCHAR(100) NOT NULL,
    cliente_ciudad  VARCHAR(60)  NOT NULL,
    UNIQUE (cliente_nombre, cliente_ciudad)
);

CREATE TABLE {esquema}.dim_producto (
    id_producto_sk   INTEGER PRIMARY KEY,
    producto_nombre  VARCHAR(100)  NOT NULL,
    categoria_prod   VARCHAR(60)   NOT NULL,
    precio_unitario  NUMERIC(12,2) NOT NULL CHECK (precio_unitario > 0)
);

CREATE TABLE {esquema}.dim_tiempo (
    id_tiempo_sk    INTEGER PRIMARY KEY,          -- formato AAAAMMDD
    fecha_completa  DATE        NOT NULL UNIQUE,
    anio            SMALLINT    NOT NULL,
    mes             SMALLINT    NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia             SMALLINT    NOT NULL CHECK (dia BETWEEN 1 AND 31),
    trimestre       SMALLINT    NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    nombre_mes      VARCHAR(12) NOT NULL,
    semana_anio     SMALLINT    NOT NULL,
    dia_semana_num  SMALLINT    NOT NULL CHECK (dia_semana_num BETWEEN 1 AND 7),
    dia_semana      VARCHAR(10) NOT NULL,
    es_fin_semana   BOOLEAN     NOT NULL
);

CREATE TABLE {esquema}.dim_canal (
    id_canal_sk  INTEGER PRIMARY KEY,
    canal_venta  VARCHAR(30) NOT NULL UNIQUE,
    tipo_canal   VARCHAR(20) NOT NULL
);

CREATE TABLE {esquema}.fact_ventas (
    id_transaccion       INTEGER PRIMARY KEY,
    id_cliente_sk        INTEGER NOT NULL REFERENCES {esquema}.dim_cliente (id_cliente_sk),
    id_producto_sk       INTEGER NOT NULL REFERENCES {esquema}.dim_producto (id_producto_sk),
    id_tiempo_sk         INTEGER NOT NULL REFERENCES {esquema}.dim_tiempo (id_tiempo_sk),
    id_canal_sk          INTEGER NOT NULL REFERENCES {esquema}.dim_canal (id_canal_sk),
    hora                 SMALLINT      NOT NULL CHECK (hora BETWEEN 0 AND 23),
    cantidad             INTEGER       NOT NULL CHECK (cantidad > 0),
    precio_unitario      NUMERIC(12,2) NOT NULL,
    monto_total          NUMERIC(14,2) NOT NULL,
    descuento            NUMERIC(14,2) NOT NULL DEFAULT 0,
    iva                  NUMERIC(14,2) NOT NULL,
    monto_final          NUMERIC(14,2) NOT NULL,
    monto_acumulado      NUMERIC(16,2) NOT NULL,
    monto_acumulado_dia  NUMERIC(16,2) NOT NULL
);

CREATE INDEX idx_fact_cliente  ON {esquema}.fact_ventas (id_cliente_sk);
CREATE INDEX idx_fact_producto ON {esquema}.fact_ventas (id_producto_sk);
CREATE INDEX idx_fact_tiempo   ON {esquema}.fact_ventas (id_tiempo_sk);
CREATE INDEX idx_fact_canal    ON {esquema}.fact_ventas (id_canal_sk);
"""


def consultas_olap(esquema):
    """Consultas analíticas sobre el esquema estrella (nombre -> SQL)."""
    e = esquema
    return {
        "Ventas por categoría y producto (ROLLUP con subtotales)": f"""
SELECT COALESCE(p.categoria_prod, 'TOTAL GENERAL') AS categoria,
       COALESCE(p.producto_nombre, '(subtotal)')   AS producto,
       SUM(f.cantidad)    AS unidades,
       SUM(f.monto_final) AS ventas
FROM {e}.fact_ventas f
JOIN {e}.dim_producto p ON p.id_producto_sk = f.id_producto_sk
GROUP BY ROLLUP (p.categoria_prod, p.producto_nombre)
ORDER BY p.categoria_prod NULLS LAST, p.producto_nombre NULLS LAST;""",
        "Ventas entre semana vs. fin de semana": f"""
SELECT CASE WHEN t.es_fin_semana THEN 'Fin de semana' ELSE 'Entre semana' END AS tipo_dia,
       COUNT(DISTINCT t.id_tiempo_sk)  AS dias,
       COUNT(*)                         AS transacciones,
       SUM(f.monto_final)               AS ventas,
       ROUND(SUM(f.monto_final) / COUNT(DISTINCT t.id_tiempo_sk), 2) AS venta_promedio_dia
FROM {e}.fact_ventas f
JOIN {e}.dim_tiempo t ON t.id_tiempo_sk = f.id_tiempo_sk
GROUP BY t.es_fin_semana
ORDER BY t.es_fin_semana;""",
        "Ventas por cliente y canal": f"""
SELECT c.cliente_nombre, c.cliente_ciudad, k.canal_venta,
       COUNT(*) AS transacciones, SUM(f.monto_final) AS ventas
FROM {e}.fact_ventas f
JOIN {e}.dim_cliente c ON c.id_cliente_sk = f.id_cliente_sk
JOIN {e}.dim_canal   k ON k.id_canal_sk   = f.id_canal_sk
GROUP BY c.cliente_nombre, c.cliente_ciudad, k.canal_venta
ORDER BY c.cliente_nombre, ventas DESC;""",
        "Ventas por día de la semana": f"""
SELECT t.dia_semana_num, t.dia_semana,
       COUNT(*) AS transacciones, SUM(f.monto_final) AS ventas
FROM {e}.fact_ventas f
JOIN {e}.dim_tiempo t ON t.id_tiempo_sk = f.id_tiempo_sk
GROUP BY t.dia_semana_num, t.dia_semana
ORDER BY t.dia_semana_num;""",
    }


def valor_sql(v):
    """Convierte un valor de Python/pandas en literal SQL."""
    if isinstance(v, (bool, np.bool_)):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, np.integer)):
        return str(int(v))
    if isinstance(v, (float, np.floating)):
        return f"{float(v):.2f}"
    if isinstance(v, pd.Timestamp):
        return f"'{v.date()}'"
    return "'" + str(v).replace("'", "''") + "'"


def dml(esquema, tablas):
    """Sentencias INSERT para todas las tablas (dimensiones primero, hechos al final)."""
    lineas = ["-- DML: carga del Data Warehouse de Ventas", "BEGIN;"]
    for nombre, df in tablas.items():
        lineas.append(f"\n-- {nombre}: {len(df)} registros")
        columnas = ", ".join(df.columns)
        for fila in df.itertuples(index=False):
            valores = ", ".join(valor_sql(v) for v in fila)
            lineas.append(f"INSERT INTO {esquema}.{nombre} ({columnas}) VALUES ({valores});")
    lineas.append("\nCOMMIT;")
    return "\n".join(lineas) + "\n"


def escribir_sql(esquema, tablas):
    CARPETA_SQL.mkdir(exist_ok=True)
    (CARPETA_SQL / "01_ddl_esquema_estrella.sql").write_text(ddl(esquema), encoding="utf-8")
    (CARPETA_SQL / "02_dml_carga.sql").write_text(dml(esquema, tablas), encoding="utf-8")
    consultas = "\n\n".join(f"-- {titulo}{sql}" for titulo, sql in consultas_olap(esquema).items())
    (CARPETA_SQL / "03_consultas_olap.sql").write_text(
        "-- Consultas analíticas (OLAP) sobre el esquema estrella\n\n" + consultas + "\n", encoding="utf-8")
    print(f"Scripts generados en {CARPETA_SQL}: 01_ddl_esquema_estrella.sql, 02_dml_carga.sql, 03_consultas_olap.sql")


def leer_config():
    """Lee las credenciales de config.yaml (este archivo NO se sube a Git)."""
    if not ARCHIVO_CONFIG.exists():
        sys.exit("ERROR: no existe config.yaml.\n"
                 "Copia config.example.yaml como config.yaml y escribe tus datos de PostgreSQL.")
    with open(ARCHIVO_CONFIG, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)["postgres"]
    cfg.setdefault("schema", "dw")
    return cfg


def crear_engine(cfg, database=None):
    url = URL.create("postgresql+psycopg2", username=cfg["user"], password=str(cfg["password"]),
                     host=cfg["host"], port=cfg["port"], database=database or cfg["database"])
    return create_engine(url, connect_args={"client_encoding": "utf8"})


def crear_base_si_no_existe(cfg):
    """Crea la base de datos del DW si todavía no existe."""
    engine = crear_engine(cfg, database="postgres").execution_options(isolation_level="AUTOCOMMIT")
    with engine.connect() as con:
        existe = con.execute(text("SELECT 1 FROM pg_database WHERE datname = :n"),
                             {"n": cfg["database"]}).scalar()
        if not existe:
            con.execute(text(f'CREATE DATABASE "{cfg["database"]}"'))
            print(f"Base de datos creada: {cfg['database']}")
        else:
            print(f"Base de datos existente: {cfg['database']}")
    engine.dispose()


def cargar_postgres(cfg, tablas):
    esquema = cfg["schema"]
    try:
        crear_base_si_no_existe(cfg)
        engine = crear_engine(cfg)
        with engine.begin() as con:                       # una sola transacción
            con.exec_driver_sql(ddl(esquema))
            print(f"Esquema '{esquema}' y tablas creadas (DDL)")
            for nombre, df in tablas.items():
                datos = df.copy()
                if "fecha_completa" in datos:
                    datos["fecha_completa"] = datos["fecha_completa"].dt.date
                datos.to_sql(nombre, con, schema=esquema, if_exists="append", index=False, method="multi")
                print(f"  Cargada {esquema}.{nombre}: {len(datos)} registros")
    except UnicodeDecodeError:
        sys.exit("ERROR: PostgreSQL rechazó la conexión (en Windows en español el mensaje no se puede\n"
                 "decodificar). Revisa usuario y contraseña en config.yaml.")
    except OperationalError as e:
        sys.exit(f"ERROR de conexión con PostgreSQL. Revisa que el servicio esté iniciado y los datos\n"
                 f"de config.yaml (host, puerto, usuario, contraseña).\nDetalle: {e.orig}")
    except ProgrammingError as e:
        sys.exit(f"ERROR de SQL al cargar: {e.orig}")

    print("\nVerificación en PostgreSQL:")
    with engine.connect() as con:
        for nombre, df in tablas.items():
            n = con.execute(text(f"SELECT COUNT(*) FROM {esquema}.{nombre}")).scalar()
            print(f"  {nombre:<13} filas en BD = {n:>4} | filas en pandas = {len(df):>4} | "
                  f"{'OK' if n == len(df) else 'DIFERENCIA'}")
        huerfanos = con.execute(text(f"""
            SELECT COUNT(*) FROM {esquema}.fact_ventas f
            LEFT JOIN {esquema}.dim_cliente c ON c.id_cliente_sk = f.id_cliente_sk
            WHERE c.id_cliente_sk IS NULL""")).scalar()
        print(f"  Hechos sin cliente (integridad referencial): {huerfanos}")

        for titulo, sql in consultas_olap(esquema).items():
            print(f"\n--- Consulta OLAP: {titulo} ---")
            print(pd.read_sql(text(sql), con).to_string(index=False))
    engine.dispose()


# ======================================================================
# Programa principal
# ======================================================================
def main(sin_bd=False):
    print("=" * 70)
    print("1. EXTRACT: ingesta simulada del sistema OLTP")
    print("=" * 70)
    df_operativo = extraer_oltp()
    print(df_operativo.head().to_string(index=False))
    print("\nDiagnóstico de calidad de los datos crudos:")
    diagnosticar_oltp(df_operativo)

    print("\n" + "=" * 70)
    print("2. TRANSFORM: corrección y modelado dimensional")
    print("=" * 70)
    df_operativo = corregir_con_catalogos(df_operativo)
    print("Datos corregidos con catálogos maestros:")
    diagnosticar_oltp(df_operativo)

    dim_cliente = construir_dim_cliente(df_operativo)
    dim_producto = construir_dim_producto(df_operativo)
    dim_canal = construir_dim_canal(df_operativo)
    dim_tiempo = construir_dim_tiempo(df_operativo)
    fact_ventas = construir_fact_ventas(df_operativo, dim_cliente, dim_producto, dim_tiempo, dim_canal)

    print("\n=== DIM_CLIENTE ===");  print(dim_cliente.to_string(index=False))
    print("\n=== DIM_PRODUCTO ==="); print(dim_producto.to_string(index=False))
    print("\n=== DIM_CANAL ===");    print(dim_canal.to_string(index=False))
    print(f"\n=== DIM_TIEMPO ({len(dim_tiempo)} días) ===")
    print(dim_tiempo.head(7).to_string(index=False))
    print(f"\n=== TABLA DE HECHOS (FACT_VENTAS) ({len(fact_ventas)} filas) ===")
    print(fact_ventas.head().to_string(index=False))

    print("\nTotales de control:")
    print(f"  Unidades vendidas: {fact_ventas['cantidad'].sum():,}")
    print(f"  Subtotal (monto_total): ${fact_ventas['monto_total'].sum():,.2f}")
    print(f"  Descuentos: ${fact_ventas['descuento'].sum():,.2f}")
    print(f"  IVA: ${fact_ventas['iva'].sum():,.2f}")
    print(f"  Monto final: ${fact_ventas['monto_final'].sum():,.2f}")
    print(f"  Último monto acumulado: ${fact_ventas['monto_acumulado'].iloc[-1]:,.2f}")

    tablas = {"dim_cliente": dim_cliente, "dim_producto": dim_producto, "dim_tiempo": dim_tiempo,
              "dim_canal": dim_canal, "fact_ventas": fact_ventas}

    CARPETA_SALIDAS.mkdir(exist_ok=True)
    for nombre, df in tablas.items():
        df.to_csv(CARPETA_SALIDAS / f"{nombre}.csv", index=False, encoding="utf-8-sig")
    print(f"\nCSV de dimensiones y hechos guardados en {CARPETA_SALIDAS}")

    print("\n" + "=" * 70)
    print("3. LOAD: scripts SQL y carga a PostgreSQL")
    print("=" * 70)
    esquema = "dw" if sin_bd else leer_config()["schema"]
    escribir_sql(esquema, tablas)
    if sin_bd:
        print("Modo --sin-bd: no se conectó a PostgreSQL.")
        return
    cargar_postgres(leer_config(), tablas)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL de un esquema estrella de ventas")
    parser.add_argument("--sin-bd", action="store_true", help="no conectar a PostgreSQL")
    main(parser.parse_args().sin_bd)
