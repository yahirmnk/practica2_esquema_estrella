-- DDL: Esquema estrella del Data Warehouse de Ventas
CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.fact_ventas;
DROP TABLE IF EXISTS dw.dim_cliente;
DROP TABLE IF EXISTS dw.dim_producto;
DROP TABLE IF EXISTS dw.dim_tiempo;
DROP TABLE IF EXISTS dw.dim_canal;

CREATE TABLE dw.dim_cliente (
    id_cliente_sk   INTEGER PRIMARY KEY,
    cliente_nombre  VARCHAR(100) NOT NULL,
    cliente_ciudad  VARCHAR(60)  NOT NULL,
    UNIQUE (cliente_nombre, cliente_ciudad)
);

CREATE TABLE dw.dim_producto (
    id_producto_sk   INTEGER PRIMARY KEY,
    producto_nombre  VARCHAR(100)  NOT NULL,
    categoria_prod   VARCHAR(60)   NOT NULL,
    precio_unitario  NUMERIC(12,2) NOT NULL CHECK (precio_unitario > 0)
);

CREATE TABLE dw.dim_tiempo (
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

CREATE TABLE dw.dim_canal (
    id_canal_sk  INTEGER PRIMARY KEY,
    canal_venta  VARCHAR(30) NOT NULL UNIQUE,
    tipo_canal   VARCHAR(20) NOT NULL
);

CREATE TABLE dw.fact_ventas (
    id_transaccion       INTEGER PRIMARY KEY,
    id_cliente_sk        INTEGER NOT NULL REFERENCES dw.dim_cliente (id_cliente_sk),
    id_producto_sk       INTEGER NOT NULL REFERENCES dw.dim_producto (id_producto_sk),
    id_tiempo_sk         INTEGER NOT NULL REFERENCES dw.dim_tiempo (id_tiempo_sk),
    id_canal_sk          INTEGER NOT NULL REFERENCES dw.dim_canal (id_canal_sk),
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

CREATE INDEX idx_fact_cliente  ON dw.fact_ventas (id_cliente_sk);
CREATE INDEX idx_fact_producto ON dw.fact_ventas (id_producto_sk);
CREATE INDEX idx_fact_tiempo   ON dw.fact_ventas (id_tiempo_sk);
CREATE INDEX idx_fact_canal    ON dw.fact_ventas (id_canal_sk);
