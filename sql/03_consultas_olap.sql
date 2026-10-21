-- Consultas analíticas (OLAP) sobre el esquema estrella

-- Ventas por categoría y producto (ROLLUP con subtotales)
SELECT COALESCE(p.categoria_prod, 'TOTAL GENERAL') AS categoria,
       COALESCE(p.producto_nombre, '(subtotal)')   AS producto,
       SUM(f.cantidad)    AS unidades,
       SUM(f.monto_final) AS ventas
FROM dw.fact_ventas f
JOIN dw.dim_producto p ON p.id_producto_sk = f.id_producto_sk
GROUP BY ROLLUP (p.categoria_prod, p.producto_nombre)
ORDER BY p.categoria_prod NULLS LAST, p.producto_nombre NULLS LAST;

-- Ventas entre semana vs. fin de semana
SELECT CASE WHEN t.es_fin_semana THEN 'Fin de semana' ELSE 'Entre semana' END AS tipo_dia,
       COUNT(DISTINCT t.id_tiempo_sk)  AS dias,
       COUNT(*)                         AS transacciones,
       SUM(f.monto_final)               AS ventas,
       ROUND(SUM(f.monto_final) / COUNT(DISTINCT t.id_tiempo_sk), 2) AS venta_promedio_dia
FROM dw.fact_ventas f
JOIN dw.dim_tiempo t ON t.id_tiempo_sk = f.id_tiempo_sk
GROUP BY t.es_fin_semana
ORDER BY t.es_fin_semana;

-- Ventas por cliente y canal
SELECT c.cliente_nombre, c.cliente_ciudad, k.canal_venta,
       COUNT(*) AS transacciones, SUM(f.monto_final) AS ventas
FROM dw.fact_ventas f
JOIN dw.dim_cliente c ON c.id_cliente_sk = f.id_cliente_sk
JOIN dw.dim_canal   k ON k.id_canal_sk   = f.id_canal_sk
GROUP BY c.cliente_nombre, c.cliente_ciudad, k.canal_venta
ORDER BY c.cliente_nombre, ventas DESC;

-- Ventas por día de la semana
SELECT t.dia_semana_num, t.dia_semana,
       COUNT(*) AS transacciones, SUM(f.monto_final) AS ventas
FROM dw.fact_ventas f
JOIN dw.dim_tiempo t ON t.id_tiempo_sk = f.id_tiempo_sk
GROUP BY t.dia_semana_num, t.dia_semana
ORDER BY t.dia_semana_num;
