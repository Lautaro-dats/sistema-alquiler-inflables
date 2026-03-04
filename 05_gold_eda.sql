-- Databricks notebook source
-- EDA: Resumen general del negocio
SELECT 
  COUNT(DISTINCT id_cliente) as total_clientes,
  COUNT(DISTINCT nombre_inflable) as total_inflables,
  COUNT(*) as total_reservas
FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

-- EDA: Inflables más alquilados
SELECT 
  nombre_inflable,
  COUNT(*) as veces_alquilado
FROM alquiler_inflables.silver_reservas
GROUP BY nombre_inflable
ORDER BY veces_alquilado DESC;

-- COMMAND ----------

-- EDA: Reservas por estado
SELECT 
  estado,
  COUNT(*) as cantidad
FROM alquiler_inflables.silver_reservas
GROUP BY estado
ORDER BY cantidad DESC;

-- COMMAND ----------

-- GOLD: Detección de conflictos de doble reserva
SELECT 
  nombre_inflable,
  fecha,
  hora,
  COUNT(*) as cantidad_reservas,
  'CONFLICTO' as alerta
FROM alquiler_inflables.silver_reservas
GROUP BY nombre_inflable, fecha, hora
HAVING COUNT(*) > 1
ORDER BY fecha, hora;

-- COMMAND ----------

-- GOLD: Disponibilidad por fin de semana
SELECT
  nombre_inflable,
  fecha,
  COUNT(*) as reservas_del_dia,
  CASE 
    WHEN COUNT(*) = 0 THEN 'DISPONIBLE'
    ELSE 'OCUPADO'
  END as disponibilidad
FROM alquiler_inflables.silver_reservas
GROUP BY nombre_inflable, fecha
ORDER BY fecha, nombre_inflable;

-- COMMAND ----------

-- GOLD: Reporte final completo
SELECT 
  r.id_reserva,
  c.nombre as cliente,
  r.nombre_inflable,
  r.fecha,
  r.hora,
  r.estado
FROM alquiler_inflables.silver_reservas r
JOIN alquiler_inflables.silver_clientes c 
  ON r.id_cliente = c.id_cliente
ORDER BY r.fecha, r.hora;