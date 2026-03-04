-- Databricks notebook source
-- CAPA SILVER: Inflables con ID y datos limpios
CREATE TABLE IF NOT EXISTS alquiler_inflables.silver_inflables AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY Inflables) as id_inflable,
  Inflables as nombre_inflable,
  Medidas_metros as medidas
FROM alquiler_inflables.bronze_inflables;

SELECT * FROM alquiler_inflables.silver_inflables;

-- COMMAND ----------

-- CAPA SILVER: Clientes con datos limpios
CREATE TABLE IF NOT EXISTS alquiler_inflables.silver_clientes AS
SELECT
  id_cliente,
  UPPER(nombre) as nombre,
  telefono,
  email
FROM alquiler_inflables.bronze_clientes;

SELECT * FROM alquiler_inflables.silver_clientes;

-- COMMAND ----------

-- CAPA SILVER: Reservas con estados detallados
CREATE TABLE IF NOT EXISTS alquiler_inflables.silver_reservas AS
SELECT
  r.id_reserva,
  r.id_cliente,
  s.id_inflable,
  r.inflable as nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON r.inflable = s.nombre_inflable;

SELECT * FROM alquiler_inflables.silver_reservas

-- COMMAND ----------

-- Limpiar y recrear silver_reservas sin duplicados
DROP TABLE IF EXISTS alquiler_inflables.silver_reservas;

CREATE TABLE alquiler_inflables.silver_reservas AS
SELECT DISTINCT
  r.id_reserva,
  r.id_cliente,
  s.id_inflable,
  r.inflable as nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON r.inflable = s.nombre_inflable;

SELECT COUNT(*) as total_reservas FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

-- Ver cómo están los nombres en cada tabla
SELECT 'bronze_reservas' as tabla, inflable as nombre FROM alquiler_inflables.bronze_reservas
UNION ALL
SELECT 'silver_inflables' as tabla, nombre_inflable as nombre FROM alquiler_inflables.silver_inflables
ORDER BY nombre;

-- COMMAND ----------

DROP TABLE IF EXISTS alquiler_inflables.silver_reservas;

CREATE TABLE alquiler_inflables.silver_reservas AS
SELECT DISTINCT
  r.id_reserva,
  r.id_cliente,
  s.id_inflable,
  r.inflable as nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable));

SELECT COUNT(*) as total_reservas FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

-- Corregir el nombre mal escrito en silver_inflables
UPDATE alquiler_inflables.silver_inflables
SET nombre_inflable = 'Castillo combo real'
WHERE nombre_inflable = 'Castilo combo real';

-- Verificar que quedó bien
SELECT * FROM alquiler_inflables.silver_inflables

-- COMMAND ----------

-- Corregir el nombre mal escrito en silver_inflables
UPDATE alquiler_inflables.silver_inflables
SET nombre_inflable = 'Castillo combo real'
WHERE nombre_inflable = 'Castilo combo real';

-- Verificar que quedó bien
SELECT * FROM alquiler_inflables.silver_inflables

-- COMMAND ----------

DROP TABLE IF EXISTS alquiler_inflables.silver_reservas;

CREATE TABLE alquiler_inflables.silver_reservas AS
SELECT DISTINCT
  r.id_reserva,
  r.id_cliente,
  s.id_inflable,
  s.nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable));

SELECT COUNT(*) as total_reservas FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

-- Ver qué reservas NO encuentran su inflable
SELECT DISTINCT r.inflable
FROM alquiler_inflables.bronze_reservas r
LEFT JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable))
WHERE s.id_inflable IS NULL;

-- COMMAND ----------

SELECT COUNT(*) as total FROM alquiler_inflables.bronze_reservas;

-- COMMAND ----------

DROP TABLE IF EXISTS alquiler_inflables.silver_reservas;

CREATE TABLE alquiler_inflables.silver_reservas AS
SELECT DISTINCT
  r.id_reserva,
  r.id_cliente,
  s.id_inflable,
  s.nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable));

SELECT COUNT(*) as total_reservas FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

-- Ver exactamente qué hay en bronze_reservas
SELECT * FROM alquiler_inflables.bronze_reservas;

-- COMMAND ----------

DROP TABLE IF EXISTS alquiler_inflables.bronze_reservas;

CREATE TABLE alquiler_inflables.bronze_reservas
AS SELECT * FROM alquiler_inflables.reservas;

SELECT COUNT(*) as total FROM alquiler_inflables.bronze_reservas;

-- COMMAND ----------

DROP TABLE IF EXISTS alquiler_inflables.bronze_reservas;

CREATE TABLE alquiler_inflables.bronze_reservas AS
SELECT DISTINCT * FROM alquiler_inflables.reservas;

SELECT COUNT(*) as total FROM alquiler_inflables.bronze_reservas;

-- COMMAND ----------

DROP TABLE IF EXISTS alquiler_inflables.silver_reservas;

CREATE TABLE alquiler_inflables.silver_reservas AS
SELECT
  MIN(r.id_reserva) as id_reserva,
  r.id_cliente,
  s.id_inflable,
  s.nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable))
GROUP BY r.id_cliente, s.id_inflable, s.nombre_inflable, r.fecha, r.hora;

SELECT COUNT(*) as total FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

SELECT DISTINCT r.inflable
FROM alquiler_inflables.bronze_reservas r
LEFT JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable))
WHERE s.id_inflable IS NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE alquiler_inflables.silver_reservas AS
SELECT
  r.id_reserva,
  r.id_cliente,
  s.id_inflable,
  s.nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable));

SELECT COUNT(*) as total FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

SELECT nombre_inflable, COUNT(*) as cantidad
FROM alquiler_inflables.silver_inflables
GROUP BY nombre_inflable
HAVING COUNT(*) > 1;

-- COMMAND ----------

CREATE OR REPLACE TABLE alquiler_inflables.silver_inflables AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY nombre_inflable) as id_inflable,
  nombre_inflable,
  medidas
FROM (
  SELECT DISTINCT TRIM(Inflables) as nombre_inflable, Medidas_metros as medidas
  FROM alquiler_inflables.bronze_inflables
);

SELECT COUNT(*) as total FROM alquiler_inflables.silver_inflables;

-- COMMAND ----------

CREATE OR REPLACE TABLE alquiler_inflables.silver_reservas AS
SELECT
  r.id_reserva,
  r.id_cliente,
  s.id_inflable,
  s.nombre_inflable,
  r.fecha,
  r.hora,
  CASE
    WHEN r.hora < '12:00' THEN 'en_entrega'
    WHEN r.hora >= '12:00' AND r.hora < '20:00' THEN 'en_uso'
    WHEN r.hora >= '20:00' THEN 'en_limpieza'
    ELSE 'disponible'
  END as estado
FROM alquiler_inflables.bronze_reservas r
JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable));

SELECT COUNT(*) as total FROM alquiler_inflables.silver_reservas;

-- COMMAND ----------

SELECT DISTINCT r.inflable
FROM alquiler_inflables.bronze_reservas r
LEFT JOIN alquiler_inflables.silver_inflables s 
  ON LOWER(TRIM(r.inflable)) = LOWER(TRIM(s.nombre_inflable))
WHERE s.id_inflable IS NULL;

-- COMMAND ----------

SELECT 'bronze_reservas' as tabla, inflable as nombre 
FROM alquiler_inflables.bronze_reservas 
WHERE LOWER(inflable) LIKE '%castillo combo%'
UNION ALL
SELECT 'silver_inflables' as tabla, nombre_inflable as nombre 
FROM alquiler_inflables.silver_inflables 
WHERE LOWER(nombre_inflable) LIKE '%castillo combo%';

-- COMMAND ----------

UPDATE alquiler_inflables.bronze_reservas
SET inflable = 'Castillo combo real'
WHERE LOWER(TRIM(inflable)) = 'castillo combo real';

UPDATE alquiler_inflables.silver_inflables
SET nombre_inflable = 'Castillo combo real'
WHERE LOWER(TRIM(nombre_inflable)) LIKE '%castilo combo%';