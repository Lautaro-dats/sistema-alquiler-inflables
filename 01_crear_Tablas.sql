-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS alquiler_inflables;
USE alquiler_inflables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import subprocess
-- MAGIC subprocess.run(["pip", "install", "openpyxl"], capture_output=True)
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC df = pd.read_excel("/Volumes/workspace/alquiler_inflables/proyecto/Hoja de cálculo sin título.xlsx")
-- MAGIC
-- MAGIC display(df)
-- MAGIC import pandas as pd
-- MAGIC df = pd.read_excel("/Volumes/workspace/alquiler_inflables/proyecto/Hoja de cálculo sin título.xlsx")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Renombrar columna para quitar los paréntesis
-- MAGIC df.columns = ['Inflables', 'Medidas_metros']
-- MAGIC
-- MAGIC spark_df = spark.createDataFrame(df)
-- MAGIC spark_df.write.mode("overwrite").saveAsTable("alquiler_inflables.inflables")
-- MAGIC
-- MAGIC print("Tabla guardada correctamente ✓")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS alquiler_inflables.clientes (
  id_cliente INT,
  nombre STRING,
  telefono STRING,
  email STRING
);

INSERT INTO alquiler_inflables.clientes VALUES
(1, 'María González', '1123456789', 'maria@gmail.com'),
(2, 'Carlos Pérez', '1187654321', 'carlos@gmail.com'),
(3, 'Laura Martínez', '1199887766', 'laura@gmail.com');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS alquiler_inflables.reservas (
  id_reserva INT,
  id_cliente INT,
  inflable STRING,
  fecha DATE,
  hora STRING,
  estado STRING
);

INSERT INTO alquiler_inflables.reservas VALUES
(1, 1, 'Castillo arcoiris', '2025-03-08', '10:00', 'confirmado'),
(2, 2, 'Castillo arcoiris', '2025-03-08', '10:00', 'confirmado'),
(3, 3, 'Isla dino', '2025-03-08', '14:00', 'confirmado'),
(4, 1, 'Curvo marino', '2025-03-09', '11:00', 'confirmado');

-- COMMAND ----------

-- Detectar conflictos de doble reserva
SELECT 
  inflable,
  fecha,
  hora,
  COUNT(*) as cantidad_reservas
FROM alquiler_inflables.reservas
GROUP BY inflable, fecha, hora
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- Ver todas las reservas con nombre del cliente
SELECT 
  r.id_reserva,
  c.nombre,
  r.inflable,
  r.fecha,
  r.hora,
  r.estado
FROM alquiler_inflables.reservas r
JOIN alquiler_inflables.clientes c ON r.id_cliente = c.id_cliente
ORDER BY r.fecha, r.hora;

-- COMMAND ----------

-- SOLUCIÓN: Ver disponibilidad de inflables
SELECT 
  i.Inflables as inflable,
  i.Medidas_metros,
  CASE 
    WHEN COUNT(r.id_reserva) = 0 THEN 'DISPONIBLE'
    ELSE 'OCUPADO'
  END as disponibilidad
FROM alquiler_inflables.inflables i
LEFT JOIN alquiler_inflables.reservas r 
  ON i.Inflables = r.inflable
  AND r.fecha = '2025-03-08'
  AND r.hora = '10:00'
GROUP BY i.Inflables, i.Medidas_metros;

-- COMMAND ----------

-- Limpiar tabla reservas original
CREATE OR REPLACE TABLE alquiler_inflables.reservas AS
SELECT DISTINCT * FROM alquiler_inflables.reservas;

SELECT COUNT(*) as total FROM alquiler_inflables.reservas;

-- COMMAND ----------

SELECT id_reserva, COUNT(*) as cantidad
FROM alquiler_inflables.reservas
GROUP BY id_reserva
HAVING COUNT(*) > 1;