-- Databricks notebook source
-- CAPA BRONZE: Datos crudos de inflables
CREATE TABLE IF NOT EXISTS alquiler_inflables.bronze_inflables
AS SELECT * FROM alquiler_inflables.inflables;

-- CAPA BRONZE: Datos crudos de clientes  
CREATE TABLE IF NOT EXISTS alquiler_inflables.bronze_clientes
AS SELECT * FROM alquiler_inflables.clientes;

-- CAPA BRONZE: Datos crudos de reservas
CREATE TABLE IF NOT EXISTS alquiler_inflables.bronze_reservas
AS SELECT * FROM alquiler_inflables.reservas;

SELECT 'Bronze cargado correctamente' as estado;

-- COMMAND ----------

DROP TABLE IF EXISTS alquiler_inflables.bronze_reservas;

CREATE TABLE alquiler_inflables.bronze_reservas AS
SELECT DISTINCT * FROM alquiler_inflables.reservas
WHERE id_reserva IS NOT NULL;

SELECT COUNT(*) as total FROM alquiler_inflables.bronze_reservas;