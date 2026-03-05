# 🎪 Sistema de Gestión de Alquiler de Inflables

## ¿Qué problema resuelve?
En mi trabajo alquilamos inflables y peloteros para eventos y cumpleaños. 
Cuando hay muchas reservas en un fin de semana, se nos pasaba confirmar 
el mismo pelotero a dos clientes distintos en el mismo horario, 
generando conflictos y mala experiencia al cliente.

## Solución
Base de datos en Databricks con arquitectura Medallion (Bronze/Silver/Gold) 
que detecta automáticamente los conflictos de doble reserva.

## Arquitectura
- **Bronze**: Datos crudos sin modificar
- **Silver**: Datos limpios, normalizados y con estados
- **Gold**: Análisis, reportes y detección de conflictos

## Tecnologías utilizadas
- Databricks (Community Edition)
- SQL / PySpark
- Python (Pandas)
- GitHub

## Dashboard
[Insertar capturas ![WhatsApp Image 2026-03-05 at 12 04 31](https://github.com/user-attachments/assets/5cca3af8-686a-4b9c-ab34-81da058c819b)
del dashboard acá]
