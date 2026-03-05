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
- 
## Preguntas de negocio que resuelve

1. **¿Qué inflables son los más demandados?**  
   Identificamos que el Castillo arcoiris es el más alquilado, lo que ayuda a priorizar su mantenimiento.

2. **¿En qué estado operativo están los inflables?**  
   El sistema muestra en tiempo real si cada inflable está en entrega, en uso o en limpieza.

3. **¿Existen conflictos de doble reserva?**  
   La consulta principal detecta automáticamente cuando el mismo inflable fue reservado dos veces en el mismo horario, evitando conflictos con clientes.

4. **¿Cuál es la disponibilidad por fecha?**  
   Se puede consultar qué inflables están disponibles u ocupados para cualquier fecha específica.
## Tecnologías utilizadas
- Databricks (Community Edition)
- SQL / PySpark
- Python (Pandas)
- GitHub

## Dashboard
 ![WhatsApp Image 2026-03-05 at 12 04 31](https://github.com/user-attachments/assets/5cca3af8-686a-4b9c-ab34-81da058c819b)
## Sobre el autor

Soy Lautaro, actualmente haciendo un bootcamp de ingeniería de datos.
Este proyecto nació de un problema real en mi trabajo — una empresa 
de alquiler de inflables y peloteros para eventos y cumpleaños.

La necesidad concreta de resolver un problema del día a día fue lo que 
me motivó a aplicar los conceptos aprendidos en el bootcamp a un caso real.

📫 GitHub: [Lautaro-dats](https://github.com/Lautaro-dats)
