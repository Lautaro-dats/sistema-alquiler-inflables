-- Databricks notebook source
-- Más clientes
INSERT INTO alquiler_inflables.clientes VALUES
(4, 'Diego Fernández', '1144556677', 'diego@gmail.com'),
(5, 'Sofía López', '1155443322', 'sofia@gmail.com'),
(6, 'Martín Rodríguez', '1166778899', 'martin@gmail.com'),
(7, 'Valentina García', '1177889900', 'vale@gmail.com'),
(8, 'Lucas Sánchez', '1188990011', 'lucas@gmail.com');

-- Más reservas de fin de semana
INSERT INTO alquiler_inflables.reservas VALUES
(5, 4, 'Curvo marino', '2025-03-15', '09:00', 'en_entrega'),
(6, 5, 'Isla dino', '2025-03-15', '09:00', 'en_entrega'),
(7, 6, 'Castillo combo real', '2025-03-15', '15:00', 'en_uso'),
(8, 7, 'Aventura', '2025-03-15', '15:00', 'en_uso'),
(9, 8, 'Curvo selva', '2025-03-16', '09:00', 'en_entrega'),
(10, 4, 'Rampa escaloneta', '2025-03-16', '15:00', 'en_uso'),
(11, 5, 'Carrera lego', '2025-03-16', '15:00', 'en_uso'),
(12, 6, 'Castillo arcoiris', '2025-03-16', '20:00', 'en_limpieza');

SELECT 'Datos de prueba cargados correctamente' as estado;