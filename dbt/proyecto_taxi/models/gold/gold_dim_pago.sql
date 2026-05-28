{{ config(materialized='table', schema='gold_taxi') }}
SELECT * FROM (VALUES
  (1, 'Credit Card', 'Tarjeta de Credito'),
  (2, 'Cash',        'Efectivo'),
  (3, 'No Charge',   'Sin Cargo'),
  (4, 'Dispute',     'Disputa'),
  (5, 'Unknown',     'Desconocido'),
  (6, 'Voided Trip', 'Viaje Anulado')
) AS t(payment_type, description_en, description_es)