# Pipeline BI — NYC Yellow Taxi (187M Viajes)
### PySpark · Confluent Kafka · dbt-databricks · BigQuery · Power BI

> **Universidad Popular del Cesar — Bases de Datos Avanzadas 2026-I**  
> Docente: Ing. Amilkar Sierra Romano

---

## Equipo

| Integrante | Rol |
|---|---|
| Alex Santiago Morales | Ingesta Bronze / Power BI |
| Alexander Amaya | Kafka Producer / BigQuery |
| Mario Poveda | dbt Silver / Gold / Repositorio |

---

## Descripcion del Proyecto

Pipeline completo de Business Intelligence sobre **187,203,269 viajes reales** de taxis amarillos de Nueva York (enero 2018 - diciembre 2019), siguiendo la arquitectura Medallion: **Bronze / Silver / Gold**.

El pipeline combina:
- **Ingesta historica directa** con PySpark para 180M viajes (enero 2018 - noviembre 2019)
- **Ingesta en tiempo real** via Apache Kafka (Confluent Cloud) para diciembre 2019
- **Transformacion SQL** con dbt-databricks (Bronze → Silver → Gold)
- **Star Schema** en Google BigQuery consumido por Power BI Desktop

> ⚠️ **Por que PySpark es obligatorio**: 187M viajes × 500 bytes = ~93 GB de RAM con pandas — imposible en cualquier PC. PySpark distribuye el procesamiento sin restricciones de memoria.

---

## Stack Tecnologico

| Herramienta | Version | Rol |
|---|---|---|
| PySpark (Databricks Serverless) | — | Motor de procesamiento de 187M viajes |
| Confluent Cloud (Kafka) | Free Tier 5GB | Mensajeria en tiempo real |
| confluent-kafka (Python) | 2.14.0 | Libreria cliente Kafka |
| Delta Lake (Unity Catalog) | Databricks managed | Almacenamiento Bronze/Silver |
| dbt-databricks | 1.12.0 | Transformacion SQL con tests |
| Google BigQuery | olist-bi-pipeline | Data Warehouse Gold |
| Power BI Desktop | Gratuito | 3 dashboards interactivos |
| GitHub | pipeline-taxi-bi | Control de versiones |

---

## Arquitectura del Pipeline

```
━━━━━━━━ PREPARACION (antes de la presentacion) ━━━━━━━━

PASO 1 — Datos historicos (180,306,952 viajes)
  Databricks lee enero 2018 → noviembre 2019 con PySpark
  ↓ directamente desde /databricks-datasets/nyctaxi/
  Bronze: bronze_taxi.trips — 180M filas (Delta Lake)
  ↓ dbt-databricks
  Silver: silver_taxi.silver_trips — 175M filas limpias
  ↓ dbt-databricks
  Gold: gold_taxi.gold_fact_viajes — Star Schema
  ↓ Python copia a BigQuery
  BigQuery: gold_taxi — 175M viajes listos para Power BI

━━━━━━━━ DEMO EN VIVO (frente al profesor) ━━━━━━━━

PASO 2 — 500,000 viajes de diciembre 2019
  PySpark publica 500K viajes en Confluent Cloud (~1.5 min)
  ↓ [profesor ve mensajes llegando en tiempo real en Confluent]
  PySpark consume → Bronze append: 180.8M filas
  ↓ dbt run → Silver y Gold actualizados
  ↓ BigQuery actualizado
  Power BI actualizado → profesor ve el dato recorrer el pipeline
```

---

## Estructura del Repositorio

```
pipeline-taxi-bi/
├── README.md
├── .gitignore
├── databricks/
│   ├── 00_Exploracion.ipynb          # Verificacion del dataset
│   ├── 01_Bronze_Directo.ipynb       # Carga 180M viajes historicos
│   ├── 02_Kafka_Producer.ipynb       # Publica diciembre en Confluent
│   ├── 03_Bronze_Kafka.ipynb         # Consume Kafka → Bronze append
│   └── 04_Gold_to_BigQuery.ipynb     # Copia Gold a BigQuery
├── dbt/
│   └── proyecto_taxi/
│       ├── dbt_project.yml
│       ├── macros/
│       │   └── generate_schema_name.sql
│       ├── models/
│       │   ├── silver/
│       │   │   ├── silver_trips.sql
│       │   │   └── schema.yml
│       │   └── gold/
│       │       ├── gold_dim_tiempo.sql
│       │       ├── gold_dim_zona.sql
│       │       ├── gold_dim_pago.sql
│       │       └── gold_fact_viajes.sql
└── docs/
    ├── dashboard_ejecutivo.png
    ├── dashboard_operacional.png
    └── dashboard_calidad.png
```

---

## Volumetria

| Tabla | Capa | Filas |
|---|---|---|
| bronze_taxi.trips | Bronze | 180,656,952 |
| silver_taxi.silver_trips | Silver | ~175,403,132 |
| gold_taxi.gold_fact_viajes | Gold / BigQuery | 175,403,132 |
| gold_taxi.gold_dim_tiempo | Gold / BigQuery | 17,205 |
| gold_taxi.gold_dim_zona | Gold / BigQuery | 31 |
| gold_taxi.gold_dim_pago | Gold / BigQuery | 6 |

---

## Star Schema

```
                    gold_dim_tiempo
                    (17,205 horas)
                          |
                       date_id
                          |
gold_dim_zona ── zone_id ── gold_fact_viajes ── payment_type ── gold_dim_pago
  (31 zonas)              (175,403,132 filas)                    (6 tipos)
```

**Relaciones Power BI:**
- `gold_fact_viajes[date_id]` → `gold_dim_tiempo[date_id]`
- `gold_fact_viajes[pickup_location_id]` → `gold_dim_zona[zone_id]`
- `gold_fact_viajes[payment_type]` → `gold_dim_pago[payment_type]`

---

## Tests de Calidad (dbt test)

```
dbt test --select silver

PASS not_null_silver_trips_pickup_at
PASS not_null_silver_trips_total_amount
PASS not_null_silver_trips_trip_distance
PASS not_null_silver_trips_fare_amount
PASS not_null_silver_trips_passenger_count
PASS accepted_values_silver_trips_payment_type

Done. PASS=6 WARN=0 ERROR=0 SKIP=0 TOTAL=6
```

---

## Reproducir el Pipeline

### Requisitos previos
- Cuenta Databricks (Managed Trial con Serverless)
- Cuenta Confluent Cloud (Free Tier)
- Proyecto GCP con BigQuery habilitado
- Python 3.x con dbt-databricks instalado

### Configuracion dbt
```bash
pip install dbt-databricks
cd dbt/proyecto_taxi
dbt debug  # Verificar conexion
```

El archivo `profiles.yml` debe estar en `~/.dbt/` con las credenciales del SQL Warehouse de Databricks. **No se sube a GitHub.**

### Ejecucion completa
```bash
# 1. Ejecutar notebooks en Databricks en orden:
#    01_Bronze_Directo → 02_Kafka_Producer → 03_Bronze_Kafka → 04_Gold_to_BigQuery

# 2. Transformar con dbt:
dbt run --select silver gold
dbt test --select silver

# 3. Conectar Power BI a BigQuery dataset gold_taxi
```

---

## Dashboards Power BI

| Dashboard | Descripcion |
|---|---|
| Ejecutivo | Revenue total, viajes por hora, distribucion por pago, matriz dia/hora |
| Operacional | Top zonas, revenue por borough, dispersion distancia/tarifa, rush hour |
| Calidad | Gauge vs meta 187M, propinas por tipo de pago, analisis mensual |

---

## KPIs Finales

| KPI | Valor |
|---|---|
| Total viajes procesados | 175,403,132 |
| Tests dbt | 6/6 PASS |
| Tiempo Kafka 200K viajes | 42 segundos |
| Tiempo dbt run silver+gold | ~73 segundos |
| Costo BigQuery estimado | ~$0.74/mes |
| Consumo Confluent Cloud | ~2.25 GB / 5 GB gratuitos |

---

## Preguntas del Docente

| Pregunta | Respuesta |
|---|---|
| ¿Por que PySpark y no pandas? | 187M viajes = ~93 GB RAM con pandas. Imposible en cualquier PC. PySpark distribuye sin limites. |
| ¿Por que Kafka si el dato ya esta en Databricks? | Los historicos se cargan directamente (eficiente). Kafka simula el flujo en tiempo real de datos nuevos llegando — como si fueran los taxis reportando en ese momento. |
| ¿Por que Confluent Cloud? | Implementacion de Kafka mas usada en produccion. Interfaz visual para ver mensajes en tiempo real durante la demo. 2.25 GB de 5 GB gratuitos. |
| ¿Por que BigQuery y no SQL Warehouse? | Power BI no conecta al SQL Warehouse de Databricks Serverless (confirmado en proyecto anterior). BigQuery si funciona con conector nativo. Arquitectura estandar en la industria. |
| ¿Por que Delta Lake y no Parquet? | Delta Lake agrega transacciones ACID y modo append. Sin Delta Lake no se podria agregar diciembre sin borrar los 180M de viajes anteriores. |
| ¿Por que Star Schema y no tabla plana? | Una tabla plana repetiria zona y pago 175M veces. El Star Schema normaliza en dims de 31 y 6 filas — reduce almacenamiento y mejora rendimiento en Power BI. |
