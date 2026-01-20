# 🌤️ OpenWeather ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data_Processing-150458?style=for-the-badge&logo=pandas&logoColor=white)

## 📖 Sobre el Proyecto

Este proyecto implementa un pipeline de datos batch **End-to-End** modularizado y contenerizado. Su objetivo es extraer datos climáticos y de contaminación desde la API de OpenWeather, procesarlos para asegurar su calidad y disponibilidad, y generar reportes analíticos.

El sistema simula un entorno productivo siguiendo la arquitectura **Medallion (Bronze/Silver/Gold)**, priorizando el manejo de errores, la limpieza de datos y la trazabilidad mediante logs.

<img width="1209" height="738" alt="pipeline_diagram" src="https://github.com/user-attachments/assets/bcb2a51a-2000-43da-ae69-8c0f2ee6b0ce" />


## 🏗️ Arquitectura de Datos

El flujo está diseñado para transformar datos crudos en insights de negocio:

### 1. 🥉 Bronze Layer (Ingesta Raw)
* **Fuente:** API OpenWeather (Endpoints `/weather` y `/air_pollution`).
* **Proceso:** Extracción vía `requests` validando códigos de estado HTTP (200 OK).
* **Almacenamiento:** Archivos JSON crudos guardados localmente para auditoría histórica.

### 2. 🥈 Silver Layer (Limpieza y Normalización)
* **Transformación:**
    * **Flattening:** Aplanamiento de estructuras JSON anidadas (diccionarios dentro de listas) usando Python y Pandas.
    * **Data Quality:** Conversión de tipos de datos, eliminación de duplicados y filtrado de valores atípicos.
* **Almacenamiento:** Archivos **CSV** con particionamiento tipo Hive (`year=YYYY/month=MM/day=DD`) para optimizar la organización y consulta.
* **Carga DB:** Ingesta de datos limpios hacia **PostgreSQL** mediante `SQLAlchemy`.

### 3. 🥇 Gold Layer (Reportes de Negocio)
* **Lógica de Negocio:** Enriquecimiento de datos aplicando reglas de clasificación (ej. Calidad de aire "Peligrosa" si PM2.5 > 55).
* **Outputs Generados:** Rankings de contaminación y resúmenes nacionales agrupados.

---

## 🛠️ Tech Stack & Librerías

El proyecto utiliza herramientas estándar de la industria, definidas en `requirements.txt`:

* **Ingeniería:** `Python`, `Pandas` (Manipulación de DataFrames), `SQLAlchemy` (Gestión de Conexión de Base de Datos).
* **Infraestructura:** `Docker` & `Docker Compose` (Sistema aislado)
* **Configuración:** `PyYAML` (Gestión de config de ciudades), `Python-dotenv` (Variables de entorno seguras).
* **Calidad:** `Pytest` (Tests unitarios), `Logging` (Trazabilidad de ejecución).

---

## 📂 Estructura del Repositorio

```bash
openweather-etl-pipeline/
├── config/              # Configuraciones (YAML)
├── data/                # Data Lake Local
│   ├── bronze/          # Raw JSONs
│   ├── silver/          # Datos Limpios (Particionados)
│   │   └── year=YYYY/month=MM/day=DD/
│   └── gold/            # Reportes de Negocio
│       ├── ranking/     # Top contaminación (.csv)
│       └── summary/     # Promedios por país (.csv)
├── pipeline/            # Código fuente modular
│   ├── ingestion/       # batch_ingest.py
│   ├── transform/       # batch_transform.py
│   ├── load/            # load_database.py
│   └── reports_to_gold/ # gold_report.py
├── pytests/             # Pruebas unitarias
├── Dockerfile           # Imagen del entorno
├── docker-compose.yml   # Orquestación de servicios
└── requirements.txt     # Dependencias



## Apache Airflow

En desarrollo...





