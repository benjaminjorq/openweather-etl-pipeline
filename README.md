# 🌤️ OpenWeather ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data_Processing-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)

##  Sobre el Proyecto

Este proyecto implementa un pipeline de datos batch **End-to-End** modularizado y contenerizado. Su objetivo es extraer datos climáticos y de contaminación desde la API de OpenWeather (Current Weather Data & Air Pollution), procesarlos para asegurar su calidad y disponibilidad, y generar reportes analíticos.

El sistema simula un entorno productivo siguiendo la arquitectura **Medallion (Bronze/Silver/Gold)**, priorizando el manejo de errores, la limpieza de datos y la trazabilidad mediante logs.

<div align="center">
  <img width="100%" alt="pipeline_diagram" src="https://github.com/user-attachments/assets/bcb2a51a-2000-43da-ae69-8c0f2ee6b0ce" />
</div>

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

## 💡 Decisiones de Diseño & FAQ

*Justificación de las elecciones técnicas para este proyecto:*

**1. ¿Por qué se optó por una arquitectura Medallion (Bronze/Silver/Gold)?**
Se eligió la arquitectura Medallion para estructurar el pipeline en capas con responsabilidades claramente definidas: almacenamiento de datos sin procesar (Bronze), limpieza y validación de calidad (Silver), y transformaciones orientadas a negocio (Gold), con el objetivo de que los reportes finales consuman siempre datos confiables y coherentes.

**2. ¿Por qué utilizar CSV en la capa Silver en lugar de Parquet?**
Se optó por CSV para priorizar la simplicidad y facilidad de inspección en un entorno local y batch de bajo volumen. En escenarios de mayor escala, Parquet seria preferible por compresión y mejor performance de lectura (sistemas distribuidos).

**3. ¿Cómo se garantiza la Idempotencia del pipeline?**
El pipeline controla duplicados en la capa Silver mediante `drop_duplicates`, sin embargo para reforzar la idempotencia se aplicó una restricción en la base de datos mediante una `PRIMARY KEY` compuesta (city, processed_timestamp) en PostgreSQL, evitando la inserción de registros duplicados ante re-ejecuciones del pipeline.

**4. ¿Por qué Orquestar con Airflow en lugar de CRON scripts?**
Se eligió Airflow porque permite tener mayor control sobre el flujo completo del pipeline, gestionando dependencias (`Ingest >> Transform >> Load`), reintentos y monitoreo desde una sola interfaz. A diferencia de cron, ofrece visibilidad del estado de cada tarea y facilita escalar el workflow si el proyecto crece en complejidad.

---

## 🛠️ Tech Stack & Librerías

El proyecto utiliza herramientas estándar de la industria, definidas en `requirements.txt`:

* **Ingeniería:** `Python`, `Pandas` (Manipulación de DataFrames), `SQLAlchemy` (Gestión de Conexión de Base de Datos).
* **Infraestructura:** `Docker` & `Docker Compose` (Gestión del entorno aislado)
* **Configuración:** `PyYAML` (Gestión de config de ciudades), `Python-dotenv` (Variables de entorno seguras).
* **Calidad:** `Pytest` (Tests unitarios), `Logging` (Trazabilidad de ejecución).

---

## ⚙️ Orquestación

La automatización y el control del flujo de datos se gestionan con Apache Airflow. Su implementación permite coordinar las dependencias entre tareas, gestionar reintentos automáticos y mantener un registro claro (logs) de cada ejecución para asegurar la calidad del dato.

<p align="center">
  <img width="965" alt="graph airflow" src="https://github.com/user-attachments/assets/28a59102-26b0-451a-a09c-b1a7b39b27f8" />
  <br>
  <em>Vista del DAG en Airflow: Ejecución exitosa de todas las etapas del pipeline.</em>
</p>

---

### 📑 Monitoreo y Logs

El sistema genera logs detallados en cada etapa para facilitar el monitoreo y asegurar la calidad de los datos. Puedes expandir cada sección para ver la evidencia técnica:

<details>
<summary><b>1. Ingesta de Datos (Bronze Layer)</b></summary>
Evidencia de la extracción batch desde la API de OpenWeather y el almacenamiento exitoso de los datos crudos en formato JSON.
<br><br>
<img width="909" alt="log ingesta" src="https://github.com/user-attachments/assets/270fd292-4366-48b1-a6f6-447ad490480d" />
</details>

<details>
<summary><b>2. Transformación y Limpieza (Silver Layer)</b></summary>
Logs del proceso de limpieza, normalización de esquemas y aplanamiento de estructuras anidadas mediante Pandas.
<br><br>
<img width="960" alt="log transform" src="https://github.com/user-attachments/assets/996f31d5-1670-4295-bae3-c1c9cdf1b99b" />
</details>

<details>
<summary><b>3. Carga a PostgreSQL</b></summary>
Confirmación de la ingesta de datos limpios hacia la base de datos relacional PostgreSQL para persistencia a largo plazo.
<br><br>
<img width="827" alt="log load" src="https://github.com/user-attachments/assets/8fad41f9-e2f4-4204-9892-58e350c9cbe5" />
</details>

<details>
<summary><b>4. Generación de Reportes (Gold Layer)</b></summary>
Evidencia de la lógica de negocio aplicada: creación de rankings de contaminación y resúmenes estadísticos con niveles de calidad de aire.
<br><br>
<img width="944" height="455" alt="log gold" src="https://github.com/user-attachments/assets/414faf12-8739-4e77-8ff7-64a3e04ae2f9" />
</details>

---

## 🚀 Requerimientos

Antes de comenzar, asegúrate de tener instalado:
* [Docker Desktop](https://www.docker.com/products/docker-desktop)
* Una API Key activa de [OpenWeatherMap](https://openweathermap.org/api)

## 🔧 Configuración e Instalación

1.  **Clonar el repositorio:**
    ```bash
    git clone https://github.com/benjaminjorq/openweather-etl-pipeline.git
    cd openweather-etl-pipeline
    ```

2.  **Configurar variables de entorno:**
    Crea un archivo `.env` en la raíz del proyecto y agrega tus credenciales (puedes usar el archivo `.env.example` como guía):
    ```env
    AIRFLOW_UID=50000
    OPENWEATHER_API_KEY=tu_api_key_aqui
    POSTGRES_USER=airflow
    POSTGRES_PASSWORD=tu_password
    POSTGRES_DB=weather_db
    ```

3.  **Iniciar los servicios:**
    Ejecuta el siguiente comando para levantar Airflow y PostgreSQL:
    ```bash
    docker-compose up -d
    ```
---

## 📂 Estructura del Repositorio

```bash
openweather-etl-pipeline/
├── config/              # Configuraciones (YAML)
├── dags/                # Orquestación (DAGs de Airflow)
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
├── docker-compose.yml   # Configuración Docker


