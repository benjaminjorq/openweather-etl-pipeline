# ☀️ OpenWeather ETL Pipeline

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

---

## Contexto y Alcance del Proyecto (Business Case & Scope)

Este pipeline se diseñó con un doble propósito: resolver un problema analítico a través de un proceso ETL (ingesta, normalización de datos anidados y carga de la información en bases de datos relacional) y servir como una implementación práctica para estudiar fundamentos sólidos de Ingeniería de Datos.

Más que buscar el procesamiento de grandes volúmenes, el diseño del pipeline prioriza la modularidad, la trazabilidad y el manejo de escenarios reales de integración de datos.

**El objetivo de este pipeline es:**
1. **Automatizar** Automatizar la extracción diaria para 25 ciudades (Chile y Sudamérica) y estudiar los niveles de contaminación del Aire (PM2.5, PM10, CO, NO2, O3, AQI).
   * *Tech note:* La selección de ciudades se desacopló del código mediante un archivo `cities.yaml` para evitar cambios en el script al agregar o quitar ciudades de interés y respetar los *rate limits* de la API gratuita.
2. **Estandarizar y limpiar** la información en un repositorio centralizado.
3. **Generar valor inmediato** automatizando reportes (Ranking de ciudades más contaminadas y resúmenes) listos para ser consumidos por herramientas de BI.

---

## Arquitectura de Datos

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

## Decisiones de Diseño & FAQ

*Justificación de las elecciones técnicas para este proyecto:*

**1. ¿Por qué se optó por una arquitectura Medallion (Bronze/Silver/Gold)?**

Se eligió la arquitectura Medallion para estructurar el pipeline en capas con responsabilidades claramente definidas: almacenamiento de datos sin procesar (Bronze), limpieza y validación de calidad (Silver), y transformaciones orientadas a negocio (Gold), con el objetivo de que los reportes finales consuman siempre datos confiables y coherentes.

**2. ¿Por qué utilizar CSV en la capa Silver en lugar de Parquet?**

Se optó por CSV para priorizar la simplicidad y facilidad de inspección en un entorno local y batch de bajo volumen. En escenarios de mayor escala, Parquet seria preferible por compresión y mejor performance de lectura (sistemas distribuidos).

**3. ¿Cómo se garantiza la Idempotencia del pipeline?**

El pipeline controla duplicados en la capa Silver mediante `drop_duplicates`, sin embargo para reforzar la idempotencia se aplicó una restricción en la base de datos mediante una `PRIMARY KEY` compuesta (city, processed_timestamp) en PostgreSQL, evitando la inserción de registros duplicados ante re-ejecuciones del pipeline.

**4. ¿Por qué Orquestar con Airflow en lugar de CRON scripts?**

Se eligió Airflow porque permite tener mayor control sobre el flujo completo del pipeline, gestionando dependencias (`Ingest >> Transform >> Load >> Report`), reintentos y monitoreo desde una sola interfaz. A diferencia de cron, ofrece visibilidad del estado de cada tarea y facilita escalar el workflow si el proyecto crece en complejidad.

---

## 🗄️ Modelado Dimensional (Capa Gold / Data Warehouse)

Para habilitar consultas analíticas rápidas y eficientes, los datos planos de la capa Silver se transforman y cargan (proceso ELT) en un esquema de **Data Warehouse (`dwh`)** dentro de PostgreSQL siguiendo la metodología de **Modelo Estrella (Kimball)**.

Esta arquitectura separa el contexto descriptivo de las métricas cuantitativas, garantizando la integridad referencial y optimizando la base de datos para herramientas de Business Intelligence (BI).

<div align="center">
<img width="582" height="423" alt="erd" src="https://github.com/user-attachments/assets/7ef004a6-4ebc-4ccf-81f1-5558c72009eb" />
  <br>
  <em>Diagrama Entidad-Relación (ERD) generado desde PostgreSQL mostrando las Primary Keys y Foreign Keys.</em><br>
  <br>
</div>

### Estructura del Modelo Estrella

* **Dimensiones (Contexto):**
  * `dim_location`: Entidad geográfica única (`city`, `country`).
  * `dim_time`: Dimensión de tiempo granular para análisis.
  * `dim_weather_condition`: Catálogo estandarizado de condiciones climáticas.
  * `dim_air_quality`: Clasificación del Índice de Calidad del Aire (AQI) y su estado descriptivo (ej. Bueno, Moderado, Peligroso).
* **Tabla de Hechos (Métricas):**
  * `fact_weather_metrics`: Tabla central optimizada con `Surrogate Keys` (IDs numéricos incrementales) que almacena las métricas meteorológicas y de calidad del aire (`temperature_c`, `pm2_5_level`, `wind_speed_ms`, etc.).

### Muestra de Datos: La Tabla de Hechos

Como resultado del modelado, la tabla central almacena las llaves foráneas que referencian a las tablas de dimensión en lugar de repetir información descriptiva. Esto permite mantener una estructura más eficiente, reducir la redundancia de datos y asegurar la integridad mediante restricciones como `UNIQUE`.

<div align="center">
  <br>
  <em>Vista de la tabla de hechos: métricas puras y llaves foráneas listas para cruzar.</em>
</div>

<img width="1876" height="417" alt="image" src="https://github.com/user-attachments/assets/82c852c3-df46-4376-b971-b3777bdf013c" />


> *Nota: Todo el contexto descriptivo (como el nombre de la ciudad o la descripción del clima) fue extraído hacia las dimensiones, dejando la capa Gold limpia y lista para su consumo analítico.*

### Casos de Uso Analítico (Business Value)
El diseño relacional permite responder preguntas de negocio complejas mediante consultas SQL eficientes. Algunos ejemplos implementados en este proyecto:

<details>
<summary><b>🔍 Ver SQL: Top 5 Ciudades más calurosas y su contaminación máxima</b></summary>

```sql
SELECT 
    l.city AS ciudad,
    l.country AS pais,
    ROUND(AVG(f.temperature_c)::NUMERIC, 1) AS temperatura_promedio_c,
    MAX(f.pm2_5_level) AS pico_maximo_pm25
FROM dwh.fact_weather_metrics f
JOIN dwh.dim_location l ON f.location_id = l.location_id
GROUP BY l.city, l.country
ORDER BY temperatura_promedio_c DESC
LIMIT 5; ```
</details>

---

## Tech Stack & Librerías

El proyecto utiliza herramientas estándar de la industria, definidas en `requirements.txt`:

* **Ingeniería:** `Python`, `Pandas` (Manipulación de DataFrames), `SQLAlchemy` (Gestión de Conexión de Base de Datos).
* **Infraestructura:** `Docker` & `Docker Compose` (Gestión del entorno aislado)
* **Configuración:** `PyYAML` (Gestión de config de ciudades), `Python-dotenv` (Variables de entorno seguras).
* **Calidad:** `Pytest` (Tests unitarios), `Logging` (Trazabilidad de ejecución).

---

## Orquestación

La automatización y el control del flujo de datos se gestionan con Apache Airflow. Su implementación permite coordinar las dependencias entre tareas, gestionar reintentos automáticos y mantener un registro claro (logs) de cada ejecución para asegurar la calidad del dato.

### 1. Pipeline ETL Principal (Ejecución Horaria)
Este flujo se encarga de la extracción, transformación y carga en base de datos PostgreSQL con ejecuciones cada 1 hora (12:00 Hrs.. 13:00 Hrs..)

<p align="center">
  <img width="638" alt="graph airflow" src="https://github.com/user-attachments/assets/e9bc10f2-4b2c-4bd1-94d4-c946bdc730e5" />
  <br>
  <em>Vista del DAG principal: Ejecución exitosa de las etapas de Ingesta, Transformación y Carga.</em>
</p>

### 2. Generación de Reportes Gold (Ejecución Diaria)
Se diseñó un DAG aislado que se ejecuta una sola vez al final del día (23:50 Hrs). Su objetivo es consolidar todo el historial de datos recopilados durante el dia, calcular los promedios numéricos y generar las tablas analíticas finales optimizadas para herramientas de Business Intelligence.

<p align="center">
  <img width="650" height="214" alt="graphh2" src="https://github.com/user-attachments/assets/e14cf57a-8cf0-437c-88c2-b2ae77d72b62" />
  <br>
  <em>Vista del DAG de reportes: Ejecución exitosa de la etapa de Reporteria.</em>
</p>

---

## Pruebas Unitarias (Testing)

En este proyecto se implementó un conjunto de pruebas automatizadas utilizando **Pytest** para estudiar y garantizar la integridad, consistencia y calidad de los datos antes de su inserción en PostgreSQL. Para lograrlo, se diseñó un entorno que inyecta un *dataset* simulado con errores comunes (espacios extra, valores nulos, inconsistencia de mayúsculas y tipos de datos mixtos) directamente en la función de transformación de la capa Silver (`clean_and_normalize`). Esto asegura que los datos cumplan con los formatos requeridos.

### Casos de Prueba Cubiertos
* **Limpieza de Cadenas (`test_string_cleaning`):** Verifica la eliminación de espacios en blanco (`strip`), la corrección de capitalización en descripciones y el manejo seguro de valores nulos.
* **Conversión Numérica (`test_numeric_casting`):** Valida que las métricas extraídas de la API (temperatura, humedad, velocidad del viento) se conviertan correctamente a tipos numéricos manipulables.
* **Manejo de Fechas (`test_date_conversion`):** Asegura que la columna de tiempo se formatee estrictamente al estándar temporal de Pandas (`datetime64[ns]`).

### Ejecución de las Pruebas
Para correr las pruebas localmente y verificar la lógica de transformación, ejecuta el siguiente comando desde la raíz del proyecto:

```bash
python -m pytest pytests/test_transform.py
```
```text
============================= test session starts =============================
platform win32 -- Python 3.12.1, pytest-9.0.2, pluggy-1.6.0
rootdir: C:\Users\Benjamin\airwatch
configfile: pytest.ini
plugins: anyio-4.12.0
collected 3 items

pytests\test_transform.py ...                                            [100%]

============================== 3 passed in 1.94s ==============================
```
---
### Monitoreo y Logs

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
  <img width="857" height="453" alt="log gold" src="https://github.com/user-attachments/assets/5f32409c-6720-480f-8aa4-a5f5745329f6" />
Evidencia de la lógica de negocio aplicada: creación de rankings de contaminación y resúmenes estadísticos con niveles de calidad de aire.
<br><br>

</details>

---

## Requerimientos

Antes de comenzar, asegúrate de tener instalado:
* [Docker Desktop](https://www.docker.com/products/docker-desktop)
* Una API Key activa de [OpenWeatherMap](https://openweathermap.org/api)

## Configuración e Instalación

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
├── src/                 # Código fuente modular
│   ├── ingestion/       # batch_ingest.py
│   ├── transform/       # batch_transform.py
│   ├── load/            # load_database.py
│   └── reports_to_gold/ # gold_report.py
├── pytests/             # Pruebas unitarias
├── Dockerfile           # Imagen del entorno
├── docker-compose.yml   # Configuración Docker


