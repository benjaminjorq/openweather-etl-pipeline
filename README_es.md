# ☀️ OpenWeather ETL Pipeline

---

*🌍 Leer en otros idiomas: [Inglés](README.md) | [Español](README_es.md)*

---

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data_Processing-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)



##  Sobre el Proyecto

Este proyecto implementa un pipeline de datos batch **End-to-End** modularizado y contenerizado, que automatiza la extracción de datos climáticos y de contaminación del aire (PM2.5, PM10, CO, NO2, O3, AQI) desde la API de OpenWeather (Current Weather Data & Air Pollution), con el objetivo de procesar la información y estudiar el estado de calidad del aire en ciudades de interés de Chile y/o Sudamérica.

El pipeline está diseñado bajo la arquitectura **Medallion (Bronze/Silver/Gold)**, priorizando la calidad de los datos, idempotencia, la trazabilidad y el manejo de escenarios reales de integración de datos (*error-handling*).

Como resultado, el sistema genera datasets limpios y estructurados, junto a métricas agregadas y rankings de contaminación listos para consumo analítico en herramientas de BI.

<div align="center">
  <img width="100%" alt="pipeline_diagram" src="https://github.com/user-attachments/assets/bcb2a51a-2000-43da-ae69-8c0f2ee6b0ce" />
</div>

---

##  Métricas de Calidad del Aire

El pipeline analiza indicadores clave de contaminación:

- **`AQI`:** Nivel general de calidad del aire (bueno, moderado, etc.).
- **`PM2.5 / PM10`:** Partículas en el aire; el PM2.5 es más perjudicial por su tamaño.
- **`CO`, `NO₂`, `O₃`:** Gases contaminantes provenientes principalmente del tráfico, la industria y reacciones químicas en presencia de luz solar.

También se consideran variables climáticas como presión, viento, temperatura y humedad, ya que influyen en la dispersión o acumulación de contaminantes.

---

## Diagrama ETL

<div align="center">

```text
Orquestación con Apache Airflow
(Ejecución Batch Programada y Automatización de Flujos Idempotentes)
          ↓
Extracción API de OpenWeather
(Recolección de Datos Meteorológicos y de Calidad del Aire)
          ↓
Ingesta de Datos y Capa Bronze
(Extracción de Datos y Almacenamiento Crudo en JSON)
          ↓
Capa de Procesamiento Silver
(Limpieza de Datos, Transformación y Validaciones con Pandas)
          ↓
Almacenamiento de Datos Estructurados
(Archivos CSV Particionados en formato estilo Hive)
          ↓
Modelado Dimensional y Data Warehouse
(Carga de datos en PostgreSQL hacia Esquema Estrella: Tablas de Hechos y Dimensiones)
          ↓
Capa Analítica Gold
(Agregaciones de Negocio, Rankings y Tablas de Reporte)
          ↓
Capa de Consumo BI
(Visualizaciones Analíticas e Insights Geoespaciales)

```
</div>

---

## Arquitectura de Datos

El flujo está diseñado para transformar datos crudos en insights de negocio:

### 1. Bronze Layer (Ingesta Raw)

* **Fuente:** API OpenWeather (Endpoints `/weather` y `/air_pollution`).
* **Proceso:** Extracción vía `requests` validando códigos de estado HTTP (200 OK).
* **Almacenamiento:** Archivos JSON crudos guardados localmente para auditoría histórica.

### 2. Silver Layer (Limpieza y Normalización)

* **Transformación:**
    * **Flattening:** Aplanamiento de estructuras JSON anidadas (diccionarios dentro de listas) usando Python y Pandas.
    * **Calidad de datos:** Conversión de tipos de datos, eliminación de duplicados y filtrado de valores atípicos.
* **Almacenamiento:** Archivos **CSV** con particionamiento tipo Hive (`year=YYYY/month=MM/day=DD`) para optimizar la organización y consulta.
* **Carga DB:** Ingesta de datos limpios hacia **PostgreSQL** mediante `SQLAlchemy`.

### 3. Gold Layer (Reportes de Negocio)
* **Lógica de Negocio:** Enriquecimiento de datos aplicando reglas de clasificación (ej. Calidad de aire "Peligrosa" si PM2.5 > 55).
* **Outputs Generados:** Rankings de contaminación y resúmenes nacionales agrupados.

---

## Decisiones de Diseño & FAQ

*Justificación de las elecciones técnicas para este proyecto:*

**1. ¿Por qué se optó por una arquitectura Medallion (Bronze/Silver/Gold)?**

Se eligió la arquitectura Medallion para estructurar el pipeline en capas con responsabilidades claramente definidas: almacenamiento de datos sin procesar (Bronze), limpieza y validación de calidad (Silver), y transformaciones orientadas a negocio (Gold), con el objetivo de que los reportes finales consuman siempre datos confiables y coherentes.

**2. ¿Por qué utilizar CSV en la capa Silver en lugar de Parquet?**

Se optó por CSV para priorizar la simplicidad y facilidad de inspección en un entorno local y batch de bajo volumen. En escenarios de mayor escala, Parquet sería preferible por compresión y mejor performance de lectura (sistemas distribuidos).

**3. ¿Cómo se garantiza la Idempotencia del pipeline?**

La idempotencia se implementa mediante un enfoque por capas:

* **Capa Bronze:** Los datos crudos (raw) se almacenan de forma inmutable, incorporando marcas de tiempo de ingesta para asegurar trazabilidad y evitar sobrescrituras.
* **Capa Silver:** Se eliminan posibles duplicados en memoria utilizando `drop_duplicates`, asegurando consistencia a nivel de dataset antes de la carga.
* **Capa Gold:** La idempotencia se refuerza truncando la tabla de staging en cada ejecución (TRUNCATE) para garantizar un estado limpio, y mediante restricciones UNIQUE (location_id, time_id) junto con `ON CONFLICT DO NOTHING` en dimensiones y tabla de hechos, evitando duplicados ante múltiples ejecuciones del pipeline.

**4. ¿Por qué Orquestar con Airflow en lugar de CRON scripts?**

Se eligió Airflow porque permite tener mayor control sobre el flujo completo del pipeline, gestionando dependencias (`Ingest >> Transform >> Load >> Report`), reintentos y monitoreo desde una sola interfaz. A diferencia de cron, ofrece visibilidad del estado de cada tarea y facilita escalar el workflow si el proyecto crece en complejidad.

**5. ¿Cómo se gestionan las ubicaciones a procesar?**

La selección de ciudades se desacopló del código mediante un archivo de configuración `cities.yaml` para evitar cambios en el script al agregar o quitar ciudades de interés y respetar los *rate limits* de la API gratuita (*60 peticiones/minuto*).

---

## Modelado Dimensional (Star Schema)

Para realizar consultas analíticas, los datos planos de la capa Silver se transforman y cargan en un esquema de **Data Warehouse (`dwh`)** dentro de PostgreSQL siguiendo la metodología de **Modelo Estrella (Kimball)**.

Esta arquitectura separa lo descriptivo de las métricas cuantitativas, garantizando la integridad referencial y optimizando la base de datos para herramientas de Business Intelligence (BI).

<div align="center">
  <img width="1091" height="704" alt="erd white" src="https://github.com/user-attachments/assets/fc531fd3-0b4a-4ec6-b25a-81ae78b6fe15" />
  <br>
  <em>Diagrama Entidad-Relación (ERD) generado desde PostgreSQL mostrando las Primary Keys y Foreign Keys.</em>
  <br>
</div>

### Estructura del Modelo Estrella

* **Dimensiones (Contexto):**

  * `dim_location`: Entidad geográfica única (`city`, `country`).
  * `dim_time`: Dimensión de tiempo granular para análisis.
  * `dim_weather_condition`: Catálogo de condiciones climáticas.
  * `dim_air_quality`: Clasificación del Índice de Calidad del Aire (AQI) y su estado descriptivo (ej. Bueno, Moderado, Peligroso).
* **Tabla de Hechos (Métricas):**
  * `fact_weather_metrics`: Tabla central optimizada con `Surrogate Keys` (IDs numéricos incrementales) que almacena las métricas meteorológicas y de calidad del aire (`temperature_c`, `pm2_5_level`, `wind_speed_ms`, etc.).

### Muestra de Datos: La Tabla de Hechos

Como resultado del modelado, la tabla central almacena las llaves foráneas que referencian a las tablas de dimensión en lugar de repetir información descriptiva. Esto permite mantener una estructura más eficiente, reducir la redundancia de datos y asegurar la integridad mediante restricciones como `UNIQUE`.

<div align="center">
  <br>
  <em>Vista de la tabla de hechos: métricas y llaves foráneas.</em>
</div>
<img width="1876" height="417" alt="image" src="https://github.com/user-attachments/assets/82c852c3-df46-4376-b971-b3777bdf013c" />


### Casos de Uso Analítico (Business Value)

El diseño relacional del Data Warehouse permite responder preguntas de negocio uniendo las tablas de hechos y dimensiones. A continuación, algunas consultas analíticas ejecutadas directamente en PostgreSQL:

<details>
  
<summary><b>Consulta 1 SQL: Top 5 Ciudades más Contaminadas (Clasificación de Riesgo según Promedios de PM2.5)</b></summary>

*Identifica las ciudades con los niveles promedio más críticos de partículas finas (PM2.5).

```sql
SELECT 
    l.city AS ciudad,
    l.country AS pais,
    ROUND(AVG(f.pm2_5_level)::NUMERIC, 2) AS pm2_5_promedio,
    CASE 
        WHEN AVG(f.pm2_5_level) <= 12 THEN 'Bueno'
        WHEN AVG(f.pm2_5_level) <= 35 THEN 'Moderado'
        ELSE 'Malo'
    END AS clasificacion
FROM dwh.fact_weather_metrics AS f
JOIN dwh.dim_location AS l 
ON f.location_id = l.location_id
GROUP BY l.city, l.country
ORDER BY pm2_5_promedio DESC
LIMIT 5;

```

<img width="647" height="188" alt="sql 1" src="https://github.com/user-attachments/assets/00a6cb26-91fa-4a06-a79d-5bdd64b7bcea" />

</details>
<details>

<summary><b>Consulta 2 SQL: Top 10 Ciudades con Aire más Limpio en Chile </b></summary>

*Identifica las 10 ciudades de Chile con los niveles más bajos de partículas suspendidas (PM10) y emisiones vehiculares (CO), generando un ranking nacional de limpieza junto a su temperatura promedio.

```sql
SELECT 
    l.city AS ciudad,
    l.country AS pais,
    ROUND(AVG(f.pm10_level)::NUMERIC, 2) AS pm10_promedio,
    ROUND(AVG(f.co_level)::NUMERIC, 2) AS co_promedio,
    ROUND(AVG(f.temperature_c)::NUMERIC, 1) AS temperatura_promedio_c,
    RANK() OVER(ORDER BY AVG(f.pm10_level) ASC) AS ranking_nacional_aire_limpio,
    MAX(a.estado) AS estado_general_aire
FROM dwh.fact_weather_metrics AS f
JOIN dwh.dim_location AS l 
ON f.location_id = l.location_id
JOIN dwh.dim_air_quality AS a 
ON f.aqi_id = a.aqi_id
WHERE l.country = 'CL'
GROUP BY l.city, l.country
ORDER BY pm10_promedio ASC
LIMIT 10;

```
<img width="1188" height="327" alt="sql2" src="https://github.com/user-attachments/assets/2298e3a8-29a6-4d70-b982-ab4416e7152d" />
</details>
<details>

<summary><b>Consulta 3 SQL: Estado Térmico Actual por Ciudad (Simulación) </b></summary>

*Analiza el confort térmico y las variables del entorno por ciudad, verifica datos de sensación térmica, humedad y velocidad del viento en zonas de interés.

```sql
WITH reporte_clima AS (
    SELECT 
        location_id,
        ROUND(AVG(temperature_c)::NUMERIC, 1) AS temperatura_c,
        ROUND(AVG(feels_like_c)::NUMERIC, 1) AS sensacion_termica_c,
        ROUND(AVG(humidity_pct)::NUMERIC, 1) AS humedad_pct,
        ROUND(AVG(pressure_hpa)::NUMERIC, 0) AS presion_hpa,
        ROUND(AVG(wind_speed_ms)::NUMERIC, 1) AS velocidad_viento_ms
    FROM dwh.fact_weather_metrics
    GROUP BY location_id
)
SELECT 
    l.city AS ciudad,
    l.country AS pais,
    rc.temperatura_c,
    rc.sensacion_termica_c,
    rc.humedad_pct,
    rc.presion_hpa,
    rc.velocidad_viento_ms
FROM dwh.dim_location AS l
JOIN reporte_clima AS rc
ON l.location_id = rc.location_id
ORDER BY rc.temperatura_c DESC
LIMIT 10;

```
<img width="1076" height="401" alt="sql3" src="https://github.com/user-attachments/assets/20590d53-ede3-4535-a92f-e24d235bc55f" />
</details>

---

## Visualización

Como etapa final de validación del pipeline, los datos consolidados en la capa Gold fueron utilizados en visualizaciones analíticas para verificar la calidad, consistencia y coherencia de las métricas generadas tras el proceso ETL.

<br>

<div align="center">
  <img 
    width="950" 
    alt="Ranking de ciudades por PM2.5 en Chile" 
    src="https://github.com/user-attachments/assets/0de95ff2-c065-4768-b1ea-6c228b0ea155" 
  />
</div>

<br>

<div align="center">
  <img 
    width="450" 
    alt="Mapa geoespacial de calidad del aire en Chile" 
    src="https://github.com/user-attachments/assets/78ae23b8-58bb-4c3c-9151-65991f85cb2b" 
  />

  <br>

  <p>
    <i><b>Insights Geoespaciales:</b> Mapa interactivo desarrollado en <b>Tableau</b>. Ejemplo: <b>Calama</b> (PM2.5: 8.58 µg/m³) [Imagen Renderizada con IA].</i>
  </p>
</div>

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

Este flujo se encarga de la extracción, transformación y carga en base de datos PostgreSQL con ejecuciones cada 1 hora (12:00 Hrs, 13:00 Hrs, etc.)

<p align="center">
  <img width="800" alt="airflow etl dag" src="https://github.com/user-attachments/assets/cadcbb02-6cb8-46e0-8d9f-392a59381556" />
  <br>
  <em>Vista del DAG principal: Ejecución existosa de la etapa de Ingesta, Transformación y Carga hacia PostgreSQL.</em>
</p>

### 2. Generación de Reportes Gold (Ejecución Diaria)

Se diseñó un DAG aislado que se ejecuta una sola vez al final del día (23:50 Hrs). Su objetivo es consolidar todo el historial de datos recopilados durante el dia, calcular los promedios numéricos y generar las tablas analíticas finales optimizadas para herramientas de Business Intelligence.

<p align="center">
  <img width="650" alt="airflow gold dag" src="https://github.com/user-attachments/assets/f7b16f47-3557-478f-a82f-c8053fbb1d62" />
  <br>
  <em>Vista del DAG de reportes: Ejecución exitosa de la etapa de Reporteria.</em>
</p>

### 3. Observabilidad y Alertas en Tiempo Real (Discord Webhooks)

Para garantizar una respuesta rápida ante problemas de infraestructura o datos (como límites de la API o caídas en la conexión a la base de datos), el pipeline implementa un sistema de alertas en tiempo real mediante **Discord Webhooks**.

Aprovechando el callback `on_failure_callback` de Airflow, cualquier tarea que falle genera automáticamente una notificación en un canal dedicado de Discord. La alerta proporciona contexto inmediato, incluyendo el nombre del DAG, la tarea específica que falló, la hora de ejecución y un enlace directo a los logs de Airflow para facilitar el debugging.

<p align="center">
  <img 
    width="800" 
    alt="discord alert screenshot" 
    src="https://github.com/user-attachments/assets/c1fd8266-adf8-47f9-a1fe-bf15a4da6b98"
  />
  <br>
  <em>Alerta en Discord: notificación en tiempo real mostrando una falla simulada dentro del pipeline.</em>
</p>

---

## Pruebas Unitarias (Testing)

En este proyecto se implementó un conjunto de pruebas automatizadas utilizando **Pytest** para estudiar y garantizar la integridad, consistencia y calidad de los datos antes de su inserción en PostgreSQL. Para lograrlo, se implementó una **fixture** llamada `raw_dataset` encargada de generar un DataFrame con errores comunes (espacios extra, valores nulos, inconsistencia de mayúsculas y tipos de datos mixtos) directamente en la función de transformación de la capa Silver (`clean_and_normalize`). Esto asegura que los datos cumplan con los formatos requeridos.

### Casos de Prueba Cubiertos

* **Limpieza de Cadenas (`test_string_cleaning`):** Verifica la eliminación de espacios en blanco (`strip`), la corrección de capitalización en descripciones y el manejo seguro de valores nulos.
* **Conversión Numérica (`test_numeric_casting`):** Valida que las métricas extraídas de la API (temperatura, humedad, velocidad del viento) se conviertan correctamente a tipos numéricos manipulables.
* **Manejo de Fechas (`test_date_conversion`):** Asegura que la columna de tiempo se formatee estrictamente al estándar temporal de Pandas (`datetime64[ns]`).

### Ejecución de las Pruebas

Para correr las pruebas localmente y verificar la lógica de transformación, ejecuta el siguiente comando desde la raíz del proyecto:

```bash
pytest pytests/test_transform.py -v
```

```text

============================= test session starts ==============================
platform win32 -- Python 3.12.1, pytest-9.0.2, pluggy-1.6.0 -- c:\Users\Benjamin\airwatch\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\Benjamin\airwatch
configfile: pytest.ini
plugins: anyio-4.12.0
collected 3 items

pytests/test_transform.py::test_string_cleaning PASSED                   [ 33%]
pytests/test_transform.py::test_numeric_casting PASSED                   [ 66%]
pytests/test_transform.py::test_date_conversion PASSED                   [100%]

============================== 3 passed in 1.86s ===============================

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
Evidencia de la lógica de negocio aplicada: creación de rankings de contaminación y resúmenes estadísticos con niveles de calidad de aire.
<br><br>
<img width="857" height="453" alt="log gold" src="https://github.com/user-attachments/assets/5f32409c-6720-480f-8aa4-a5f5745329f6" />
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
    POSTGRES_PASSWORD=airflow
    POSTGRES_DB=weather_db

    ```

3.  **Iniciar los servicios:**

    Ejecuta el siguiente comando para levantar Airflow y PostgreSQL:
    
    ```bash
    docker-compose up -d
    ```

4. **Acceder a la interfaz de Airflow:**

Abre tu navegador y dirígete a:

    ```bash
    http://localhost:8080
    ```

Credenciales por defecto:

    ```bash
    Usuario: airflow
    Contraseña: airflow
    ```
---

## 📂 Estructura del Repositorio

```bash
openweather-etl-pipeline/
├── config/                                 # Archivos de configuración (YAML)
│   └── cities.yaml                         # Configuración de ciudades objetivo
├── dags/                                   # Orquestación (DAGs de Airflow)
├── data/                                   # Data Lake local (Excluido de git)
│   ├── bronze/                             # JSONs sin procesar
│   ├── silver/                             # Datos limpios (Particionados)
│   │   └── year=YYYY/month=MM/day=DD/
│   └── gold/                               # Reportes de negocio
│       ├── ranking/                        # Ranking de contaminación (.csv)
│       └── summary/                        # Promedios por país (.csv)
├── logs/                                   # Logs de Airflow y del pipeline (Excluido de git)
├── src/                                    # Código fuente modular
│   ├── ingestion/               
│   │   └── openweather_batch_ingest.py     # Extracción de API e ingesta raw
│   ├── transform/               
│   │   └── openweather_batch_transform.py  # Lógica de limpieza y normalización
│   ├── load/                    
│   │   └── openweather_load_database.py    # Proceso de carga en PostgreSQL
│   ├── reports_to_gold/         
│   │   └── openweather_gold_report.py      # Agregaciones y reportes de negocio
│   └── utils/                   
│       └── alerts.py                       # Integración de alertas con Discord Webhooks
├── pytests/                                # Pruebas unitarias
│   └── test_transform.py                   # Pruebas de calidad de datos y limpieza
├── Dockerfile                              # Imagen de entorno
├── docker-compose.yml                      # Configuración de Docker
├── pytest.ini                              # Configuración de Pytest
├── README.md                               # Documentación principal (Inglés)
├── README_es.md                            # Documentación en español
├── .gitignore                              # Archivos excluidos del control de versiones
├── .env                                    # Credenciales locales (Excluido de git)
└── requirements.txt                        # Dependencias de Python

```
---

## 🚀 Mejoras Futuras (Roadmap)

Estas mejoras reflejan una evolución natural desde un entorno local hacia un pipeline más cercano a producción, priorizando escalabilidad, observabilidad y calidad de datos.

- **Observabilidad:** Implementación de alertas ante fallos (Slack/Email) en Apache Airflow.
- **Configuración:** Validación del archivo `cities.yaml` para asegurar integridad y consistencia de configuración.
- **Optimización de Almacenamiento:** Migración de la capa Silver desde CSV a formato columnar (Parquet) para mejorar performance y eficiencia.
- **Escalabilidad:** Despliegue en entornos cloud (AWS/GCP) y adaptación a procesamiento distribuido con Apache Spark.
- **CI/CD (Integración Continua):** Implementación de flujos de trabajo con **GitHub Actions** para automatizar la ejecución de pruebas unitarias (`pytest`) y validaciones en cada *Push* o *Pull Request*.





