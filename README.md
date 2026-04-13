# ☀️ OpenWeather ETL Pipeline

---

*🌍 Read this in other languages: [English](README.md) | [Spanish](README_es.md)*

---

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data_Processing-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)



## About the Project

This project implements an **end-to-end batch data pipeline** designed to ingest, process, and analyze weather and air pollution data (PM2.5, PM10, CO, NO2, O3, AQI) from the OpenWeather API.

The pipeline is designed under the **Medallion architecture (Bronze/Silver/Gold)**, prioritizing data quality, idempotency, traceability, and handling real-world data integration challenges (failures, retries, and inconsistencies)

As a result, the system generates clean and structured datasets, along with aggregated metrics and pollution rankings ready for analytical consumption in BI tools.

<div align="center">
  <img width="100%" alt="pipeline_diagram" src="https://github.com/user-attachments/assets/bcb2a51a-2000-43da-ae69-8c0f2ee6b0ce" />
</div>

---

## ETL Diagram

<div align="center">

```text
Apache Airflow Orchestration
(Scheduled Batch Execution and Idempotent Workflow Automation)
          ↓
OpenWeather API Extraction
(Weather and Air Pollution Data Collection)
          ↓
Data Ingestion & Bronze Layer
(Data Extraction and Raw JSON Storage)
          ↓
Silver Processing Layer
(Data Cleaning, Transformation and Pytest Validation)
          ↓
Structured Data Storage
(Partitioned CSV Files in Hive-style format)
          ↓
Dimensional Modeling & Data Warehouse
(PostgreSQL Upsert to Star Schema: Fact and Dimension Tables)
          ↓
Gold Analytics Layer
(Business Aggregations, Rankings, and Reporting Tables)
          ↓
BI Consumption Layer
(Analytical Visualizations and Geospatial Insights)
```
</div>

---

## Data Architecture

The flow is designed to transform raw data into business insights:

### 1. Bronze Layer (Raw Ingestion)

* **Source:** OpenWeather API (`/weather` and `/air_pollution` endpoints).
* **Process:** Extraction via `requests` validating HTTP status codes (200 OK).
* **Storage:** Raw JSON files stored locally for historical auditing.

### 2. Silver Layer (Cleaning and Normalization)

* **Transformation:**
    * **Flattening:** Unnesting of nested JSON structures (dictionaries within lists) using Python and Pandas.
    * **Data quality:** Data type conversion, duplicate removal, and outlier filtering.
* **Storage:** **CSV** files with Hive-style partitioning (`year=YYYY/month=MM/day=DD`) to optimize organization and querying.
* **DB Load:** Clean data ingested into **PostgreSQL** via `SQLAlchemy`.

### 3. Gold Layer (Business Reports)
* **Business Logic:** Data enrichment applying classification rules (e.g., air quality "Hazardous" if PM2.5 > 55).
* **Generated Outputs:** Pollution rankings and aggregated summaries at city and country level, ready for analytical consumption.

---

## Design Decisions & FAQ

*Justification of technical choices for this project:*

**1. Why was the Medallion architecture (Bronze/Silver/Gold) chosen?**

The Medallion architecture was selected to structure the pipeline into layers with clearly defined responsibilities: raw data storage (Bronze), quality cleaning and validation (Silver), and business-oriented transformations (Gold), ensuring that final reports always consume reliable and consistent data.

**2. Why use CSV in the Silver layer instead of Parquet?**

CSV was chosen to prioritize simplicity and ease of inspection in a local, low-volume batch environment. In higher-scale scenarios, Parquet would be preferred for its compression and better read performance (distributed systems).

**3. How is pipeline idempotency guaranteed?**

Idempotency is ensured through a layered approach: in Bronze, data is stored immutably to guarantee traceability; in Silver, duplicates are removed with `drop_duplicates`; and at the load stage it is reinforced via a composite PRIMARY KEY `(city, processed_timestamp)` in PostgreSQL using `ON CONFLICT DO NOTHING`. This prevents duplications and ensures consistency across pipeline re-executions.

**4. Why orchestrate with Airflow instead of CRON scripts?**

Airflow was chosen because it provides greater control over the complete pipeline flow, managing dependencies (`Ingest >> Transform >> Load >> Report`), retries, and monitoring from a single interface. Unlike cron, it offers visibility into the state of each task and makes it easier to scale the workflow as the project grows in complexity.

**5. How are the locations to be processed managed?**

City selection was decoupled from the code through a `cities.yaml` configuration file to avoid changes to the script when adding or removing cities of interest, and to respect the free API tier *rate limits* (*60 requests/minute*).

---

## Dimensional Modeling (Star Schema)

For analytical queries, the flat data from the Silver layer is transformed and loaded into a **Data Warehouse (`dwh`)** schema within PostgreSQL following the **Star Schema (Kimball)** methodology.

This architecture separates descriptive attributes from quantitative metrics, ensuring referential integrity and optimizing the database for Business Intelligence (BI) tools.

<div align="center">
<img width="1156" height="528" alt="erd white" src="https://github.com/user-attachments/assets/ba981cd0-08bf-4dc0-81b6-c7cd6108c12f" />
  <br>
  <em>Entity-Relationship Diagram (ERD) generated from PostgreSQL showing Primary Keys and Foreign Keys.</em><br>
  <br>
</div>

### Star Schema Structure

* **Dimensions (Context):**
  * `dim_location`: Unique geographic entity (`city`, `country`).
  * `dim_time`: Granular time dimension for analysis.
  * `dim_weather_condition`: Catalog of weather conditions.
  * `dim_air_quality`: Air Quality Index (AQI) classification and its descriptive status (e.g., Good, Moderate, Hazardous).
* **Fact Table (Metrics):**
  * `fact_weather_metrics`: Central table optimized with `Surrogate Keys` (incremental numeric IDs) storing weather and air quality metrics (`temperature_c`, `pm2_5_level`, `wind_speed_ms`, etc.).

### Data Sample: The Fact Table

As a result of the modeling, the central table stores foreign keys referencing the dimension tables instead of repeating descriptive information. This allows for a more efficient structure, reduces data redundancy, and ensures integrity through constraints such as `UNIQUE`.

<div align="center">
  <br>
  <em>Fact table view: metrics and foreign keys.</em>
</div>
<img width="1876" height="417" alt="image" src="https://github.com/user-attachments/assets/82c852c3-df46-4376-b971-b3777bdf013c" />


### Analytical Use Cases (Business Value)

The relational design of the Data Warehouse enables answering business questions by joining fact and dimension tables. Below are some analytical queries executed directly in PostgreSQL:

<details>
  
<summary><b>SQL Query 1: Top 5 Most Polluted Cities (Risk Classification by PM2.5 Averages)</b></summary>

*Identifies cities with the most critical average levels of fine particles (PM2.5).*

```sql
SELECT 
    l.city AS city,
    l.country AS country,
    ROUND(AVG(f.pm2_5_level)::NUMERIC, 2) AS avg_pm2_5,
    CASE 
        WHEN AVG(f.pm2_5_level) <= 12 THEN 'Good'
        WHEN AVG(f.pm2_5_level) <= 35 THEN 'Moderate'
        ELSE 'Bad'
    END AS classification
FROM dwh.fact_weather_metrics AS f
JOIN dwh.dim_location AS l 
ON f.location_id = l.location_id
GROUP BY l.city, l.country
ORDER BY avg_pm2_5 DESC
LIMIT 5;
```

<img width="609" height="188" alt="sql 1" src="https://github.com/user-attachments/assets/58303240-0d90-418c-a8ad-b7959cf1c265" />

</details>

<details>

<summary><b>SQL Query 2: Top 10 Cities with Cleanest Air in Chile</b></summary>

*Identifies the 10 Chilean cities with the lowest levels of suspended particles (PM10) and vehicular emissions (CO), generating a national cleanliness ranking alongside their average temperature.*

```sql
SELECT 
    l.city AS city,
    l.country AS country,
    ROUND(AVG(f.pm10_level)::NUMERIC, 2) AS avg_pm10,
    ROUND(AVG(f.co_level)::NUMERIC, 2) AS avg_co,
    ROUND(AVG(f.temperature_c)::NUMERIC, 1) AS avg_temperature_c,
    RANK() OVER(ORDER BY AVG(f.pm10_level) ASC) AS national_clean_air_ranking,
    MAX(a.estado) AS overall_air_status
FROM dwh.fact_weather_metrics AS f
JOIN dwh.dim_location AS l 
ON f.location_id = l.location_id
JOIN dwh.dim_air_quality AS a 
ON f.aqi_id = a.aqi_id
WHERE l.country = 'CL'
GROUP BY l.city, l.country
ORDER BY avg_pm10 ASC
LIMIT 10;
```

<img width="1050" height="369" alt="sql 2" src="https://github.com/user-attachments/assets/22a7a162-ef83-43d4-9ef5-f485168e2dfb" />

</details>

<details>

<summary><b>SQL Query 3: Current Thermal Status by City (Simulation)</b></summary>

*Analyzes thermal comfort and environmental variables by city, verifying heat index, humidity, and wind speed data in areas of interest.*

```sql
WITH climate_report AS (
    SELECT 
        location_id,
        ROUND(AVG(temperature_c)::NUMERIC, 1) AS temperature_c,
        ROUND(AVG(feels_like_c)::NUMERIC, 1) AS feels_like_c,
        ROUND(AVG(humidity_pct)::NUMERIC, 1) AS humidity_pct,
        ROUND(AVG(pressure_hpa)::NUMERIC, 0) AS pressure_hpa,
        ROUND(AVG(wind_speed_ms)::NUMERIC, 1) AS wind_speed_ms
    FROM dwh.fact_weather_metrics
    GROUP BY location_id
)
SELECT 
    l.city AS city,
    l.country AS country,
    rc.temperature_c,
    rc.feels_like_c,
    rc.humidity_pct,
    rc.pressure_hpa,
    rc.wind_speed_ms
FROM dwh.dim_location AS l
JOIN climate_report AS rc
ON l.location_id = rc.location_id
ORDER BY rc.temperature_c DESC
LIMIT 10;
```

<img width="979" height="331" alt="sql 3" src="https://github.com/user-attachments/assets/603ec8a1-c883-4df9-81e0-4611f85f05bf" />

</details>

---

## Visualization

As a final pipeline validation stage, the data consolidated in the Gold layer was used in analytical visualizations to verify the quality, consistency, and coherence of the metrics generated after the ETL process.

<div align="center">
  <img 
    width="100%" 
    alt="City ranking by PM2.5 in Chile" 
    src="https://github.com/user-attachments/assets/5a8ddc13-543d-470a-9486-964bf362698b" 
  />
</div>

---

## Tech Stack & Libraries

The project uses industry-standard tools, defined in `requirements.txt`:

* **Engineering:** `Python`, `Pandas` (DataFrame manipulation), `SQLAlchemy` (Database connection management).
* **Infrastructure:** `Docker` & `Docker Compose` (Isolated environment management).
* **Configuration:** `PyYAML` (City config management), `Python-dotenv` (Secure environment variables).
* **Quality:** `Pytest` (Unit testing), `Logging` (Execution traceability).

---

## Orchestration

Data flow automation and control are managed with Apache Airflow. Its implementation coordinates task dependencies, manages automatic retries, and maintains a clear log of each execution to ensure data quality.

### 1. Main ETL Pipeline (Hourly Execution)

This flow handles extraction, transformation, and loading into PostgreSQL with executions every 1 hour (12:00, 13:00, etc.).

<p align="center">
  <img width="638" alt="graph airflow" src="https://github.com/user-attachments/assets/e9bc10f2-4b2c-4bd1-94d4-c946bdc730e5" />
  <br>
  <em>Main DAG view: Successful execution of the Ingestion, Transformation, and Load stages.</em>
</p>

### 2. Gold Report Generation (Daily Execution)

A separate DAG was designed to run once at the end of the day (23:50). Its purpose is to consolidate all data collected throughout the day, calculate numerical averages, and generate the final analytical tables optimized for Business Intelligence tools.

<p align="center">
  <img width="650" height="214" alt="graphh2" src="https://github.com/user-attachments/assets/e14cf57a-8cf0-437c-88c2-b2ae77d72b62" />
  <br>
  <em>Reporting DAG view: Successful execution of the Reporting stage.</em>
</p>

---

## Unit Testing

This project implements a set of automated tests using **Pytest** to ensure data integrity, consistency, and quality before loading into PostgreSQL. A testing environment was designed to inject a simulated *dataset* with common errors (extra whitespace, null values, case inconsistencies, and mixed data types) directly into the Silver layer transformation function (`clean_and_normalize`). This ensures that data meets the required formats.

### Test Cases Covered

* **String Cleaning (`test_string_cleaning`):** Verifies whitespace removal (`strip`), capitalization correction in descriptions, and safe handling of null values.
* **Numeric Casting (`test_numeric_casting`):** Validates that metrics extracted from the API (temperature, humidity, wind speed) are correctly converted to manipulable numeric types.
* **Date Handling (`test_date_conversion`):** Ensures the time column is strictly formatted to the Pandas temporal standard (`datetime64[ns]`).

### Running the Tests

To run the tests locally and verify the transformation logic, execute the following command from the project root:

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

### Monitoring and Logs

The system generates detailed logs at each stage to facilitate monitoring and ensure data quality. Expand each section to see the technical evidence:

<details>
<summary><b>1. Data Ingestion (Bronze Layer)</b></summary>
<br>
Evidence of batch extraction from the OpenWeather API and successful storage of raw data in JSON format.

```text
[2026-01-20, 16:41:24 -03] INFO - Output:
[2026-01-20, 16:41:24 -03] INFO - Starting ingestion process
[2026-01-20, 16:41:24 -03] INFO - Downloading data for city: Arica
[2026-01-20, 16:41:25 -03] INFO - Downloading data for city: Iquique
[2026-01-20, 16:41:26 -03] INFO - Downloading data for city: Antofagasta
[2026-01-20, 16:41:27 -03] INFO - Downloading data for city: Calama
[2026-01-20, 16:41:28 -03] INFO - Downloading data for city: La Serena
[2026-01-20, 16:41:30 -03] INFO - Downloading data for city: Valparaiso
[2026-01-20, 16:41:31 -03] INFO - Downloading data for city: Santiago
[2026-01-20, 16:41:32 -03] INFO - Downloading data for city: Rancagua
[2026-01-20, 16:41:33 -03] INFO - Downloading data for city: Talca
[2026-01-20, 16:41:34 -03] INFO - Downloading data for city: Chillan
[2026-01-20, 16:41:36 -03] INFO - Downloading data for city: Concepcion
[2026-01-20, 16:41:37 -03] INFO - Downloading data for city: Temuco
[2026-01-20, 16:41:38 -03] INFO - Downloading data for city: Valdivia
[2026-01-20, 16:41:40 -03] INFO - Downloading data for city: Puerto Montt
[2026-01-20, 16:41:41 -03] INFO - Downloading data for city: Punta Arenas
[2026-01-20, 16:41:41 -03] INFO - Downloading data for city: Buenos Aires
[2026-01-20, 16:41:42 -03] INFO - Downloading data for city: Lima
[2026-01-20, 16:41:43 -03] INFO - Downloading data for city: Bogota
[2026-01-20, 16:41:43 -03] INFO - Downloading data for city: Sao Paulo
[2026-01-20, 16:41:45 -03] INFO - Downloading data for city: Rio de Janeiro
[2026-01-20, 16:41:46 -03] INFO - Downloading data for city: Montevideo
[2026-01-20, 16:41:47 -03] INFO - Downloading data for city: La Paz
[2026-01-20, 16:41:48 -03] INFO - Downloading data for city: Quito
[2026-01-20, 16:41:49 -03] INFO - Downloading data for city: Asuncion
[2026-01-20, 16:41:50 -03] INFO - Downloading data for city: Caracas
[2026-01-20, 16:41:50 -03] INFO - File successfully saved: raw_weather_data_2026_01_20_194150.json
```

</details>

<details>
<summary><b>2. Transformation and Cleaning (Silver Layer)</b></summary>
<br>
Logs from the cleaning, schema normalization, and flattening of nested structures via Pandas.

```text
[2026-01-20, 16:41:51 -03] INFO - Starting transformation process
[2026-01-20, 16:41:51 -03] INFO - Processing file: raw_weather_data_2026_01_20_194150.json
[2026-01-20, 16:41:51 -03] INFO - Transformation completed. CSV saved: clean_weather_data_19_41_51.csv
```

</details>

<details>
<summary><b>3. Load to PostgreSQL</b></summary>
<br>
Confirmation of clean data ingestion into the PostgreSQL relational database for long-term persistence.

```text
[2026-01-20, 16:41:52 -03] INFO - Starting load process
[2026-01-20, 16:41:52 -03] INFO - Processing file: clean_weather_data_19_41_51.csv
[2026-01-20, 16:41:52 -03] INFO - Data successfully loaded into PostgreSQL
```

</details>

<details>
<summary><b>4. Report Generation (Gold Layer)</b></summary>
<br>
Evidence of applied business logic: creation of pollution rankings and statistical summaries with air quality levels.

```text
[2026-03-11, 20:42:44 UTC] INFO - Report generation started
[2026-03-11, 20:42:44 UTC] INFO - Successful consolidation. Total records: 25
[2026-03-11, 20:42:44 UTC] INFO - Unique records after daily aggregation: 25

[INFO] Top 7 Pollution Ranking (Preview):

City        Temp  Humidity  Wind  Pressure  AQI  PM2.5  PM10  CO     NO2  O3    Status
Antofagasta 22.6  80.0      3.1   1013.0    2.0  13.2   23.7  60.8   0.1  51.6  Good
Calama      23.3  15.0      7.7   1012.0    2.0  13.2   44.1  67.9   0.3  79.0  Good
Santiago    29.4  34.0      6.2   1013.0    2.0  8.3    11.4  85.6   0.8  70.8  Good
Rancagua    29.2  27.0      4.3   1012.0    2.0  7.2    9.4   94.1   0.8  72.1  Good
Caracas     22.6  74.0      1.5   1013.0    2.0  4.2    5.8   128.4  1.2  67.8  Good
Iquique     22.5  78.0      1.8   1013.0    1.0  9.0    17.6  63.4   0.1  47.4  Excellent
Bogota      18.2  65.0      5.8   1012.0    1.0  8.0    8.2   171.4  0.7  46.0  Excellent

[INFO] File saved: ranking_pollution_2026_03_11.csv

[INFO] Country Summary (Preview):

Country  Temp  Humidity  Wind  Pressure  AQI  PM2.5  PM10  CO     NO2  O3    Status
PE       22.6  74.0      1.5   1013.0    2.0  4.2    5.8   128.4  1.2  67.8  Good
CL       23.2  53.2      4.7   1012.8    1.3  5.3    11.2  68.7   0.4  48.6  Excellent
AR       24.5  61.0      8.8   1015.0    1.0  1.8    3.7   64.0   0.4  51.3  Excellent
BO       15.0  56.0      3.1   1010.0    1.0  1.1    1.8   73.4   0.3  32.3  Excellent
BR       22.2  93.0      3.8   1013.5    1.0  1.1    3.0   56.2   0.3  37.2  Excellent
CO       18.2  65.0      5.8   1012.0    1.0  8.0    8.2   171.4  0.7  46.0  Excellent
EC       12.6  82.0      2.6   1015.0    1.0  4.2    4.2   178.0  4.8  48.2  Excellent
PY       30.9  62.0      8.2   1007.0    1.0  5.5    5.8   122.5  1.2  35.0  Excellent
UY       22.5  86.0      7.7   1016.0    1.0  1.6    2.6   55.8   0.0  35.6  Excellent

[INFO] File saved: summary_country_2026_03_11.csv
```

</details>

---

## Requirements

Before getting started, make sure you have installed:

* [Docker Desktop](https://www.docker.com/products/docker-desktop)
* An active API Key from [OpenWeatherMap](https://openweathermap.org/api)

## Setup and Installation

1. **Clone the repository:**

```bash
    git clone https://github.com/benjaminjorq/openweather-etl-pipeline.git
    cd openweather-etl-pipeline
```

2. **Configure environment variables:**

    Create a `.env` file in the project root and add your credentials (you can use the `.env.example` file as a guide):

```env
    AIRFLOW_UID=50000
    OPENWEATHER_API_KEY=your_api_key_here
    POSTGRES_USER=airflow
    POSTGRES_PASSWORD=airflow
    POSTGRES_DB=weather_db
```

3. **Start the services:**

    Run the following command to bring up Airflow and PostgreSQL:

```bash
    docker-compose up -d
```

---

## 📂 Repository Structure

```bash
openweather-etl-pipeline/
├── config/              # Configuration files (YAML)
├── dags/                # Orchestration (Airflow DAGs)
├── data/                # Local Data Lake
│   ├── bronze/          # Raw JSONs
│   ├── silver/          # Clean Data (Partitioned)
│   │   └── year=YYYY/month=MM/day=DD/
│   └── gold/            # Business Reports
│       ├── ranking/     # Top pollution (.csv)
│       └── summary/     # Country averages (.csv)
├── src/                 # Modular source code
│   ├── ingestion/       # batch_ingest.py
│   ├── transform/       # batch_transform.py
│   ├── load/            # load_database.py
│   └── reports_to_gold/ # gold_report.py
├── pytests/             # Unit tests
├── Dockerfile           # Environment image
├── docker-compose.yml   # Docker configuration
```

---

## 🚀 Future Improvements (Roadmap)

These improvements reflect a natural evolution from a local environment toward a more production-ready pipeline, prioritizing scalability, observability, and data quality.

- **Observability:** Failure alerting via Airflow callbacks (Slack/Email integrations)
- **Data Contracts:** Validation of the `cities.yaml` file to ensure configuration integrity and consistency.
- **Advanced Testing:** Incorporation of fixtures in Pytest to improve coverage and isolate test scenarios.
- **Storage Optimization:** Migration of the Silver layer from CSV to columnar format (Parquet) for improved performance and efficiency.
- **Scalability:** Deployment in cloud environments (AWS/GCP) and adaptation to distributed processing with Apache Spark.
