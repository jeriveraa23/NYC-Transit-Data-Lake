# NYC Transit Data Lake: End-to-End Data Engineering & ML Pipeline

This project implements a fully automated, containerized **ELT + Machine Learning** pipeline built on New York City's Yellow Taxi trip data. It covers the complete data lifecycle: monthly ingestion, multi-layer transformation with Medallion Architecture, four ML models retrained automatically each month, and an interactive dashboard powered by a GraphQL API.

> Personal development project. Runs entirely on local with Docker Compose. AWS credentials are stored in a local `.env` file and are not versioned. Historical data loaded: **January 2023 – February 2026**.

---

### Data Source: NYC TLC

The New York City Taxi and Limousine Commission (TLC) publishes monthly Parquet files containing between **13 and 14 million trip records** each — pickup/dropoff zones, distances, fares, tips, and payment methods. Files are downloaded automatically from the TLC's CloudFront CDN.

The largest single Bronze file weighs ~70 MB. The total Bronze layer across all processed months weighs ~2.1 GB. The pipeline processes a new month automatically as data becomes available.

---

### Infrastructure (Docker)

The project uses **Docker Compose** to orchestrate five isolated containers:

* **airflow-webserver**: Airflow UI to manage, monitor and trigger DAGs.
* **airflow-scheduler**: Executes the monthly pipeline automatically. Handles S3 connectivity and runs both the ELT and ML jobs. The main DAG chains the ELT job followed by the ML job, guaranteeing models only train on already-validated data.
* **postgres**: Serves as the Airflow metastore, persisting pipeline execution state across restarts.
* **nyc-api**: FastAPI + Strawberry GraphQL API that reads Parquet files directly from S3 and serves six queries to the frontend.
* **nyc-dashboard**: React + Vite dashboard consuming the GraphQL API via Apollo Client.

S3 is not a container — it is an external AWS service used as the Data Lake, storing all layers partitioned by year and month.

---

### Pipeline Architecture — Medallion


---

### Core Components

* **Bronze — Raw Fidelity**: The monthly Parquet is uploaded to S3 without any modification. Immutable backup that allows full reprocessing at any time without re-downloading.

* **Silver — Data Quality**: Spark applies deep cleaning over Bronze: type corrections, removal of invalid records (zero distance, negative fares, zero passengers), date filtering to discard source dataset errors, tip normalization by payment method, trip duration calculation, and an `is_total_correct` flag auditing whether financial components sum to the reported total.

* **Gold — Business Intelligence**: Aggregates Silver to daily level per TLC zone. Generates revenue, tips, average distance, average duration, and the **Data Trust Score** — the percentage of trips with consistent financial data per day. Primary source consumed by the API.

* **ML Layer**: Four models retrained monthly with the last 6 months of historical data using PySpark MLlib and Facebook Prophet. Training data is read directly from the Silver layer in S3. Results saved to S3 partitioned by year and month.

* **GraphQL API**: Reads Parquet files directly from S3 using `s3fs` and `pandas` — no intermediate database. Exposes six queries covering KPIs, regression metrics, tip distribution, zone rankings, clustering results, and demand forecast.

* **Data Quality**: `is_total_correct` flag on every Silver record, Data Trust Score aggregated on every Gold row, and model performance metrics (RMSE, Accuracy, F1, Silhouette) computed and stored alongside each monthly prediction batch.

---

### Machine Learning Layer

| Model | Algorithm | Target | Input |
|---|---|---|---|
| Regression | RandomForestRegressor (MLlib) | `total_amount` | Distance, duration, zones, hour |
| Classification | RandomForestClassifier (MLlib) | Tip level: High / Medium / Low | Distance, zones, hour, payment type |
| Clustering | K-Means (MLlib) | Zone segment | Avg revenue, distance, duration, tip efficiency |
| Forecasting | Facebook Prophet | Daily trip demand | Historical daily trip counts from Gold |

All models use a **6-month sliding training window** — recent enough to reflect current behavior while capturing enough seasonal variation.

---

### Dashboard

<!-- IMAGE: Screenshot of Monthly Analytics dashboard -->
> *Monthly Analytics dashboard screenshot goes here*

<!-- IMAGE: Screenshot of Demand Forecast page -->
> *Demand Forecast page screenshot goes here*

The dashboard has two pages:

**Monthly Analytics** — five KPIs at the top (total revenue, total trips, average tip, Data Trust Score, and regression model accuracy) and four charts: actual vs predicted comparison, tip category distribution, top zones by revenue, and zone clustering scatter plot.

**Demand Forecast** — Prophet forecast with 95% confidence interval, recent historical series, and a table with the 5 highest-demand projected days of the upcoming month.

Available at `http://localhost:3000`. GraphQL explorer available at `http://localhost:8000/graphql`.

---

### Real Costs

**Estimated monthly cost: ~$2.36 USD.** The entire S3 bucket, including all tiers (Bronze, Silver, Gold, and ML), reached approximately 4.8 GB, slightly approaching the AWS free tier (5 GB). The cost is due to downloading the files for ML training from S3.

---

### How to Run the Project

**1. Clone the repository:**
```bash
git clone https://github.com/jeriveraa23/nyc-transit-data-lake.git
cd nyc-transit-data-lake
```

**2. Create the `.env` file at the project root:**
```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW__CORE__FERNET_KEY=...
S3_BUCKET_NAME=nyc-transit-data-lake
VITE_API_URL=http://localhost:8000/graphql
```

**3. Start all services:**
```bash
docker-compose up --build
```

**4. Activate the pipeline:**
- Open Airflow at `http://localhost:8080` (user: `admin` / password: `admin`)
- Enable the `nyc_taxi_pipeline_monolithic` DAG
- Trigger manually with the year and month to process

---

### Repository Structure

```
NYC-Transit-Data-Lake/
├── api/
│   ├── resolvers/         # kpi_resolver.py, regression_resolver.py,
│   │                      # classification_resolver.py, gold_resolver.py,
│   │                      # clustering_resolver.py, forecast_resolver.py
│   ├── services/          # s3_service.py — Parquet reading from S3
│   └── schema.py          # GraphQL types and queries with Strawberry
├── dags/
│   └── dag_nyc_ingestion.py   # Main DAG — ELT >> ML
├── src/
│   ├── connectors/        # S3 client with boto3
│   ├── extractors/        # HTTP download from NYC TLC + Spark reader
│   ├── loaders/           # File upload to S3
│   ├── transformers/      # Silver and Gold transformations with PySpark
│   ├── jobs/
│   │   ├── nyc_taxi_job.py    # ELT orchestration
│   │   └── nyc_ml_job.py      # ML orchestration with 6-month sliding window
│   └── ml/
│       ├── feature_engineering.py
│       ├── regression.py
│       ├── classification.py
│       ├── clustering.py
│       └── forecasting.py
├── dashboard/             # React + Vite + Apollo Client + Recharts
├── scripts/
│   └── entrypoint.sh      # Airflow initialization
├── docker-compose.yml
├── Dockerfile             # Airflow + Java 11 + Spark + ML libs
└── Dockerfile.api         # FastAPI + Strawberry + s3fs
```