# Pharma Cold-Chain DWH + BI — Phase 1: Sales & Demand Intelligence (SDI) Mart

**Intelligent Data Architectures · Fall 2026 · Milestone 1 (pipeline up to the analytical query layer)**

An automated data-engineering pipeline, orchestrated by **Apache Airflow 3**, that oversamples the 7-file
pharmaceutical supply-chain dataset, ingests it through a **FastAPI ingestion API** + Python **file watcher**,
stages it in **PostgreSQL** (bronze ODS → validated silver staging), models the **SDI data mart** as a star schema
in **DuckDB** (SCD-2, junk, role-playing and degenerate dimensions, two fact tables, summary tables), and serves a
timed **OLAP query layer**. Every layer emits metrics that drive four **Grafana** data-engineering dashboards.

## 👥 Group members

| # | Name | Roll number |
|---|------|-------------|
| 1 | Zuhaa Asif | 29169 |
| 2 | Kashaf Saleem | 29175 |

Repository: https://github.com/ZuhaaAsif/Pharma-Supply-Chain-DWH

## 📦 Milestone 1 deliverables (in this repo)

| Deliverable | Location |
|---|---|
| Project report (PDF, ≤5 pages) | [`deliverables/Milestone.1.SDI.DWH.Report.pdf`](deliverables/Milestone.1.SDI.DWH.Report.pdf) · LaTeX source: [`docs/report_latex/`](docs/report_latex/) |
| Excel workbook | [`deliverables/Milestone.1.SDI.DWH.Excel.xlsx`](deliverables/Milestone.1.SDI.DWH.Excel.xlsx) |
| Demo video (≤5 min) | [`deliverables/Milestone.1.SDI.DWH.Video.mp4`](deliverables/Milestone.1.SDI.DWH.Video.mp4) · _YouTube/Drive link: add here_ |
| All code (Airflow DAGs, pipeline package, SQL, Grafana) | `dags/`, `pipeline/`, `sql/`, `grafana/`, `scripts/` |
| Oversampled data + data dictionary | [`data/oversampled/`](data/oversampled/) · [`DATA_DICTIONARY.md`](data/oversampled/DATA_DICTIONARY.md) |

## 🏗️ Architecture

![Airflow pipeline](docs/diagrams/airflow_pipeline.png)

| Layer | Implementation |
|---|---|
| Oversampling | `pipeline/oversampling/generator.py` — seasonal temporal bootstrap, growth scaling, lineage re-keying, rule re-derivation, SCD-2 price seed, fault injection · DAG `sdi_oversampling` |
| Ingestion (batch) | Landing zone → `LandingZoneWatcher` (stable-size + SHA-256 de-dup) → `POST /ingest/batch` → `ods.raw_*` (COPY) · DAG `sdi_batch_pipeline` |
| Ingestion (stream) | IoT replayed in 500-event micro-batches → `POST /ingest/stream/iot` → `staging.stg_iot` · DAG `sdi_stream_ingest` |
| Storage & staging | Postgres `ods` (bronze, raw text) → `staging` (silver, typed) via SQL rules R1–R8, rejects in `staging.rejects` |
| Dimensional model | DuckDB `mart` — `fact_sales`, `fact_hospital_demand`, SCD-2 `dim_medicine`, role-playing `dim_date`, junk dims, aggregates; incremental by watermark |
| Query layer | `pipeline/query/catalog.py` — 15 widget queries (+ ROLLUP, CUBE, slice/dice, PIVOT, drill-across, SCD-2 audit), timed baseline vs optimized |
| Observability | `ops.*` tables → Grafana dashboards in `grafana/dashboards/` (previews in `docs/dashboards/`) |

![Star schema](docs/diagrams/star_schema.png)

## 🚀 Run it (docker — recommended)

```bash
cp .env.example .env                      # on Linux also set AIRFLOW_UID=$(id -u)
# put the 7 instructor CSVs in data/raw/   (not committed: 05_pharmacy_sales_data.csv is 218 MB > GitHub limit)
docker compose up -d --build              # first build takes a few minutes
```
* Airflow → http://localhost:8080 · Ingestion API docs → http://localhost:8000/docs · Grafana → http://localhost:3000 (admin/admin)

Then, in this order (the demo in the video):
1. **Base load:** `cp data/raw/*.csv data/landing/` → trigger `sdi_batch_pipeline` (or wait ≤30 min).
2. **Refresh 1:** trigger `sdi_oversampling` with conf `{"refresh_id": 1}` → it publishes files and the
   `sdi_batch_pipeline` + `sdi_stream_ingest` DAGs start automatically (Airflow Assets).
3. **Refresh 2:** same with `{"refresh_id": 2}`.
4. Watch the Grafana dashboards change after each step.

Optional event-driven watcher: `docker compose --profile watcher up -d file-watcher`.

## 🧪 Run it without docker

```bash
pip install -r requirements.txt "apache-airflow==3.1.8"
export SDI_PG_DSN=postgresql://pharma:pharma@localhost:5432/pharma_dwh PYTHONPATH=$PWD
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
airflow db migrate && airflow pools set duckdb_mart 1 "DuckDB single writer"
uvicorn pipeline.ingestion.api:app --port 8000 &
./scripts/demo_local.sh                   # base + refresh 1 + refresh 2 + fault drill + dashboard previews
pytest -q tests
```

## 📁 Repository layout

```
dags/                      Airflow DAGs (declarative; logic lives in pipeline/)
pipeline/
  config.py, schemas.py    sources, refresh plan, frozen data contract
  oversampling/            data generator
  ingestion/               API, file watcher, stream simulator, ODS loader, DDL
  staging/validate.py      bronze -> silver validation (R1-R8)
  mart/transform.py        silver -> gold star schema (SCD2, junk, facts, aggregates)
  query/                   OLAP catalog + timed runner
  monitoring/storage.py    storage & coverage metrics
sql/                       Postgres bootstrap, DuckDB mart DDL
grafana/                   provisioning + 4 dashboard JSONs
data/oversampled/          generated refreshes + data dictionary (committed)
docs/                      diagrams, dashboard previews, run evidence
deliverables/              report PDF, Excel workbook, demo video
```
