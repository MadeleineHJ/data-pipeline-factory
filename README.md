# [Data Pipeline Factory](https://madeleinehj.github.io/data-pipeline-factory/)


End-to-end data engineering project demonstrating a config-driven ELT platform, built across three connected GitHub repositories. The infrastructure is source-agnostic by design: adding a new data source means writing one spider, one set of dbt models, and one YAML entry; nothing else changes. Premier League data from football-data.org is used throughout as the worked example.

## TL;DR

The infrastructure, not the football data, is the point. A single YAML config file drives the entire platform: registering a Scrapy spider, generating its Airflow DAG, and wiring its dbt transformation into the schedule. Each repository owns one layer: extraction, orchestration, or transformation, so a change in one never forces a change in another.

## Architecture

```
Any data source (API · website · file)
        ↓
Scrapy spider (scrapers repo)
        ↓
Docker image → GHCR
        ↓
Airflow DAG factory (airflow repo)
        ↓
BigQuery raw dataset (unified JSON schema)
        ↓
dbt bronze layer — parsed & typed
        ↓
dbt silver layer — clean & business-ready
        ↓
dbt gold layer — aggregated for BI (planned)
```

## Tech stack

| Layer | Tool |
|---|---|
| Ingestion | Scrapy (API spiders) |
| Containerisation | Docker, GitHub Container Registry (GHCR) |
| CI/CD | GitHub Actions |
| Orchestration | Apache Airflow (Astronomer Astro) |
| dbt integration | Astro Cosmos (DbtTaskGroup) |
| Warehouse | Google BigQuery |
| Transformation | dbt Core, dbt-bigquery adapter |

## Repository structure

The platform spans three repositories, each owning one layer:

```
scrapers/                          # github.com/MadeleineHJ/scrapers
├── scraper/
│   ├── spiders/
│   │   └── api_spiders/
│   │       ├── football_standings.py
│   │       ├── football_matches.py
│   │       └── football_top_scorers.py
│   ├── pipelines/
│   │   └── bigquery_pipeline.py   # batch loads to BigQuery
│   ├── items.py                   # unified ScrapedItem schema
│   └── settings.py
├── entrypoint.sh                  # writes GCP credentials at runtime
├── Dockerfile
└── .github/workflows/ci.yml       # test → build → push to GHCR

airflow/                           # github.com/MadeleineHJ/airflow
├── dags/factories/scrapers/
│   ├── dag_factory.py             # generates all DAGs from YAML
│   └── spiders_config.yaml        # single source of truth
├── docker-compose.override.yml    # mounts dbt project into container
└── requirements.txt

madeleine-portfolio/                # github.com/MadeleineHJ/dbt
├── data_transformation/
│   ├── dbt_project.yml
│   ├── macros/
│   │   ├── flatten_json.sql        # parses raw JSON into typed columns
│   │   ├── generate_schema_name.sql
│   │   └── scd_type2.sql           # reusable SCD Type 2 snapshot logic
│   ├── models/
│   │   ├── bronze_layer/
│   │   │   └── <source>/           # one folder per source, e.g. bronze_football
│   │   └── silver_layer/
│   │       └── <source>/
│   └── tests/
│       ├── generic/                # reusable custom test definitions
│       └── singular/<source>/      # business-rule tests per source
├── documentation/
│   ├── readme/<source>_README.md       # what the source is, how it's modelled
│   └── changelog/<source>_CHANGELOG.md # dated log of changes per source
└── README.md
```

## Phase-by-phase summary

| Phase | Layer | Deliverable |
|---|---|---|
| 1 | Ingestion | Spiders writing a unified raw schema to BigQuery, one table per source |
| 2 | Containerisation | Dockerfile + entrypoint.sh, image published to GHCR on every push to main |
| 3 | Orchestration | Config-driven DAG factory generating one DAG per spider plus one dbt DAG per pipeline |
| 4 | Coordination | ExternalTaskSensors gating dbt runs on successful spider completion |
| 5 | Bronze | JSON flattened into typed columns via a reusable `flatten_json` macro, per source |
| 6 | Silver | Cleaned, deduplicated, business-rule models, with `scd_type2` available for slowly changing dimensions |
| 7 | Quality | Generic and singular dbt tests organised per source |
| 8 | Gold (planned) | Aggregated marts for BI consumption |

## Key engineering decisions

- **Config-driven factory pattern.** No DAG file is ever written by hand. `dag_factory.py` reads `spiders_config.yaml` and generates every spider DAG and dbt DAG automatically. Adding a pipeline means adding one YAML block, not Python code.

- **Unified raw schema across all sources.** Every spider, regardless of source, writes to the same seven-field schema: `run_id, data, extraction_type, execution_date, start_date, end_date, endpoint`. The source-specific payload lives in `data` as a JSON string, keeping the warehouse landing structure stable as sources change.

- **Repository separation by responsibility.** Spiders live only in the scrapers repo. Orchestration logic lives only in the airflow repo. Transformation logic lives only in the dbt repo. This means a spider update never touches Airflow code, and a dbt model update never triggers a Docker rebuild.

- **Per-source folders in dbt, not per-pipeline files.** Bronze and silver models, tests, and documentation are all organised under a `<source>` folder. Adding a new source means adding a new folder in four predictable places — models, tests, and documentation — without touching anything that already exists.

- **Configurable write disposition per source.** `WRITE_TRUNCATE` is used for snapshot-style data like weekly standings where only the latest state matters. `WRITE_APPEND` is reserved for sources where historical accumulation matters. The choice is a one-line setting on the BigQuery pipeline class, not a structural change.

- **Sensors before transformation, not a fixed delay.** The dbt DAG does not assume spiders finished in time — `ExternalTaskSensor` tasks confirm each spider DAG succeeded for the same logical date before any dbt model runs. This prevents dbt from ever transforming incomplete or stale source data.

- **Cosmos over a single dbt command.** Astro Cosmos renders each dbt model as its own Airflow task rather than running `dbt run` as one opaque step. A failed model can be retried individually, and the dbt dependency graph is visible directly in the Airflow UI.

## Quickstart

Requires Python 3.11+, Docker Desktop, and Astronomer Astro CLI. Tested on Windows PowerShell.

**1. Run a spider locally**

```powershell
cd scrapers
pip install -r requirements.txt
cp .env.example .env   # fill in GCP_PROJECT_ID, BQ_DATASET, FOOTBALL_API_KEY
scrapy crawl football_standings
```

**2. Build and push the scraper image**

```powershell
git add .
git commit -m "feat: update spider"
git push origin main   # GitHub Actions builds and pushes to GHCR automatically
```

**3. Run Airflow locally**

```powershell
cd airflow
astro dev start
astro dev ps            # find the assigned port
# open http://localhost:{port}
```

**4. Run dbt directly**

```powershell
cd madeleine-portfolio/data_transformation
dbt run --select bronze_layer.bronze_football --profile dbt_project --target dev
dbt run --select silver_layer.silver_football --profile dbt_project --target dev
```

Expected: bronze and silver tables populated in `brz_football` and `slv_football` datasets in BigQuery.

## Documentation

- [Scrapers repo README](https://github.com/MadeleineHJ/scrapers/blob/main/README.md) — spider structure, unified item schema, CI/CD
- [Airflow repo README](https://github.com/MadeleineHJ/airflow/blob/main/README.md) — DAG factory, YAML config, sensors
- [dbt repo README](https://github.com/MadeleineHJ/madeleine-portfolio/blob/main/README.md) — medallion layers, macros, tests, per-source documentation

## Skills demonstrated

- Config-driven DAG factory pattern (Airflow)
- Source-agnostic ingestion design (unified raw schema)
- Containerised extraction with automated CI/CD (Docker, GHCR, GitHub Actions)
- Cross-DAG coordination (ExternalTaskSensor)
- dbt-as-tasks orchestration (Astro Cosmos)
- Medallion architecture (bronze/silver/gold)
- Reusable dbt macros for dynamic JSON parsing and SCD Type 2
- Per-source dbt testing (generic + singular)
- Multi-repository architecture with clean separation of concerns

## Next steps

- **Data quality and alerting** — dbt tests already catch issues at each layer; next is an Airflow `on_failure_callback` to send Slack notifications with the failing model and remediation steps
- **Pipeline monitoring dashboard** — real-time view of last successful run, row counts, data freshness, and failure history per source, built on Airflow metadata and BigQuery audit logs
- **Advanced modelling** — extend `scd_type2` usage to more dimensions and add incremental models for new sources.

## Data source

football-data.org API, free tier. See https://www.football-data.org

## License

MIT for project code. Data is governed by football-data.org's terms of use.
