"""Narration for Milestone.1.SDI.DWH.Video — one entry per scene (also exported as docs/VIDEO_SCRIPT.md)."""
SCENES = [
    ("title", "Milestone 1: Sales and Demand Intelligence data warehouse",
     "Assalam o alaikum. This is our Milestone 1 demo for the pharmaceutical cold chain project. We built the Sales and Demand "
     "Intelligence data mart, up to the analytical query layer, as a fully automated pipeline orchestrated by Apache Airflow."),
    ("arch", "Pipeline design: three Airflow DAGs",
     "The pipeline has three DAGs. The oversampling DAG generates a new quarter of data for all seven source files and publishes them. "
     "Publishing emits Airflow assets, which automatically start the batch pipeline and the streaming DAG. The batch DAG watches the "
     "landing zone, ingests every file through our FastAPI ingestion API into the Postgres operational data store, validates it into "
     "staging, builds the star schema in DuckDB, and runs the timed query layer. The streaming DAG replays IoT telemetry in micro-batches."),
    ("oversampling", "Oversampling strategy: seasonal temporal bootstrap",
     "Each refresh appends one calendar quarter: first 2026 quarter one, then quarter two. We resample whole rows from the same quarter "
     "last year, so seasonality and the relationships between columns survive. Volume follows the data's own twelve month growth trend. "
     "Keys continue their sequences, lineage is re-pointed to newly generated batches and lots, and pricing rules are re-applied. "
     "We also inject a small number of bad rows on purpose, to test validation."),
    ("airflow", "The final run in Airflow: 7 DAG runs, 59 tasks, all green",
     "Here is the final end-to-end run. A base load of nine hundred and thirty four thousand rows, then two refreshes. Every one of the "
     "fifty nine task instances succeeded. Notice the dynamic task mapping: one ingestion task per file."),
    ("validation", "Validation: every injected fault was caught",
     "Validation rules check for null keys and SKUs, prices greater than zero, valid dates, foreign keys and duplicates. Refresh one "
     "injected one hundred and nineteen bad rows, and exactly one hundred and nineteen were rejected. Refresh two, ninety and ninety."),
    ("dash1", "Dashboard 1: batch ingestion",
     "The batch ingestion dashboard shows load duration per file. The large sales file dominates the base load. The success rate is "
     "ninety seven point four percent: the one failure is a deliberate fault drill, a file with missing columns that the API rejected."),
    ("dash2", "Dashboard 2: streaming ingestion",
     "The streaming dashboard tracks throughput per micro-batch, around seventeen thousand events per second, with end-to-end latency "
     "of about thirty five milliseconds. A hundred and thirty seven malformed events were dropped and counted."),
    ("star", "Dimensional model: SDI star schema in DuckDB",
     "The mart has two fact tables sharing conformed dimensions: sales at transaction grain, and hospital demand as a monthly snapshot. "
     "The medicine dimension is slowly changing type two, so price changes create new versions. We also use junk dimensions, a "
     "role-playing date dimension, degenerate dimensions and summary tables."),
    ("dash3", "Dashboard 3: storage and staging",
     "The storage dashboard shows rows and megabytes per domain, and date coverage from January 2023 to June 2026. The cumulative bars "
     "prove the warehouse is appended to, never emptied."),
    ("dash4", "Dashboard 4: analytical query layer",
     "The query dashboard times every widget query. All two hundred and seven executions succeeded, all under twenty five milliseconds. "
     "Summary tables make the heaviest queries up to seven times faster."),
    ("results", "The answers change after every refresh",
     "And the results move with the new data. Revenue grows after each refresh, and the top fast-moving medicines change every quarter. "
     "In quarter two, respiratory medicines lead, which reflects seasonality."),
    ("close", "Summary",
     "To summarise: automated oversampling, API and file-watcher ingestion, validated staging, a star schema with slowly changing "
     "dimensions, and a timed query layer, all monitored on four dashboards. Milestone 2 adds the BI dashboards. Thank you."),
]
