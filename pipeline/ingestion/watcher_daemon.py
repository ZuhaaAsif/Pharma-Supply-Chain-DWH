"""
Standalone, event-driven file watcher (watchdog). Optional companion to the Airflow sensor:
it reacts to filesystem events in the landing zone the moment a file is closed/renamed and
asks Airflow (REST API) to run the batch pipeline DAG. The Airflow DAG itself also contains a
polling sensor built on the same LandingZoneWatcher class, so the pipeline works with or
without this daemon.

Run:  python -m pipeline.ingestion.watcher_daemon
"""
from __future__ import annotations

import logging
import os
import time

import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from pipeline import config

log = logging.getLogger("watcher")
AIRFLOW_API = os.getenv("SDI_AIRFLOW_API", "http://airflow-apiserver:8080/api/v2")
AIRFLOW_USER = os.getenv("SDI_AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("SDI_AIRFLOW_PASS", "airflow")
DAG_ID = "sdi_batch_pipeline"


def _token() -> str | None:
    try:
        r = requests.post(AIRFLOW_API.replace("/api/v2", "/auth/token"),
                          json={"username": AIRFLOW_USER, "password": AIRFLOW_PASS}, timeout=10)
        return r.json().get("access_token")
    except Exception:
        return None


def trigger_pipeline(reason: str) -> None:
    tok = _token()
    headers = {"Authorization": f"Bearer {tok}"} if tok else {}
    r = requests.post(f"{AIRFLOW_API}/dags/{DAG_ID}/dagRuns", headers=headers,
                      json={"logical_date": None, "conf": {"trigger": reason}}, timeout=15)
    log.info("triggered %s -> %s", DAG_ID, r.status_code)


class LandingHandler(FileSystemEventHandler):
    def __init__(self, debounce_s: float = 5.0):
        self.debounce_s, self.last = debounce_s, 0.0

    def _maybe_trigger(self, path: str):
        if not path.endswith(".csv"):
            return  # ignore *.part files still being written
        now = time.time()
        if now - self.last > self.debounce_s:  # many files arrive together -> one DAG run
            self.last = now
            trigger_pipeline(f"file landed: {os.path.basename(path)}")

    def on_created(self, event):
        if not event.is_directory:
            self._maybe_trigger(event.src_path)

    def on_moved(self, event):  # atomic rename *.part -> *.csv
        self._maybe_trigger(event.dest_path)


def main():
    logging.basicConfig(level=logging.INFO)
    config.ensure_dirs()
    obs = Observer()
    obs.schedule(LandingHandler(), str(config.LANDING_DIR), recursive=False)
    obs.start()
    log.info("watching %s", config.LANDING_DIR)
    try:
        while True:
            time.sleep(1)
    finally:
        obs.stop(); obs.join()


if __name__ == "__main__":
    main()
