# app.py
import os
import json
import uuid
import time
import traceback
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# DB
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime, Text
)
from sqlalchemy.orm import sessionmaker, declarative_base

# Data utils
import pandas as pd

# Your unmodified scraper class lives in scraper.py
from scraper import CFETariffScraperSimplified

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/cfe_tariffs")
DB_URL = os.environ.get("DB_URL", f"sqlite:///{os.path.join(OUTPUT_DIR, 'cfe.db')}")
EXCEL_PATH = os.path.join(OUTPUT_DIR, "english_tariff_latest.xlsx")
EN_JSON_PATH = os.path.join(OUTPUT_DIR, "cfe_tariff_data_english.json")
ES_JSON_PATH = os.path.join(OUTPUT_DIR, "cfe_tariff_data_spanish.json")
FAIL_JSON_PATH = os.path.join(OUTPUT_DIR, "failed_extractions.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------------------------------------------
# DB setup
# -------------------------------------------------------------------
Base = declarative_base()
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

class Run(Base):
    __tablename__ = "runs"
    id = Column(String, primary_key=True)
    status = Column(String, default="queued")  # queued|running|succeeded|failed
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    message = Column(Text, nullable=True)

class TariffEN(Base):
    __tablename__ = "tariffs_en"
    id = Column(Integer, primary_key=True, autoincrement=True)
    # store the scraper's record id so we can deduplicate
    record_id = Column(String, index=True, unique=True)
    run_id = Column(String, index=True)
    region = Column(String)
    municipality = Column(String)
    division = Column(String)
    year = Column(String)
    month = Column(String)
    month_name = Column(String)
    extracted_at = Column(String)
    fare = Column(String)
    post = Column(String)
    units = Column(String)
    tariff_value = Column(String)

class Failure(Base):
    __tablename__ = "failures"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, index=True)
    timestamp = Column(String)
    fare_type = Column(String)
    region = Column(String)
    municipality = Column(String)
    division = Column(String)
    year = Column(String)
    month = Column(String)
    error = Column(Text)

Base.metadata.create_all(engine)

# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------
class StartScrapeBody(BaseModel):
    headless: bool = True
    # optional: override output dir
    output_dir: Optional[str] = None

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def load_json_safe(path: str):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def persist_to_db_from_json(run_id: str, en_json_path: str, fail_json_path: str):
    """Read the generated English JSON + failures and write into DB tables."""
    session = SessionLocal()
    try:
        data = load_json_safe(en_json_path)
        if data:
            # dedupe by record_id to avoid duplicates across runs
            existing_ids = {
                rid for (rid,) in session.query(TariffEN.record_id).all()
            }
            rows = []
            for rec in data:
                rid = rec.get("id")
                if not rid or rid in existing_ids:
                    continue
                rows.append(TariffEN(
                    record_id=rid,
                    run_id=run_id,
                    region=rec.get("region"),
                    municipality=rec.get("municipality"),
                    division=rec.get("division"),
                    year=str(rec.get("year")),
                    month=str(rec.get("month")),
                    month_name=rec.get("month_name"),
                    extracted_at=rec.get("extracted_at"),
                    fare=rec.get("fare"),
                    post=rec.get("post"),
                    units=rec.get("units"),
                    tariff_value=str(rec.get("tariff_value")),
                ))
            if rows:
                session.add_all(rows)
                session.commit()

        fails = load_json_safe(fail_json_path)
        if fails:
            for fr in fails:
                session.add(Failure(
                    run_id=run_id,
                    timestamp=fr.get("timestamp"),
                    fare_type=fr.get("fare_type"),
                    region=fr.get("region"),
                    municipality=fr.get("municipality"),
                    division=fr.get("division"),
                    year=str(fr.get("year")),
                    month=str(fr.get("month")),
                    error=fr.get("error"),
                ))
            session.commit()
    finally:
        session.close()

def make_excel_from_en_json(en_json_path: str, excel_out: str):
    data = load_json_safe(en_json_path)
    if not data:
        # Create an empty file to avoid 404s
        df = pd.DataFrame(columns=[
            "id","region","municipality","division","year","month","month_name",
            "extracted_at","fare","post","units","tariff_value"
        ])
    else:
        df = pd.DataFrame(data)
    # Sort a bit for readability
    sort_cols = [c for c in ["year","month","region","municipality","division","fare"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, kind="stable")
    df.to_excel(excel_out, index=False)

def run_scraper_job(run_id: str, headless: bool, output_dir_override: Optional[str]):
    """Background job: run scraper, then persist to DB and build Excel."""
    session = SessionLocal()
    try:
        run = session.query(Run).get(run_id)
        run.status = "running"
        run.started_at = datetime.utcnow()
        session.commit()

        out_dir = output_dir_override or OUTPUT_DIR
        os.makedirs(out_dir, exist_ok=True)

        # Instantiate and run your scraper AS-IS
        scraper = CFETariffScraperSimplified(out_dir, headless=headless)
        scraper.scrape_all_data()

        # Persist to DB from files the scraper created
        persist_to_db_from_json(run_id, EN_JSON_PATH if out_dir == OUTPUT_DIR else os.path.join(out_dir, "cfe_tariff_data_english.json"),
                                FAIL_JSON_PATH if out_dir == OUTPUT_DIR else os.path.join(out_dir, "failed_extractions.json"))

        # Build the English Excel for download
        excel_path = EXCEL_PATH if out_dir == OUTPUT_DIR else os.path.join(out_dir, "english_tariff_latest.xlsx")
        make_excel_from_en_json(EN_JSON_PATH if out_dir == OUTPUT_DIR else os.path.join(out_dir, "cfe_tariff_data_english.json"),
                                excel_path)

        run.status = "succeeded"
        run.finished_at = datetime.utcnow()
        run.message = "Scrape completed."
        session.commit()
    except Exception as e:
        # On failure, still mark the run
        if 'session' in locals():
            run = session.query(Run).get(run_id)
            run.status = "failed"
            run.finished_at = datetime.utcnow()
            run.message = f"{e}\n{traceback.format_exc()}"
            session.commit()
    finally:
        session.close()

# -------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------
app = FastAPI(title="CFE Tariff Scraper API", version="1.0.0")

@app.post("/scrape/start")
def start_scrape(body: StartScrapeBody, tasks: BackgroundTasks):
    run_id = str(uuid.uuid4())
    session = SessionLocal()
    try:
        session.add(Run(id=run_id, status="queued"))
        session.commit()
    finally:
        session.close()

    tasks.add_task(run_scraper_job, run_id, body.headless, body.output_dir)
    return {"run_id": run_id, "status": "queued"}

@app.get("/scrape/status/{run_id}")
def get_status(run_id: str):
    session = SessionLocal()
    try:
        run = session.query(Run).get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return {
            "run_id": run.id,
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "message": run.message,
        }
    finally:
        session.close()

@app.get("/download/english-excel")
def download_english_excel(run_id: Optional[str] = Query(default=None, description="Optional run id; if omitted, serves the latest file")):
    # For simplicity we always serve the latest Excel in OUTPUT_DIR
    path = EXCEL_PATH
    if not os.path.exists(path):
        # Try to build from JSON if Excel not present yet
        if os.path.exists(EN_JSON_PATH):
            make_excel_from_en_json(EN_JSON_PATH, path)
        else:
            raise HTTPException(status_code=404, detail="No English data available yet.")
    filename = f"english_tariff_latest.xlsx"
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=filename)

@app.get("/download/english-json")
def download_english_json():
    path = EN_JSON_PATH
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="No English JSON available yet.")
    filename = "cfe_tariff_data_english.json"
    return FileResponse(path, media_type="application/json", filename=filename)

@app.get("/failures")
def list_failures(limit: int = 100):
    session = SessionLocal()
    try:
        q = session.query(Failure).order_by(Failure.id.desc()).limit(limit).all()
        return [dict(
            id=f.id, run_id=f.run_id, timestamp=f.timestamp, fare_type=f.fare_type,
            region=f.region, municipality=f.municipality, division=f.division,
            year=f.year, month=f.month, error=f.error
        ) for f in q]
    finally:
        session.close()

@app.get("/records")
def list_records(limit: int = 100, region: Optional[str] = None, municipality: Optional[str] = None, division: Optional[str] = None, fare: Optional[str] = None):
    session = SessionLocal()
    try:
        q = session.query(TariffEN).order_by(TariffEN.id.desc())
        if region:
            q = q.filter(TariffEN.region == region)
        if municipality:
            q = q.filter(TariffEN.municipality == municipality)
        if division:
            q = q.filter(TariffEN.division == division)
        if fare:
            q = q.filter(TariffEN.fare == fare)
        q = q.limit(limit).all()
        return [dict(
            record_id=r.record_id, run_id=r.run_id, region=r.region, municipality=r.municipality,
            division=r.division, year=r.year, month=r.month, month_name=r.month_name,
            extracted_at=r.extracted_at, fare=r.fare, post=r.post, units=r.units,
            tariff_value=r.tariff_value
        ) for r in q]
    finally:
        session.close()
