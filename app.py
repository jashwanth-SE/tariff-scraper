# server.py
import uuid
import threading
import traceback
from typing import Dict, Optional
import os
import json
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from pathlib import Path
import logging

# Import your unchanged class
from scraper import CFETariffScraperSimplified  # <-- your file name if different, adjust the import

app = FastAPI(title="CFE Tariff Scraper Service")

# Basic logging to file and console
LOG_DIR = Path("./logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("cfe_api")

class StartJobRequest(BaseModel):
    output_dir: str = Field(default="/app/data", description="Directory to store outputs")
    headless: bool = Field(True, description="Run Chrome headless on server")
    # Optional: allow limiting fare types later without changing scraper structure.
    # We won’t use it here since you asked not to change code logic.

class JobStatus(BaseModel):
    job_id: str
    status: str           # pending|running|finished|failed
    message: Optional[str] = None
    output_dir: Optional[str] = None

class _Job:
    def __init__(self, job_id: str, output_dir: str, headless: bool):
        self.job_id = job_id
        self.output_dir = output_dir
        self.headless = headless
        self.status = "pending"
        self.message: Optional[str] = None
        self._thread: Optional[threading.Thread] = None

    def run(self):
        try:
            self.status = "running"
            logger.info(f"[{self.job_id}] Starting scrape to {self.output_dir} (headless={self.headless})")
            scraper = CFETariffScraperSimplified(self.output_dir, headless=self.headless)
            scraper.scrape_all_data()
            self.status = "finished"
            self.message = "Scrape completed."
            logger.info(f"[{self.job_id}] Completed.")
        except Exception as e:
            self.status = "failed"
            self.message = f"Error: {e}"
            logger.error(f"[{self.job_id}] Failed with error: {e}\n{traceback.format_exc()}")

# In-memory job store (simple & effective for one box)
_JOBS: Dict[str, _Job] = {}

@app.post("/scrape/start", response_model=JobStatus)
def start_scrape(req: StartJobRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    
    # Use a proper output directory if not provided or invalid
    output_dir = req.output_dir
    if not output_dir or output_dir == "string" or not os.path.isabs(output_dir):
        output_dir = "/app/data"
    
    job = _Job(job_id, output_dir, req.headless)
    _JOBS[job_id] = job

    # Start background thread
    def target():
        job.run()

    t = threading.Thread(target=target, daemon=True)
    job._thread = t
    t.start()

    return JobStatus(job_id=job_id, status=job.status, message="Started.", output_dir=output_dir)

@app.get("/scrape/status/{job_id}", response_model=JobStatus)
def get_status(job_id: str):
    job = _JOBS.get(job_id)
    if not job:
        return JobStatus(job_id=job_id, status="unknown", message="No such job.")
    return JobStatus(job_id=job.job_id, status=job.status, message=job.message, output_dir=job.output_dir)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/scrape/download/{job_id}/spanish")
def download_spanish_data(job_id: str):
    job = _JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    spanish_file = os.path.join(job.output_dir, "cfe_tariff_data_spanish.json")
    if not os.path.exists(spanish_file):
        raise HTTPException(status_code=404, detail="Spanish data file not found")
    
    return FileResponse(spanish_file, filename="cfe_tariff_data_spanish.json")

@app.get("/scrape/download/{job_id}/english")
def download_english_data(job_id: str):
    job = _JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    english_file = os.path.join(job.output_dir, "cfe_tariff_data_english.json")
    if not os.path.exists(english_file):
        raise HTTPException(status_code=404, detail="English data file not found")
    
    return FileResponse(english_file, filename="cfe_tariff_data_english.json")

@app.get("/scrape/failures/{job_id}")
def get_failed_extractions(job_id: str):
    job = _JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    failures_file = os.path.join(job.output_dir, "failed_extractions.json")
    if not os.path.exists(failures_file):
        return JSONResponse(content={"failures": [], "total": 0})
    
    try:
        with open(failures_file, 'r', encoding='utf-8') as f:
            failures = json.load(f)
        return JSONResponse(content={"failures": failures, "total": len(failures)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading failures: {str(e)}")

@app.get("/scrape/list")
def list_all_jobs():
    jobs = []
    for job_id, job in _JOBS.items():
        jobs.append({
            "job_id": job_id,
            "status": job.status,
            "output_dir": job.output_dir,
            "message": job.message
        })
    return {"jobs": jobs}

@app.get("/scrape/data-status/{job_id}")
def get_data_status(job_id: str):
    job = _JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    spanish_file = os.path.join(job.output_dir, "cfe_tariff_data_spanish.json")
    english_file = os.path.join(job.output_dir, "cfe_tariff_data_english.json")
    failures_file = os.path.join(job.output_dir, "failed_extractions.json")
    
    status = {
        "job_id": job_id,
        "output_dir": job.output_dir,
        "files": {
            "spanish_data": {
                "exists": os.path.exists(spanish_file),
                "size": os.path.getsize(spanish_file) if os.path.exists(spanish_file) else 0,
                "records": 0
            },
            "english_data": {
                "exists": os.path.exists(english_file),
                "size": os.path.getsize(english_file) if os.path.exists(english_file) else 0,
                "records": 0
            },
            "failures": {
                "exists": os.path.exists(failures_file),
                "size": os.path.getsize(failures_file) if os.path.exists(failures_file) else 0,
                "count": 0
            }
        }
    }
    
    # Count records in Spanish file
    if os.path.exists(spanish_file):
        try:
            with open(spanish_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                status["files"]["spanish_data"]["records"] = len(data)
        except:
            pass
    
    # Count records in English file
    if os.path.exists(english_file):
        try:
            with open(english_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                status["files"]["english_data"]["records"] = len(data)
        except:
            pass
    
    # Count failures
    if os.path.exists(failures_file):
        try:
            with open(failures_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                status["files"]["failures"]["count"] = len(data)
        except:
            pass
    
    return status
