from functools import lru_cache
from fastapi import FastAPI, File, UploadFile, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, RedirectResponse
import shutil
import tempfile
import os
import time
from process_data_dask import tsv_to_gpkg
from dw_service import is_valid_download_request
from fastapi.middleware.cors import CORSMiddleware
import logging
from threading import Lock
from typing import Literal
import settings

# Configure logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dictionary to track conversion statuses and file paths
conversion_status = {}
status_lock = Lock()

#dict for mapping crs to their EPSG-id
crs_map = {
  "euref": "EPSG:3067",
  "wgs84": "EPSG:4326"
}

geo_map = {
  "point": "points",
  "bbox": "bbox",
  "footprint": "original"
}

@lru_cache
def get_settings():
  return settings.Settings()

def convert_tsv_to_gpkg(id, zip_path, geo, crs, background_tasks, uploaded_file):
    file_size = os.path.getsize(zip_path)
    output_gpkg = tempfile.NamedTemporaryFile(delete=False, suffix=".gpkg").name
    conversion_id = id

    geo = geo_map[geo]
    crs = crs_map[crs]
    
    def process_file():
        """Handles file processing."""
        try:
            tsv_to_gpkg(zip_path, output_gpkg, geom_type=geo, crs=crs)
            with status_lock:
                conversion_status[conversion_id] = {
                    "status": "completed",
                    "output": output_gpkg,
                    "timestamp": time.time(),
                    "uploaded_file": uploaded_file,
                }
        except Exception as e:
            logging.error(f"Error during conversion: {e}")
            with status_lock:
                conversion_status[conversion_id] = {"status": "failed"}
        finally:
            if uploaded_file:
              os.remove(zip_path)

    if file_size < 10 * 1024 * 1024:  # Small files: process immediately
        process_file()
        return {
            "id": conversion_id,
            "status": "completed",
            "download_url": f"/output/{conversion_id}"
        }
    else:  # Large files: process in the background
        with status_lock:
            conversion_status[conversion_id] = {
                "status": "processing",
                "output": output_gpkg,
                "timestamp": time.time(),
                "uploaded_file": uploaded_file,
            }
        background_tasks.add_task(process_file)
        return {
            "id": conversion_id,
            "status": "processing",
            "status_url": f"/status/{conversion_id}",
            "download_url": f"/output/{conversion_id}"
        }

@app.get("/convert/{id}/{fmt}/{geo}/{crs}")
async def convert_with_id(
  id: str,
  fmt: Literal["gpkg"],
  geo: Literal["bbox", "point", "footprint"],
  crs: Literal["euref","wgs84"],
  personToken: str | None = None,
  background_tasks: BackgroundTasks = None
):
    """API enpoint to start converting file stored in dw"""
    if not is_valid_download_request(id, personToken):
      raise HTTPException(status_code=403, detail="Permission denied.")

    zip_path = get_settings().FILE_PATH + id + ".zip"
    return convert_tsv_to_gpkg(id, zip_path, geo, crs, background_tasks, False)

@app.post("/convert/{id}/{fmt}/{geo}/{crs}")
async def convert_with_file(
    id: str,
    fmt: Literal["gpkg"],
    geo: Literal["bbox", "point", "footprint"],
    crs: Literal["euref","wgs84"],
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None
):
    """API endpoint to receive ZIP TSV file and return a GeoPackage."""

    logging.info(f"Received file: {file.filename}, geo: {geo}, crs: {crs}")

    # Create a temporary file to store the uploaded zip
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip:
        shutil.copyfileobj(file.file, temp_zip)
        temp_zip_path = temp_zip.name

    return convert_tsv_to_gpkg(id, temp_zip_path, geo, crs, background_tasks, True)

@app.get("/status/{id}")
async def get_status(id: str):
    """ Endpoint to check the status of a conversion. """
    logging.info(f"Checking status for conversion ID: {id}")
    with status_lock:
        if id not in conversion_status:
            raise HTTPException(status_code=404, detail="Conversion ID not found.")
        status = conversion_status[id]
        return {"status": status["status"]}

@app.get("/output/{id}")
async def get_output(id: str, personToken: str | None = None):
    """ Endpoint to retrieve the output file for a completed conversion. """
    logging.info(f"Retrieving output for conversion ID: {id}")
    with status_lock:
        if id not in conversion_status:
            raise HTTPException(status_code=404, detail="Conversion ID not found.")
        status = conversion_status[id]
        if status["status"] != "completed":
            raise HTTPException(status_code=400, detail="Conversion not completed yet.")
        output_path = status["output"]
        if not os.path.exists(output_path):
            raise HTTPException(status_code=404, detail="Output file not found.")
        uploaded_file = status['uploaded_file']
        if not uploaded_file and not is_valid_download_request(id, personToken):
            raise HTTPException(status_code=403, detail="Permission denied.")
        return FileResponse(output_path, filename="output.gpkg", media_type="application/geopackage+sqlite3")

@app.on_event("startup")
async def cleanup_old_files():
    """ Periodically clean up files older than 24 hours. """
    def cleanup():
        logging.info("Starting cleanup thread...")
        while True:
            time.sleep(3600)  # Run cleanup every hour
            with status_lock:
                current_time = time.time()
                for id, status in list(conversion_status.items()):
                    if current_time - status["timestamp"] > 86400:  # 24 hours
                        if os.path.exists(status["output"]):
                            os.remove(status["output"])
                        del conversion_status[id]
    import threading
    threading.Thread(target=cleanup, daemon=True).start()