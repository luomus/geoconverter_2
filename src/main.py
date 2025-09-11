from functools import lru_cache
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
import shutil
import tempfile
import os
import time
from table_to_gpkg import handle_conversion_request
from dw_service import is_valid_download_request
from fastapi.middleware.cors import CORSMiddleware
import logging
from typing import Literal, Optional
import settings
from gis_to_table import gis_to_table

# Get settings and configure logging
app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)

# Configure logging
logging.basicConfig(level=log_level)

app = FastAPI()

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@lru_cache
def get_settings():
  return settings.Settings()

@app.post("/convert-to-table")
async def convert_gis_to_table(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """
    Convert a GIS file to a tabular CSV using the gis_to_table() function.
    Returns the saved CSV file with geometry as WKT.
    """
    logging.info(f"Received GIS file: {file.filename}, format: {file.content_type}")

    # Validate file extension
    SUPPORTED_EXTENSIONS = {'.shp', '.geojson', '.json', '.gpkg', '.kml', '.gml', '.zip'}
    basename, suffix = os.path.splitext(file.filename)
    if suffix not in SUPPORTED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {suffix}")


    try:
        # Save uploaded GIS file to temp location
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name

        # Perform conversion (this saves CSV alongside the GIS file)
        csv_path = gis_to_table(tmp_path)

        if not os.path.exists(csv_path):
            raise RuntimeError("CSV output file not found after conversion.")

        # Schedule cleanup
        if background_tasks:
            background_tasks.add_task(os.remove, tmp_path)
            background_tasks.add_task(os.remove, csv_path)

            
        logging.debug(f"Returning CSV file: {csv_path}")
        return FileResponse(csv_path, filename=basename, media_type="text/csv")

    except Exception as e:
        logging.error(f"GIS-to-table conversion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/convert/{id}/{lang}/{geo}/{crs}")
async def convert_with_id(
  id: str,
  lang: Literal["fi", "en", "tech"],
  geo: Literal["bbox", "point", "footprint"],
  crs: Literal["euref","wgs84"],
  background_tasks: BackgroundTasks,
  personToken: Optional[str]= None
):
    """API enpoint to start converting file stored in dw"""

    logging.info(f"Received request to convert ID: {id}, lang: {lang}, geo: {geo}, crs: {crs}")

    if not is_valid_download_request(id, personToken):
      raise HTTPException(status_code=403, detail="Permission denied.")

    zip_path = get_settings().FILE_PATH + id + ".zip"
    return handle_conversion_request(id, zip_path, lang, geo, crs, background_tasks, False)

@app.post("/convert/{id}/{lang}/{geo}/{crs}")
async def convert_with_file(
    id: str,
    lang: Literal["fi", "en", "tech"],
    geo: Literal["bbox", "point", "footprint"],
    crs: Literal["euref","wgs84"],
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """API endpoint to receive ZIP TSV file and return a GeoPackage."""

    logging.info(f"Received file: {file.filename}, language: {lang}, geo: {geo}, crs: {crs}")

    # Create a temporary file to store the uploaded zip
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip:
        shutil.copyfileobj(file.file, temp_zip)
        temp_zip_path = temp_zip.name

    return handle_conversion_request(id, temp_zip_path, lang, geo, crs, background_tasks, True)

@app.get("/status/{id}")
async def get_status(id: str):
    """ Endpoint to check the status of a conversion. """
    logging.info(f"Checking status for conversion ID: {id}")
    from table_to_gpkg import conversion_status, status_lock
    with status_lock:
        if id not in conversion_status:
            raise HTTPException(status_code=404, detail="Conversion ID not found.")
        status = conversion_status[id]
        return {"status": status["status"]}

@app.get("/output/{id}")
async def get_output(id: str, personToken: Optional[str] = None):
    """ Endpoint to retrieve the output file for a completed conversion. """
    logging.info(f"Retrieving output for conversion ID: {id}")
    from table_to_gpkg import conversion_status, status_lock
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
        
        # Use original filename if available, otherwise use the conversion ID
        original_filename = status.get("original_filename", id)
        return FileResponse(output_path, filename=f"{original_filename}.zip", media_type="application/zip")

@app.on_event("startup")
async def cleanup_old_files():
    """ Periodically clean up files older than 24 hours. """
    def cleanup():
        logging.info("Starting cleanup thread...")
        while True:
            time.sleep(3600)  # Run cleanup every hour
            from table_to_gpkg import conversion_status, status_lock
            with status_lock:
                current_time = time.time()
                for id, status in list(conversion_status.items()):
                    if current_time - status["timestamp"] > 86400:  # 24 hours
                        if os.path.exists(status["output"]):
                            os.remove(status["output"])
                        del conversion_status[id]
    import threading
    threading.Thread(target=cleanup, daemon=True).start()