from fastapi import FastAPI, File, UploadFile, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, RedirectResponse
import shutil
import tempfile
import os
import time
from process_data_dask import zip_to_gpkg
from fastapi.middleware.cors import CORSMiddleware
import logging
from threading import Lock

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

@app.post("/convert/")
async def convert_tsv_to_gpkg(
    file: UploadFile = File(...),
    geom_type: str = Query("original", description="Geometry type: original, points, or bbox"),
    crs: str = Query("EPSG:3067", description="Coordinate Reference System (CRS)"),
    background_tasks: BackgroundTasks = None
):
    """API endpoint to receive ZIP TSV file and return a GeoPackage."""
    
    logging.info(f"Received file: {file.filename}, geom_type: {geom_type}, crs: {crs}")
    
    # Create a temporary file to store the uploaded zip
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip:
        shutil.copyfileobj(file.file, temp_zip)
        temp_zip_path = temp_zip.name

    file_size = os.path.getsize(temp_zip_path)
    output_gpkg = tempfile.NamedTemporaryFile(delete=False, suffix=".gpkg").name
    conversion_id = file.filename

    def process_file():
        """Handles file processing."""
        try:
            zip_to_gpkg(temp_zip_path, output_gpkg, geom_type=geom_type, crs=crs)
            with status_lock:
                conversion_status[conversion_id] = {
                    "status": "completed",
                    "output": output_gpkg,
                    "timestamp": time.time(),
                }
        except Exception as e:
            logging.error(f"Error during conversion: {e}")
            with status_lock:
                conversion_status[conversion_id] = {"status": "failed"}
        finally:
            os.remove(temp_zip_path)

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
            }
        background_tasks.add_task(process_file)
        return {
            "id": conversion_id,
            "status": "processing",
            "status_url": f"/status/{conversion_id}",
            "download_url": f"/output/{conversion_id}"
        }

@app.get("/status/{id}")
async def get_status(id: str):
    """ Endpoint to check the status of a conversion. """
    logging.info(f"Checking status for conversion ID: {id}")
    with status_lock:
        if id not in conversion_status:
            raise HTTPException(status_code=404, detail="Conversion ID not found.")
        status = conversion_status[id]
        if status["status"] == "completed":
            return RedirectResponse(url=f"/output/{id}")
        return {"status": status["status"]}

@app.get("/output/{id}")
async def get_output(id: str):
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