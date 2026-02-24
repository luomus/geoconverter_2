from functools import lru_cache
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Query, Path
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
import shutil
import tempfile
import os
import time
from table_to_gpkg import handle_zip_conversion_request, handle_tsv_conversion_request
from dw_service import is_valid_download_request
from fastapi.middleware.cors import CORSMiddleware
import logging
from typing import Literal, Optional
import settings
from gis_to_table import gis_to_table
from email_notifications import notify_failure

# Pydantic models for API responses
class StatusResponse(BaseModel):
    id: str = Field(..., description="Unique conversion identifier", examples=["dataset123_tech_point_wgs84"])
    status: Literal["processing", "complete", "failed"] = Field(..., description="Current conversion status", examples=["processing"])
    progress_percent: int = Field(..., description="Completion percentage (0-100)", examples=[45], ge=0, le=100)

class HealthResponse(BaseModel):
    status: str = Field(..., description="Service health status", examples=["ok"])
    processing: str = Field(..., description="Number of active conversions", examples=["2 conversions right now"])

class ErrorResponse(BaseModel):
    detail: str = Field(..., description="Error message", examples=["Conversion ID not found"])

# Get settings and configure logging
app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)

# Configure logging
logging.basicConfig(level=log_level)

app = FastAPI(
    title="Geoconverter API",
    description="A service for converting between GIS formats and tabular data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

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

@app.post("/convert-to-table",
    summary="Convert GIS file to CSV table",
    description="Upload a GIS file (Shapefile, GeoJSON, GPKG, etc.) and get back a CSV file with geometry as WKT",
    tags=["File Conversion"],
    responses={
        200: {"description": "CSV file with converted data", "content": {"text/csv": {}}},
        400: {"model": ErrorResponse, "description": "Unsupported file type"},
        500: {"model": ErrorResponse, "description": "Conversion failed"}
    }
)
async def convert_gis_to_table(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="GIS file to convert (SHP, GeoJSON, GPKG, KML, GML, ZIP)")
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
        
        # Send email notification for API failure
        notify_failure(f"API Error in /convert-to-table: {str(e)}")
        
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/",
    summary="Convert uploaded ZIP file to a zipped GeoPackage",
    description="Upload a ZIP file containing TSV data ('occurrences.tsv') and convert it to a zipped GeoPackage format. ID is generated based on the original filename and parameters.",
    tags=["File Conversion"],
    response_model=str,
    responses={
        200: {"description": "Conversion started successfully. Returns the conversion ID string.", "content": {"text/plain": {"example": "dataset123_tech_point_wgs84"}}},
        400: {"model": ErrorResponse, "description": "Invalid file or parameters"},
        500: {"model": ErrorResponse, "description": "Conversion failed"}
    }
)
async def convert_with_file(
    background_tasks: BackgroundTasks,
    lang: Literal["fi", "en", "tech"] = Query("tech", description="Language for field names (fi=Finnish, en=English, tech=technical)"),
    geometryType: Literal["bbox", "point", "footprint"] = Query(..., description="Geometry type to use"),
    crs: Literal["euref","wgs84"] = Query(..., description="Coordinate reference system"),
    file: UploadFile = File(..., description="ZIP file containing TSV data")
):
    """API endpoint to receive ZIP TSV file and return a GeoPackage."""

    logging.info(f"Received file: {file.filename}, content-type: {file.content_type}, language: {lang}, geometryType: {geometryType}, crs: {crs}")

    base_id = os.path.splitext(file.filename)[0]
    id = f"{base_id}_{lang}_{geometryType}_{crs}"

    # Check if TSV file by extension or content type
    is_tsv = (file.filename.lower().endswith('.tsv') or 
              file.content_type == "text/tab-separated-values")

    if is_tsv:
        # Process TSV directly (simple data download)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".tsv") as temp_tsv:
            shutil.copyfileobj(file.file, temp_tsv)
            temp_tsv_path = temp_tsv.name
        
        return handle_tsv_conversion_request(id, temp_tsv_path, lang, geometryType, crs, background_tasks, original_filename=base_id)

    else:
        # Process zip files (citable data download)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip:
            shutil.copyfileobj(file.file, temp_zip)
            temp_zip_path = temp_zip.name

        return handle_zip_conversion_request(id, temp_zip_path, lang, geometryType, crs, background_tasks, True, original_filename=base_id)

@app.get("/status/{id}",
    summary="Check conversion status",
    description="Get the current status of a file conversion process",
    tags=["Status"],
    response_model=StatusResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Conversion ID not found"},
        500: {"model": ErrorResponse, "description": "Conversion failed"}
    }
)
async def get_status(id: str = Path(..., description="Conversion ID to check")):
    """ Endpoint to check the status of a conversion. """
    logging.info(f"Checking status for conversion ID: {id}")
    from helpers import status_manager
    
    if not status_manager.has(id):
        raise HTTPException(status_code=404, detail="Conversion ID not found.")
    
    status = status_manager.get(id)
    
    response_data = {
        "id": id,
        "status": status["status"],
        "progress_percent": status.get("progress_percent", 0)
    }
    
    # Return 500 status code if conversion failed
    if status["status"] == "failed":
        return JSONResponse(content=response_data, status_code=500)
    
    return response_data
    
@app.get("/health",
    summary="Health check",
    description="Verify that the service is running and healthy. Returns processing status of active conversions.",
    tags=["System"],
    response_model=HealthResponse
)
async def health_check():
    """ Health check endpoint to verify the service is running."""
    from helpers import status_manager
    
    all_statuses = status_manager.get_all()
    processing_count = sum(s["status"] == "processing" for s in all_statuses.values())
    
    return {
        "status": "ok",
        "processing": f"{processing_count} conversions right now"
    }

@app.get("/output/{id}",
    summary="Download conversion output",
    description="Download the converted file for a completed conversion",
    tags=["File Download"],
    responses={
        200: {"description": "Output file", "content": {"application/zip": {}}},
        400: {"model": ErrorResponse, "description": "Conversion not completed"},
        403: {"model": ErrorResponse, "description": "Permission denied"},
        404: {"model": ErrorResponse, "description": "Conversion ID or output file not found"}
    }
)
async def get_output(
    id: str = Path(..., description="Conversion ID"),
    personToken: Optional[str] = Query(None, description="Authentication token for private data")
):
    """ Endpoint to retrieve the output file for a completed conversion. TODO: Ensure which id should be in use?"""
    logging.info(f"Retrieving output for conversion ID: {id}")

    # Get the original base ID without language/geometryType/crs suffixes
    if '_fi_' in id:
        base_id = id.split('_fi_')[0]
    elif '_en_' in id:
        base_id = id.split('_en_')[0]
    elif '_tech_' in id:
        base_id = id.split('_tech_')[0]
    else:
        base_id = id

    from helpers import status_manager
    
    if not status_manager.has(id):
        raise HTTPException(status_code=404, detail="Conversion ID not found.")
    
    status = status_manager.get(id)
    if status["status"] != "complete":
        raise HTTPException(status_code=400, detail="Conversion not completed yet.")
    output_path = status["output"]
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Output file not found.")
    uploaded_file = status['uploaded_file']
    if not uploaded_file and not is_valid_download_request(base_id, personToken):
        raise HTTPException(status_code=403, detail="Permission denied.")
    
    # Use original filename if available, otherwise use the conversion ID
    original_filename = status.get("original_filename", base_id)
    logging.debug(f"Download request - ID: {id}, base_id: {base_id}, original_filename from status: {original_filename}, uploaded_file: {uploaded_file}")
    return FileResponse(output_path, filename=f"{original_filename}.zip", media_type="application/zip")

@app.get("/{id}",
    summary="Convert TSV file from the data warehouse to a zipped GeoPackage",
    description="Convert a TSV file that is stored in the data warehouse to a zipped GeoPackage format",
    tags=["File Conversion"],
    responses={
        200: {"description": "Conversion started successfully. Returns the conversion ID string.", "content": {"text/plain": {"example": "dataset123_tech_point_wgs84"}}},
        403: {"model": ErrorResponse, "description": "Permission denied"},
        404: {"model": ErrorResponse, "description": "File not found in data warehouse"},
        500: {"model": ErrorResponse, "description": "Conversion failed"}
    }
)
async def convert_with_id(
    background_tasks: BackgroundTasks,
    id: str = Path(..., description="ID of the file in the data warehouse"),
    lang: Literal["fi", "en", "tech"] = Query("tech", description="Language for field names (fi=Finnish, en=English, tech=technical)"),
    geometryType: Literal["bbox", "point", "footprint"] = Query(..., description="Geometry type to use"),
    crs: Literal["euref","wgs84"] = Query(..., description="Coordinate reference system"),
    personToken: Optional[str] = Query(None, description="Authentication token for private data")
):
    """API enpoint to start converting file stored in dw"""

    logging.info(f"Received request to convert ID: {id}, lang: {lang}, geometryType: {geometryType}, crs: {crs}")

    if not is_valid_download_request(id, personToken):
      raise HTTPException(status_code=403, detail="Permission denied.")

    # Create unique conversion ID with parameters
    conversion_id = f"{id}_{lang}_{geometryType}_{crs}"
    zip_path = get_settings().FILE_PATH + id + ".zip"
    return handle_zip_conversion_request(conversion_id, zip_path, lang, geometryType, crs, background_tasks, False, original_filename=id)

@app.on_event("startup")
async def cleanup_old_files():
    """ Periodically clean up files older than 24 hours. """
    def cleanup():
        logging.info("Starting cleanup thread...")
        while True:
            time.sleep(3600)  # Run cleanup every hour
            from helpers import status_manager
            
            output_path = get_settings().OUTPUT_PATH
            if not os.path.exists(output_path):
                logging.warning(f"Output path does not exist: {output_path}")
                continue
            
            current_time = time.time()
            
            # Scan output folder for old files
            for filename in os.listdir(output_path):
                file_path = os.path.join(output_path, filename)
                if not os.path.isfile(file_path):
                    continue
                
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > 86400: # 24 hours
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted old file: {filename} (age: {file_age / 3600:.1f} hours)")
                        
                        # Also remove from status_manager if present
                        conversion_id = os.path.splitext(filename)[0]
                        if status_manager.has(conversion_id):
                            status_manager.remove(conversion_id)
                            logging.info(f"Removed {conversion_id} from status tracking")
                    except Exception as e:
                        logging.error(f"Failed to delete file {filename}: {e}")
    
    import threading
    threading.Thread(target=cleanup, daemon=True).start()
