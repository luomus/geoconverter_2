from fastapi import FastAPI, File, UploadFile, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse
import shutil
import tempfile
import os
from process_data_dask import zip_to_gpkg
from fastapi.middleware.cors import CORSMiddleware
import logging

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

@app.post("/convert/")
async def convert_tsv_to_gpkg(
    file: UploadFile = File(...),
    geom_type: str = Query("original", description="Geometry type: original, points, or bbox"),
    crs: str = Query("EPSG:3067", description="Coordinate Reference System (CRS)"),
    background_tasks: BackgroundTasks = None
):
    """ API endpoint to receive ZIP TSV file and return a GeoPackage. """
    
    logging.info(f"Received file: {file.filename}, geom_type: {geom_type}, crs: {crs}")
    
    # Create a temporary file to store the uploaded zip
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip:
        shutil.copyfileobj(file.file, temp_zip)
        temp_zip_path = temp_zip.name

    output_gpkg = tempfile.NamedTemporaryFile(delete=False, suffix=".gpkg").name

    try:
        zip_to_gpkg(temp_zip_path, output_gpkg, geom_type=geom_type, crs=crs)
        
        # Ensure the output file exists before returning it
        if not os.path.exists(output_gpkg):
            raise HTTPException(status_code=500, detail="Failed to generate GeoPackage file.")
        
        background_tasks.add_task(os.remove, temp_zip_path)
        background_tasks.add_task(os.remove, output_gpkg)

        return FileResponse(output_gpkg, filename="output.gpkg", media_type="application/geopackage+sqlite3")
    
    except Exception as e:
        logging.error(f"Error during conversion: {e}")
        raise HTTPException(status_code=500, detail=str(e))
