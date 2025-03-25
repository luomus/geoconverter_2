# GeoConverter 2

GeoConverter 2 is a tool for converting geospatial data from TSV files (contained in ZIP archives) into GeoPackage format. It is tailored specifically for TSV files from FinBIF data downloads.

## API Endpoints

### `/convert/` (POST)

**Description:** Converts a ZIP file containing TSV data into a GeoPackage. Should have the same schema and contant as in downloadable file from FinBIF.

**Parameters:**
- `file` (UploadFile): The ZIP file containing TSV file that has `rows` in its name.
- `geom_type` (Query, optional): Geometry type to use (`original`, `points`, or `bbox`). Default is `original`.
- `crs` (Query, optional): Coordinate Reference System (CRS) for the output. Can be any EPSG code. Default is `EPSG:3067`.

**Response:**
- Returns the generated GeoPackage file.

**Example Request:**
```bash
$ curl 
    -X 'POST' 'http://127.0.0.1:8000/convert/?geom_type=original&crs=EPSG:3067' 
    -H "accept: application/json" 
    -H "Content-Type: multipart/form-data" 
    -F "file=@input.zip" 
    -o output.gpkg
```

## Local Installation

1. Clone the repository:
 ```bash
 git clone https://github.com/your-repo/geoconverter_2.git
 cd geoconverter_2
 ```

2. Run in a Docker
```docker build -t geoconverter_2 .
docker run -p 8000:8000 --memory=4g geoconveter_2
```

3. Make a POST curl to ensure it is working on the localhost:
```
$ curl -X 'POST' 'http://127.0.0.1:8000/convert/?geom_type=ORIGINAL&crs=EPSG:3067' -H "accept: application/json" -H "Content-Type: multipart/form-data" -F "file=@input.zip" -o output.gpkg
```

or locally without the API, edit the last row of `process_data_dask.py` and run it.
```
zip_to_gpkg("test_data/test.zip", f"output.gpkg", geom_type="original", crs="EPSG:3067")
```

## How It Works

1. **Input Handling:** The API accepts a ZIP file containing TSV files.
2. **Data Processing:** 
   - Reads TSV files using Dask for parallel processing.
   - Converts geometries from WKT format to Shapely objects.
   - Supports geometry transformations (centroids, bounding boxes) when required.
   - Handles column mapping and type conversions based on a lookup table (`lookup_table.csv`).
3. **Output:** Writes the processed data to a GeoPackage file using Pyogrio.

## Requirements

See the `requirements.txt` file.
