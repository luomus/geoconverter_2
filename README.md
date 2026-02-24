# GeoConverter 2

GeoConverter 2 is a comprehensive tool for converting geospatial data between different formats:
- **TSV to GeoPackage**: Converts TSV files (contained in ZIP archives) into GeoPackage format, specifically tailored for FinBIF data downloads
- **GIS to Table**: Converts various GIS formats (GeoJSON, GeoPackage, KML, GML, Shapefiles) into CSV format with WKT geometries

## Features

- Support for multiple input formats: ZIP archives with TSV data, GeoJSON, GeoPackage, KML, GML, Shapefiles
- Multiple geometry processing options: original footprints, centroids (points), or bounding boxes
- Coordinate system transformation support (WGS84, EUREF-FIN)
- Asynchronous processing for large files
- **Unique conversion IDs**: Each combination of file and parameters gets a unique ID to prevent conflicts
- Docker containerization for easy deployment
- RESTful API with status tracking

## API Endpoints

### `/convert-to-table` (POST)

**Description:** Converts GIS formats (`.geojson`, `.json`, `.gpkg`, `.kml`, `.gml`) or compressed GIS files (e.g. `.shp` inside a `.zip`) into a TSV file. The filename is inferred from the uploaded file.

**Response:**
- Returns converted CSV file, where the geometry is converted to WKT format. Uses field names suitable for FinBIF Data Bank / Vihko file upload.

**Example Request:**
```bash
curl -X 'POST' 'http://127.0.0.1:8000/convert-to-table' \
    -H "Accept: text/csv" \
    -H "Content-Type: multipart/form-data" \
    -F "file=@geopackage_in_zip.zip" \
    -o "output.csv"
```

*Note: The `Accept` header specifies the expected response format, while `Content-Type` is automatically set by curl when using `-F` for file uploads but is included here for clarity.*

### `/` (POST)

**Description:** Accepts either a ZIP file containing TSV data or a TSV file uploaded directly, and converts it into a zipped GeoPackage. The TSV (or TSV inside ZIP) should have the same schema and content as a downloadable file from FinBIF.

**Query parameters:**
- `lang`: Language of the headers - one of `fi`, `en`, or `tech`
- `geometryType`: Geometry type - one of `footprint`, `bbox`, or `point`
- `crs`: Coordinate Reference System (CRS) for output - either `wgs84` or `euref`

**Response:**
- Returns a plain text string with the conversion ID (Content-Type: `text/plain`).
- The conversion ID format ensures uniqueness: `{filename}_{lang}_{geometryType}_{crs}`
- For large files, processing runs in the background — check `/status/{id}` for progress. Small files are processed synchronously but the endpoint still returns the conversion ID.

**Example Request (ZIP):**
```bash
curl -X 'POST' 'http://127.0.0.1:8000/?lang=tech&geometryTypefootprint&crs=wgs84' \
    -H "Accept: text/plain" \
    -H "Content-Type: multipart/form-data" \
    -F "file=@HBF.12345.zip"
```

**Example Request (direct TSV):**
```bash
curl -X 'POST' 'http://127.0.0.1:8000/?lang=tech&geometryType=footprint&crs=wgs84' \
    -H "Accept: text/plain" \
    -H "Content-Type: multipart/form-data" \
    -F "file=@HBF.12345.tsv"
```

> Note: when uploading a plain TSV the service detects the file by extension or content-type (`text/tab-separated-values`) and processes it directly.

**Example Response:**
```
HBF.12345_tech_footprint_wgs84
```

You can then check status:
```bash
curl http://127.0.0.1:8000/status/HBF.12345_tech_footprint_wgs84
```

### `/{id}` (GET)

**Description:** Start conversion of a file identified by `id` that is stored in the data warehouse. The `id` is provided as a path parameter.

**Path parameters:**
- `id`: ID for file download on the FinBIF server (path parameter)

**Query parameters:**
- `lang`: Language of the headers - one of `fi`, `en`, or `tech`
- `geometryType`: Geometry type - one of `footprint`, `bbox`, or `point`
- `crs`: Coordinate Reference System (CRS) for output - either `wgs84` or `euref`
- `personToken`: Optional, user's personToken to check access rights for files with sensitive data

**Response:**
- Returns a plain text string with the conversion ID (Content-Type: `text/plain`). Conversion starts (or existing conversion/output ID is returned). Check `/status/{id}` for progress.

**Example Request:**
```bash
curl -X 'GET' 'http://127.0.0.1:8000/HBF.12345?lang=fi&geometryType=footprint&crs=wgs84&personToken=your_token_here' \
    -H "Accept: text/plain"
```

**Example Response:**
```
HBF.12345_fi_footprint_wgs84
```

### `/status/{id}` (GET)

**Description:** Gets the status of a file conversion

**Path parameters:**
- `id`: Conversion ID (includes parameters: `{filename}_{lang}_{geometryType}_{crs}`)
  
**Response:**
- Returns a JSON response describing the status of the given file conversion

**Example Request:**
```bash
curl -X 'GET' 'http://127.0.0.1:8000/status/HBF.12345_fi_footprint_wgs84' \
    -H "Accept: application/json"
```

### `/output/{id}` (GET)

**Description:** Downloads the converted file

**Path parameters:**
- `id`: Conversion ID (includes parameters: `{filename}_{lang}_{geometryType}_{crs}`)

**Query parameters:**
- `personToken`: Optional, user's personToken to check access right for the file with sensitive data.

**Response:**
- Returns a ZIP package containing the converted GPKG file

**Example Request:**
```bash
curl -X 'GET' 'http://127.0.0.1:8000/output/HBF.12345_fi_footprint_wgs84?personToken=your_token_here' \
    -o converted_HBF.12345.zip
```

### `/health` (GET)

**Description:** Health check endpoint to verify the service is running and check active conversions

**Response:**
- Returns service status and the number of conversions currently being processed

**Example Request:**
```bash
curl http://127.0.0.1:8000/health
```

**Example Response:**
```json
{
  "status": "ok",
  "processing": "2 conversions right now"
}
```

## Installation and Usage

### Docker Deployment (Recommended)

1. Clone the repository:
```bash
git clone https://github.com/luomus/geoconverter_2.git
cd geoconverter_2
```

2. Build and run with Docker:
   ```bash
   docker build -t geoconverter_2 .
   docker run -p 8000:8000 geoconverter_2
   ```

3. Test the API:
   ```bash
   curl -X 'POST' 'http://127.0.0.1:8000/?lang=fi&geometryType=footprint&crs=euref' \
       -H "Accept: application/json" \
       -H "Content-Type: multipart/form-data" \
       -F "file=@test_data/HBF.12345.zip"
   ```

   Or upload the TSV directly:
   ```bash
   curl -X 'POST' 'http://127.0.0.1:8000/?lang=fi&geometryType=footprint&crs=euref' \
       -H "Accept: application/json" \
       -H "Content-Type: multipart/form-data" \
       -F "file=@test_data/HBF.12345.tsv"
   ```

4. Check service health:
   ```bash
   curl http://127.0.0.1:8000/health
   ```

## How It Works

### Unique Conversion IDs

To prevent conflicts when converting the same file with different parameters, the system generates unique conversion IDs by combining the filename with the conversion parameters:

- **Format**: `{filename}_{language}_{geometryType}_{crs}`
- **Example**: `HBF.12345_fi_footprint_wgs84`

This ensures that:
- Multiple conversions of the same file with different parameters don't overwrite each other
- Each parameter combination has its own status tracking and output file
- Users can download different versions of the same dataset

### TSV to GeoPackage Conversion

1. **Input Handling**: The API accepts ZIP files containing TSV data from FinBIF downloads
2. **Data Processing**: 
   - Reads TSV files using Dask for efficient parallel processing of large datasets
   - Converts WKT (Well-Known Text) geometries to Shapely objects
   - Supports geometry transformations:
     - **Original**: Preserves original footprint geometries
     - **Point**: Converts to centroid points
     - **Bbox**: Converts to bounding box polygons
   - Handles coordinate system transformations (WGS84 ↔ EUREF-FIN)
   - Processes data in partitions for memory efficiency
3. **Output**: Writes processed data to a Zipped GeoPackage format using PyOGRIO. Other files in the zip folder remains the same.

### GIS to Table Conversion

1. **Input Handling**: Accepts various GIS formats (GeoJSON, GeoPackage, KML, GML, Shapefiles in ZIP)
2. **Processing**: Uses GeoPandas to read and process spatial data
3. **Output**: Converts geometries to WKT format and exports as CSV

## Architecture

- **FastAPI**: RESTful API framework with async support
- **Dask**: Parallel computing for processing large datasets
- **GeoPandas/Shapely**: Geospatial data manipulation
- **PyOGRIO**: High-performance I/O for geospatial formats
- **Docker**: Containerized deployment

## Requirements

See `requirements.txt` for complete dependency list. Key dependencies include:
- FastAPI 0.115+
- Dask 2025.2+ and Dask-GeoPandas 0.5+
- GeoPandas 1.1+
- Shapely 2.1+
- PyOGRIO 0.7+
- Pandas 2.3+
- Pydantic Settings 2.9+

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
