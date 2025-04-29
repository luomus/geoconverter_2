# GeoConverter 2

GeoConverter 2 is a tool for converting geospatial data from TSV files (contained in ZIP archives) into GeoPackage format. It is tailored specifically for TSV files from FinBIF data downloads.

## API Endpoints

### `/convert/{id}/{fmt}/{geo}/{crs}` (POST)

**Description:** Converts a ZIP file containing TSV data into a GeoPackage. Should have the same schema and contant as in downloadable file from FinBIF.

**Path parameters:**
- `id`: name for converted file.
- `fmt`: Output format, only acceptable value `gpkg`.
- `geo`: Gemetry type, one of `footprint`, `bbox` or `point`.
- `crs`: Coordinate Reference System (CRS) for output. Either `wgs84` or `euref`.

**Response:**
- Returns an JSON describing the status of conversion and path to download the output from when conversion is ready.

**Example Requests:**
```bash
$ curl 
    -X 'POST' 'http://127.0.0.1:8000/convert/<ID>/gpkg/footprint/wgs84' 
    -H "Accept: application/json" 
    -H "Content-Type: multipart/form-data" 
    -F "file=@input.zip" 
```

### `/convert/{id}/{fmt}/{geo}/{crs}` (GET)

**Description:** Converts a download by id from files on the server.

**Path parameters:**
- `id`: ID for file download on the server.
- `fmt`: Output format, only acceptable value `gpkg`.
- `geo`: Gemetry type, one of `footprint`, `bbox` or `point`.
- `crs`: Coordinate Reference System (CRS) for output. Either `wgs84` or `euref`.

**Query parameters:**
- `personToken`: Optional, users personToken to check access right to the file with sensitive data.

**Response:**
- Returns an JSON describing the status of conversion and path to download the output from when conversion is ready.

**Example Requests:**
```bash
$ curl 
    -X 'GET' 'http://127.0.0.1:8000/convert/<ID>/gpkg/footprint/wgs84?personToken=<personToken>' 
    -H "Accept: application/json" 
```

### `/status/{id}` (GET)

**Description:** Gets the status of a file conversion

**Path parameters:**
- `id`: ID for file under conversion
  
**Response:**
- Returns an JSON dscribing the status of the given file conversion

**Example Requests:**
```bash
$ curl 
    -X 'GET' 'http://127.0.0.1:8000/status/<ID> 
    -H "Accept: application/json" 
```

### `/output/{id}` (GET)

**Description:** Gets the converted file

**Path parameters:**
- `id`: ID for converted file

**Query parameters:**
- `personToken`: Optional, users personToken to check access right to the file with sensitive data.

**Response:**
- Returns a .zip package containing the converted gpkg-file.

**Example Requests:**
```bash
$ curl 
    -X 'GET' 'http://127.0.0.1:8000/output/<ID>?personToken=<personToken> 
```

## Local Installation

1. Clone the repository:
 ```bash
 git clone https://github.com/your-repo/geoconverter_2.git
 cd geoconverter_2
 ```

2. Run in a Docker
```docker build -t geoconverter_2 .
docker run -p 8000:8000 geoconverter_2
```

3. Make a POST curl to ensure it is working on the localhost:
```
$ curl -X 'POST' 'http://127.0.0.1:8000/convert/output/gpkg/footprint/euref' -H "Accept: application/json" -H "Content-Type: multipart/form-data" -F "file=@input.zip" -o output.gpkg
```

or locally without the API, edit the last row of `process_data_dask.py` and run it.
```
tsv_to_gpkg("test_data/test.zip", f"output.gpkg", geom_type="original", crs="EPSG:3067")
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
