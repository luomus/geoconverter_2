import os, sys
import tempfile
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import zipfile

sys.path.append('src/')
import gis_to_table

# run with:
# python -m pytest tests/test_gis_to_table.py -v

def test_read_gis_file_with_geojson():
    with tempfile.TemporaryDirectory() as tmp:
        path1 = os.path.join(tmp, "simple.geojson")
        path2 = os.path.join(tmp, "simple.gpkg")

        gdf = gpd.GeoDataFrame(
            {"name": ["X"]},
            geometry=[Point(5, 5)],
            crs="EPSG:4326"
        )
        gdf.to_file(path1, driver="GeoJSON")
        gdf.to_file(path2, driver="GPKG")

        result = gis_to_table.read_gis_file(path1)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 1

        result = gis_to_table.read_gis_file(path2)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 1

def test_gis_to_table_with_zip():
    with tempfile.TemporaryDirectory() as tmp:
        geojson_path = os.path.join(tmp, "data.geojson")

        gdf = gpd.GeoDataFrame(
            {"name": ["Z"]},
            geometry=[Point(1, 1)],
            crs="EPSG:4326"
        )
        gdf.to_file(geojson_path, driver="GeoJSON")

        zip_path = os.path.join(tmp, "test.zip")
        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.write(geojson_path, arcname="data.geojson")

        output_path = gis_to_table.gis_to_table(zip_path)
        assert os.path.exists(output_path)
        assert output_path.endswith(".csv")
        df = pd.read_csv(output_path)
        assert not df.empty
        assert "geometry_wkt" in df.columns