"""
Test suite for gis_to_table module.

Run with: python -m pytest tests/test_gis_to_table.py -v
"""

import os
import sys
import tempfile
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import zipfile
from unittest.mock import MagicMock

# Mock settings before importing the module
class MockSettings:
    LOGGING = "INFO"

mock_settings = MagicMock()
mock_settings.Settings.return_value = MockSettings()
sys.modules['settings'] = mock_settings

# Add src to path and import
sys.path.insert(0, 'src/')
import gis_to_table

def test_read_file_with_encoding_basic():
    """Test basic functionality of read_file_with_encoding with different formats."""
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

        result = gis_to_table.read_file_with_encoding(path1)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 1
        assert result['name'].iloc[0] == "X"

        result = gis_to_table.read_file_with_encoding(path2)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 1
        assert result['name'].iloc[0] == "X"

def test_read_multilayer_file():
    """Test reading multi-layer files like GPKG."""
    with tempfile.TemporaryDirectory() as tmp:
        gpkg_path = os.path.join(tmp, "multi.gpkg")
        
        # Create a multi-layer GPKG
        gdf1 = gpd.GeoDataFrame({"name": ["Layer1"]}, geometry=[Point(1, 1)], crs="EPSG:4326")
        gdf2 = gpd.GeoDataFrame({"name": ["Layer2"]}, geometry=[Point(2, 2)], crs="EPSG:4326")
        
        gdf1.to_file(gpkg_path, layer="layer1", driver="GPKG")
        gdf2.to_file(gpkg_path, layer="layer2", driver="GPKG", mode="a")
        
        result = gis_to_table.read_multilayer_file(gpkg_path)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 2
        assert "Layer1" in result['name'].values
        assert "Layer2" in result['name'].values


def test_gis_to_table_conversion():
    """Test complete GIS to table conversion."""
    with tempfile.TemporaryDirectory() as tmp:
        geojson_path = os.path.join(tmp, "data.geojson")

        gdf = gpd.GeoDataFrame(
            {"name": ["TestPoint"], "value": [42]},
            geometry=[Point(1, 1)],
            crs="EPSG:4326"
        )
        gdf.to_file(geojson_path, driver="GeoJSON")

        output_path = gis_to_table.gis_to_table(geojson_path)
        assert os.path.exists(output_path)
        assert output_path.endswith(".csv")
        
        df = pd.read_csv(output_path)
        assert not df.empty
        assert "geometry_wkt" in df.columns
        assert "crs" in df.columns
        assert df["name"].iloc[0] == "TestPoint"
        assert df["value"].iloc[0] == 42
        assert "POINT" in df["geometry_wkt"].iloc[0]


def test_gis_to_table_zip_conversion():
    """Test complete GIS to table conversion with ZIP archive."""
    with tempfile.TemporaryDirectory() as tmp:
        geojson_path = os.path.join(tmp, "data.geojson")

        gdf = gpd.GeoDataFrame(
            {"name": ["ZipTest"]},
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
        assert "crs" in df.columns
        assert df["name"].iloc[0] == "ZipTest"