"""
Test suite for helpers module.

Run with: python -m pytest tests/test_helpers.py -v
"""

import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString, MultiPoint, MultiPolygon, MultiLineString, GeometryCollection
from shapely.wkt import loads
import tempfile
import os
import sys
from unittest.mock import MagicMock

class MockSettings:
    LOGGING = "INFO"

mock_settings = MagicMock()
mock_settings.Settings.return_value = MockSettings()
sys.modules['settings'] = mock_settings

sys.path.append('src/')
from helpers import (
    safely_parse_wkt,
    convert_boolean_value,
    convert_numeric_with_na,
    normalize_geometry_collection,
    buffer_and_dissolve_mixed_geometries,
    apply_geometry_transformation,
    get_default_column_types,
    get_converters
)

def test_safely_parse_wkt():
    """Test safely_parse_wkt with various inputs."""
    # Valid WKT string
    wkt = "POINT(10 20)"
    result = safely_parse_wkt(wkt)
    assert result is not None
    assert isinstance(result, Point)
    assert result.x == 10
    assert result.y == 20
    
    # Invalid WKT string
    wkt = "INVALID WKT STRING"
    result = safely_parse_wkt(wkt)
    assert result is None
    
    # Empty geometry
    wkt = ''
    result = safely_parse_wkt(wkt)
    assert result is None


def test_convert_boolean_value():
    """Test convert_boolean_value with various inputs."""
    # True values
    assert convert_boolean_value("true") is True
    assert convert_boolean_value("TRUE") is True
    assert convert_boolean_value("1") is True
    
    # False values
    assert convert_boolean_value("false") is False
    assert convert_boolean_value("FALSE") is False
    assert convert_boolean_value("0") is False
    
    # Invalid values
    assert convert_boolean_value("maybe") is None
    assert convert_boolean_value("2") is None
    assert convert_boolean_value("") is None
    
    # Non-string input (converted to string)
    assert convert_boolean_value(str(123)) is None


def test_convert_numeric_with_na():
    """Test convert_numeric_with_na with various inputs."""
    # Valid numeric strings
    assert convert_numeric_with_na("123") == 123.0
    assert convert_numeric_with_na("123.45") == 123.45
    assert convert_numeric_with_na("-10") == -10.0
    
    # Invalid strings
    assert convert_numeric_with_na("not_a_number") is None
    assert convert_numeric_with_na("") is None


def test_normalize_geometry_collection():
    """Test normalize_geometry_collection with various geometry collections."""
    # Single point collection
    point = Point(10, 20)
    collection = GeometryCollection([point])
    result = normalize_geometry_collection(collection)
    assert isinstance(result, Point)
    assert result.equals(point)
    
    # Multiple points
    points = [Point(0, 0), Point(1, 1), Point(2, 2)]
    collection = GeometryCollection(points)
    result = normalize_geometry_collection(collection)
    assert isinstance(result, MultiPoint)
    
    # Multiple polygons
    poly1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    poly2 = Polygon([(2, 2), (3, 2), (3, 3), (2, 3)])
    collection = GeometryCollection([poly1, poly2])
    result = normalize_geometry_collection(collection)
    assert isinstance(result, MultiPolygon)
    
    # Non-collection geometry (should return as-is)
    point = Point(10, 20)
    result = normalize_geometry_collection(point)
    assert result is point


def test_buffer_and_dissolve_mixed_geometries():
    """Test buffer_and_dissolve_mixed_geometries with mixed geometry types."""
    point = Point(0, 0)
    line = LineString([(1, 1), (2, 2)])
    polygon = Polygon([(3, 3), (4, 3), (4, 4), (3, 4)])
    
    geometries = [point, line, polygon]
    result = buffer_and_dissolve_mixed_geometries(geometries)
    
    # Should return some form of polygon/multipolygon
    assert hasattr(result, 'geom_type')
    assert result.geom_type in ['Polygon', 'MultiPolygon']
    assert result.area > 1


def test_apply_geometry_transformation():
    """Test apply_geometry_transformation with different transformation types."""
    polygon = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    gdf = gpd.GeoDataFrame({'id': [1]}, geometry=[polygon])
    
    # Test points transformation
    result = apply_geometry_transformation(gdf, "points")
    geom = result.geometry.iloc[0]
    assert isinstance(geom, Point)
    # Centroid of the square should be at (1, 1)
    assert geom.x == 1.0
    assert geom.y == 1.0
    
    # Test bbox transformation
    complex_polygon = Polygon([(0, 0), (2, 1), (1, 2), (0, 1)])
    gdf_complex = gpd.GeoDataFrame({'id': [1]}, geometry=[complex_polygon])
    result = apply_geometry_transformation(gdf_complex, "bbox")
    geom = result.geometry.iloc[0]
    assert isinstance(geom, Polygon)
    bounds = geom.bounds
    assert bounds == (0.0, 0.0, 2.0, 2.0)
    
    # Test original transformation
    result = apply_geometry_transformation(gdf, "original")
    assert isinstance(result.geometry.iloc[0], Polygon)
    assert len(result) == 1


def test_get_default_column_types():
    """Test get_default_column_types with different languages."""
    # Create a temporary lookup table file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
        f.write("tech\tfi\ten\tdtype\n")
        f.write("id\ttunniste\tid\tint\n")
        f.write("name\tnimi\tname\tstr\n")
        f.write("active\taktiivinen\tactive\tbool\n")
        temp_file = f.name
    
    try:
        # Test English
        result = get_default_column_types("en", temp_file)
        expected = {
            "id": "int",
            "name": "str", 
            "active": "bool"
        }
        assert result == expected
        
        # Test Finnish
        result = get_default_column_types("fi", temp_file)
        expected = {
            "tunniste": "int",
            "nimi": "str",
            "aktiivinen": "bool"
        }
        assert result == expected
        
    finally:
        # Clean up temporary file
        os.unlink(temp_file)


def test_get_converters():
    """Test get_converters function with different column types."""
    column_types = {
        "id": "int",
        "Suomenkielinen_nimi": "str", 
        "Sensitiivinen laji": "bool",
        "Maara": "int",
        "Muuttaja": "bool",
        "Maarittaja": "str"
    }
    
    result = get_converters(column_types)
    
    # Should have converters for bool and int columns only
    expected_keys = {"Sensitiivinen laji", "Muuttaja", "id", "Maara"}
    assert set(result.keys()) == expected_keys
    
    # Check that bool columns get convert_boolean_value
    assert result["Sensitiivinen laji"] == convert_boolean_value
    assert result["Muuttaja"] == convert_boolean_value
    
    # Check that int columns get convert_numeric_with_na
    assert result["id"] == convert_numeric_with_na
    assert result["Maara"] == convert_numeric_with_na

    # str columns should not have converters
    assert "Suomenkielinen_nimi" not in result
    assert "Maarittaja" not in result
