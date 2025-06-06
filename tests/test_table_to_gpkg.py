import sys
import os
from shapely.geometry import Point, LineString, Polygon, GeometryCollection, MultiPolygon
from unittest.mock import patch, MagicMock
import pandas as pd
import tempfile
from zipfile import ZipFile

sys.path.append('src/')
import table_to_gpkg

# run with:
# python -m pytest tests/test_table_to_gpkg.py -v

# Load the real lookup_table.csv for mapping/dtype/date tests
lookup_df = pd.read_csv("src/lookup_table.csv", sep=",", header=0, encoding='utf-8')

def test_safe_loads():
    assert table_to_gpkg.safe_loads("POINT (1 1)") == Point(1, 1)
    assert table_to_gpkg.safe_loads("INVALID WKT") is None

def test_get_column_mapping():
    mapping = table_to_gpkg.get_column_mapping(lookup_df, column_name="finbif_api_var")
    assert isinstance(mapping, dict)
    assert "Unit.UnitID" in mapping
    assert mapping["Unit.UnitID"] == "record_id"
    assert mapping["Unit.Sex"] == "sex"

def test_get_dtypes():
    dtypes = table_to_gpkg.get_dtypes(lookup_df)
    assert isinstance(dtypes, dict)
    assert "Unit.UnitOrder" in dtypes
    assert dtypes["Unit.UnitOrder"] == "int"

def test_convert_bool():
    assert table_to_gpkg.convert_bool("true") is True
    assert table_to_gpkg.convert_bool("1") is True
    assert table_to_gpkg.convert_bool("false") is False
    assert table_to_gpkg.convert_bool("0") is False
    assert table_to_gpkg.convert_bool("invalid") is None

def test_convert_int_with_na():
    assert table_to_gpkg.convert_int_with_na("123") == 123.0
    assert table_to_gpkg.convert_int_with_na("invalid") is None

def test_process_geometry():
    geom1 = GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])])
    geom2 = GeometryCollection([Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])])
    result1 = table_to_gpkg.process_geometry(geom1)
    result2 = table_to_gpkg.process_geometry(geom2)
    assert isinstance(result1, MultiPolygon)
    assert isinstance(result2, Polygon)
    assert result2.equals(Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]))

@patch("table_to_gpkg.write_dataframe")
def test_save_partition(mock_write_dataframe):
    df = pd.DataFrame({"geometry": [Point(0, 0)]})
    partition = MagicMock()
    partition.compute.return_value = df
    table_to_gpkg.save_partition(partition, "EPSG:3067", "dummy.gpkg", "points")
    table_to_gpkg.save_partition(partition, "EPSG:4326", "dummy2.gpkg", "bbox")
    assert mock_write_dataframe.call_count == 2

def test_read_tsv_to_ddf():
    test_tsv = """Unit.UnitID\tUnit.UnitOrder\tGathering.Conversions.WGS84_WKT\nid1\t1\tPOINT(1 1)"""
    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as tmp_file:
        tsv_path = tmp_file.name
        tmp_file.write(test_tsv.strip())

    dtype_dict = {
        "Unit.UnitID": "object",
        "Unit.UnitOrder": "int64",
        "Gathering.Conversions.WGS84_WKT": "object"
    }
    converters = {}
    wkt_column = "Gathering.Conversions.WGS84_WKT"

    ddf = table_to_gpkg.read_tsv_to_ddf(tsv_path, dtype_dict, converters, wkt_column)
    df = ddf.compute()
    assert "geometry" in df.columns
    assert "Unit.UnitID" in df.columns

def test_compress_gpkg_to_zip():
    with tempfile.TemporaryDirectory() as tmpdir:
        gpkg = os.path.join(tmpdir, "test.gpkg")
        with open(gpkg, "w") as f:
            f.write("dummy")
        table_to_gpkg.compress_gpkg_to_zip(gpkg)
        assert os.path.exists(gpkg.replace(".gpkg", ".zip"))
        assert not os.path.exists(gpkg)

@patch("table_to_gpkg.save_partition")
@patch("table_to_gpkg.read_tsv_to_ddf")
@patch("table_to_gpkg.compress_gpkg_to_zip")
@patch("os.path.exists", return_value=False)
def test_process_tsv_source(mock_exists, mock_compress, mock_read_tsv_to_ddf, mock_save_partition):
    ddf = MagicMock()
    ddf.rename.return_value = ddf
    ddf.__getitem__.return_value = ddf
    ddf.set_geometry.return_value = ddf
    ddf.columns = ["a", "b", "geometry"]
    ddf.to_delayed.return_value = [MagicMock()]
    mock_read_tsv_to_ddf.return_value = ddf

    table_to_gpkg.process_tsv_source(
        "dummy.tsv", "dummy.gpkg", "original", "EPSG:3067",
        {}, {}, [], "geometry", cleanup_temp=False, facts=None
    )

    mock_save_partition.assert_called_once()

def test_extract_and_process_facts():
    tsv = """Parent\tFact\tValue\tIntValue\tDecimalValue\n
    1\tA\tfoo\t\t\n
    1\tB\tbar\t\t\n
    2\tA\tbaz\t\t\n
    """
    columns = ["A", "B"]
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "facts.zip")
        with ZipFile(zip_path, "w") as zf:
            zf.writestr("facts.tsv", tsv)
        with ZipFile(zip_path, "r") as zf:
            facts = table_to_gpkg.extract_and_process_facts("facts.tsv", zf, columns)
            assert "A" in facts.columns
            assert "B" in facts.columns
            assert "foo" in facts["A"].values


@patch("table_to_gpkg.extract_and_process_facts")
def test_get_facts(mock_extract):
    mock_extract.side_effect = [
        pd.DataFrame({"Parent": [1], "Museo, johon lajista kerätty näyte on talletettu": ["foo"]}),
        pd.DataFrame({"Parent": [1], "Vesistöalue": ["bar"]}),
        pd.DataFrame({"Parent": [1], "Seurattava laji": ["baz"]}),
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "facts.zip")
        with ZipFile(zip_path, "w") as zf:
            zf.writestr("unit_facts_.tsv", "")
            zf.writestr("gathering_facts_.tsv", "")
            zf.writestr("document_facts_.tsv", "")
        facts = table_to_gpkg.get_facts(zip_path)
        print(facts)
        print(facts.columns)
        print(facts.info)
        assert "record_fact_Museo_johon_lajista_kerätty_näyte_on_talletettu" in facts.columns
        assert "event_fact_Vesistöalue" in facts.columns
        assert "baz" in facts["document_fact_Seurattava_laji"].values
        assert facts["document_fact_Seurattava_laji"].notnull().all()