import warnings
import unittest
import sys
import os
from shapely.geometry import Point, LineString, Polygon, GeometryCollection, MultiPolygon
from unittest.mock import patch, MagicMock
import pandas as pd
import tempfile

sys.path.append('src/')
from process_data_dask import *

class TestProcessDataDask(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load the real lookup_table.csv for mapping/dtype/date tests
        cls.lookup_df = pd.read_csv("src/lookup_table.csv", sep=",", header=0, encoding='utf-8')

    def test_safe_loads(self):
        self.assertEqual(safe_loads("POINT (1 1)"), Point(1, 1))
        self.assertIsNone(safe_loads("INVALID WKT"))

    def test_get_column_mapping(self):
        mapping = get_column_mapping(self.lookup_df, column_name="finbif_api_var")
        self.assertIsInstance(mapping, dict)
        self.assertIn("Unit.UnitID", mapping)
        self.assertEqual(mapping["Unit.UnitID"], "record_id")
        self.assertEqual(mapping["Unit.Sex"], "sex")

    def test_get_dtypes(self):
        dtypes = get_dtypes(self.lookup_df)
        self.assertIsInstance(dtypes, dict)
        self.assertIn("Unit.UnitOrder", dtypes)
        self.assertEqual(dtypes["Unit.UnitOrder"], "int")

    def test_convert_bool(self):
        self.assertTrue(convert_bool("true"))
        self.assertTrue(convert_bool("1"))
        self.assertFalse(convert_bool("false"))
        self.assertFalse(convert_bool("0"))
        self.assertIsNone(convert_bool("invalid"))

    def test_convert_int_with_na(self):
        self.assertEqual(convert_int_with_na("123"), 123.0)
        self.assertIsNone(convert_int_with_na("invalid"))

    def test_process_geometry(self):
        geom1 = GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])])
        geom2 = GeometryCollection([Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])])
        result1 = process_geometry(geom1)
        result2 = process_geometry(geom2)
        self.assertIsInstance(result1, MultiPolygon)
        self.assertIsInstance(result2, Polygon)
        self.assertEqual(result2, Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]))

    @patch("process_data_dask.write_dataframe")
    def test_save_partition(self, mock_write_dataframe):
        df = pd.DataFrame({"geometry": [Point(0, 0)]})
        partition = MagicMock()
        partition.compute.return_value = df
        save_partition(partition, "EPSG:3067", "dummy.gpkg", "points")
        save_partition(partition, "EPSG:4326", "dummy2.gpkg", "bbox")
        self.assertEqual(mock_write_dataframe.call_count, 2)

    def test_read_tsv_to_ddf(self):
        # Use a real TSV file to test reading and geometry parsing
        test_tsv = """Unit.UnitID\tUnit.UnitOrder\tGathering.Conversions.WGS84_WKT\nid1\t1\tPOINT(1 1)"""
        
        # Create a temporary TSV file
        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as tmp_file:
            tsv_path = tmp_file.name
            tmp_file.write(test_tsv.strip())
            
        # Minimal dtype/converter setup for a few columns
        dtype_dict = {
            "Unit.UnitID": "object",
            "Unit.UnitOrder": "int64",
            "Gathering.Conversions.WGS84_WKT": "object"
        }
        converters = {}
        wkt_column = "Gathering.Conversions.WGS84_WKT"
        # Call the function
        ddf = read_tsv_to_ddf(tsv_path, dtype_dict, converters, wkt_column)
        # Compute a small sample to test
        df = ddf.compute()
        self.assertIn("geometry", df.columns)

    def test_compress_gpkg_to_zip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gpkg = f"{tmpdir}/test.gpkg"
            with open(gpkg, "w") as f:
                f.write("dummy")
            compress_gpkg_to_zip(gpkg)
            self.assertTrue(os.path.exists(gpkg.replace(".gpkg", ".zip")))
            self.assertFalse(os.path.exists(gpkg))

    def test_extract_and_process_facts(self):
        import zipfile
        tsv = "Parent\tFact\tValue\tIntValue\tDecimalValue\n1\tA\tfoo\t\t\n1\tB\tbar\t\t\n2\tA\tbaz\t\t\n"
        columns = ["A", "B"]
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = f"{tmpdir}/facts.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("facts.tsv", tsv)
            with zipfile.ZipFile(zip_path, "r") as zf:
                facts = extract_and_process_facts("facts.tsv", zf, columns)
                self.assertIn("A", facts.columns)
                self.assertIn("B", facts.columns)
                self.assertIn("Parent", facts.columns)

    @patch("process_data_dask.extract_and_process_facts")
    def test_get_facts(self, mock_extract):
        mock_extract.side_effect = [
            pd.DataFrame({"Parent": [1], "Museo, johon lajista kerätty näyte on talletettu": ["foo"]}),
            pd.DataFrame({"Parent": [1], "Vesistöalue": ["bar"]}),
            pd.DataFrame({"Parent": [1], "Seurattava laji": ["baz"]}),
        ]
        import zipfile
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = f"{tmpdir}/facts.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("unit_facts_.tsv", "")
                zf.writestr("gathering_facts_.tsv", "")
                zf.writestr("document_facts_.tsv", "")
            facts = get_facts(zip_path)
            self.assertIn("record_fact_Museo_johon_lajista_kerätty_näyte_on_talletettu", facts.columns)
            self.assertIn("event_fact_Vesistöalue", facts.columns)
            self.assertIn("document_fact_Seurattava_laji", facts.columns)
            self.assertTrue(facts["document_fact_Seurattava_laji"].notnull().all())

    @patch("process_data_dask.save_partition")
    @patch("process_data_dask.read_tsv_to_ddf")
    @patch("process_data_dask.compress_gpkg_to_zip")
    @patch("os.path.exists", return_value=False)
    def test_process_tsv_source(self, mock_exists, mock_compress, mock_read_tsv_to_ddf, mock_save_partition):
        ddf = MagicMock()
        ddf.rename.return_value = ddf
        ddf.__getitem__.return_value = ddf
        ddf.set_geometry.return_value = ddf
        ddf.columns = ["a", "b", "geometry"]
        ddf.to_delayed.return_value = [MagicMock()]
        mock_read_tsv_to_ddf.return_value = ddf
        process_tsv_source("dummy.tsv", "dummy.gpkg", "original", "EPSG:3067", {}, {}, [], "geometry", cleanup_temp=False, facts=None)
        mock_save_partition.assert_called_once()

if __name__ == "__main__":
    unittest.main()
