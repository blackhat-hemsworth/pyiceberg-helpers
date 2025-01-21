import pytest
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
from pyiceberg_helpers.write import sync_table_schema_evolve
from pyiceberg_helpers.read import get_table_status
import shutil
import os
from datetime import datetime


@pytest.fixture(scope="class")
def iceberg_cat():
    warehouse_path = "tests/tmp/warehouse"
    if not os.path.exists(warehouse_path):
        os.makedirs(warehouse_path)

    iceberg_cat = SqlCatalog("default",
                             **{
                                 "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                                 "warehouse": f"file://{warehouse_path}",
                             },
                             )
    if ("test",) not in iceberg_cat.list_namespaces():
        iceberg_cat.create_namespace("test")

    iceberg_cat.create_table_if_not_exists("test.table_exists",
                                           pa.schema([
                                               ('some_int', pa.int32()),
                                               ('some_string', pa.string())
                                           ])
                                           )

    yield iceberg_cat

    iceberg_cat.destroy_tables()
    shutil.rmtree(warehouse_path)


class TestIcebergCatalogFunctions:
    def test_append_with_retry_and_refresh(self, iceberg_cat):
        pass  # taken straight from pyiceberg docs

    def test_sync_iceberg_table_no_ma(self, iceberg_cat):
        t = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
                                 names=["meta.some_int", "values.some_string"])
        r = sync_table_schema_evolve("test.table_exists", t, iceberg_cat, "tests/tmp/warehouse/")
        assert r[0] == "success"

    def test_sync_iceberg_table_ma(self, iceberg_cat):
        t = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
                                 names=["keys.some_int", "meta.action"])
        r = sync_table_schema_evolve("test.table_exists", t, iceberg_cat, "tests/tmp/warehouse/")
        assert r[0] == "success"

    def test_sync_iceberg_table_not_exists(self, iceberg_cat):
        t = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
                                 names=["keys.some_int", "meta.action"])
        r = sync_table_schema_evolve("test.table_exists_NOT", t, iceberg_cat, "tests/tmp/warehouse/")
        assert r[0] == "success"

    def test_get_status_needs_init(self, iceberg_cat):
        assert get_table_status("test.table_DNE", iceberg_cat) == (
            "table_DNE", "needs_init", datetime.fromtimestamp(0))

    def test_get_status_needs_sync(self, iceberg_cat):
        r = get_table_status("test.table_exists", iceberg_cat)
        assert r[0] == "table_exists"
        assert r[1] == "needs_sync"
        assert isinstance(r[2], datetime)
