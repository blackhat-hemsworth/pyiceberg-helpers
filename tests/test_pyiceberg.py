import pytest
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
from pyiceberg_helpers.write import sync_table_schema_evolve, merge_into_ish, IdNotFound
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

    yield iceberg_cat

    iceberg_cat.destroy_tables()
    shutil.rmtree(warehouse_path + "/test.db")
    shutil.rmtree(warehouse_path + "/test")


class TestIcebergCatalogFunctions:
    @pytest.fixture(autouse=True)
    def set_table(self, iceberg_cat):
        pat = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "b", "c"])],
                                   names=["id", "some_string"])
        t = iceberg_cat.create_table_if_not_exists("test.table",
                                                   pa.schema([
                                                       ('id', pa.int32()),
                                                       ('some_string', pa.string())
                                                   ])
                                                   )
        t.overwrite(pat)
        yield t
        t.refresh()
        t.overwrite(pat)

    def test_merge_new_rows(self, iceberg_cat):
        i = iceberg_cat.load_table("test.table")
        a = pa.Table.from_arrays(arrays=[pa.array([1, 4, 5], type=pa.int32()), pa.array(["a", "d", "e"])],
                                 names=["id", "some_string"])
        merge_into_ish(i, a)
        assert len(i.refresh().scan().to_arrow()) == 5

    def test_merge_same_rows(self, iceberg_cat):
        i = iceberg_cat.load_table("test.table")
        a = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "d", "e"])],
                                 names=["id", "some_string"])
        merge_into_ish(i, a)
        assert len(i.refresh().scan().to_arrow()) == 3

    def test_merge_no_rows(self, iceberg_cat):
        i = iceberg_cat.load_table("test.table")
        a = pa.Table.from_arrays(arrays=[pa.array([], type=pa.int32()), pa.array([], type=pa.string())],
                                 names=["id", "some_string"])
        merge_into_ish(i, a)
        assert len(i.refresh().scan().to_arrow()) == 3

    def test_sync_iceberg_table_new_col(self, iceberg_cat):
        t = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "b", "c"])],
                                 names=["id", "action"])
        r = sync_table_schema_evolve("test.table", t, iceberg_cat, "tests/tmp/warehouse/")
        assert r[0] == "success"
        assert r[1] == 3
        assert len(iceberg_cat.load_table("test.table").scan().to_arrow()) == 3

    def test_sync_iceberg_table_no_id(self, iceberg_cat):
        t = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "b", "c"])],
                                 names=["table_name_id", "action"])
        with pytest.raises(IdNotFound):
            sync_table_schema_evolve("test.table", t, iceberg_cat, "tests/tmp/warehouse/")

    def test_sync_iceberg_table_not_exists(self, iceberg_cat):
        t = pa.Table.from_arrays(arrays=[pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "b", "c"])],
                                 names=["id", "action"])
        r = sync_table_schema_evolve("test.table_exists_NOT", t, iceberg_cat, "tests/tmp/warehouse/")
        assert r[0] == "success"

    def test_get_status_needs_init(self, iceberg_cat):
        assert get_table_status("test.table_DNE", iceberg_cat) == (
            "table_DNE", "needs_init", datetime.fromtimestamp(0))

    def test_get_status_needs_sync(self, iceberg_cat):
        r = get_table_status("test.table", iceberg_cat)
        assert r[0] == "table"
        assert r[1] == "needs_sync"
        assert isinstance(r[2], datetime)
