import pyarrow as pa
from pyiceberg.catalog import Table, Catalog
import time


class IdNotFound(Exception):
    pass


# following https://juhache.substack.com/p/pyiceberg-current-state-and-roadmap
def merge_into_ish(table: Table, data: pa.Table, max_attempts: int = 5, sleep_interval: int = 2) -> bool:
    if len(data) == 0:
        return True

    id_list = data.column("id").to_pylist()
    attempts = 0
    while attempts < max_attempts:
        try:
            table.refresh()
            table.overwrite(data, overwrite_filter=f"id In ({','.join(str(i) for i in id_list)})")
            return True
        except Exception:
            attempts += 1
            time.sleep(sleep_interval)
    return False


def sync_table_schema_evolve(table_name: str, pa_table: pa.Table, catalog: Catalog, iceberg_dir: str) -> tuple[str, int]:
    if "id" not in pa_table.column_names:
        raise IdNotFound()

    ib_table = catalog.create_table_if_not_exists(
        table_name,
        pa_table.schema,
        location=f'{iceberg_dir}{table_name.replace(".", "/")}'
    )

    with ib_table.update_schema() as update:
        update.union_by_name(pa_table.schema)

    if merge_into_ish(ib_table, pa_table):
        return "success", pa_table.num_rows
    else:
        return "fail", -1
