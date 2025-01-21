import pyarrow as pa
from pyiceberg.catalog import Table, Catalog
import time


def append_with_retry_and_refresh(table: Table, data: pa.Table, max_attempts: int = 5,
                                  sleep_interval: int = 5) -> bool:
    attempts = 0
    while attempts < max_attempts:
        try:
            # Refresh table metadata
            table.refresh()
            # Attempt to append data
            table.append(data)
            return True
        except Exception as e:
            print(f"Error appending data: {e}")
            attempts += 1
            time.sleep(sleep_interval)
    return False


def sync_table_schema_evolve(table_name: str, pa_table: pa.Table, catalog: Catalog, iceberg_dir: str) -> tuple[str, int]:
    ib_table = catalog.create_table_if_not_exists(
        table_name,
        pa_table.schema,
        location=f'{iceberg_dir}{table_name.replace(".", "/")}'
    )

    with ib_table.update_schema() as update:
        update.union_by_name(pa_table.schema)

    if append_with_retry_and_refresh(ib_table, pa_table):
        return "success", pa_table.num_rows
    else:
        return "fail", -1
