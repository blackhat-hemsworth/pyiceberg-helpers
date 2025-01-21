from pyiceberg.catalog import Catalog
from datetime import datetime, timezone


def get_table_status(table_name: str, ib_catalog: Catalog) -> tuple[str, str, datetime]:
    """
    Gets the status of a table in the Iceberg catalog.

    Args:
        table_name (str): The name of the table.
        ib_catalog (Catalog): The Iceberg catalog.

    Returns:
        tuple: A tuple containing the table name, status, and last updated timestamp.
    """
    if ib_catalog.table_exists(table_name):
        t = ib_catalog.load_table(table_name)
        return table_name.split(".")[1], "needs_sync", datetime.fromtimestamp(t.metadata.last_updated_ms / 1000,
                                                                              tz=timezone.utc)
    else:
        return table_name.split(".")[1], "needs_init", datetime.fromtimestamp(0)
