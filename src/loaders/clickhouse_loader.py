from typing import Iterable, Sequence, Mapping, Any
from src.utils.clickhouse_client import get_client

def insert_rows(table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]):
    client = get_client()
    client.insert(table, rows, column_names=list(columns))
