from sqlalchemy import Engine, Connection, Row
from typing import Any, Optional, Sequence, Self, Iterable
import pyarrow

class SqlResult:

    def __init__(self, column_keys: Iterable[str], rows: Sequence[Row[Any]]) -> None:
        self.column_keys = [column_name.lower() for column_name in column_keys]
        self.rows = rows

    def transpose_to_json(self,) -> list[dict[str,Any]]:
        result = []

        for row in self.rows:
            temp = {}
            for i, key in enumerate(self.column_keys):
                temp[key] = row[i]
            result.append(temp)

        return result

    def test_pyarrow_table(self,)->pyarrow.Table:
        result = {}
        for i, column_name in enumerate(self.column_keys):
            array = []
            for row in self.rows:
                array.append(row[i])
            result[column_name] = pyarrow.array(array)
        
        return pyarrow.table(result)


def generate_data(engine: Engine,
                  query: str,*,
                  execution_options_kwargs: Optional[dict[str, Any]] = None) -> Optional[SqlResult]:
    if execution_options_kwargs is None:
        try:
            with engine.connect() as _conn:
                result = _conn.exec_driver_sql(statement=query,)
        except Exception as SqlError:
            print(f"There was an error in the execution of the SQL Query.\n{'='*40}\n{SqlError}\n{'='*40}")
        else:
            return SqlResult(result.keys(), result.fetchall())

    elif isinstance(execution_options_kwargs, dict):
        try:
            with engine.connect() as _conn:
                result = _conn.execution_options(**execution_options_kwargs).exec_driver_sql(statement=query)
        except Exception as SqlError:
            print(f"There was an error in the execution of the SQL Query.\n{'='*40}\n{SqlError}\n{'='*40}")
        else:
            return SqlResult(result.keys(), result.fetchall())
    else:
        raise ValueError(f"Cannot assign {SqlResult:=}")