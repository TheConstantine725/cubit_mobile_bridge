from sqlalchemy import Engine, Connection, Row, CursorResult
from typing import Any, Optional, Sequence, Self, Iterable, Generator, Literal, Union
import pyarrow

TRANSFROMATION_FORMATS = Literal["pyarrow","json"]

class SqlResult:

    def __init__(self, cursor_result: CursorResult) -> None:
        self.column_keys: Sequence[str]= [key.lower() for key in cursor_result.keys()]
        self.rows: Generator[Sequence[Row[Any]]] = self.__generate_results(cursor_result)

    def __generate_results(self, _cursor_res: CursorResult) -> Generator[Sequence[Row[Any]]]:
        yield _cursor_res.fetchall()

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


def generate_source_data(engine: Engine,
                  query: str,*,
                  execution_options_kwargs: Optional[dict[str, Any]] = None,
                  extraction_format: Literal["pyarrow", "json"]
                  ) -> Optional[SqlResult]:
    if execution_options_kwargs is None:
        try:
            with engine.connect() as _conn:
                result = _conn.exec_driver_sql(statement=query,)
        except Exception as SqlError:
            print(f"There was an error in the execution of the SQL Query.\n{'='*40}\n{SqlError}\n{'='*40}")
        else:
            return SqlResult(result)

    elif isinstance(execution_options_kwargs, dict):
        try:
            with engine.connect() as _conn:
                result = _conn.execution_options(**execution_options_kwargs).exec_driver_sql(statement=query)
        except Exception as SqlError:
            print(f"There was an error in the execution of the SQL Query.\n{'='*40}\n{SqlError}\n{'='*40}")
        else:
            return SqlResult(result)
    else:
        raise ValueError(f"Cannot assign {SqlResult:=}")
    
def transform_final_data(source_data: SqlResult, 
                         format_transformation: TRANSFROMATION_FORMATS) -> Union[list[dict[str,Any]], pyarrow.Table, None]:
    if format_transformation == "pyarrow":
        return source_data.test_pyarrow_table()
    elif format_transformation == "json":
        return source_data.transpose_to_json()
    else:
        raise ValueError("this transformation format is not valid")

if __name__ == "__main__":
    pass