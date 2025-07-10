from sqlalchemy import Engine, Connection, Row, CursorResult
from typing import Any, Optional, Sequence, Self, Iterable, Generator, Literal, Union
import pyarrow

TRANSFROMATION_FORMATS = Literal["pyarrow","json"]

class SqlResult:

    def __init__(self, cursor_result: CursorResult) -> None:
        self.cursor_result = cursor_result
       
    @property
    def __generate_rows(self,) -> Generator[Sequence[Row[Any]], Any, None]:
        yield from self.cursor_result
    
    @property
    def __generate_keys(self,) -> Sequence[str]:
        return [key.lower() for key in self.cursor_result.keys()]


    def transpose_to_json(self,) -> list[dict[str,Any]]:
        result = []

        for row in self.__generate_rows:
            temp = {}
            for i, key in enumerate(self.__generate_keys):
                temp[key] = row[i]
            result.append(temp)

        return result

    def test_pyarrow_table(self,)->Any:
        _pydict = {}
        for i, column in enumerate(self.__generate_keys):
            temp_pylist = []
            for r in self.__generate_rows:
                temp_pylist.append(r)

            _pydict[column] = temp_pylist
                
            print(temp_pylist)




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
            return transform_final_data(SqlResult(result), extraction_format)

    elif isinstance(execution_options_kwargs, dict):
        try:
            with engine.connect() as _conn:
                result = _conn.execution_options(**execution_options_kwargs).exec_driver_sql(statement=query)
        except Exception as SqlError:
            print(f"There was an error in the execution of the SQL Query.\n{'='*40}\n{repr(SqlError)}\n{'='*40}")
        else:
            return transform_final_data(SqlResult(result), extraction_format)
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
