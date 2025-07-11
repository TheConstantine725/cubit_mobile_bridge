from sqlalchemy import Engine,  Row, CursorResult
from typing import Any, Optional, Sequence,  Generator, Literal, Union
from itertools import repeat
import pyarrow
import numpy as np

TRANSFROMATION_FORMATS = Literal["pyarrow","json"]
EXTRACTED_RESULTS = Union[pyarrow.Table, list[dict[str,Any]], None]

class SqlResult:

    def __init__(self, cursor_result: CursorResult) -> None:
        self.column_keys: Sequence[str]= [key.lower() for key in cursor_result.keys()]
        self.rows: Generator[Sequence[Row[Any]]] = self.__generate_results(cursor_result)
        # self.__base_pydict = 


    def __generate_results(self, _cursor_res: CursorResult) -> Generator[Sequence[Row[Any]],Any, None]:
        yield from _cursor_res

    def transpose_to_json(self,) -> list[dict[str,Any]]:
        result = []

        for row in self.rows:
            temp = {}
            for i, key in enumerate(self.column_keys):
                temp[key] = row[i]
            result.append(temp)
        return result

    def transpose_to_pyarrow(self,)-> Any:
        __base_pydict = {col:[] for col in self.column_keys}
        for row in self.rows:
            for col, r in zip(self.column_keys, row):
                __base_pydict[col].append(r)
        
        for col_name, col_data in __base_pydict.items():
            __base_pydict[col_name] = pyarrow.array(np.array(col_data))
        return pyarrow.table(__base_pydict)


def generate_source_data(engine: Engine,
                  query: str,*,
                  execution_options_kwargs: Optional[dict[str, Any]] = None,
                  extraction_format: Literal["pyarrow", "json"]
                  ) -> Union[list[dict[str,Any]], pyarrow.Table, None]:
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
        return source_data.transpose_to_pyarrow()
    elif format_transformation == "json":
        return source_data.transpose_to_json()
    else:
        raise ValueError("this transformation format is not valid")

if __name__ == "__main__":
    pass
