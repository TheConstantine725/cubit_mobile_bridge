import asyncio
from sqlalchemy import create_engine, Connection, Engine, text
import pendulum as pend
from typing import Optional, Any
from utils.limits import __safe_threading_limit
from utils.configs import BridgeConfigs
from utils.query_reader import QueryReader
from pathlib import Path

SAFE_THREADING_LIMIT = __safe_threading_limit()

SOURCE_EXTRACTION_CONFIG = BridgeConfigs().generate_extraction_config()

def _generate_semaphore(limit: int = SAFE_THREADING_LIMIT) -> asyncio.Semaphore:
    return asyncio.Semaphore(limit)

async def main():
    semaphore = _generate_semaphore()
    pass    

def _get_configs():
    pass

def _generate_engines(connection_string: str) -> Engine:
    return create_engine(url = connection_string)

def _generate_queries(query_alias: str) -> str:
    return QueryReader().read_source_extraction(query_alias) #type: ignore

def _generate_data(engine: Engine, query: str, execution_options_kwargs: Optional[dict[str, Any]])->Optional[dict[str,Any]]:
    if execution_options_kwargs is not None:
        try:
            with engine.connect() as _conn:
                result = _conn.execution_options(**execution_options_kwargs).exec_driver_sql(statement=query)
        
        except Exception as SqlError:
            print(SqlError)

        else:
            columns = [key.lower() for key in result.keys()]
            rows = result.fetchall()
            return {"columns":columns, "rows":rows}
    else:
        try:
            with engine.connect() as _conn:
                result = _conn.exec_driver_sql(statement=query)
        
        except Exception as SqlError:
            print(SqlError)

        else:
            columns = [key.lower() for key in result.keys()]
            rows = result.fetchall()
            return {"columns":columns, "rows":rows}

if __name__ == "__main__":
    for column, items in SOURCE_EXTRACTION_CONFIG.items():
        print(column, items["connection_string"], items["query_alias"])