import asyncio
from sqlalchemy import create_engine, Connection, Engine, text
import pendulum 
from typing import Optional, Any
from _limits import __safe_threading_limit
from _configs import BridgeConfigs
from source_extractor import QueryReader, engine_creator, generate_data
from pathlib import Path
from json import dump

SAFE_THREADING_LIMIT = __safe_threading_limit()

SOURCE_EXTRACTION_CONFIG = BridgeConfigs().generate_extraction_config()

def _generate_semaphore(limit: int = SAFE_THREADING_LIMIT) -> asyncio.Semaphore:
    return asyncio.Semaphore(limit)

async def main():
    semaphore = _generate_semaphore()
    pass    

if __name__ == "__main__":
    source_1 = SOURCE_EXTRACTION_CONFIG["dev"]
    print(query := QueryReader().read_source_extraction(source_1["query_alias"]))
    print(engine := engine_creator(source_1["connection_string"],))
    sql_result = generate_data(engine,query).test_pyarrow_table()
    _path = Path.cwd().joinpath("output")
    _path.mkdir(exist_ok=True)
    import daft
    
    print(daft.from_arrow(sql_result))