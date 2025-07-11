import asyncio
from sqlalchemy import create_engine, Connection, Engine, text
import pendulum 
from typing import Optional, Any
from pathlib import Path
from json import dump
from ._limits import __safe_threading_limit
from ._configs import BridgeConfigs
from .source_extractor import QueryReader, engine_creator, generate_source_data, transform_final_data

SAFE_THREADING_LIMIT = __safe_threading_limit()

SOURCE_EXTRACTION_CONFIG = BridgeConfigs().generate_extraction_config()

def _generate_semaphore(limit: int = SAFE_THREADING_LIMIT) -> asyncio.Semaphore:
    return asyncio.Semaphore(limit)

# async def main():
#     _generate_semaphore() #type: ignore
#     pass    


def main():
    source_1 = SOURCE_EXTRACTION_CONFIG["dev"]
    (query := QueryReader().read_source_extraction(source_1["query_alias"]))
    (engine := engine_creator(source_1["connection_string"],))
    print("\n\n")
    sql_result = generate_source_data(engine,query, extraction_format="pyarrow")
    return sql_result 
