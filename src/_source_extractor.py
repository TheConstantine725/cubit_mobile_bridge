import asyncio
from sqlalchemy import create_engine, Connection
import pendulum as pend
import psutil

def __safe_threading_limit() -> int:
    cpu_count = psutil.cpu_count()
    if cpu_count is None:
        return 1
    else:
        return int(cpu_count / 2)


SAFE_THREADING_LIMIT = __safe_threading_limit()

async def main():
    ...

def _generate_connection():
    ...

def _generate_data():
    ...

if __name__ == "__main__":
    ...