from sqlalchemy import create_engine, Engine
from typing import Optional, Any

def engine_creator(url_string: str,additional_kwargs: Optional[dict[str, Any]] = None) -> Engine:
    if additional_kwargs is None:
        return create_engine(url = url_string)
    else:
        return create_engine(url = url_string, **additional_kwargs)
