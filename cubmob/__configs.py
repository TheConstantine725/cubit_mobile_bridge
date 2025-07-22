from yaml import safe_load
from typing import Union, Optional
from pathlib import Path
from sqlalchemy import Engine, Connection, create_engine

class DefaultConfig:
    __config_path__ = Path.cwd().joinpath("config/cubmob.yaml")

    @classmethod
    def read_config_yaml(cls):
        with cls.__config_path__.open("r", encoding="utf-8") as reader:
            return safe_load(reader)

class QueryConfig(DefaultConfig):

    def __init__(self,store: str, *, overide_config_path: Union[str,Path,None] = None) -> None:
        if isinstance(overide_config_path, str):
            self.__config_path__ = Path(overide_config_path)
        elif isinstance(overide_config_path, Path):
            self.__config_path__ = overide_config_path
        self.store = store
        print(f"Store Alias: {store}, Config Path: {self.__config_path__}")

    def read_query(self,) -> str:
        with Path(self.read_config_yaml()[self.store]["query"]).open("r", encoding="utf-8") as reader:
            return reader.read()

class ConnectionConfig(DefaultConfig):
    def __init__(self, store: str, *, 
                 overide_config_path: Union[str, Path, None] = None) -> None:
        if isinstance(overide_config_path, str):
            self.__config_path__ = Path(overide_config_path)
        elif isinstance(overide_config_path, Path):
            self.__config_path__ = overide_config_path
        self.store = store

    def create_connection(self,):
        with Path(self.read_config_yaml()[self.store]["source_connection"]).open("r",encoding="utf-8") as reader:
            _result:dict = safe_load(reader)[self.store]
            return create_engine(**_result)

class ApiConfig(DefaultConfig):
    __base_url__ = "https://api-portal-eu.vusion.io/"

    def __init__(self, store: str, *,
                 overide_config_path: Union[str, Path, None] = None) -> None:
        if isinstance(overide_config_path, str):
            self.__config_path__ = Path(overide_config_path)
        elif isinstance(overide_config_path, Path):
            self.__config_path__ = overide_config_path
        self.store = store
    
    def set_up_api(self,):
        with Path(self.read_config_yaml()[self.store]["api_endpoint"]).open("r", encoding="utf-8") as reader:
           return safe_load(reader) 

def config(store: str, overide_config: Path) -> dict[str, DefaultConfig]:
    return {"query": QueryConfig(store, overide_config_path=overide_config),
            "connection": ConnectionConfig(store, overide_config_path=overide_config),
            "api": ApiConfig(store, overide_config_path=overide_config),}
        

if __name__ == "__main__":
    print(QueryConfig("dev").read_query())
    print(ConnectionConfig("dev").create_connection())