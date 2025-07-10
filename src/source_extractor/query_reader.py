from pathlib import Path
from typing import Union, Optional, Any

DEFAULT_QUERIES_PARENT_FOLDER = Path("S:/HO/IT/__analytics_development/cubit_mobile_bridge").joinpath("queries")


class QueryReader:

    def __init__(self, query_home_folder: Union[str, Path] = DEFAULT_QUERIES_PARENT_FOLDER) -> None:
        self.query_home_folder = self.__transform_query_home_folder(query_home_folder)
    
    def __transform_query_home_folder(self, pathlike: Union[Path,str]) -> Optional[Path]:
        if isinstance(pathlike, str):
            try:
                final_path = Path(pathlike)
            except Exception as TransformingQueriesHomePathError:
                print("An error Occured during the initialization of the QueryReader Class, when setting the query homepath folder path.\n",
                      f"{'-'*40}\n{repr(TransformingQueriesHomePathError):=}\n{'='*40}",sep = None)
            else:
                return final_path
        elif isinstance(pathlike, Path):
            return pathlike
        
        elif not isinstance(pathlike, (Path, str)):
            raise TypeError(f"The {pathlike:=} is not of type {type(str)} or {type(Path)}")
        else:
            raise Exception(f"An unspecified error has occured during the initialization of {self.__class__}")
        
    def read_source_extraction(self, query_alias: str)->str: #type: ignore
        if isinstance(query_alias, str):
            source_extractor_file = self.query_home_folder.joinpath("source_extractor").joinpath(f"{query_alias}.sql") # type: ignore
            try:
                with source_extractor_file.open(mode = "r", encoding="utf-8") as query_reader:
                    final_query = query_reader.read()
            except Exception as IOerror:
                print("="*40)
                repr(IOerror)
                print("="*40)
                print(f"There was an error while reading the {source_extractor_file.as_posix():=}")
                print("="*40)
            else:
                return final_query
            
        else:
            raise TypeError(f"The {query_alias:=} is not of type {type(str)}")
