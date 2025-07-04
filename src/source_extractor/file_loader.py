from aiohttp import Payload
from pendulum import Date
from pathlib import Path
from typing import Any, Literal, Iterable, Sequence
import csv
import json
import pyarrow

DEFAULT_OUTPUT_FOLDER = Path("S:/HO/IT/__analytics_development/cubit_mobile_bridge").joinpath("output")

class FileLoader:
    mid_dirs = ("source_extractor", "api_loader")
    def __init__(self,
                 output_directory: Path = DEFAULT_OUTPUT_FOLDER) -> None:
        self.output_directory = output_directory
        self.output_paths = {}
        
        if not self.output_directory.exists():
            self.output_directory.mkdir(parents=True, exist_ok=True)
            for path_suffix in self.mid_dirs:
                _temp = self.output_directory.joinpath(path_suffix)
                _temp.mkdir(parents=True, exist_ok=True)
                self.output_paths[path_suffix] = _temp

    
    def load_source_extractor(self, staging_folder:str, 
                              payload: Any, 
                              loader_format: Literal["json", "csv", "parquet"]) -> None:
        source_folder: Path = self.output_paths["source_extractor"]
        _parent_fold= source_folder.joinpath(f"{Date.today().format("%Y-%m-%d")}")
        _parent_fold.mkdir(parents=True, exist_ok=True)

        if loader_format == "csv":
            if isinstance(payload, pyarrow.Table):
                from pyarrow.csv import write_csv
                _csv_file = _parent_fold.joinpath(f"{staging_folder}.csv").as_posix()
                write_csv(payload, _csv_file)
            else:
                raise TypeError(f"The payload has to be of type {type(pyarrow.Table)}")

        elif loader_format == "parquet":
            if isinstance(payload, pyarrow.Table):
                from pyarrow.parquet import write_table
                _parq_file = _parent_fold.joinpath(f"{staging_folder}.parquet").as_posix()
                write_table(table= payload, where= _parq_file)
            else:
                raise TypeError(f"The payload must be of {type(pyarrow.Table)}.")
        
        elif loader_format == "json":
            if isinstance(payload, dict):
                with open(file = _parent_fold.joinpath(f"{staging_folder}.json"), 
                        mode = "w", encoding="utf-8") as _json_writer:
                    json.dump(payload, _json_writer, indent=4)

if __name__ == "__main__":
    pass