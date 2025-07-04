from pathlib import Path
import yaml
from typing import Union, Any, Optional

DEFAULT_EXTRACTION_CONFIG = Path("S:/HO/IT/__analytics_development/cubit_mobile_bridge").joinpath("config/extraction_config.yaml")
DEFAULT_API_CONFIG = Path("S:/HO/IT/__analytics_development/cubit_mobile_bridge").joinpath("config/api_config.yaml")
    
class BridgeConfigs:

    def __init__(self, source_extraction_config_path: Union[Path, str]= DEFAULT_EXTRACTION_CONFIG,
                api_config_path: Union[Path, str] = DEFAULT_API_CONFIG,
                 ) -> None:
        self.extraction_config_path = self.__create_path(source_extraction_config_path)
        self.api_config_path = self.__create_path(api_config_path)

    def __create_path(self, pathlike: Union[str,Path],) -> Optional[Path]:
        if isinstance(pathlike, str):
            try:
                _final_path = Path(pathlike)
            except Exception as PathlikeObjectError:
                print(f"There was an error with {pathlike:=}\n{'='*40}\n{PathlikeObjectError}\n{'='*40}")
            else:
                return _final_path
        elif isinstance(pathlike, Path):
            return pathlike
        
        elif not isinstance(pathlike, (Path, str)):
            raise TypeError(f"The value {pathlike:=} is not of the correct datatype. {type(pathlike)}")
        
        else:
            raise Exception(f"General Error with the initialization of the {self.__class__}")


    def generate_extraction_config(self,) -> dict[str, Any]:
        if isinstance(self.extraction_config_path, Path):
            with self.extraction_config_path.open(mode = "r", encoding= "utf-8") as _reader:
                return yaml.safe_load(_reader)
        else:
            raise TypeError(f"The variable {self.extraction_config_path} is not of the correct Path type")

    def generate_api_config(self, ) -> dict[str, Any]:
        if isinstance(self.api_config_path, Path):
            with self.api_config_path.open(mode = "r", encoding="utf-8") as _reader:
                return yaml.safe_load(_reader)
        else:
            raise TypeError(f"The variable {self.api_config_path} is not of the correct Path type")