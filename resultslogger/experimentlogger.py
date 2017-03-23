from typing import Any, List, Dict
import pandas as pd
from itertools import chain


class ExperimentLogger:
    """
    Log all experiment results within the class.
    """
    def __init__(self, parameter_names: List[str], result_columns: List[str]) -> None:
        self.__results_frame = pd.DataFrame(columns=parameter_names+result_columns)

    def log_experiment(self, parameters: Dict[str, Any], results: Dict[str, Any]) -> None:
        joined_dict = dict(chain(parameters.items(), results.items()))
        self.__results_frame = self.__results_frame.append(joined_dict, ignore_index=True)

    @property
    def all_results(self)-> pd.DataFrame:
        return self.__results_frame

    def save_results_csv(self, filename:str) -> None:
        self.__results_frame.to_csv(filename)