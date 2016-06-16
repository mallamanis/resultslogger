import pandas as pd
from itertools import chain


class ExperimentLogger:
    """
    Log all experiment results within the class.
    """
    def __init__(self, parameter_names: list, result_columns: list):
        self.__results_frame = pd.DataFrame(columns=parameter_names+result_columns)

    def log_experiment(self, parameters: dict, results: dict):
        joined_dict = dict(chain(parameters.items(), results.items()))
        self.__results_frame = self.__results_frame.append(joined_dict, ignore_index=True)

    @property
    def all_results(self)-> pd.DataFrame:
        return self.__results_frame

    def save_results_csv(self, filename:str):
        self.__results_frame.to_csv(filename)