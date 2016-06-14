from flask import Flask, request
from resultslogger.constants import ResultLoggerConstants
import json
import sys
import os

class ResultsLoggerServer:
    def __init__(self, list_of_experiments_path: str, results_columns_path: str, output_filepath: str):
        """

        :param list_of_experiments:
        :param results_columns_path:
        :param output_filepath:
        :param lease_timout_secs: the number of secs that a lease times out. Defaults to 2 days
        """
        self.__app = Flask(__name__)

        @self.__app.route(ResultLoggerConstants.ROUTE_LEASE_EXPERIMENT, methods=['POST'])
        def lease_next_experiment():
           client = request.form[ResultLoggerConstants.FIELD_CLIENT]
           # TODO: Get parameters and store lease
           params = dict(a=1, b=.2)
           return json.dumps(params)

        @self.__app.route(ResultLoggerConstants.ROUTE_STORE_EXPERIMENT, methods=['POST'])
        def store_experiment():
            client = request.form[ResultLoggerConstants.FIELD_CLIENT]
            experiment_parameters = request.form[ResultLoggerConstants.FIELD_PARAMETERS]
            results = request.form[ResultLoggerConstants.FIELD_RESULTS]
            print(client, json.loads(experiment_parameters), json.loads(results))
            # TODO: Store experiments appropriately
            self.autosave()
            return ResultLoggerConstants.OK

        @self.__app.route(ResultLoggerConstants.ROUTE_EXPERIMENTS_SUMMARY)
        def show_summary_html():
            # TODO: Pivot and Export Pandas frame to html and return content
            return "TODO"

        self.__output_filepath = output_filepath
        with open(results_columns_path) as f:
            self.__result_columns = f.read().split()



    def run(self):
        self.__app.run()

    def autosave(self):
        pass  # TODO: save to .csv .pkl and self


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage <listOfExperiments.csv> <resultColumns> <outputPkl>")
        sys.exit(-1)
    list_of_experiments_csv_path = sys.argv[1]
    assert os.path.exists(list_of_experiments_csv_path)

    result_columns_path = sys.argv[2]
    assert os.path.exists(result_columns_path)

    output_filepath = sys.argv[3]

    logger = ResultsLoggerServer(list_of_experiments_csv_path, result_columns_path, output_filepath)
    logger.run()