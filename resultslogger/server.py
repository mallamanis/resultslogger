from flask import Flask, request
from resultslogger.constants import ResultLoggerConstants
import json

class ResultsLoggerServer:
    def __init__(self):
        self.app = Flask(__name__)

        @self.app.route(ResultLoggerConstants.ROUTE_LEASE_EXPERIMENT, methods=['POST'])
        def lease_next_experiment():
           client = request.form[ResultLoggerConstants.FIELD_CLIENT]
           # TODO: Get parameters and store lease
           params = dict(a=1, b=.2)
           return json.dumps(params)

        @self.app.route(ResultLoggerConstants.ROUTE_STORE_EXPERIMENT, methods=['POST'])
        def store_experiment():
            client = request.form[ResultLoggerConstants.FIELD_CLIENT]
            experiment_parameters = request.form[ResultLoggerConstants.FIELD_PARAMETERS]
            results = request.form[ResultLoggerConstants.FIELD_RESULTS]
            print(client, json.loads(experiment_parameters), json.loads(results))
            # TODO: Store experiments!
            self.autosave()
            return ResultLoggerConstants.OK

        @self.app.route(ResultLoggerConstants.ROUTE_EXPERIMENTS_SUMMARY)
        def show_summary_html():
            # TODO: Pivot and Export Pandas frame to html and return content
            return "TODO"

    def run(self):
        self.app.run()

    def autosave(self):
        pass  # TODO: save to .csv .pkl


if __name__ == "__main__":
    logger = ResultsLoggerServer()
    logger.run()