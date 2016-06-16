import json
import os
import sys

from flask import Flask, request
import pystache

from resultslogger.constants import ResultLoggerConstants
from resultslogger.experimentlogger import ExperimentLogger
from resultslogger.experimentqueue import ExperimentQueue


def load_template(relative_filename: str):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_filename)) as f:
        return pystache.parse(f.read())

class ResultsLoggerServer:

    PAGE_TEMPLATE = load_template("resources/page.mustache")

    def __init__(self, experiment_name: str, list_of_experiments_path: str, results_columns_path: str, output_filepath: str):
        """

        :param list_of_experiments:
        :param results_columns_path:
        :param output_filepath:
        :param lease_timout_secs: the number of secs that a lease times out. Defaults to 2 days
        """
        self.__app = Flask(__name__)
        self.__queue = ExperimentQueue(list_of_experiments_path)

        self.__renderer = pystache.Renderer()
        self.__experiment_name = experiment_name

        @self.__app.route(ResultLoggerConstants.ROUTE_LEASE_EXPERIMENT, methods=['POST'])
        def lease_next_experiment():
           client = request.form[ResultLoggerConstants.FIELD_CLIENT]
           next_lease = self.__queue.lease_new(client)
           if next_lease is None:
               return ResultLoggerConstants.END
           params, experiment_id = next_lease
           return json.dumps(dict(parameters=params, experiment_id=experiment_id))


        @self.__app.route(ResultLoggerConstants.ROUTE_STORE_EXPERIMENT, methods=['POST'])
        def store_experiment():
            client = request.form[ResultLoggerConstants.FIELD_CLIENT]
            experiment_parameters = request.form[ResultLoggerConstants.FIELD_PARAMETERS]
            results = request.form[ResultLoggerConstants.FIELD_RESULTS]
            experiment_id = request.form[ResultLoggerConstants.FIELD_EXPERIMENT_ID]
            parameters = json.loads(experiment_parameters)
            results = json.loads(results)
            self.__logger.log_experiment(parameters, results)
            self.__queue.complete(int(experiment_id), parameters, client)
            self.autosave()
            return ResultLoggerConstants.OK

        @self.__app.route(ResultLoggerConstants.ROUTE_EXPERIMENTS_SUMMARY)
        def show_summary_html():
            # TODO: Pivot and Export Pandas frame to html and return content
            return self.__renderer.render(self.PAGE_TEMPLATE,
                                          {'title': 'Results Summary',
                                           'experiment_name' : self.__experiment_name,
                                           'body': self.__logger.all_results.to_html(classes=['table', 'table-striped', 'table-condensed', 'table-hover']),
                                           'in_results': True})

        @self.__app.route(ResultLoggerConstants.ROUTE_EXPERIMENTS_QUEUE)
        def experiment_queue_html():
            pct_completed = self.__queue.completed_percent * 100
            pct_leased = self.__queue.leased_percent * 100
            return self.__renderer.render(self.PAGE_TEMPLATE,
                                          {'title': 'Experiments Queue',
                                           'experiment_name' : self.__experiment_name,
                                           'body': self.__queue.all_experiments.to_html(classes=['table', 'table-striped', 'table-condensed', 'table-hover']),
                                           'progress': pct_completed + pct_leased > 0,
                                           'progress_complete': int(pct_completed) if pct_completed > 0 else False,
                                           'progress_leased': int(pct_leased) if pct_leased > 0 else False,
                                           'in_queue': True})

        self.__output_filepath = output_filepath

        with open(results_columns_path) as f:
            self.__result_columns = f.read().split()
        self.__logger = ExperimentLogger(self.__queue.experiment_parameters, self.__result_columns)


    def run(self):
        self.__app.run(host='0.0.0.0')

    def autosave(self):
        pass  # TODO: save to .csv .pkl and self


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage <experimentName> <listOfExperiments.csv> <resultColumns> <outputPkl>")
        sys.exit(-1)
    list_of_experiments_csv_path = sys.argv[2]
    assert os.path.exists(list_of_experiments_csv_path)

    result_columns_path = sys.argv[3]
    assert os.path.exists(result_columns_path)

    output_filepath = sys.argv[4]

    logger = ResultsLoggerServer(sys.argv[1], list_of_experiments_csv_path, result_columns_path, output_filepath)
    logger.run()