import json
import os
import sys
import pickle

from flask import Flask, request, make_response
import pystache

from resultslogger.constants import ResultLoggerConstants
from resultslogger.experimentlogger import ExperimentLogger
from resultslogger.experimentqueue import ExperimentQueue


def load_template(relative_filename: str):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_filename)) as f:
        return pystache.parse(f.read())

class ResultsLoggerServer:

    PAGE_TEMPLATE = load_template("resources/page.mustache")

    def __init__(self, experiment_name: str, list_of_experiments_path: str=None, results_columns_path: str=None,
                 autosave_path: str='.', queue: ExperimentQueue=None, experiment_logger: ExperimentLogger=None,
                 allow_unsolicited_results: bool=True):
        """

        :param list_of_experiments_path: the path of the csv that contains all the experiments (with their parameters). Ignored if queue is not None.
        :param results_columns_path: the path of a file that contains the columns within the results. Ignored if experiment_logger is not None.
        :param autosave_path: the path where to autosave the results
        :param lease_timout_secs: the number of secs that a lease times out. Defaults to 2 days
        :param allow_unsolicited_results: allow clients to report results about experiments that are not in the queue/have been leased.
        """
        self.__app = Flask(__name__)
        self.__queue = ExperimentQueue(list_of_experiments_path) if queue is None else queue

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
            experiment_id = int(experiment_id)
            if experiment_id == -1 and not self.__allow_unsolicited_results:
                assert False, "Unsolicited experiment returned"
            elif experiment_id >= 0:
                self.__queue.complete(experiment_id, parameters, client)
            self.autosave()
            return ResultLoggerConstants.OK

        @self.__app.route(ResultLoggerConstants.ROUTE_EXPERIMENTS_ALL_RESULTS)
        def show_results_html():
            return self.__renderer.render(self.PAGE_TEMPLATE,
                                          {'title': 'All Results',
                                           'experiment_name' : self.__experiment_name,
                                           'body': self.__pandas_to_html_table(self.__logger.all_results),
                                           'summary_links': self.__get_groupby_links(set()),
                                           'in_results': True})

        @self.__app.route(ResultLoggerConstants.ROUTE_CSV_DUMP)
        def download_csv():
            response = make_response(self.__logger.all_results.to_csv())
            response.headers['Content-Description'] = 'File Transfer'
            response.headers['Cache-Control'] = 'no-cache'
            response.headers['Content-Type'] = 'application/octet-stream'
            response.headers['Content-Disposition'] = 'attachment; filename=%s' % self.__experiment_name+'.csv'
            return response

        @self.__app.route(ResultLoggerConstants.ROUTE_EXPERIMENTS_SUMMARY)
        def show_summary_html():
            group_by_values = request.args.get(ResultLoggerConstants.FIELD_GROUPBY).split(',')
            return self.__renderer.render(self.PAGE_TEMPLATE,
                                          {'title': 'Results Summary',
                                           'experiment_name': self.__experiment_name,
                                           'body': self.__pandas_to_html_table(self.__logger.all_results.groupby(group_by_values).mean()),
                                           'summary_links': self.__get_groupby_links(set(group_by_values)),
                                           'in_summary':True
                                           })

        @self.__app.route(ResultLoggerConstants.ROUTE_EXPERIMENTS_QUEUE)
        def experiment_queue_html():
            pct_completed = self.__queue.completed_percent * 100
            pct_leased = self.__queue.leased_percent * 100
            return self.__renderer.render(self.PAGE_TEMPLATE,
                                          {'title': 'Experiments Queue',
                                           'experiment_name' : self.__experiment_name,
                                           'body': self.__pandas_to_html_table(self.__queue.all_experiments),
                                           'progress': pct_completed + pct_leased > 0,
                                           'progress_complete': int(pct_completed) if pct_completed > 0 else False,
                                           'progress_leased': int(pct_leased) if pct_leased > 0 else False,
                                           'summary_links': self.__get_groupby_links(set()),
                                           'in_queue': True})

        self.__autosave_path = autosave_path
        self.__allow_unsolicited_results = allow_unsolicited_results

        if experiment_logger is None:
            with open(results_columns_path) as f:
                self.__result_columns = f.read().split()
            self.__logger = ExperimentLogger(self.__queue.experiment_parameters, self.__result_columns)
        else:
            self.__logger = experiment_logger


    def run(self):
        self.__app.run(host='0.0.0.0')

    def __pandas_to_html_table(self, frame):
        return frame.to_html(classes=['table', 'table-striped', 'table-condensed', 'table-hover']).replace('border="1"', 'border="0"')

    def __get_groupby_links(self, current_parameters:set)->list:
        """The group, by links are incremental, toggle-like"""
        def get_parameters(name_of_param)-> tuple:
            active = name_of_param in current_parameters
            if active:
                params_to_use = current_parameters - {name_of_param}
                if len(params_to_use) > 0:
                    return ','.join(params_to_use)
                else:
                    return ','.join(current_parameters)
            else:
                return ','.join(current_parameters | {name_of_param})
        return [{'name': n, 'link': get_parameters(n), 'active': n in current_parameters} for n in self.__queue.experiment_parameters]

    def autosave(self):
        self.__logger.save_results_csv(os.path.join(self.__autosave_path, self.__experiment_name + "_results.csv"))
        with open(os.path.join(self.__autosave_path, self.__experiment_name + ".pkl"), 'wb') as f:
            pickle.dump((self.__queue, self.__logger), f, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(filename: str, experiment_name: str):
        """
        Load previously saved experiment data and progress.
        """
        with open(filename, 'rb') as f:
            queue, experiment_logger = pickle.load(f)
            return ResultsLoggerServer(experiment_name, queue=queue, experiment_logger=experiment_logger)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage <experimentName> <listOfExperiments.csv> <resultColumns> <outputPklDirectory>")
        sys.exit(-1)
    list_of_experiments_csv_path = sys.argv[2]
    assert os.path.exists(list_of_experiments_csv_path)

    result_columns_path = sys.argv[3]
    assert os.path.exists(result_columns_path)

    output_filepath = sys.argv[4]

    logger = ResultsLoggerServer(sys.argv[1], list_of_experiments_csv_path, result_columns_path, output_filepath)
    logger.run()