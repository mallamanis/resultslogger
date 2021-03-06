import os
import socket
import json
from copy import copy
from typing import Tuple, Dict, Any, Optional, Callable

import requests
import sys

from resultslogger.constants import ResultLoggerConstants


class ResultsLoggerClient:
    def __init__(self, servername: str):
        self.__servername = servername

    @property
    def client_name(self)-> str:
        """
        The name of the client
        :return:
        """
        return str(socket.gethostname()) + "-pid:" + str(os.getpid())

    def lease_next_experiment(self)-> Tuple:
        """
        Lease a new experiment to this client
        :return: a dict with the necessary parameters
        """
        r = requests.post(self.__servername + ResultLoggerConstants.ROUTE_LEASE_EXPERIMENT,
                          data={ResultLoggerConstants.FIELD_CLIENT: self.client_name})
        assert r.status_code == requests.codes.ok, r
        if r.text == ResultLoggerConstants.END:
            return None
        data = r.json()
        return data['experiment_id'], data['parameters']

    def store_experiment_results(self, experiment_id: int, parameters: Dict[str, Any], results: Dict[str, Any], minimized_result: Optional[float]=None):
        """
        Store the results of an experiment.
        :param parameters: the used parameters
        :param results: the results
        """
        if minimized_result is not None:
            parameters = copy(parameters)
            parameters[ResultLoggerConstants.BASE_RESULT_FIELD] = minimized_result
        data = {ResultLoggerConstants.FIELD_CLIENT: self.client_name,
                ResultLoggerConstants.FIELD_PARAMETERS: json.dumps(parameters),
                ResultLoggerConstants.FIELD_RESULTS: json.dumps(results),
                ResultLoggerConstants.FIELD_EXPERIMENT_ID: experiment_id}

        r = requests.post(self.__servername + ResultLoggerConstants.ROUTE_STORE_EXPERIMENT, data=data)
        assert r.status_code == requests.codes.ok, r.content
        assert r.text == ResultLoggerConstants.OK

    def compute_in_loop(self, result_computer: Callable[[Dict[str, Any]], Tuple[float, Dict[str, Any]]], output_stream=sys.stdout):
        """
        Keep asking and running new experiments, until there are no more experiments available.
        :param result_computer: a lambda that accepts a dict of parameters and returns a dict of results. The method
        receives a copy of the parameters.
        :param output_stream: the file to output (default stdout)
        """
        while True:
            print('[%s] Requesting new experiment...' % self.client_name, file=output_stream)
            next_experiment = self.lease_next_experiment()
            if next_experiment is None:
                print('[%s] No more experiments available...' % self.client_name, file=output_stream)
                break
            experiment_id, parameters = next_experiment
            print("Running with new parameters %s" % parameters, file=output_stream)
            optimized_result, results = result_computer(dict(parameters))
            self.store_experiment_results(experiment_id, parameters, results, minimized_result=optimized_result)
            print("[%s] Finished experiments with parameters %s and results %s" % (self.client_name, parameters, results), file=output_stream)


# Sample
#import random
#cl = ResultsLoggerClient("http://localhost:5000")
#experiment_id, params = cl.lease_next_experiment()
#print(experiment_id)
#print(params)
#cl.store_experiment_results(experiment_id, params, {'name_f1':random.random(),
#                                                    'name_recall':random.random(),
#                                                    'name_precision':random.random(),
#                                                    'nargs_f1':random.random(),
#                                                    'nargs_recall':random.random(),
#                                                    'nargs_precision':random.random()})