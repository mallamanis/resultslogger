import os
import socket
import json
import requests
import sys

from resultslogger.constants import ResultLoggerConstants


class ResultsLoggerClient:
    def __init__(self, servername: str):
        self.__servername = servername

    @property
    def client_name(self):
        """
        The name of the client
        :return:
        """
        return str(socket.gethostname()) + "-pid:" + str(os.getpid())

    def lease_next_experiment(self)-> dict:
        """
        Lease a new experiment to this client
        :return: a dict with the necessary parameters
        """
        r = requests.post(self.__servername + ResultLoggerConstants.ROUTE_LEASE_EXPERIMENT,
                          data={ResultLoggerConstants.FIELD_CLIENT:self.client_name})
        assert r.status_code == requests.codes.ok, r
        if r.text == ResultLoggerConstants.END:
            return None
        return r.json()

    def store_experiment_results(self, parameters: dict, results: dict):
        """
        Store the results of an experiment.
        :param parameters: the used parameters
        :param results: the results
        """
        data = {ResultLoggerConstants.FIELD_CLIENT:self.client_name,
                ResultLoggerConstants.FIELD_PARAMETERS:json.dumps(parameters),
                ResultLoggerConstants.FIELD_RESULTS:json.dumps(results)}
        r = requests.post(self.__servername + ResultLoggerConstants.ROUTE_STORE_EXPERIMENT, data=data)
        assert r.status_code == requests.codes.ok, r.content
        assert r.text == ResultLoggerConstants.OK

    def compute_in_loop(self, result_computer, output_stream=sys.stdout):
        """
        Keep asking and running new experiments, until there are no more experiments available.
        :param result_computer: a lambda that accepts a dict of parameters and returns a dict of results
        :param output_stream: the file to output (default stdout)
        """
        while True:
            print('Requesting new experiment...', file=output_stream)
            parameters = self.lease_next_experiment()
            if parameters is None:
                print('No more experiments available...', file=output_stream)
                break
            print("Running with new parameters %s" % parameters, file=output_stream)
            results = result_computer(dict(parameters))
            self.store_experiment_results(parameters, results)
            print("Finished experiments with parameters %s and results %s" % (parameters, results), file=output_stream)



cl = ResultsLoggerClient("http://localhost:5000")
print(cl.lease_next_experiment())
cl.store_experiment_results({'a':1, 'b':2}, {'q':3, 'f':4.2})