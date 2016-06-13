import os
import socket
import json
import requests

from resultslogger.constants import ResultLoggerConstants


class ResultsLoggerClient:
    def __init__(self, servername):
        self.__servername = servername

    @property
    def client_name(self):
        return str(socket.gethostname()) + "-pid:" + str(os.getpid())

    def lease_next_experiment(self):
        """
        Lease a new experiment to this client
        :return: a dict with the necessary parameters
        """
        r = requests.post(self.__servername + ResultLoggerConstants.ROUTE_LEASE_EXPERIMENT,
                          data={ResultLoggerConstants.FIELD_CLIENT:self.client_name})
        assert r.status_code == requests.codes.ok, r
        return r.json()

    def store_experiment_results(self, parameters, results):
        data = {ResultLoggerConstants.FIELD_CLIENT:self.client_name,
                ResultLoggerConstants.FIELD_PARAMETERS:json.dumps(parameters),
                ResultLoggerConstants.FIELD_RESULTS:json.dumps(results)}
        r = requests.post(self.__servername + ResultLoggerConstants.ROUTE_STORE_EXPERIMENT, data=data)
        assert r.status_code == requests.codes.ok, r.content
        assert r.text == ResultLoggerConstants.OK



cl = ResultsLoggerClient("http://localhost:5000")
print(cl.lease_next_experiment())
cl.store_experiment_results({'a':1, 'b':2}, {'q':3, 'f':4.2})