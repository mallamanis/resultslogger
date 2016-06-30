import json

import pandas as pd


class ExperimentQueue:
    """
    A queue of experiments. Each experiment may have been leased, be waiting for a process to lease it or it may be
    done.
    """

    DONE = 'DONE'
    WAITING = 'WAITING'
    LEASED = 'LEASED'

    def __init__(self, list_of_experiments_path: str, lease_timout='2 days'):
        """
        :param list_of_experiments_path: The path to a csv file containing all possible experiments.
        """
        self.__all_experiments = pd.read_csv(list_of_experiments_path)
        self.__all_experiments['status'] = [self.WAITING] * len(self.__all_experiments)
        self.__all_experiments['last_update'] = pd.Series(pd.Timestamp(float('NaN')))
        self.__all_experiments['client'] = [""] * len(self.__all_experiments)
        self.__lease_duration = pd.to_timedelta(lease_timout)

        self.__non_parameter_fields = {'status', 'last_update', 'client'}

    @property
    def all_experiments(self)-> pd.DataFrame:
        """
        :return: The PandasFrame containing the details for all the experiments in the queue
        """
        return self.__all_experiments

    def __str__(self):
        return str(self.__all_experiments)

    @property
    def completed_percent(self):
        return float(len(self.__all_experiments[self.all_experiments.status == self.DONE])) / len(self.__all_experiments)

    @property
    def leased_percent(self):
        return float(len(self.__all_experiments[self.all_experiments.status == self.LEASED])) / len(self.__all_experiments)

    @property
    def experiment_parameters(self)-> list:
        return [c for c in self.__all_experiments.columns if c not in self.__non_parameter_fields]

    def lease_new(self, client_name: str) -> tuple:
        """
        Lease a new experiment lock. Select first any waiting experiments and then re-lease expired ones
        :param client_name: The name of the leasing client
        :return: a tuple (id, parameters) or None if nothing is available
        """
        available = self.__all_experiments[self.all_experiments.status == self.WAITING]

        if len(available) == 0:
            available = self.__all_experiments[(self.__all_experiments.status == self.LEASED) &
             (self.__all_experiments.last_update + self.__lease_duration < pd.Timestamp('now'))]

        if len(available) == 0:
            return None

        #Pick the first non-leased element
        leased_params = json.loads(available.iloc[0].to_json())
        selected_id = int(available.index[0])

        if self.__all_experiments.status.loc[selected_id] == self.LEASED:
            print("Re-leasing experiment %s since it expired" % selected_id)

        self.__all_experiments.loc[selected_id, 'status'] = self.LEASED
        self.__all_experiments.loc[selected_id, 'last_update'] = pd.Timestamp('now')
        self.__all_experiments.loc[selected_id, 'client'] = client_name

        for k in self.__non_parameter_fields:
            del leased_params[k]
        return leased_params, selected_id

    def complete(self, experiment_id: int, parameters: dict, client: str):
        selected_experiment = self.__all_experiments.iloc[experiment_id]
        original_params = selected_experiment.to_dict()

        for k in self.__non_parameter_fields:
            del original_params[k]
        assert original_params == parameters, "Experiment Parameters do not match!"

        if selected_experiment.client != client:
            print("Experiment returned from non-leased (or expired) client")

        self.__all_experiments.loc[experiment_id, 'status'] = self.DONE
        self.__all_experiments.loc[experiment_id, 'last_update'] = pd.Timestamp('now')
        self.__all_experiments.loc[experiment_id, 'client'] = client
        # TODO: Add duration of experiment
