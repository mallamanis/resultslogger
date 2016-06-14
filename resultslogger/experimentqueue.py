import pandas
import time

class ExperimentQueue:

    DONE = 'DONE'
    WAITING = 'WAITING'
    LEASED = 'LEASED'

    def __init__(self, list_of_experiments_path: str, lease_timout_secs: int=172800):
        """
        :param list_of_experiments_path: The path to a csv file containing all possible experiments.
        """
        self.__all_experiments = pandas.read_csv(list_of_experiments_path)
        self.__all_experiments['id'] = pandas.Series(data=list(range(len(self.all_experiments))))
        self.__all_experiments.set_index(['id'])
        self.__all_experiments['status'] = [self.WAITING] * len(self.__all_experiments)
        self.__all_experiments['lease_time'] = [0] * len(self.__all_experiments)
        self.__all_experiments['lease_client'] = [""] * len(self.__all_experiments)
        self.__lease_duration = lease_timout_secs

        self.__non_parameter_fields = ['id', 'status', 'lease_time', 'lease_client']

    @property
    def all_experiments(self):
        return self.__all_experiments

    def __str__(self):
        return str(self.__all_experiments)

    def lease_new(self, client_name: str)-> tuple:
        """
        Lease a new experiment lock
        :param client_name: The name of the leasing client
        :return: a tuple (id, parameters) or None if nothing is available
        """
        available = self.__all_experiments[(self.all_experiments.status == self.WAITING) |
                                           ((self.__all_experiments.status == self.LEASED) &
                                            (self.__all_experiments.lease_time + self.__lease_duration < time.time())
                                            )]

        if len(available) == 0:
            return None

        #Pick the first element
        leased_params = available.iloc[0].to_dict()
        selected_id = available.iloc[0].id
        if self.__all_experiments.status.loc[selected_id] == self.LEASED:
            print("Re-leasing experiment %s since it expired" % selected_id)
        self.__all_experiments.status.loc[selected_id] = self.LEASED
        self.__all_experiments.lease_time.loc[selected_id] = int(time.time())
        self.__all_experiments.lease_client.loc[selected_id] = client_name

        for k in self.__non_parameter_fields:
            del leased_params[k]

        return (selected_id, leased_params)


    def complete(self, id, parameters, client):
        selected_experiment = self.__all_experiments.iloc[id]
        original_params = selected_experiment.to_dict()
        for k in self.__non_parameter_fields:
            del original_params[k]
        assert original_params == parameters, "Experiment Parameters do not match!"
        if self.__all_experiments.iloc[id].lease_client != client:
            print("Experiment returned from non-leased (or expired) client")

        self.__all_experiments.status.loc[id] = self.DONE
        self.__all_experiments.lease_time.loc[id] = int(time.time())
        self.__all_experiments.lease_client.loc[id] = client
