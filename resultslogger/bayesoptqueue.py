from collections import OrderedDict
from typing import List, Tuple, Dict, Any

import pandas as pd
from functools import reduce
from typing import List
import warnings
import json

import numpy as np
from skopt import Optimizer
from skopt.acquisition import gaussian_ei, _gaussian_acquisition
from skopt.learning import GaussianProcessRegressor

from skopt.learning.gaussian_process.kernels import ConstantKernel
from skopt.learning.gaussian_process.kernels import HammingKernel
from skopt.learning.gaussian_process.kernels import Matern
from skopt.space import check_dimension
from skopt.space import Categorical, Real, Integer
from skopt.space import Space
from sklearn.base import clone

from resultslogger.experimentqueue import ExperimentQueue


class BayesianOptimizedExperimentQueue(ExperimentQueue):
    def __init__(self, dimensions_file: str, min_num_results_to_fit: int=8, lease_timout='2 days'):
        self.__all_experiments = pd.DataFrame()
        self.__all_experiments['status'] = [self.WAITING] * len(self.__all_experiments)
        self.__all_experiments['last_update'] = pd.Series(pd.Timestamp(float('NaN')))
        self.__all_experiments['client'] = [""] * len(self.__all_experiments)

        self.__lease_duration = pd.to_timedelta(lease_timout)
        self.__leased_experiments = []

        dims = self.__load_dimensions(dimensions_file)
        self.__dimension_names = list(dims.keys())
        self.__dimensions = list(dims.values())
        self.__min_num_results_to_fit = min_num_results_to_fit

        # Initialize

        dim_types = [check_dimension(d) for d in self.__dimensions]
        is_cat = all([isinstance(check_dimension(d), Categorical) for d in dim_types])
        if is_cat:
            transformed_dims = [check_dimension(d, transform="identity") for d in self.__dimensions]
        else:
            transformed_dims = []
            for dim_type, dim in zip(dim_types, self.__dimensions):
                if isinstance(dim_type, Categorical):
                    transformed_dims.append(check_dimension(dim, transform="onehot"))
                # To make sure that GP operates in the [0, 1] space
                else:
                    transformed_dims.append(check_dimension(dim, transform="normalize"))

        space = Space(transformed_dims)
        # Default GP
        cov_amplitude = ConstantKernel(1.0, (0.01, 1000.0))

        if is_cat:
            other_kernel = HammingKernel(length_scale=np.ones(space.transformed_n_dims))
            acq_optimizer = "lbfgs"
        else:
            other_kernel = Matern(
                length_scale=np.ones(space.transformed_n_dims),
                length_scale_bounds=[(0.01, 100)] * space.transformed_n_dims,
                nu=2.5)

        base_estimator = GaussianProcessRegressor(
            kernel=cov_amplitude * other_kernel,
            normalize_y=True, random_state=None, alpha=0.0, noise='gaussian',
            n_restarts_optimizer=2)

        self.__opt = Optimizer(self.__dimensions, base_estimator, acq_optimizer="lbfgs",
                               n_random_starts=100, acq_optimizer_kwargs=dict(n_points=10000))

    @property
    def all_experiments(self) -> pd.DataFrame:
        """
        :return: The PandasFrame containing the details for all the experiments in the queue.
        """
        return self.__all_experiments

    @property
    def completed_percent(self) -> float:
        return 0.

    @property
    def leased_percent(self) -> float:
        return 0

    @property
    def experiment_parameters(self) -> List:
        return self.__dimension_names

    def lease_new(self, client_name: str) -> Tuple[int, Dict]:
        """
        Lease a new experiment lock. Select first any waiting experiments and then re-lease expired ones
        :param client_name: The name of the leasing client
        :return: a tuple (id, parameters) or None if nothing is available
        """
        experiment_params = self.__opt.ask()
        if experiment_params in self.__leased_experiments:
            experiment_params = self.__compute_alternative_params()
        self.__leased_experiments.append(experiment_params)
        # TODO: Add to all experiments, use Ids

        def parse_dim_val(value, dim_type):
            if type(dim_type) is Real:
                return float(value)
            elif type(dim_type) is Integer:
                return int(value)
            return value
        return {name: parse_dim_val(value, dim_type) for name, dim_type, value in zip(self.__dimension_names, self.__dimensions, experiment_params)}, -1

    def __compute_alternative_params(self):
        # Copied directly from skopt
        transformed_bounds = np.array(self.__opt.space.transformed_bounds)
        est = clone(self.__opt.base_estimator)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            est.fit(self.__opt.space.transform(self.__opt.Xi), self.__opt.yi)

        X = self.__opt.space.transform(self.__opt.space.rvs(
            n_samples=self.__opt.n_points, random_state=self.__opt.rng))

        values = _gaussian_acquisition(X=X, model=est, y_opt=np.min(self.__opt.yi),
                                       acq_func='EI',
                                       acq_func_kwargs=dict(n_points=10000))

        print('original point ei: %s' % np.min(values))
        discount_width = .5
        values = self.__discount_leased_params(X, values, discount_width)
        while np.min(values) > -1e-5 and discount_width > 1e-2:
            discount_width *= .9
            values = _gaussian_acquisition(X=X, model=est, y_opt=np.min(self.__opt.yi),
                                           acq_func='EI',
                                           acq_func_kwargs=dict(n_points=10000))
            values = self.__discount_leased_params(X, values, discount_width)
        next_x = X[np.argmin(values)]
        print('new point ei: %s' % np.min(values))

        if not self.__opt.space.is_categorical:
            next_x = np.clip(next_x, transformed_bounds[:, 0], transformed_bounds[:, 1])

        return self.__opt.space.inverse_transform(next_x.reshape((1, -1)))[0]

    @staticmethod
    def leased_discount(center, width, x_values):
        """Triangular (cone) discount"""
        distance_from_center = np.linalg.norm(x_values - center, 2, axis=1)
        discount = -distance_from_center / width + 1
        discount[discount < 0] = 0
        return discount

    def __discount_leased_params(self, X, values, discount_width_size):
        transformed_leased_params = self.__opt.space.transform(np.array(self.__leased_experiments))
        discount_factor = reduce(lambda x, y: x * y,
                                 (self.leased_discount(p, discount_width_size, X) for p in self.__leased_experiments),
                                 np.ones(values.shape[0]))
        out_vals = values * (1. - discount_factor)
        return out_vals

    def complete(self, experiment_id: int, parameters: Dict, client: str, result: float = 0) -> None:
        """
        Declare an experiment to be completed.
        :param experiment_id: the id of the experiment or -1 if unknown
        :param client: the client id
        :param result: the output results of the experiment. This may be used in optimizing queues.
        """
        parameters = [parameters[n] for n in self.__dimension_names]
        if parameters in self.__leased_experiments:
            self.__leased_experiments.remove(parameters)
        do_fit_model = len(self.__opt.yi) >= self.__min_num_results_to_fit
        # Unfortunate hack: this depends on the internals.
        if do_fit_model:
            self.__opt._n_random_starts = 0  # Since we have adequately many results, stop using random
        self.__opt.tell(parameters, result, fit=do_fit_model)

    def __load_dimensions(self, dimensions_file:str)->Dict:
        with open(dimensions_file) as f:
            dimensions = json.load(f)

        def parse_dimension(specs: Dict[str, Any]):
            if specs['type'] == 'Real':
                return specs['name'], Real(specs['low'], specs['high'])
            elif specs['type'] == 'Integer':
                return specs['name'], Integer(specs['low'], specs['high'])
            elif specs['type'] == 'Categorical':
                return specs['name'], Categorical(specs['categories'])
            else:
                raise Exception('Unrecognized dimension type %s' % specs['type'])

        return OrderedDict([parse_dimension(d) for d in dimensions])