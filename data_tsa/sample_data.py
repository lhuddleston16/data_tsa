import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class SampleData:

    def __init__(self, n):
        '''Generates a sample pandas.DataFrame.

        Args:
            n (int): The desired length of the example DataFrame.

        Returns:
            pandas.DataFrame
        '''
        if n < 30:
            print('WARNING: n values under 30 may result in incomplete test sets.')
        self.n = n
        self.df = pd.DataFrame()

    def _generate_list_from_domain(self, domain):
        return [np.random.choice(domain) for _ in range(self.n)]

    def _get_id(self, name):
        self.df[name] = np.arange(self.n).tolist()

    def _get_partial_null(self, name):
        d = [np.NaN, 0]
        self.df[name] = self._generate_list_from_domain(d)

    def _get_created_at(self, name):
        base = datetime(2018, 1, 1)
        arr = [base + timedelta(hours=i*6) for i in range(self.n)]
        self.df[name] = arr

    def _get_string_slicer(self, name):
        d = ['A', 'B', 'C', 'D']
        self.df[name] = self._generate_list_from_domain(d)

    def _get_duplicate_string(self, name):
        d = ['Test', 'test', 'testing', 'Testing', 'test    ']
        self.df[name] = self._generate_list_from_domain(d)

    def _get_mixed_precision_datetime(self, name):
        d = [datetime(2019, 1, 1),
             datetime(2019, 1, 1, 1),
             datetime(2019, 1, 1, 1, 1),
             datetime(2019, 1, 1, 1, 1, 1)]
        self.df[name] = self._generate_list_from_domain(d)

    def _get_date_string(self, name):
        d = ['1/1/2019',
             '1/1/2019 12:00',
             '1/1/2019 12:00:00']
        self.df[name] = self._generate_list_from_domain(d)

    def _get_mixed_sign_numbers(self, name):
        d = [-1, 1]
        self.df[name] = self._generate_list_from_domain(d)

    def get_sample_data(self):
        '''Generates a sample pandas.DataFrame'''
        self._get_id('id')
        self._get_partial_null('partial_null')
        self._get_created_at('created_at')
        self._get_duplicate_string('duplicate_string')
        self._get_string_slicer('string_slicer')
        self._get_mixed_precision_datetime('mixed_precision_datetime')
        self._get_date_string('date_string')
        self._get_mixed_sign_numbers('mixed_sign_numbers')
        return self.df.copy()

if __name__ == '__main__':
    t = SampleData(30)
    df = t.get_sample_data()
    df.head()
