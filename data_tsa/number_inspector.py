'''
This module contains the NumberInspector class, as well as attributes
for distinguishing number and float numpy dtypes.

Attributes:
    number_dtypes (list): A list of all numpy int and float dtypes.
    float_dtypes (list): A list of all numpy float dtypes.
'''

import numpy as np
from data_tsa.inspector import Inspector

number_dtypes = [np.int,
                 np.int0,
                 np.int8,
                 np.int16,
                 np.int32,
                 np.int64,
                 np.float,
                 np.float16,
                 np.float32,
                 np.float64]

float_dtypes =  [np.float,
                 np.float16,
                 np.float32,
                 np.float64]

class NumberInspector(Inspector):

    def __init__(self, series):
        '''Inspects a numerical pandas.Series object for quality & consistency.

        Args:
            series (pandas.Series): A pandas.Series object
        '''
        super().__init__(series)

    def get_negative_ratio(self):
        '''Returns the percentage of negative values out of all values.'''
        return self.series[self.series < 0].count() / self.get_row_count()

    def get_float_indicator(self):
        '''Returns True if the series dtype is a float.'''
        return 1 if self.series.dtypes in float_dtypes else 0

    def get_mean_value(self):
        '''Returns the mean value of the series.'''
        return self.series.mean()

    def get_median_value(self):
        '''Returns the median value of the series.'''
        return self.series.median()

    def get_mode(self):
        '''Returns the mode of the series.'''
        return self.series.mode()#[0]

    def get_stdev(self):
        '''Returns the standard deviation of the series.'''
        return self.series.std()

    def get_zero_ratio(self):
        '''Returns the percentage of zero values out of all values.'''
        return self.series[self.series == 0].count() / self.get_row_count()

    def get_top_five_value_counts(self):
        '''Returns a dictionary of the top five values by count.'''
        return self.series.value_counts().nlargest(5).to_dict()

    def get_bottom_five_value_counts(self):
        '''Returns a dictionary of the bottom five values by count.'''
        return self.series.value_counts().nsmallest(5).to_dict()

    def get_value_skew(self):
        '''Returns an indicator of data skew.'''
        if sum(self.series.value_counts().nlargest(5)) == 0:
            return None
        return  sum(self.series.value_counts().nsmallest(5)) \
                /sum(self.series.value_counts().nlargest(5))

    def inspect(self):
        '''Inspects the provided pandas.Series

        Returns:
            Dictionary containing measures and values
        '''
        insp = self.core_inspect()
        insp['min_value'] = self.get_min_value()
        insp['max_value'] = self.get_max_value()
        insp['negative_ratio'] = self.get_negative_ratio()
#         insp['float_indicator'] = self.get_float_indicator()
        insp['mean_value'] = self.get_mean_value()
        insp['median_value'] = self.get_median_value()
#         insp['mode'] = self.get_mode()
        insp['stdev'] = self.get_stdev()
        insp['zero_ratio'] = self. get_zero_ratio()
        insp['top_five_value_counts'] = self. get_top_five_value_counts()
        insp['bottom_five_value_counts'] = self.get_bottom_five_value_counts()
        insp['value_skew'] = self.get_value_skew()
        return insp
