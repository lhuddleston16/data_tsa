from pandas import isnull

class Inspector:

    def __init__(self, series):
        '''Inspects a pandas.Series object for data quality & consitency.

        Args:
            series (pandas.Series): A pandas.Series object
        '''
        self.series = series

    def get_row_count(self):
        '''Returns the number of items.'''
        return len(self.series)

    def get_null_ratio(self):
        '''Returns the percentage of numpy.NaN values out of all values.'''
        return len(self.series[isnull(self.series)]) / self.get_row_count()

    def get_min_value(self):
        '''Returns the minimum value.'''
        return self.series.min()

    def get_max_value(self):
        '''Returns the maximum value.'''
        return self.series.max()

    def core_inspect(self):
        insp = {}
        insp['row_count'] = self.get_row_count()
        insp['null_ratio'] = self.get_null_ratio()
        insp['min_value'] = self.get_min_value()
        insp['max_value'] = self.get_max_value()
        return insp

    def inspect(self):
        '''Inspects the provided pandas.Series

        Returns:
            Dictionary containing measures and values
        '''
        return self.core_inspect()
