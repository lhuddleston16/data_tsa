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

    def get_distinct_count(self):
        '''Returns the number of distinct values'''
        return len(self.series.unique())

    def get_null_ratio(self):
        '''Returns the percentage of numpy.NaN values out of all values.'''
        return len(self.series[isnull(self.series)]) / self.get_row_count()

    def get_min_value(self):
        '''Returns the minimum value.'''
        try:
            return self.series.min()
        except Exception:
            return None

    def get_max_value(self):
        '''Returns the maximum value.'''
        try:
            return self.series.max()
        except Exception:
            return None

    def core_inspect(self):
        insp = {}
        insp['row_count'] = self.get_row_count()
        insp['distinct_count'] = self.get_distinct_count()
        insp['null_ratio'] = self.get_null_ratio()
        return insp

    def inspect(self):
        '''Inspects the provided pandas.Series

        Returns:
            Dictionary containing measures and values
        '''
        return self.core_inspect()
