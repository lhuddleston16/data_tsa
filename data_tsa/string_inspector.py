from data_tsa.inspector import Inspector
from re import search

class StringInspector(Inspector):

    def __init__(self, series):
        '''Inspects a pandas.Series object with string data types.

        Args:
            series (pandas.Series): A pandas.Series object
        '''
        super().__init__(series)

    def get_distinct_count(self):
        '''Returns the number of distinct values.'''
        return len(self.series.unique())

    def _get_standardized_values(self):
        s = self.series.tolist()
        return [_.lower().strip() for _ in s]

    def _re_search(self, pattern):
        return [_ for _ in self.series if search(pattern, _)]

    def get_strict_distinct_count(self):
        '''Returns the count of normalized distinct values.'''
        return len(set(self._get_standardized_values()))

    def get_redundancy_indicator(self):
        '''Returns 1 if redundant values are detected.'''
        if self.get_distinct_count() > self.get_strict_distinct_count():
            return 1
        return 0

    def get_empty_ratio(self):
        '''Returns the percentage of empty ('') values out of all values.'''
        return len([_ for _ in self.series if _ == '']) / self.get_row_count()

    def get_special_character_ratio(self):
        '''Returns the percentage of rows with special characters out of
        all values.
        '''
        return len(self._re_search(r'[^A-Za-z0-9\s]')) / self.get_row_count()

    def get_email_ratio(self):
        '''Returns the percentage of email addresses out of all values.'''
        email_count = len(self._re_search(r'[^@]+@[^@]+\.[^@]+'))
        return email_count / self.get_row_count()

    def get_trim_required_ratio(self):
        '''Returns the percentage of records with extra whitespace out
        of all values.
        '''
        trim_req = len([_ for _ in self.series if _ and (_[0] == ' ' or
                                                         _[-1] == ' ')])
        return trim_req / self.get_row_count()

    def inspect(self):
        '''Inspects the provided pandas.Series

        Returns:
            Dictionary containing measures and values
        '''
        result = self.core_inspect()
        result['distinct_count'] = self.get_distinct_count()
        result['strict_distinct_count'] = self.get_strict_distinct_count()
        result['empty_ratio'] = self.get_empty_ratio()
        result['special_character_ratio'] = self.get_special_character_ratio()
        result['email_ratio'] = self.get_email_ratio()
        result['trim_required_ratio'] = self.get_trim_required_ratio()
        result['redundancy_indicator'] = self.get_redundancy_indicator()
        return result
