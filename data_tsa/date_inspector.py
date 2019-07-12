from data_tsa.inspector import Inspector
from numpy import datetime64, object_
from pandas import to_datetime

class DateInspector(Inspector):

    def __init__(self, series):
        '''Inspects a DateTime pandas.Series object for quality & consistency.

        Args:
            series (pandas.Series): A pandas.Series object
        '''
        super().__init__(series)

    def get_conversion_required_indicator(self):
        '''Returns 1 if the series is not currently a DateTime type.'''
        if self.series.dtypes.type != datetime64:
            self.series = to_datetime(self.series, errors='ignore')
            return 1
        return 0

    def get_conversion_error_indicator(self):
        '''Returns 1 if the series cannot be converted to a DateTime type.'''
        if self.series.dtype.type == object_:
                return 1
        return 0
    
    def get_precision_variance(self):
        '''Returns a dictionary with the proportional frequency of different
        precisions.
        '''
        if self.get_conversion_error_indicator() == 1:
            return None
        
        # microseconds
        x = [_ for _ in self.series if _.microsecond != 0]
        x = len(x)

        # second
        s = [_ for _ in self.series if _.microsecond == 0 and _.second != 0]
        s = len(s)

        # minute
        m = [_ for _ in self.series if _.microsecond == 0 and _.second == 0 \
                                 and _.minute != 0]
        m = len(m)

        # hour
        h = [_ for _ in self.series if _.microsecond == 0 and _.second == 0 \
                                 and _.minute == 0 and _.hour != 0]
        h = len(h)

        result = {}
        result['microsecond'] = x / len(self.series)
        result['second'] = s / len(self.series)
        result['minute'] = m / len(self.series)
        result['hour'] = h / len(self.series)
        result['day'] = (len(self.series) - x - s - m - h) / len(self.series)
        return result

    def inspect(self):
        '''Inspects the provided pandas.Series

        Returns:
            Dictionary containing measures and values
        '''
        insp = self.core_inspect()
        insp['conversion_error_indicator'] = self.get_conversion_required_indicator()
        insp['min_value'] = self.get_min_value()
        insp['max_value'] = self.get_max_value()
#         result['conversion_required'] = self.get_conversion_required_indicator()
#         result['precision_variance'] = self.get_precision_variance()
        return insp
