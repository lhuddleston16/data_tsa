from data_tsa.inspector import Inspector
from numpy import datetime64

class DateInspector(Inspector):
    
    def get_conversion_required_indicator(self):
        if self.series.dtypes.type != datetime64:
            self.series = self.series.astype(datetime64)
            return True
        return False
    
    def get_precision_variance(self):
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
        result = self.core_inspect()
        result['conversion_required'] = self.get_conversion_required_indicator()
        result['precision_variance'] = self.get_precision_variance()
        return result
    