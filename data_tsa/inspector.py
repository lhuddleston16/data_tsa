from pandas import isnull

class Inspector:
    
    def __init__(self, series):
        self.series = series
        
    def get_row_count(self):
        return len(self.series)
        
    def get_null_count(self):
        return len(self.series[isnull(self.series)])
    
    def get_min_value(self):
        return self.series.min()
    
    def get_max_value(self):
        return self.series.max()
    
    def inspect(self):
        insp = {}
        insp['row_count'] = self.get_row_count()
        insp['null_count'] = self.get_null_count()
        insp['min_value'] = self.get_min_value()
        insp['max_value'] = self.get_max_value()
        return insp