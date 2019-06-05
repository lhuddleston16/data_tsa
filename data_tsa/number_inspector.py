from data_tsa.inspector import Inspector

class NumberInspector(Inspector):
    
    def __init__(self):
        super().__init__(*args, **kwargs)
        pass
    
    def get_non_negative_count(self):
        pass
    
    def get_float_indicator(self):
        pass
    
    def get_mean_value(self):
        pass
    
    def get_median_value(self):
        pass
    
    def get_mode(self):
        pass
    
    def get_stdev(self):
        pass
    
    def get_zero_count(self):
        pass
    
    def get_top_five_value_counts(self):
        pass
    
    def get_bottom_five_value_counts(self):
        pass
    
    def get_value_skew(self, n=5):
        pass
