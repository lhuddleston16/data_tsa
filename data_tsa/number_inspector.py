import numpy as np
from data_tsa import Inspector

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
        super().__init__(series)
    
    def get_non_negative_count(self):
        return self.series[self.series <0].count()
    
    def get_float_indicator(self):
        return self.series.dtypes in float_dtypes
    
    def get_mean_value(self):
        return self.series.mean()
    
    def get_median_value(self):
        return self.series.median()
    
    def get_mode(self):
        return self.series.mode()[0]
    
    def get_stdev(self):
        return self.series.std()
    
    def get_zero_count(self):
        return self.series[self.series == 0].count()
    
    def get_top_five_value_counts(self):
        return self.series.value_counts().nlargest(5).to_dict()
    
    def get_bottom_five_value_counts(self):
        return self.series.value_counts().nsmallest(5).to_dict()
    
    def get_value_skew(self):
        return  sum(self.series.value_counts().nlargest(5)) \
                /sum(self.series.value_counts().nsmallest(5))

    def inspect(self):
        insp = self.core_inspect()
        insp['non_negative_count'] = self.get_non_negative_count()
        insp['float_indicator'] = self.get_float_indicator()
        insp['mean_value'] =self.get_mean_value()
        insp['median_value'] =self.get_median_value()
        insp['mode'] =self.get_mode() 
        insp['stdev'] =self.get_stdev()
        insp['zero_count'] =self. get_zero_count()
        insp['top_five_value_counts'] =self. get_top_five_value_counts()
        insp['bottom_five_value_counts'] =self. get_bottom_five_value_counts()
        insp['value_skew'] =self.get_value_skew()
        return insp
    