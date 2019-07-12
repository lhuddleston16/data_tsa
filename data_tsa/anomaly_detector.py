from data_tsa.profiler import Profiler
from re import findall
from pandas import Series, DataFrame

class AnomalyDetector:    
    
    def __init__(self, profiler, target_slice=None):
        '''Detects anomalies for metrics derived by a data_tsa.Profiler object.
        
        Args:
            profiler (data_tsa.Profiler): A profiler object that has generated a
                data quality profile of some input DataFrame.
            target_slice (str): A specific slice to evaluate. The default value is
                the last slice in the profiler.result DataFrame.
        '''
        self.lag_col_template = 'l{}_measure_value'
        self.default_abs_perc_delta_threshold = 0.1
        
        self.profiler = self._validate_profiler(profiler)
        self.dataframe = self._get_target_slice_dataframe(target_slice)
        self.lags = self._get_lags()
        self.ad_dataframe =  DataFrame(columns=['inspector',
                                                'column',
                                                'slice',
                                                'measure',
                                                'rule',
                                                'reference_lags',
                                                'flag',
                                                'anomaly_score'])
    
    def _validate_profiler(self, profiler):
        '''Verifies that the provided profiler is of the correct type.'''
        if type(profiler) != Profiler:
            raise TypeError('\'profiler\' argument must be a data_tsa.Profiler object.')
        return profiler
    
    def _get_target_slice_dataframe(self, target_slice):
        '''If no target_slice is provided, returns a default value.'''
        if not target_slice:
            target_slice = self._get_max_slice()
        return self.profiler.result[self.profiler.result['slice']==target_slice]
    
    def _get_max_slice(self):
        '''Returns the last slice in the profile.result DataFrame.'''
        slices = self.profiler.result['slice'].unique().tolist()
        slices.sort()
        return slices[-1]
        
    def _get_lags(self):
        '''Returns the number of lags calculated by the data_tsa.profiler object.'''
        lag_cols = [_ for _ in self.dataframe.columns if _[-14:] == '_measure_value']
        return max([int(''.join(findall(r'[0-9]', _))) for _ in lag_cols])
        
    def _validate_lag_value(self, row, lag):
        '''Verifies that all of the lagging metrics contain values.'''
        for l in range(1, lag + 1):
            if row[self.lag_col_template.format(lag)] is None:
                return False
        return True
        
    def _get_lag_columns(self, lag):
        '''Returns a list of lagging column names.'''
        return [self.lag_col_template.format(_) for _ in range(1, lag + 1)]
        
    class Decorators:
        '''Defines the decorators used by the AnomalyDetector class.'''
        
        def lag_iterator(func):
            '''Applies a rule function over multiple series of lags.
            
            Iterates through each span of lags, calling the provided function 
            on each one, and then writing the results to the self.ad_dataframe object.
            
            Args:
                func (function): A function to be applied row-wise to a dataframe.
            '''
            def inner(self, row, *args, **kwargs):
                for lag in range(1, self.lags + 1):
                    lag_cols = self._get_lag_columns(lag)
                    if not self._validate_lag_value(row, lag):
                        break
                    flag = func(self, row, lag_cols, *args, **kwargs)
                    anomaly_row = Series({'inspector': row['inspector'],
                                          'column': row['column'],
                                          'slice': row['slice'],
                                          'measure': row['measure'],
                                          'rule': func.__name__,
                                          'reference_lags': lag,
                                          'flag': flag,
                                          'anomaly_score': lag * flag})
                    self.ad_dataframe = self.ad_dataframe.append(anomaly_row, ignore_index=True)
            return inner
        
    @Decorators.lag_iterator
    def get_zero_ratio_flag(self, row, lag_cols):
        '''Returns 1 if the current slice is zero, but all lags are non-zero; else 0.'''
        non_zero_lags = [row[_] for _ in lag_cols if row[_] != 0]
        if len(non_zero_lags) == len(lag_cols) and row['measure_value'] == 0:
            return 1
        return 0
        
    @Decorators.lag_iterator
    def get_single_value_flag(self, row, lag_cols):
        '''Returns 1 if the current slice equals 1, but all lags are greater than 1; else 0.'''
        non_zero_lags = [row[_] for _ in lag_cols if row[_] > 1]
        if len(non_zero_lags) == len(lag_cols) and row['measure_value'] == 1:
            return 1
        return 0
        
    @Decorators.lag_iterator
    def get_positive_ratio_flag(self, row, lag_cols):
        '''Returns 1 if the current slice is non-zero, but all lags are zero; else 0'''
        zero_lags = [row[_] for _ in lag_cols if row[_] == 0]
        if len(zero_lags) == len(lag_cols) and row['measure_value'] != 0:
            return 1
        return 0
        
    @Decorators.lag_iterator
    def get_abs_perc_error_flag(self, row, lag_cols, threshold=None):
        '''Measures the error between the current slice and lags.
        
        Returns 1 if the absolute percentage error between the current slice and the average 
        of the lagging values is greater than the specified threshold.
        
        Args:
            threshold (float): defines the absolute percentage error above which this
                function will return 1.
        '''
        if not threshold:
            threshold = self.default_abs_perc_delta_threshold
        lag_mean = sum([row[_] for _ in lag_cols]) / len(lag_cols)
        if lag_mean == 0 and row['measure_value'] != 0:
            return 1
        if lag_mean == 0 and row['measure_value'] == 0:
            return 0
        if abs((row['measure_value'] - lag_mean) / lag_mean) > threshold:
            return 1
        return 0
        
    @Decorators.lag_iterator
    def get_consistency_flag(self, row, lag_cols, greater_than=1):
        '''Measures the consistency of aggregate values.
        
        Returns 1 if the max (min) value for the current slice is 
        greater than (less than) the max (min) value from prior slices. 
        
        Args:
            greater_than (int): default value = 1; determines the direction
                of the consistency check. 1 will check greater than, while
                any other value will check less-than.
        '''
        lags = [row[_] for _ in lag_cols]
        if greater_than:
            lag_ref = max(lags)
        else:
            lag_ref = min(lags)
        if greater_than == 1 and row['measure_value'] < lag_ref:
            return 1
        if greater_than != 1 and row['measure_value'] > lag_ref:
            return 1
        return 0
    
    def get_filtered_df(self, measure, inspector):
        '''Returns a filtered self.dataframe object
        
        Args:
            measure (str): required; specifies a measure value on which to filter.
            inspector (str): specifies an inspector value on which to filter.
        '''
        if not inspector:
            return self.dataframe[self.dataframe['measure']==measure]
        return self.dataframe[(self.dataframe['inspector']==inspector) & \
                  (self.dataframe['measure']==measure)]
    
    def apply_rule(self, measure, rule_func, inspector=None, args=()):
        '''Applies a rule to a measure.
        
        Args:
            measure (str): a target measure to evaluate.
            rule_func (function): a function to be applied.
            inspector (str): a target inspector to evaluate.
            args (tuple): arguments to be passed to the rule_func.
        '''
        df = self.get_filtered_df(measure, inspector=inspector)
        df.apply(rule_func, 1, args=args)
        
    def summary(self):
        '''Returns a summary dataframe of the anomaly detection outcome.'''
        cols = ['column', 'anomaly_score']
        df = self.ad_dataframe[cols].groupby('column').sum().reset_index()
        return df.sort_values('anomaly_score', ascending=0)
    
    def column_summary(self, column):
        '''Returns the anomaly detection outcome for a specific column.'''
        return self.ad_dataframe[self.ad_dataframe['column']==column]
    
    def rule_summary(self, rule):
        '''Returns the anomaly detection outcome for a specific rule function.'''
        return self.ad_dataframe[self.ad_dataframe['rule']==rule]
    
    def detect(self):
        '''Detects anomalies and returns a summary dataframe.'''
        self.apply_rule('null_ratio', self.get_positive_ratio_flag)
        self.apply_rule('null_ratio', self.get_zero_ratio_flag)
        self.apply_rule('row_count', self.get_abs_perc_error_flag, args=(1,))
        self.apply_rule('distinct_count', self.get_single_value_flag)
        
        self.apply_rule('empty_ratio', self.get_positive_ratio_flag, inspector='string')
        self.apply_rule('empty_ratio', self.get_zero_ratio_flag, inspector='string')
        self.apply_rule('redundancy_indicator', self.get_positive_ratio_flag, inspector='string')
        self.apply_rule('redundancy_indicator', self.get_zero_ratio_flag, inspector='string')
        self.apply_rule('special_character_ratio', self.get_positive_ratio_flag, inspector='string')
        self.apply_rule('special_character_ratio', self.get_zero_ratio_flag, inspector='string')
        self.apply_rule('trim_required_ratio', self.get_positive_ratio_flag, inspector='string')
        self.apply_rule('trim_required_ratio', self.get_zero_ratio_flag, inspector='string')
        
        self.apply_rule('max_value', self.get_abs_perc_error_flag, inspector='number', args=(1,))
        self.apply_rule('min_value', self.get_abs_perc_error_flag, inspector='number', args=(1,))
        self.apply_rule('mean_value', self.get_abs_perc_error_flag, inspector='number', args=(1,))
        self.apply_rule('median_value', self.get_abs_perc_error_flag, inspector='number', args=(1,))
        self.apply_rule('stdev', self.get_abs_perc_error_flag, inspector='number', args=(1,))
        self.apply_rule('value_skew', self.get_abs_perc_error_flag, inspector='number', args=(1,))
        self.apply_rule('negative_ratio', self.get_positive_ratio_flag, inspector='number')
        self.apply_rule('negative_ratio', self.get_zero_ratio_flag, inspector='number')
        self.apply_rule('zero_ratio', self.get_positive_ratio_flag, inspector='number')
        self.apply_rule('zero_ratio', self.get_zero_ratio_flag, inspector='number')
        
        self.apply_rule('max_value', self.get_consistency_flag, inspector='datetime')
        self.apply_rule('min_value', self.get_consistency_flag, inspector='datetime', args=(0,))
        
        self.apply_rule('false_ratio', self.get_positive_ratio_flag, inspector='bool')
        self.apply_rule('false_ratio', self.get_zero_ratio_flag, inspector='bool')
        self.apply_rule('true_ratio', self.get_positive_ratio_flag, inspector='bool')
        self.apply_rule('true_ratio', self.get_zero_ratio_flag, inspector='bool')
        
        self.ad_dataframe = self.ad_dataframe[self.ad_dataframe['anomaly_score']!=0]
        return self.summary()
    