import numpy as np
from data_tsa.dataframe_inspector import DataFrameInspector
from data_tsa.inspector import Inspector

class Profiler:
    
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.type_exceptions = []
        
    def set_type_exception(self, column, dtype):
        self.validate_column(column)
        if dtype not in ('string', 'date', 'number'):
            raise ValueError('\'dtype\' must be \'string\', \'datetime\', or \'number\'')
        self.type_exceptions.append((column, dtype))
        
    def validate_column(self, column):
        if column not in self.dataframe.columns:
            raise KeyError('\'{}\' not found in dataframe!'.format(column))
        
    def get_type_exception(self, column):
        cols = [_[0] for _ in self.type_exceptions]
        dtypes = [_[1] for _ in self.type_exceptions]
        if column in cols:
            return dtypes[cols.index(column)]
        
    def get_column_dtype(self, column):  
        type_exception = self.get_type_exception(column)
        if type_exception:
            return type_exception
        self.validate_column(column)
        number_types = [np.int,
                        np.int0,
                        np.int8,
                        np.int16,
                        np.int32,
                        np.int64,
                        np.float,
                        np.float16,
                        np.float32,
                        np.float64]
        dtype = self.dataframe[column].dtype.type
        if dtype == np.object_:
            return 'string'
        elif dtype == np.datetime64:
            return 'datetime'
        elif dtype in number_types:
            return 'number'
        else:
            return ValueError('\'{}\' did not resolve to a type of \'string\', \'datetime\', or \'number\''.format(column))
        
    def profile(self):
        print('profiling dataframe...')
        print('Exceptions:')
        for _ in self.type_exceptions:
            print('\t{} ({})'.format(_[0], _[1]))
    
        print('Column dtypes:')
        for col in self.dataframe.columns:
            print('\t{} ({})'.format(col, self.get_column_dtype(col)))
        
        print('Duplicate rows detected:')
        df_insp = DataFrameInspector(self.dataframe)
        print('\t', df_insp.get_duplicate_row_indicator())
            
        print('Profiling columns:')
        for col in self.dataframe.columns:
            print('-', col)
            insp = Inspector(self.dataframe[col])
            for k, v in insp.inspect().items():
                print('\t', k, '>', v)