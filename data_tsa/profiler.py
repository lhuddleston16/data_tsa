# -*- coding: utf-8 -*-

import numpy as np
from data_tsa import Inspector, DataFrameInspector, NumberInspector, \
    number_dtypes, StringInspector, DateInspector

class Profiler:
    
    def __init__(self, dataframe):
        '''Profiles the columns of a pandas.DataFrame.
        
        Args:
            dataframe (pandas.DataFrame): A pands DataFrame.
        '''
        self.dataframe = dataframe
        self.type_exceptions = []
        
    def set_type_exception(self, column, dtype):
        '''Specify custom target inspection types
        
        Args:
            column (str): column name
            dtype (str): 'string', 'number', or 'datetime'
        '''
        self.validate_column(column)
        if dtype not in ('string', 'date', 'number'):
            raise ValueError('\'dtype\' must be \'string\', \'datetime\', or \'number\'')
        self.type_exceptions.append((column, dtype))
        
    def validate_column(self, column):
        '''Verifies that a column exists in the provided DataFrame.
        
        Args:
            column (str): column name
            
        Returns:
            True if column exists in DataFrame, else False
        '''
        if column not in self.dataframe.columns:
            raise KeyError('\'{}\' not found in dataframe!'.format(column))
        
    def get_type_exception(self, column):
        '''Checks to see if a given column has a user-specified exception type.
        
        Args:
            column (str): column name
            
        Returns:
            'string', 'number', 'datetime', or None
        '''
        cols = [_[0] for _ in self.type_exceptions]
        dtypes = [_[1] for _ in self.type_exceptions]
        if column in cols:
            return dtypes[cols.index(column)]
        
    def get_column_dtype(self, column):  
        '''Returns the simple type of the provided column.
        
        Args:
            column (str): column name
            
        Returns:
            'string', 'number', or 'datetime'
            ValueError if simple type not detected
        '''
        type_exception = self.get_type_exception(column)
        if type_exception:
            return type_exception
        self.validate_column(column)
        dtype = self.dataframe[column].dtype.type
        if dtype == np.object_:
            return 'string'
        elif dtype == np.datetime64:
            return 'datetime'
        elif dtype in number_dtypes:
            return 'number'
        else:
            return ValueError('\'{}\' did not resolve to a type of \'string\', \'datetime\', or \'number\''.format(column))
        
    def profile(self):
        '''Executes profile of provided DataFrame.
        '''
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
            dtype = self.get_column_dtype(col)
            if dtype == 'string':
                insp = StringInspector(self.dataframe[col])
            elif dtype == 'number':
                insp = NumberInspector(self.dataframe[col])
            elif dtype == 'datetime':
                insp = DateInspector(self.dataframe[col])
            else:
                insp = Inspector(self.dataframe[col])
            for k, v in insp.inspect().items():
                print('\t', k, '>', v)