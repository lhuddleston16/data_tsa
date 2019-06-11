# -*- coding: utf-8 -*-

import numpy as np
from pandas import DataFrame
from data_tsa.inspector import Inspector
from data_tsa.number_inspector import NumberInspector, number_dtypes
from data_tsa.string_inspector import StringInspector
from data_tsa.date_inspector import DateInspector

class Profiler:

    def __init__(self, dataframe, slicer=None):
        '''Profiles the columns of a pandas.DataFrame.

        Args:
            dataframe (pandas.DataFrame): A pands DataFrame.
            slicer (str): Indicates a column containing logical ordered
                partition. When specified, the profiler will profile
                each partition in order and return the concatenated result
                set.
        '''
        self.dataframe = dataframe
        if slicer:
            self.validate_column(slicer)
        self.slicer = slicer
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

    def get_slicer_values(self):
        '''Returns a sorted list of unique slicer values.'''
        s = self.dataframe[self.slicer].unique().tolist()
        s.sort()
        return s

    def insp_dict_to_dataframe(self, column, inspection_dict):
        '''Tranforms and inspection dictionary into a pandas.DataFrame.'''
        d = {k: [v] for k, v in inspection_dict.items()}
        df = DataFrame(d).transpose().reset_index()
        df.columns = ['measure', 'measure_value']
        df['column'] = column
        return df[['column', 'measure', 'measure_value']]

    def profile(self, lags=None):
        '''Performs a column-wise evaluation of the columns in the dataframe.

        Args:
            lags (int): The number of lagging measure values to be added to
                the output table

        Returns:
            dataframe: A dataframe containing summary measures for each column.
        '''
        if self.slicer:
            slices = self.get_slicer_values()
        else:
            return self.profile_dataframe(self.dataframe, None)

        result = DataFrame()

        for s in slices:
            df = self.dataframe[self.dataframe[self.slicer]==s]
            result = result.append(self.profile_dataframe(df, s))

        result = result.sort_values(['inspector',
                                     'column',
                                     'measure',
                                     'slice'])

        if lags:
            result = self.get_lags(result, lags)

        return result

    def profile_dataframe(self, dataframe, slice_value):
        '''Profiles an individual DataFrame, usually a sliced partition.

        Args:
            dataframe (pandas.DataFrame): A pandas.DataFrame
            slice_value (str): The slicer value for a given partition.

        Returns:

        '''
        output_columns = ['inspector',
                          'column',
                          'slice',
                          'measure',
                          'measure_value']
        result = DataFrame()
        for col in dataframe.columns:
            dtype = self.get_column_dtype(col)
            if dtype == 'string':
                insp = StringInspector(dataframe[col])
                inspector_type = 'string'
            elif dtype == 'number':
                insp = NumberInspector(dataframe[col])
                inspector_type = 'number'
            elif dtype == 'datetime':
                insp = DateInspector(dataframe[col])
                inspector_type = 'datetime'
            else:
                insp = Inspector(dataframe[col])
                inspector_type = 'generic'
            insp_dict = insp.inspect()
            df = self.insp_dict_to_dataframe(col, insp_dict)
            df['inspector'] = inspector_type
            df['slice'] = slice_value
            result = result.append(df[output_columns])

        return result

    def get_lag_measure(self, row):
        if (row['column'] == row['_column'] and
            row['measure'] == row['_measure']):
            return row['_measure_value']
        else:
            return None

    def get_lags(self, dataframe, lags=1):
        for i in range(1, lags+1):
            dataframe['_column'] = dataframe['column'].shift(i)
            dataframe['_measure'] = dataframe['measure'].shift(i)
            dataframe['_measure_value'] = dataframe['measure_value'].shift(i)
            column = 'l{}_measure_value'.format(i)
            dataframe[column] = dataframe.apply(self.get_lag_measure, 1)
            dataframe = dataframe.drop(['_column',
                                        '_measure',
                                        '_measure_value'], 1)
        return dataframe
