from hashlib import md5
from uuid import uuid4

class DataFrameInspector:

    def __init__(self, dataframe):
        '''Inspects a pandas.DataFrame object for data quality & consitency.

        Args:
            dataframe (pandas.DataFrame): A pandas.DataFrame object
        '''
        self.dataframe = dataframe.copy()
        self.hash_col_name = uuid4()
        self._get_row_hash()

    def _get_row_hash_value(self, row):
        m = md5()
        m.update(''.join([str(_) for _ in row.tolist()]).encode('utf-8'))
        return m.hexdigest()

    def _get_row_hash(self):
        self.dataframe[self.hash_col_name] = self.dataframe.apply( \
                                                  self._get_row_hash_value, 1)

    def get_duplicate_row_indicator(self):
        '''Returns True if any rows in the DataFrame are exact duplicates.'''
        unique_row_ct = len(self.dataframe[self.hash_col_name].unique())
        if unique_row_ct != len(self.dataframe):
            return True
        return False

    def get_duplicate_rows(self):
        '''Returns the rows in the DataFrame that are duplciated.'''
        s = self.dataframe[self.hash_col_name]
        vc = s.value_counts()
        dupes = vc[vc.values > 1].index.tolist()
        dupe_df = self.dataframe[self.dataframe[self.hash_col_name].isin(dupes)]
        return dupe_df.sort_values([self.hash_col_name])
