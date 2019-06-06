from hashlib import md5
from uuid import uuid4

class DataFrameInspector:
    
    def __init__(self, dataframe):
        self.dataframe = dataframe.copy()
        self.hash_col_name = uuid4()
        self.get_row_hash()
        
    def get_row_hash_value(self, row):
        m = md5()
        m.update(''.join([str(_) for _ in row.tolist()]).encode('utf-8'))
        return m.hexdigest()
    
    def get_row_hash(self):
        self.dataframe[self.hash_col_name] = self.dataframe.apply( \
                                                  self.get_row_hash_value, 1)
        
    def get_duplicate_row_indicator(self):
        unique_row_ct = len(self.dataframe[self.hash_col_name].unique())
        if unique_row_ct != len(self.dataframe):
            return True
        return False
        