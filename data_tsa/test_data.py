import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class TestData:
    
    def __init__(self, n):
        if n < 30:
            print('WARNING: n values under 30 may result in incomplete test sets.')
        self.n = n
        self.df = pd.DataFrame()
    
    def _generate_list_from_domain(self, domain):
        return [np.random.choice(domain) for _ in range(self.n)]
    
    def get_id(self, name):
        self.df[name] = np.arange(self.n).tolist()
        
    def get_partial_null(self, name):
        d = [np.NaN, 0]
        self.df[name] = self._generate_list_from_domain(d)
    
    def get_created_at(self, name):
        base = datetime(2018, 1, 1)
        arr = [base + timedelta(hours=i*6) for i in range(self.n)]
        self.df[name] = arr
        
    def get_string_slicer(self, name):
        d = ['A', 'B']
        self.df[name] = self._generate_list_from_domain(d)
    
    def get_duplicate_string(self, name):
        d = ['Test', 'test', 'testing', 'Testing', 'test    ']
        self.df[name] = self._generate_list_from_domain(d)
        
    def get_test_data(self):
        self.get_id('id')
        self.get_partial_null('partial_null')
        self.get_created_at('created_at')
        self.get_duplicate_string('duplicate_string')
        self.get_string_slicer('string_slicer')
        return self.df.copy()
    
if __name__ == '__main__':
    t = TestData(30)
    df = t.get_test_data()
    df.head()