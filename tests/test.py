
import pytest

from datetime import datetime
from pandas import DataFrame, Series
from numpy import NaN

from data_tsa.inspector import Inspector
from data_tsa.date_inspector import DateInspector
from data_tsa.number_inspector import NumberInspector
from data_tsa.string_inspector import StringInspector
from data_tsa.dataframe_inspector import DataFrameInspector

@pytest.fixture
def number_series():
    s = []
    for i in range(1, 11):
        s = s + [i for _ in range(i)]
    return Series(s)


class TestDataFrameInspector:
    
    def test_get_duplicate_row_indicator(self):
        df = DataFrame({'a': [0, 1], 'b': [0, 1]})
        insp = DataFrameInspector(df)
        assert insp.get_duplicate_row_indicator() == 0
        
        df = DataFrame({'a': [0, 0], 'b': [0, 0]})
        insp = DataFrameInspector(df)
        assert insp.get_duplicate_row_indicator() == 1
        

class TestInspector:
    
    def test_null_ratio(self):
        s = Series([NaN, 'a'])
        insp = Inspector(s)
        assert insp.get_null_ratio() == 0.5
        
        
class TestDateInspctor:
    
    def test_get_date_conversion_ind(self):
        s = Series(['2019/1/1'])
        insp = DateInspector(s)
        assert insp.get_conversion_required_indicator() == 1
        
    def test_get_date_precision_variance(self):
        s = Series([datetime(2019, 1, 1),
                    datetime(2019, 1, 1, 1),
                    datetime(2019, 1, 1, 1, 1),
                    datetime(2019, 1, 1, 1, 1, 1),
                    datetime(2019, 1, 1, 1, 1, 1, 1)])
        insp = DateInspector(s)
        pvar = insp.get_precision_variance()
        assert pvar['day'] == 0.2
        assert pvar['hour'] == 0.2
        assert pvar['minute'] == 0.2
        assert pvar['second'] == 0.2
        assert pvar['microsecond'] == 0.2
        
        
class TestNumberInspector:
    
    def test_get_non_negative_ratio(self):
        s = Series([-1, -1, NaN, 1])
        insp = NumberInspector(s)
        assert insp.get_non_negative_ratio() == 0.5
        
    def test_get_float_indicator(self):
        s = Series([1.0])
        insp = NumberInspector(s)
        assert insp.get_float_indicator() == 1
        
        s = Series([1])
        insp = NumberInspector(s)
        assert insp.get_float_indicator() == 0
        
    def test_get_zero_ratio(self):
        s = Series([0, 0, NaN, 1])
        insp = NumberInspector(s)
        assert insp.get_zero_ratio() == 0.5
        
    def test_get_top_five_value_counts(self, number_series):
        insp = NumberInspector(number_series)
        top_five = insp.get_top_five_value_counts()
        keys = list(top_five.keys())
        keys.sort()
        assert keys == [6, 7, 8, 9, 10]
        for i in range(6, 11):
            assert top_five[i] == i
        
    def test_get_bottom_five_value_counts(self, number_series):
        insp = NumberInspector(number_series)
        top_five = insp.get_bottom_five_value_counts()
        keys = list(top_five.keys())
        keys.sort()
        assert keys == [1, 2, 3, 4, 5]
        for i in range(1, 6):
            assert top_five[i] == i
            
            
    def test_get_value_skew(self, number_series):
        insp = NumberInspector(number_series)
        assert insp.get_value_skew() == 0.375
        
        
class TestStringInspector:
    
    def test_get_strict_distinct_count(self):
        s = Series(['a', 'A', 'b', ' b '])
        insp = StringInspector(s)
        assert insp.get_strict_distinct_count() == 2
        
    def test_get_empty_ratio(self):
        s = Series(['', 'a'])
        insp = StringInspector(s)
        assert insp.get_empty_ratio() == 0.5
        
    def test_get_special_character_ratio(self):
        s = Series(['a', 'b!', '?', '***'])
        insp = StringInspector(s)
        assert insp.get_special_character_ratio() == 0.75
        
    def test_get_email_ratio(self):
        s = Series(['foo@bar.com',
                    'bar@foo.com',
                    'foop@foop',
                    'foop'])
        insp = StringInspector(s)
        assert insp.get_email_ratio() == 0.5
        
    def test_get_trim_required_ratio(self):
        s = Series(['a', 'a ', ' a', ' a '])
        insp = StringInspector(s)
        assert insp.get_trim_required_ratio() == 0.75
        
    def test_redundancy_indicator(self):
        s = Series(['A', 'a'])
        insp = StringInspector(s)
        assert insp.get_redundancy_indicator() == 1
        