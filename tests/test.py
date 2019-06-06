from data_tsa.test_data import TestData
from data_tsa.profiler import Profiler
from data_tsa.dataframe_inspector import DataFrameInspector

test_data = TestData(100)
df = test_data.get_test_data()

profiler = Profiler(df)
#profiler.set_type_exception('created_at', 'date')
profiler.profile()
