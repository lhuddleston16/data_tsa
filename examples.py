from data_tsa.sample_data import SampleData
from data_tsa.profiler import Profiler

# Generate test dat
samp = SampleData(1000)
df = samp.get_sample_data()

# Initialize Profiler with no slicer
profiler = Profiler(df)

# Profile dataframe columns
p1_output = profiler.profile()
p1_output.head(20)

# Initialize Profiler with slcer
profiler_slicer = Profiler(df, slicer='string_slicer')

# Profile dataframe columns over slicer partitions
p2_output = profiler_slicer.profile()
p2_output.head(20)

# Check dataframe for row duplicates
from data_tsa.dataframe_inspector import DataFrameInspector
from pandas import DataFrame

df_dupes = DataFrame({'a': [0, 0], 'b': [0, 0]})
insp = DataFrameInspector(df_dupes)
insp.get_duplicate_row_indicator()
