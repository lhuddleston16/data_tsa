from data_tsa.inspector import Inspector

class BooleanInspector(Inspector):

    def __init__(self, series):
        '''Inspects a pandas.Series object with boolean data types.

        Args:
            series (pandas.Series): A pandas.Series object
        '''
        super().__init__(series)
        
    def get_true_ratio(self):
        '''Returns the percentage of records that are True'''
        return len(self.series[self.series==True]) / len(self.series)
    
    def get_false_ratio(self):
        '''Returns the percentage of records that are False'''
        return len(self.series[self.series==False]) / len(self.series)
    
    def inspect(self):
        '''Inspects the provided pandas.Series

        Returns:
            Dictionary containing measures and values
        '''
        insp = self.core_inspect()
        insp['true_ratio'] = self.get_true_ratio()
        insp['false_ratio'] = self.get_false_ratio()
        return insp
    