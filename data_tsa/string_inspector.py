from data_tsa.inspector import Inspector

class StringInspector(Inspector):
    
    def __init__(self, series):
        super().__init__(series)
        
    def get_distinct_count(self):
        return len(self.series.unique())
    
    def _get_standardized_values(self):
        s = self.series.tolist()
        return [_.lower().strip() for _ in s]
    
    def get_strict_distinct_count(self):
        return len(set(self._get_standardized_values()))
    
    def get_estimated_duplicates_count(self):
        s = list(set(self._get_standardized_values()))
    
    def get_max_len_warning_count(self):
        pass
    
    def get_empty_count(self):
        pass
    
    def get_non_ascii_count(self):
        pass
    
    def get_email_count(self):
        pass
    
    def get_trim_required_count(self):
        pass
    
    def inspect(self):
        pass