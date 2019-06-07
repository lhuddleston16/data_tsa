from data_tsa.inspector import Inspector
from fuzzywuzzy import fuzz
from re import search

class StringInspector(Inspector):
    
    def __init__(self, series):
        super().__init__(series)
        
    def get_distinct_count(self):
        return len(self.series.unique())
    
    def _get_standardized_values(self):
        s = self.series.tolist()
        return [_.lower().strip() for _ in s]

    def _re_search(self, pattern):
        return [_ for _ in self.series if search(pattern, _)]
    
    def get_strict_distinct_count(self):
        return len(set(self._get_standardized_values()))
    
    def get_estimated_duplicates_count(self):
        pass
    
    def get_empty_count(self):
        return len([_ for _ in self.series if _ == ''])
    
    def get_special_character_count(self):
        return len(self._re_search('[^A-Za-z0-9\s]'))
    
    def get_email_count(self):
        return len(self._re_search('[^@]+@[^@]+\.[^@]+'))
    
    def get_trim_required_count(self):
        return len([_ for _ in self.series if _[0] == ' ' or _[-1] == ' '])
    
    def inspect(self):
        result = self.core_inspect()
        result['distinct_count'] = self.get_distinct_count()
        result['strict_distinct_count'] = self.get_strict_distinct_count()
        result['empty_count'] = self.get_empty_count()
        result['special_character_count'] = self.get_special_character_count()
        result['email_count'] = self.get_email_count()
        result['trim_required_count'] = self.get_trim_required_count()
        return result
    