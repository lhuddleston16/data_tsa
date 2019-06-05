from data_tsa.inspector import Inspector

class StringInspector(Inspector):
    
    def __init__(self):
        super().__init__(*args, **kwargs)
        pass
    
    def get_distinct_count(self):
        pass
    
    def get_strict_distinct_count(self):
        pass
    
    def get_estimated_duplicates_count(self):
        pass
    
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