from data_tsa.inspector import Inspector

class DateInspector(Inspector):
    
    def __init__(self):
        super().__init__(*args, **kwargs)
        pass
    
    def get_conversion_required_indicator(self):
        pass
    
    def get_distinct_format_count(self):
        pass
    
    def get_precision_variance(self):
        pass