from arctable_toolkit import BaseProcessor

class TripVolumeDataProcessor(BaseProcessor):
    def __init__(self, data_catalog, table_name = 'Trip_Volume_in_Parks_Catchment_Areas'):
        super().__init__(table_name, data_catalog)
        
        self.lookup_table = None
        self.handle_nans_and_zeros(colName = 'SUM_combined_auto')
        self._lookup_table()
        
    def _lookup_table(self):
        self.lookup_table = self.data.set_index('NAME')['SUM_combined_auto'].to_dict()
        
    def get_lookup_table(self):
        return self.lookup_table