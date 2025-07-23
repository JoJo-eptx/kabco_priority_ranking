from arctable_toolkit import BaseProcessor


class NonAutoDataProcessor(BaseProcessor):
    def __init__(self, data_catalog, table_name = 'Trip_Volume_NonAuto_in_Parks_Catchment_Areas'):
        # Initialize the BaseProcessor with the loaded DataFrame
        super().__init__(table_name, data_catalog)
        
        self.handle_nans_and_zeros(colName = 'SUM_non_auto')
        self.compute_z_scores()
        
    def compute_z_scores(self):
        self.data.fillna(0)
        
        # Calculate the mean and standard deviation for 'SUM_non_auto'
        mean_non_auto = self.data['SUM_non_auto'].mean()
        std_non_auto = self.data['SUM_non_auto'].std()
        
        # Compute the z-score for 'SUM_non_auto'
        self.data['z_score_non_auto_trips'] = (self.data['SUM_non_auto'] - mean_non_auto) / std_non_auto
        
        self.data = self.data[['NAME', 'z_score_non_auto_trips']]