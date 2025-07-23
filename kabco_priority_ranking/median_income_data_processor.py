import numpy as np
from arctable_toolkit import BaseProcessor


class MedianIncomeDataProcessor(BaseProcessor):
    def __init__(self, data_catalog, table_name = 'Median_Household_Income_in_Parks_Catchment_Areas'):
        # Initialize the BaseProcessor with the loaded DataFrame
        super().__init__(table_name, data_catalog)
        
        self.handle_nans_and_zeros(colName = 'MEAN_Median')
        self._log_transform()
        self.compute_z_scores()
    
    def _log_transform(self):        
        self.data['MEAN_Median'] = np.log(self.data['MEAN_Median'])
        
    def compute_z_scores(self):
        # Calculate the mean and standard deviation for 'SUM_non_auto'
        mean_non_auto = self.data['MEAN_Median'].mean()
        std_non_auto = self.data['MEAN_Median'].std()
        
        # Compute the z-score for 'MEAN_B08201_cal_pctNoVehE'
        self.data['z_score_median_income'] = (self.data['MEAN_Median'] - mean_non_auto) / std_non_auto
        
        self.data = self.data[['NAME', 'z_score_median_income']]