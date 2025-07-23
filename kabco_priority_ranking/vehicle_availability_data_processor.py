from arctable_toolkit import BaseProcessor


class VehicleAvailabilityDataProcessor(BaseProcessor):
    def __init__(self, data_catalog, table_name = 'Vehicle_Availability_in_Parks_Catchment_Areas'):
        # Initialize the BaseProcessor with the loaded DataFrame
        super().__init__(table_name, data_catalog)
        
        self.handle_nans_and_zeros(colName = 'MEAN_B08201_calc_pctNoVehE')
        self.compute_z_scores()
        
    def compute_z_scores(self):
        # Calculate the mean and standard deviation for 'SUM_non_auto'
        mean_non_auto = self.data['MEAN_B08201_calc_pctNoVehE'].mean()
        std_non_auto = self.data['MEAN_B08201_calc_pctNoVehE'].std()
        
        # Compute the z-score for 'MEAN_B08201_cal_pctNoVehE'
        self.data['z_score_pct_no_vehicle'] = (self.data['MEAN_B08201_calc_pctNoVehE'] - mean_non_auto) / std_non_auto
        
        self.data = self.data[['NAME', 'z_score_pct_no_vehicle']]