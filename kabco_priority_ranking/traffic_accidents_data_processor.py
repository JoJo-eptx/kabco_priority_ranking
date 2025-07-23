import pandas as pd
from arctable_toolkit import BaseProcessor
from .trip_volume_data_processor import TripVolumeDataProcessor
from .feature_layer_loader import FeatureLayerLoader


class TrafficAccidentsDataProcessor(BaseProcessor):
    def __init__(self, data_catalog, table_name = 'Traffic_Accidents_in_Parks_Catchment_Areas'):
        """Initialize DataProcessor which uses TableLoader for data loading."""
        # Initialize the BaseProcessor with the loaded DataFrame
        super().__init__(table_name, data_catalog)
        
        self.tripVolumeBySchoolCatchmentAreas = TripVolumeDataProcessor(data_catalog).get_lookup_table()
                
        self.remove_unknown_crash_severity()
        self.handle_nans_and_zeros(colName = 'FREQUENCY')
        self.standardize_by_trip_volume()
        self.compute_z_scores()
        self.get_pivot_values_by_severity()
        
    def remove_unknown_crash_severity(self):
        """Removes rows where Crash_Severity is '99 - UNKNOWN'."""
        # Filter the DataFrame to exclude rows where Crash_Severity is '99 - UNKNOWN'
        self.data = self.data[self.data['Crash_Severity'] != '99 - UNKNOWN']
    
    def sum_of_auto_trips(self):
        trip_volume_data = FeatureLayerLoader(self.trip_volume_url).load_data()
        return trip_volume_data['combined_auto'].sum()

    def compute_z_scores(self):
        """Compute the Z-score based on 'Crash_Severity'."""
        # Ensure 'Crash_Severity', 'FREQUENCY' exist in the DataFrame
        required_columns = ['Crash_Severity', 'FREQUENCY']
        if not all(col in self.data.columns for col in required_columns):
            raise ValueError(f"One or more required columns {required_columns} are missing from the DataFrame.")
        
        # Group by 'Crash_Severity' and compute the mean and std deviation for 'FREQUENCY'
        grouped = self.data.groupby('Crash_Severity')[['FREQUENCY']].agg(['mean', 'std'])
        
        # Compute Z-scores
        self.data['z_score'] = self.data.apply(self._calculate_z_score, axis=1, grouped=grouped)
        
    def _calculate_z_score(self, row, grouped):
        """Helper function to compute the Z-score for a given row."""
        category = row['Crash_Severity']
        
        # Check for missing or invalid values in Crash_Severity
        if pd.isna(category):
            return None  # Return None if the category is missing or invalid
        
        # Ensure the category exists in the grouped data
        if category not in grouped.index:
            return None  # If category is not found, return None
        
        # Get the mean and std from the grouped DataFrame using multi-level indexing
        try:
            mean_frequency = grouped.loc[category, ('FREQUENCY', 'mean')]
            std_frequency = grouped.loc[category, ('FREQUENCY', 'std')]
        except KeyError:
            # If any column is not found, return None
            return None
        
        # Number of auto trips in area
        auto_trips_in_catchment_area = self.tripVolumeBySchoolCatchmentAreas[row['NAME']]
        
        # Compute the Z-score for 'FREQUENCY'
        z_score_frequency = (row['FREQUENCY'] - mean_frequency) / std_frequency if std_frequency != 0 else 0
        
        return z_score_frequency
    
    def standardize_by_trip_volume(self):
        # self.data.fillna(0)
        
        """Compute the Z-score based on 'Crash_Severity'."""
        # Ensure 'NAME', 'FREQUENCY' exist in the DataFrame
        required_columns = ['NAME', 'FREQUENCY']
        if not all(col in self.data.columns for col in required_columns):
            raise ValueError(f"One or more required columns {required_columns} are missing from the DataFrame.")
        
        # Group by 'Crash_Severity' and compute the mean and std deviation for 'FREQUENCY'
        grouped = self.data.groupby('NAME')[['FREQUENCY']]
        
        # Compute Z-scores
        self.data['FREQUENCY'] = self.data.apply(self._standardize_by_trip_volume, axis=1, grouped=grouped)
    
    def _standardize_by_trip_volume(self, row, grouped):
        name = row['NAME']
        standardized_frequency = row['FREQUENCY']/self.tripVolumeBySchoolCatchmentAreas[name]
        
        return standardized_frequency
    
    def fill_nan_values(self):
        # Fill NaN values with empty string to avoid errors when using str.contains
        self.data['Crash_Severity'] = self.data['Crash_Severity'].fillna('')
    
    def get_pivot_values_by_severity(self, values='z_score'):
        self.fill_nan_values()
        
        # Filter the relevant Crash_Severity values
        filtered_df = self.data[self.data['Crash_Severity'].str.contains(r'^[A-KN] -', regex=True)]

        # Pivot the dataframe
        self.data = filtered_df.pivot_table(index=['NAME'], columns='Crash_Severity', values=values, aggfunc='first')

        # Reset the index to bring 'NAME' back as a regular column
        self.data.reset_index(inplace=True)