from arcgis.gis import GIS
from arcgis.features import FeatureLayer

class FeatureLayerLoader:
    def __init__(self, feature_service_url):
        """
        Initialize the FeatureLayerLoader class with the URL of a feature service.
        
        Parameters:
        - feature_service_url: URL of the feature service layer (e.g., 'https://services1.arcgis.com/...')
        """
        self.feature_service_url = feature_service_url
        self.gis = GIS("home")  # Connect to your default GIS; modify if needed
        self.feature_layer = FeatureLayer(self.feature_service_url)

    def load_data(self, where_clause="1=1", out_fields="*", return_geometry=False):
        """
        Load the attribute table of the feature layer into a Pandas DataFrame.

        Parameters:
        - where_clause: SQL expression to filter records (default is "1=1" for all records).
        - out_fields: Comma-separated list of fields to include (default is "*" for all fields).
        - return_geometry: Whether to include the geometry in the query result (default is False).
        
        Returns:
        - A Pandas DataFrame containing the attributes of the feature layer.
        """
        query_result = self.feature_layer.query(where=where_clause, out_fields=out_fields, return_geometry=return_geometry)
        df = query_result.df  # Convert query result to Pandas DataFrame
        return df