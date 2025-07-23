


import pandas as pd


class RankingBase:
    def __init__(self, data_catalog):
        self.data = None
        self.data_catalog = data_catalog
 
    def _fill_nan_values(self):
        numeric_columns = self.data.select_dtypes(include=['number']).columns  # Get numeric columns
        self.data[numeric_columns] = self.data[numeric_columns].fillna(self.data[numeric_columns].mean())

        
    def _merge_multiple_dfs(self, on_column='NAME', how='inner'):
        """
        Merge multiple dataframes on a specified column.
    
        Parameters:
        - dfs: list of pandas DataFrames to merge.
        - on_column: column name to join on (default is 'NAME').
        - how: type of join, default is 'inner' ('left', 'right', 'outer', 'inner').
    
        Returns:
        - A single merged pandas DataFrame.
        """
        self.data = self.data_list[0]  # Start with the first DataFrame
        for df in self.data_list[1:]:  # Loop through the rest of the DataFrames
            self.data = pd.merge(self.data, df, on=on_column, how=how)