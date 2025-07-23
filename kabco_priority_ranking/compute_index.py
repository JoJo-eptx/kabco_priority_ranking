import numpy as np
import pandas as pd


def sum_weights(weights: dict):
    return np.sum(
        weights.get('injury_fatal') + weights.get('injury_serious') + weights.get('injury_minor') + weights.get('injury_possible') + weights.get('injury_none')
    )

def compute_index_value(weights: dict) -> callable:
    def demographics_score(df: pd.DataFrame) -> pd.Series:
        return weights.get('demographics') * (-df['z_score_median_income'] + df['z_score_pct_no_vehicle']) / 2

    def trip_volume_score(df: pd.DataFrame) -> pd.Series:
        return weights.get('trip_volume') * df['z_score_non_auto_trips']

    def injury_scores(df: pd.DataFrame) -> pd.Series:
        injury_weights_sum = sum_weights(weights)
        return (
            (weights.get('injury_fatal') / injury_weights_sum) * df['K - FATAL INJURY'] +
            (weights.get('injury_serious') / injury_weights_sum) * df['A - SUSPECTED SERIOUS INJURY'] +
            (weights.get('injury_minor') / injury_weights_sum) * df['B - SUSPECTED MINOR INJURY'] +
            (weights.get('injury_possible') / injury_weights_sum) * df['C - POSSIBLE INJURY'] +
            (weights.get('injury_none') / injury_weights_sum) * df['N - NOT INJURED']
        )

    def accidents_score(df: pd.DataFrame) -> pd.Series:
        return injury_scores(df) * weights.get('accidents')

    def apply(df: pd.DataFrame) -> pd.DataFrame:
        return demographics_score(df) + trip_volume_score(df) + accidents_score(df)
    
    def apply(df: pd.DataFrame) -> pd.DataFrame:
        # Check if the park name is in the list of special parks
        df['adjusted_weights'] = df['NAME'].apply(lambda x: special_weight_factor if x in special_parks else 1)
        
        # Apply the adjusted weights for each park
        weighted_demographics = demographics_score(df) * df['adjusted_weights']
        weighted_trip_volume = trip_volume_score(df) * df['adjusted_weights']
        weighted_accidents = accidents_score(df) * df['adjusted_weights']
        
        return weighted_demographics + weighted_trip_volume + weighted_accidents
    
    return apply