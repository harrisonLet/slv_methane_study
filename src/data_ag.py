"""
UWML Data Aggregator
Process Aeris and Sprinter WX files into pandas DataFrames, to be expanded.
Harrison LeTourneau, U of Utah, Jan 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path


def read_aeris(filename):
    """
    Read Aeris gas analyzer data file.
    
    Parameters

    filename : str
        Path to Aeris .txt file (eg - slv_methane_study/data/raw/exampleData/20240801SLCData/Aeris/Ultra100460_240801_181546Eng.txt)
    
    Returns

    pd.DataFrame

        DataFrame with datetime index and gas measurements

    NOTE: Pulling X Y Z Columns
    """
    try:
        # Read CSV, skip the last row
        df = pd.read_csv(
            filename, 
            sep=',', 
            skipfooter=1,
            on_bad_lines='skip',
            engine='python'
        )
        
        # Parse timestamp, Aeries Format (08/01/2024 18:15:45.025)
        df['Time Stamp'] = pd.to_datetime(
            df['Time Stamp'], 
            format='%m/%d/%Y %H:%M:%S.%f',
            errors='coerce'
        )
        
        # Set as index and sort
        df.set_index('Time Stamp', inplace=True)
        df.sort_index(inplace=True)
        
        print(f"Loaded Aeris: {len(df)} records")
        return df


        print df.columns.tolist()
        
    except Exception as e:
        print(f"✗ Error reading Aeris file {filename}: {e}")
        return pd.DataFrame()


def read_sprinter_wx(filename):
    """
    Read Sprinter WX mobile weather station data.
    
    Parameters:
    -----------
    filename : str
        Path to Sprinter WX .csv file (e.g., "UWTR_WX_Sprinter_20240801_151844.csv")
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with datetime index and weather measurements
    """
    try:
        # Skip the 3 header rows
        df = pd.read_csv(filename, skiprows=3)
        
        # Parse the custom timestamp format: 151845*20240801
        # Format is HHMMSS*YYYYMMDD
        def parse_timestamp(pc_time):
            time_part = pc_time.split('*')[0]  # HHMMSS
            date_part = pc_time.split('*')[1]  # YYYYMMDD
            
            dt_string = date_part + time_part  # YYYYMMDDHHMMSS
            return datetime.strptime(dt_string, '%Y%m%d%H%M%S')
        
        df['TIMESTAMP'] = df['PC'].apply(parse_timestamp)
        df.set_index('TIMESTAMP', inplace=True)
        df.sort_index(inplace=True)
        
        print(f"✓ Loaded Sprinter WX: {len(df)} records")
        return df
        
    except Exception as e:
        print(f"✗ Error reading Sprinter WX file {filename}: {e}")
        return pd.DataFrame()


def load_data(aeris_file=None, sprinter_file=None):
    """
    Load both Aeris and Sprinter WX data.
    
    Parameters:
    -----------
    aeris_file : str, optional
        Path to Aeris file
    sprinter_file : str, optional
        Path to Sprinter WX file
    
    Returns:
    --------
    dict
        Dictionary with keys 'aeris' and 'sprinter' containing DataFrames
    """
    data = {}
    
    if aeris_file:
        print(f"Loading Aeris: {Path(aeris_file).name}")
        data['aeris'] = read_aeris(aeris_file)
    
    if sprinter_file:
        print(f"Loading Sprinter WX: {Path(sprinter_file).name}")
        data['sprinter'] = read_sprinter_wx(sprinter_file)
    
    return data


def merge_datasets(aeris_df, sprinter_df, method='nearest', tolerance='1s'):
    """
    Merge Aeris and Sprinter WX data by timestamp.
    
    Parameters:
    -----------
    aeris_df : pd.DataFrame
        Aeris data with datetime index
    sprinter_df : pd.DataFrame
        Sprinter WX data with datetime index
    method : str
        Merge method: 'nearest', 'forward', 'backward' (default: 'nearest')
    tolerance : str
        Maximum time difference for matching (default: '1s')
    
    Returns:
    --------
    pd.DataFrame
        Merged dataset with both instruments' data
    """
    # Use pandas merge_asof for time-series alignment
    merged = pd.merge_asof(
        aeris_df.reset_index().sort_values('Time Stamp'),
        sprinter_df.reset_index().sort_values('TIMESTAMP'),
        left_on='Time Stamp',
        right_on='TIMESTAMP',
        direction=method,
        tolerance=pd.Timedelta(tolerance),
        suffixes=('_aeris', '_wx')
    )
    
    # Set timestamp as index
    merged.set_index('Time Stamp', inplace=True)
    
    print(f"✓ Merged dataset: {len(merged)} records")
    return merged


# Example usage
if __name__ == "__main__":
    
    # Single file loading
    aeris = read_aeris("slv_methane_study/data/raw/exampleData/20240801SLCData/Aeris/Ultra100460_240801_181546Eng.txt")
    # sprinter = read_sprinter_wx("UWTR_WX_Sprinter_20240801_151844.csv")
    
    # Or load both at once
    # data = load_data(
    #     aeris_file="Ultra100460_240801_181546Eng.txt",
    #     sprinter_file="UWTR_WX_Sprinter_20240801_151844.csv"
    # )
    
    # Access the data
    # print("\nAeris columns:", data['aeris'].columns.tolist()[:5])
    # print("Sprinter columns:", data['sprinter'].columns.tolist()[:5])
    
    # Merge if both exist
    # if 'aeris' in data and 'sprinter' in data:
    #     if not data['aeris'].empty and not data['sprinter'].empty:
    #         merged = merge_datasets(data['aeris'], data['sprinter'])
    #         print("\nMerged data shape:"cd, merged.shape)
