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
        Path to Aeris .txt file
    
    Returns

    pd.DataFrame

        DataFrame with datetime index and gas measurements

    NOTE: Pulling ALL Columns
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
        df['TIMESTAMP'] = pd.to_datetime(
            df['Time Stamp'], 
            format='%m/%d/%Y %H:%M:%S.%f',
            errors='coerce'
        )
        
        # Set as index and sort
        df.set_index('TIMESTAMP', inplace=True)
        df.sort_index(inplace=True)
        
        print(f"Loaded Aeris: {len(df)} records")
        
        return df

        
    except Exception as e:
        print(f"Error reading Aeris file {filename}: {e}")
        return pd.DataFrame()


def read_uwml(filename):
    """
    Read UWML WX mobile weather station data.
    
    Parameters

    filename : str
        Path to UWML WX .csv file
    
    Returns

    pd.DataFrame
        DataFrame with datetime index and met data
    """
    try:
        # Skip the 3 header rows
        df = pd.read_csv(filename, skiprows=3, index_col=False)
        

        """
        Parse the custom timestamp format
        Format is HHMMSS*YYYYMMDD
        """
        def parse_uwml_timestamp(pc_time):
            pc_time = str(pc_time)
            time = pc_time.split('*')[0]  # HHMMSS
            date = pc_time.split('*')[1]  # YYYYMMDD
            
            dt_string = date + time  # YYYYMMDDHHMMSS
            return datetime.strptime(dt_string, '%Y%m%d%H%M%S')

        df['PC'] = df['PC'].apply(parse_uwml_timestamp)
        df.rename(columns={'PC': 'TIMESTAMP'}, inplace=True)
        df.set_index('TIMESTAMP', inplace=True)
        df.sort_index(inplace=True)

        df = df.drop(columns=["UTC hhmmss", "UTC Year", "UTC Month", "UTC Day"])
        
        print(f"Loaded UWML WX: {len(df)} records")

        return df
        
    except Exception as e:
        print(f"Error reading UWML WX file {filename}: {e}")
        return pd.DataFrame()


def load_data(aeris_file, uwml_file):
    """
    Load both Aeris and UWML WX data.
    
    Parameters

        aeris_file : str
        Path to Aeris file

        uwml_file : str
        Path to UWML WX file
    
    Returns
    
        dict
        Dictionary with keys 'aeris' and 'uwml' containing DataFrames
    """
    data = {}
    
    if aeris_file:
        print(f"Loading Aeris: {Path(aeris_file).name}")
        data['aeris'] = read_aeris(aeris_file)

    if uwml_file:
        print(f"Loading UWML WX: {Path(uwml_file).name}")
        data['uwml'] = read_uwml(uwml_file)
    
    return data


def merge_datasets(aeris_df, uwml_df, method='nearest', tolerance='1s'):
    """
    Merge Aeris and UWML WX data by timestamp.
    
    Parameters
    
        aeris_df : pd.DataFrame
        Aeris data with datetime type index

    uwml_df : pd.DataFrame
        UWML WX data with datetime type index

    method : str
        Merge method: 'nearest', 'forward', 'backward'

    tolerance : str
        Maximum time difference for matching
    
    Returns
    
        pd.DataFrame
        Merged dataset with both instruments' data
    """
    # Use pandas merge_asof for time-series alignment
    merged = pd.merge_asof(
        aeris_df.reset_index().sort_values('TIMESTAMP'),
        uwml_df.reset_index().sort_values('TIMESTAMP'),
        left_on='TIMESTAMP',
        right_on='TIMESTAMP',
        direction=method,
        tolerance=pd.Timedelta(tolerance),
        suffixes=('_aeris', '_wx')
    )
    
    # Set timestamp as index
    merged.set_index('TIMESTAMP', inplace=True)

    print(f"Merged dataset: {len(merged)} records")
    return merged


if __name__ == "__main__":
    
    # Single file loading
    # aeris = read_aeris("/Users/harrisonletourneau/Desktop/lair/slv_methane_study/data/exampleData/20240801SLCData/Aeris/Ultra100460_240801_181546Eng.txt")
    # sprinter = read_uwml("data/exampleData/20240801SLCData/SprinterMet/UWTR_WX_Sprinter_20240801_151844.csv")
    
    data = load_data(
        aeris_file="data/exampleData/20240801SLCData/Aeris/Ultra100460_240801_181546Eng.txt",
        uwml_file="data/exampleData/20240801SLCData/SprinterMet/UWTR_WX_Sprinter_20240801_151844.csv"
    )
    
    print("\nAeris columns:", data['aeris'].columns.tolist()[:5])
    print("UWML columns:", data['uwml'].columns.tolist()[:5])

    data['aeris'].to_csv('output/scrap/aeris.csv')
    data['uwml'].to_csv('output/scrap/uwml.csv')

    print(data['uwml'].head())

    
    # Merge
    if 'aeris' in data and 'uwml' in data:
        if not data['aeris'].empty and not data['uwml'].empty:
            merged = merge_datasets(data['aeris'], data['uwml'])
            print("\nMerged data shape:", merged.shape)

    # View in excel
    merged.to_csv('output/scrap/preview.csv')


    

