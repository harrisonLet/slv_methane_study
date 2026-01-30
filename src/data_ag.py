"""
UWML Data Aggregator
Process Aeris and Sprinter WX files into pandas DataFrames, to be expanded.
Harrison LeTourneau, U of Utah, Jan 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

def read_ARC(filename):
    """
    Read ICARTT ARC file into pandas DataFrame.
    
    Parameters

    filename : str
        Path to ARC file
    
    Returns

    pd.DataFrame

        DataFrame with datetime index and measurements

    NOTE: Converts -99999.0 to NaN
    """
    try:
        # Read the file and locate the header row that contains the variable names.
        # Strategy: find the first line that contains commas and letters and whose
        # following non-empty line starts with a digit (data row). Also parse the
        # start date from the header if available (YYYY,MM,DD,YYYY,MM,DD).
        with open(filename, 'r') as fh:
            lines = [ln.rstrip('\n') for ln in fh]

        header_row = None
        start_date = None

        import re

        # try to find a line containing the start/end date: YYYY,MM,DD,YYYY,MM,DD
        for ln in lines:
            m = re.match(r"^(\d{4}),(\d{1,2}),(\d{1,2}),(\d{4}),(\d{1,2}),(\d{1,2})$", ln.strip())
            if m:
                y,mo,d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                start_date = datetime(y, mo, d)
                break

        # find the variable-name header line by looking for a line with commas
        # and letters where the next non-empty line looks like a numeric data row
        for i, ln in enumerate(lines[:-1]):
            if ',' not in ln:
                continue
            if not re.search('[A-Za-z]', ln):
                continue
            # find the next non-empty line
            j = i + 1
            while j < len(lines) and lines[j].strip() == '':
                j += 1
            if j >= len(lines):
                continue
            next_ln = lines[j].lstrip()
            # if next line starts with a number (possibly negative/float) and has commas,
            # treat current as header row
            if re.match(r'^[-+]?\d', next_ln) and (',' in next_ln):
                header_row = i
                break

        if header_row is None:
            raise ValueError('Could not find variable header row in ARC file')

        # Read with pandas using the detected header row (0-indexed)
        df = pd.read_csv(
            filename,
            header=header_row,
            sep=',',
            engine='python',
            skipinitialspace=True,
            na_values=[-99999.0, -99999]
        )

        # Clean up column names
        df.columns = [c.strip() for c in df.columns]

        # If StartTime_seconds exists and we have a start_date from header,
        # build a proper TIMESTAMP index. Otherwise, if there's a column named
        # 'Year' etc. fall back to previous behavior.
        if 'StartTime_seconds' in df.columns:
            if start_date is None:
                # try to find a year/month/day triplet elsewhere in header lines
                for ln in lines[:header_row]:
                    m = re.match(r"^(\d{4}),(\d{1,2}),(\d{1,2}),(\d{4}),(\d{1,2}),(\d{1,2})$", ln.strip())
                    if m:
                        y,mo,d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                        start_date = datetime(y, mo, d)
                        break
            if start_date is not None:
                # Some files have StartTime_seconds as float seconds since midnight
                df['TIMESTAMP'] = pd.to_datetime(start_date) + pd.to_timedelta(df['StartTime_seconds'].astype(float), unit='s')
                df.set_index('TIMESTAMP', inplace=True)
                df.sort_index(inplace=True)

        elif set(['Year','Month','Day','Hour','Minute','Second']).issubset(df.columns):
            df['TIMESTAMP'] = pd.to_datetime(df[['Year','Month','Day','Hour','Minute','Second']])
            df.set_index('TIMESTAMP', inplace=True)
            df.sort_index(inplace=True)

        # Replace sentinel values left over (sometimes appear as floats or ints)
        df.replace([-99999.0, -99999, -77777, -88888], np.nan, inplace=True)

        # Rename selected columns to more user-friendly / standardized names
        rename_map = {
            'CH4_aeris313_ppm': 'CH4 (ppm)',
            'C2H6_aeris313_ppb': 'C2H6 (ppb)',
            'true_WD_deg': 'GPSCorWindDirTrue (deg)',
            'true_WS_m_s': 'GPSCorWindSpeed (m/s)',
            'lat_DGPS_deg': 'Latitude (DD.ddd +N)',
            'lon_DGPS_deg': 'Longitude (DDD.ddd -W)'
        }

        # Only rename columns that actually exist in the dataframe
        existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
        if existing_renames:
            df.rename(columns=existing_renames, inplace=True)

        # Ensure TIMESTAMP is the index name (if set)
        try:
            if isinstance(df.index, pd.DatetimeIndex):
                df.index.name = 'TIMESTAMP'
        except Exception:
            pass

        # Keep only the requested columns (after rename) plus the timestamp index
        desired_names = list(rename_map.values())
        existing_desired = [c for c in desired_names if c in df.columns]
        if existing_desired:
            df = df[existing_desired].copy()

        print(f"Loaded ARC: {len(df)} records, columns: {len(df.columns)}")
        return df

    except Exception as e:
        print(f"Error reading ARC file {filename}: {e}")
        return pd.DataFrame()

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
    
    # data = load_data(
    #     aeris_file="data/exampleData/20240801SLCData/Aeris/Ultra100460_240801_181546Eng.txt",
    #     uwml_file="data/exampleData/20240801SLCData/SprinterMet/UWTR_WX_Sprinter_20240801_151844.csv"
    # )

    date = '20240804'

    data = read_arc(f"/Users/harrisonletourneau/Desktop/lair/slv_methane_study/data/raw/arc_raw/USOS-ARL-Suite_ARC_{date}_RA.ict")
    
    # print("\nAeris columns:", data['aeris'].columns.tolist()[:5])
    # print("UWML columns:", data['uwml'].columns.tolist()[:5])
    print("\nARC data shape:", data.shape)

    # data['aeris'].to_csv('output/scrap/aeris.csv')
    # data['uwml'].to_csv('output/scrap/uwml.csv')
    data.to_csv(f'../output/arc//{date}/{date}.csv')


    
    # Merge
    # if 'aeris' in data and 'uwml' in data:
    #     if not data['aeris'].empty and not data['uwml'].empty:
    #         merged = merge_datasets(data['aeris'], data['uwml'])
    #         print("\nMerged data shape:", merged.shape)

    # View in excel
    # merged.to_csv('output/scrap/preview.csv')


    

