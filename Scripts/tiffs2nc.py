import os
import sys
import glob
import pandas as pd
import xarray as xr
import rioxarray as rxr
import timeit

def time_index_from_filenames(filenames):
    '''helper function to create a pandas DatetimeIndex
       Filename example: rainfall_new_day_statewide_data_map_1990_01_01.tif'''
    return pd.DatetimeIndex([pd.Timestamp('-'.join(os.path.basename(os.path.splitext(f)[0]).split('_')[6:])) for f in filenames])

years = range(1990, 2025)
outdir = r'Z:/SSD_Recovery/Extreme SSD(H)/DATA/HawaiiEP/HCDP_data/rainfall/new/year'

for year in years:
    outnc = os.path.join(outdir, f'{year}_simple.nc')
    if os.path.exists(outnc):
        continue
        
    print(f'Working on {year}...')
    start_time = timeit.default_timer()
    print()

    # Get all tiff files for the year
    filenames = glob.glob(rf'Z:/SSD_Recovery/Extreme SSD(H)/DATA/HawaiiEP/HCDP_data/rainfall/new/day/statewide/data_map/{year}/*/*.tif')
    time_var = xr.Variable('time', time_index_from_filenames(filenames))
    
    # Read all tiff files
    da = xr.concat([rxr.open_rasterio(f) for f in filenames], dim=time_var).squeeze()

    # Rename dims in data array
    da = da.rename({'x': 'lon', 'y': 'lat'})
    da.name = 'precip'
    
    # Create a new, clean dataset without spatial reference
    ds = xr.Dataset(
        data_vars={
            'precip': (['time', 'lat', 'lon'], da.values)
        },
        coords={
            'lon': ('lon', da.lon.values),
            'lat': ('lat', da.lat.values),
            'time': ('time', time_var.values)
        }
    )
    
    # Set clean, minimal metadata
    ds.lat.attrs = {'standard_name': 'latitude', 'long_name': 'Latitude', 'units': 'degrees_north', 'axis': 'Y'}
    ds.lon.attrs = {'standard_name': 'longitude', 'long_name': 'Longitude', 'units': 'degrees_east', 'axis': 'X'}
    ds.time.attrs = {'standard_name': 'time', 'long_name': 'Time'}
    ds.precip.attrs = {'units': 'kg m-2 d-1'}
    
    # Ensure no _FillValue is added to coordinate variables
    encoding = {
        'lat': {'_FillValue': None},
        'lon': {'_FillValue': None},
        'time': {'_FillValue': None},
        'precip': {'_FillValue': None}
    }
    
    # Save to netCDF, ensuring correct time units format
    time_units = f'days since {year}-01-01 00:00:00'
    ds.time.encoding.update({'units': time_units})
    
    # Save file without any spatial reference information
    ds.to_netcdf(outnc, encoding=encoding)
    
    runtime = timeit.default_timer() - start_time
    print('Done!')
    print(f'Run time: {runtime} s')
    print()
    
    # Process just the first year for testing
    if year == 1990:
        break

print("All years completed!")