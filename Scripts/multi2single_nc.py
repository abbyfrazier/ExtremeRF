import os
import sys
import glob
import pandas as pd
import xarray as xr
import rioxarray as rxr
import timeit
import dask

dir = r'B:\Data\HawaiiEP\cdd'

ncs = glob.glob(os.path.join(dir, '*.nc'))

print('Merging all NC files...')
start_time = timeit.default_timer()

merged_nc = xr.open_mfdataset(ncs, combine='by_coords')

runtime = timeit.default_timer() - start_time
print(f'Run time: {runtime} s')

print('writing to output...')
start_time = timeit.default_timer()

outnc = os.path.join(dir, os.path.basename(dir) + '.nc')
merged_nc.to_netcdf(outnc)

print('Done!')
runtime = timeit.default_timer() - start_time
print(f'Run time: {runtime} s')
