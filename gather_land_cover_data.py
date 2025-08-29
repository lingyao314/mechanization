# This srcipt is used for requesting land cover data from the European Space Agency and aggregating
# cropland at the prefecture city level in China

#%%
import cdsapi # Climate Data Store API
import os
os.environ['GDAL_DRIVER_PATH']  = 'C:/Users/yaoan/Anaconda3/envs/Mechanization/Library/lib/gdalplugins'
from osgeo import gdal # Before importing, set the GDAL_DRIVER_PATH first
import numpy as np
import rasterio
from rasterstats import zonal_stats
import geopandas as gpd
import matplotlib
matplotlib.use('TkAgg')  # or try 'QtAgg' if you have PyQt installed
import matplotlib.pyplot as plt
import csv

#%%

dataset = "satellite-land-cover"
request = {
    "variable": "all",
    "year": [
        "2016", "2017", "2018",
        "2019"
    ],
    "version": ["v2_1_1"],
    "area": [53.56, 73.5, 18.16, 134.77] # China bbox
}

client = cdsapi.Client() # my CDS token saved a .cdsapirc file in the C:\Users\Username folder
client.retrieve(dataset, request).download()

request = {
    "variable": "all",
    "year": ["2015"],
    "version": ["v2_0_7cds"],
    "area": [53.56, 73.5, 18.16, 134.77] # China bbox
}

client.retrieve(dataset, request).download()

#%% Convert the .nc file into GeoTiff (after manually extracting the zip file)
input_file = '../Land cover data/C3S-LC-L4-LCCS-Map-300m-P1Y-2019-v2.1.1.area-subset.53.56.134.77.18.16.73.5.nc'
subdataset_name = 'NETCDF:{}:lccs_class'.format(input_file)
output_file = '../Land cover data/C3S-LC-L4-LCCS-Map-300m-P1Y-2019-v2.1.1.area-subset.53.56.134.77.18.16.73.5.tif'
ds = gdal.Open(subdataset_name)
# Define the options matching gdalwarp CLI options
warp_options = gdal.WarpOptions(
    format='GTiff',
    outputBounds=[73.5, 18.16, 134.77, 53.56],   # -minx, miny, maxx, maxy
    xRes=0.002777777777778,                       # -tr x resolution
    yRes=0.002777777777778,                       # -tr y resolution
    dstSRS='EPSG:4326',                           # -t_srs
    outputType=gdal.GDT_Byte,                     # -ot Byte
    creationOptions=['COMPRESS=LZW', 'TILED=YES']
)
# Perform the warp (reprojection + resampling + output to GeoTIFF)
gdal.Warp(destNameOrDestDS=output_file, srcDSOrSrcDSTab=ds, options=warp_options)

#%%
city_shapes = gpd.read_file("C:/Users/yaoan/OneDrive/Documents/PhD Research/全国100万基础地理数据2017/全国100万基础地理数据2017/地级行政区.shp")
city_shapes = city_shapes.astype({'Shape_Area': float}, copy = False)
city_shapes = city_shapes.sort_values(by = ['ENTIID', 'Shape_Area'], ascending=[True, False]).drop_duplicates(subset=['ENTIID'], keep='first').reset_index(drop=True)

with open('../Land cover data/ESACCI-LC-Legend.csv', mode='r', newline='') as file:
    reader = csv.reader(file)
    header = next(reader) # skip the first row as the header
    cmap = {int(''.join(row).split(';')[0]): ''.join(row).split(';')[1] for row in reader}


#%% Calculate the zonal statistics https://pythonhosted.org/rasterstats/manual.html

with rasterio.open(
        '../Land cover data/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7cds.area-subset.53.56.134.77.18.16.73.5.tif') as src:
    # Read the first band
    band1 = src.read(1)
    affine = src.transform


zs = zonal_stats(city_shapes, band1, affine = affine, stat = 'count', nodata=0, categorical=True, category_map = cmap)

lc = pd.DataFrame(zs)
lc = pd.concat([lc, city_shapes['NAME']], axis=1).rename(columns = {'NAME':'city'})
lc = pd.concat([lc, city_shapes['ENTIID'].str[4:8]], axis=1).rename(columns = {'ENTIID':'city_code'})
lc['year'] = 2015
lc2015 = lc
#%%

to_save = pd.concat([lc2015, lc2016, lc2017, lc2018, lc2019], ignore_index = True)
to_save = to_save.fillna(0)
to_save['cropland_pixels'] = to_save['Cropland rainfed'] + to_save['Cropland irrigated or post-flooding']
to_save[['cropland_pixels', 'year', 'city','city_code']].to_excel("dataprocessing/output/cropland.xlsx", index = False)