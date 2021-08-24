# Import packages
import os
import glob
import shutil
import geopandas as gpd
import numpy as np
import pandas as pd
import gdal
import rasterio
from rasterio.mask import mask
import dask.array as da
from dask_rasterio import read_raster, write_raster 
import ee 
from pathlib import Path

from helpers import get_mean_confidence_val, export_final_map
from query import query_and_clip_cdl
from process_files import reclassify_layer_values

# Set paths and host variables 
google_dir_path = "H:/My Drive"
project_dir = "H:/Shared drives/APHIS  Projects/eRADS"
export_folder_name = "ee_output"
start_year = 2015
stop_year = 2020
rast_years = list(range(start_year, stop_year + 1, 1))
rast_lyr_name = 'cropland'
host_list = ["Corn"] 
threshold = 0.50 # accuracy of 50%

# Initialize Earth Engine 
ee.Initialize()

# Query Cropland Data Layer for identified years and hosts 
for year in rast_years:
    print(f'{year}: Acquiring Cropland Data Layer & checking classification accuracies...')
    start_date = f'{year}-01-01'
    end_date = f'{year}-12-31'
    img_data = ee.ImageCollection('USDA/NASS/CDL').filter(ee.Filter.date("2020-01-01", "2020-12-31")).first()
    proj = ee.Projection('EPSG:4326')
    img_data_prj = img_data.reproject(proj, None, 30)
    cdl_accuracy_df = pd.read_csv(f'{project_dir}/data/cdl_accuracy_eval/{year}/cdl_accu_filtered_wType.csv').set_index('Category')

    for host in host_list:
        if '/' in host:
            host_name = host.replace('/', '_')
        else:
            host_name = host 
        print(f'\t{host_name}: Selecting host from CDL...')

        if (cdl_accuracy_df.loc[host, 'Producer'] >= threshold) and (cdl_accuracy_df.loc[host, 'User'] >= threshold):
            print('\t\tAccuracy threshold met')
            try:
                source_tag = 'cdl'
                cdl_filtered = query_and_clip_cdl(
                    start_date=start_date,
                    end_date=end_date,
                    host=host
                    )
                confidence_val = get_mean_confidence_val(cdl_filtered)
                cdl_accuracy_df.loc[host, 'avg_confidence'] = confidence_val/100
            except:
                print(f'\t**ERROR selectin {host} from {year}')
                
            # max_pixels = calculate_number_of_pixels(cdl_clipped) + 10000
            max_pixels = 2.5e10 

            proj = ee.Projection('EPSG:4326')
            cdl_filtered_prj = cdl_filtered.reproject(proj, None, 30)
            print('\t\tReprojecting...')

            print(f'\tExporting {source_tag} for {host} with {max_pixels} pixels')
            export_final_map(
                final_host_raster=cdl_filtered_prj,
                host=host, 
                cdl_year=year, 
                lyr_name=rast_lyr_name,
                source_tag=source_tag,
                dst_folder='ee_output', 
                maxPixels=max_pixels,
            )
                
            tile_dst_path = f"{project_dir}/data/{year}/{host_name}_prj"
            if not os.path.exists(tile_dst_path):
                os.makedirs(tile_dst_path)
            files_to_move = glob.glob(f"{google_dir_path}/{export_folder_name}/*{host_name}_{year}*")
            print(f'\t\tMoving files from local drive to shared project folder')
            for file in files_to_move:
                shutil.move(file, tile_dst_path + f"/{os.path.basename(file)}")
            
            print(f'\tCreating final mosaic...')           
            GDAL_CO = [
                "BIGTIFF=YES",
                "BLOCKXSIZE=1024",
                "BLOCKYSIZE=1024",
                "TILED=YES",
                "COMPRESS=LZW",
                "NUM_THREADS=ALL_CPUS",
            ]
            mosaic_dst_dir = f"{project_dir}/data/{year}/{host_name}_mosaic_prj"
            if not os.path.exists(mosaic_dst_dir):
                os.makedirs(mosaic_dst_dir)

            tif_files = list(glob.glob(f"{tile_dst_path}/*.tif"))
            unique_ids = [Path(x).stem.split("-")[:-2] for x in tif_files]
            unique_ids = ["-".join(x) for x in unique_ids]
            unique_ids = set(unique_ids)
            print(unique_ids)

            for uid in unique_ids:
                files_to_merge = list(glob.glob(f"{tile_dst_path}/{uid}*.tif"))
                dst_tif_path = mosaic_dst_dir + f"/{uid}.tif"
                print(f"\tmosaicking image {dst_tif_path}...")
                my_ras = gdal.Warp(
                    str(dst_tif_path),
                    [str(x) for x in files_to_merge],
                    options=gdal.WarpOptions(dstSRS='EPSG:4326', creationOptions=GDAL_CO),
                )
                my_ras = None
                print("\t\t\tDONE")
                print('\n\n')
                        
            else:
                print(f"\t{host_name} did not meet accuracy threshold")
                print(cdl_accuracy_df.loc[host, 'Producer'], (cdl_accuracy_df.loc[host, 'User']))


# Create reclassified stacks of relevant CDL years 
stack_dst_dir = project_dir + f"/data/reclassified_host_stacks"
mosaic_list = sorted(glob.glob(project_dir + f"/data/20*/{host}_mosaic_prj/*.tif"))
start_year = os.path.basename(mosaic_list[0]).split('_')[1]
end_year = os.path.basename(mosaic_list[-1]).split('_')[1]

class_names = list(cdl_accuracy_df.index)
class_values = cdl_accuracy_df["class_id"]
host_dict = dict(zip(class_names, class_values))

for host in host_list:
    print(f'{host}: Stacking and reclassifying mosaics for {start_year} to {end_year}')
    reclass_stack_dst_path = stack_dst_dir + f"/{host}/{host}_stack_{start_year}-{end_year}_reclassified.tif"
    if not os.path.exists(stack_dst_dir + f"/{host}"):
        os.makedirs(stack_dst_dir + f"/{host}")
    host_code = host_dict[host]
    print('\treclassifying...')
    rast2arr = reclassify_layer_values(
        raster_list=mosaic_list, 
        present_indicator=2, 
        absent_indicator=1, 
        host_code=host_code  
    )
    print('\tstacking...')
    stack = da.stack(rast2arr).astype(np.uint32)
    
    with rasterio.open(mosaic_list[0]) as src:
        profile=src.profile
        profile.update(dtype=np.uint32)
    print('\tsaving...')
    write_raster(
        reclass_stack_dst_path,
        da.nansum(stack, axis=0),
        **profile
    )
