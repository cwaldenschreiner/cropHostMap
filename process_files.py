import os
import glob
import numpy as np
from osgeo import gdal
from pathlib import Path
import xarray 


def stitch_tiles(src_dir, dst_dir):
    GDAL_CO = [
        "BIGTIFF=YES",
        "BLOCKXSIZE=1024",
        "BLOCKYSIZE=1024",
        "TILED=YES",
        "COMPRESS=LZW",
        "NUM_THREADS=ALL_CPUS",
    ]

    tif_files = list(glob.glob(f"{src_dir}/*.tif"))
    unique_ids = [Path(x).stem.split("-")[:-2] for x in tif_files]
    unique_ids = ["-".join(x) for x in unique_ids]
    unique_ids = set(unique_ids)

    for uid in unique_ids:
        files_to_merge = list(glob.glob(f"{src_dir}/{uid}*.tif"))
        dst_tif_path = dst_dir + f"/{uid}.tif"
        print(f'Mosaicking {len(files_to_merge)} to {os.path.basename(dst_tif_path)}...')
        my_ras = gdal.Warp(
            str(dst_tif_path),
            [str(x) for x in files_to_merge],
            options=gdal.WarpOptions(creationOptions=GDAL_CO),
        )
        my_ras = None
        print("\t...done.")

def reclassify_layer_values(
    raster_list,
    present_indicator, 
    absent_indicator,
    host_code, 
):
    absent_reclass = []
    for i in reversed(range(0, len(raster_list))):
        code = f"{absent_indicator}e{i}"
        absent_reclass.append(int(float(code)))

    present_reclass = []
    for i in reversed(range(0, len(raster_list))):
        code = f"{present_indicator}e{i}"
        present_reclass.append(int(float(code)))

    rast2arr = []
    for i in range(0, len(raster_list)):
        absent_code = absent_reclass[i]
        present_code = present_reclass[i]
        print(f"\tReclassifying layer {i} to present: {present_code} | absent: {absent_code}")
        arr = xarray.open_rasterio(raster_list[i], chunks={'band': 1, 'x': 1024, 'y': 1024}).astype(np.uint32)
        arr_reclass = xarray.where(arr == host_code, present_code, absent_code)
        rast2arr.append(arr_reclass)
    return rast2arr