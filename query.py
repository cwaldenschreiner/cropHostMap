import sys 
import ee 

from helpers import get_classNameValue_dict

def query_and_clip_cdl(start_date, end_date, host):
  cdl_data = ee.ImageCollection('USDA/NASS/CDL').filter(ee.Filter.date(start_date, end_date)).first()
  # Create dictionary of host names and corresponding CDL class value
  # to filter CDL raster to host of interest 
  host_attr_dict = get_classNameValue_dict(cdl_data, 'cropland')
  try:
    host_value = host_attr_dict[host]
    print('\t', host_value)
    # Filter CDL to host of interest
    print(f'\tFiltering to {host}...')
    cdl_filtered = cdl_data.updateMask(cdl_data.select('cropland').eq(host_value)) 
    return cdl_filtered 
  
  except KeyError or SyntaxError:
    print(f"\t{host} not available in CDL.") 
    sys.exit()