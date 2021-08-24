import sys
import time  
import ee 


def calculate_number_of_pixels(input_raster):
    imgDescription = ee.Algorithms.Describe(input_raster)
    height = ee.List(ee.Dictionary(ee.List(ee.Dictionary(imgDescription ).get("bands")).get(0)).get("dimensions")).get(0)
    width = ee.List(ee.Dictionary(ee.List(ee.Dictionary(imgDescription ).get("bands")).get(0)).get("dimensions")).get(1)
    num_pixels = height.getInfo() * width.getInfo()
    return num_pixels

def image_metadata(data, layer_name):
    num_pixels = calculate_number_of_pixels(data)
    print(f'Number of pixels: {num_pixels}')
    print(f'Image size: {(data.get("system:asset_size").getInfo())/ 1e9} gb')
    print(f'Image resolution: {data.select(layer_name).projection().nominalScale().getInfo()}')
    print(f"Image CRS: {data.select(layer_name).projection().getInfo()}")

def get_classNameValue_dict(data, layer_name):
  class_names = data.get(f'{layer_name}_class_names').getInfo()
  class_values = data.get(f'{layer_name}_class_values').getInfo()
  class_attr_dict = dict(zip(class_names, class_values))
  return class_attr_dict

def get_bbox(cdl_filtered):
  """
  Returns bounding box for clipping raster based on extent of host of interest
  in the CDL

  Parameters:
  ----------
  cdl_filtered : ee.ImageCollection
    CDL masked to host(s) of interest
  
  Returns:
  --------
  host_bbox : str
    Coordinates of bounding box in json format
  
  """

  classes = cdl_filtered.select('cropland').reduceToVectors(
    reducer=ee.Reducer.countEvery(), 
    scale=30,
    geometryType='polygon',
    bestEffort=True
  )

  if len(classes.getInfo()['features']) > 0:
    # Minimum bounding geometry based on host polygons to be used 
    # to clip the filtered host raster
    host_polys = ee.FeatureCollection(classes)
    host_bbox = host_polys.geometry().bounds()
    return host_bbox
  else:
    print(f'No pixels for host and year selection')
    sys.exit()


def get_mean_confidence_val(data):
  """
  Calculates average confidence value for pixels of interest. Based on
  confidence layer that spatially represents the predicted confidence that
  associated with that output pixel, based upon the rule(s) that were used 
  to classify it. 
  """
  mean_confidence = ee.Image.reduceRegion(
    data.select('confidence'), 
    ee.Reducer.mean(), 
    bestEffort=True
  )
  return mean_confidence.getInfo()['confidence']

def export_final_map(final_host_raster, host, cdl_year, lyr_name, source_tag, dst_folder, maxPixels):
  if '/' in host:
    host_name = host.replace('/', '_')
  else:
    host_name = host 
  task = ee.batch.Export.image.toDrive(
      image=final_host_raster.select(lyr_name),
      description=f'{host_name}_{cdl_year}_{source_tag}',
      folder=dst_folder,
      scale=30,
      fileNamePrefix=f'{host_name}_{cdl_year}_{source_tag}',
      fileFormat='GeoTIFF',
      maxPixels=maxPixels)
  task.start()
  while task.active():
    print(f"\t\tSTATUS: id: {task.id})\t {task.status()['state']}")
    time.sleep(60)

  if task.status()['state'] == 'COMPLETED':
    print("\t\tEXPORT SUCCEEDED")
    print(f"\t\t{task.status()}")

  if task.status()['state'] == 'FAILED':
    print("\t\tEXPORT FAILED")
    print(f"\t\t{task.status()}")