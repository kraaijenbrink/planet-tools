from planet import api
import os
import json
import time
import io
from datetime import datetime
from shapely.geometry import shape, GeometryCollection, Polygon
from shapely.ops import transform
import pyproj
import geopandas as gpd

# settings
perform_dryrun  = True          # first perform a dryrun to check settings and return before ordering massive amounts!
order_name      = 'Fieldwork France 2024'
aoi_file        = 'filters/fieldwork-france-aoi.geojson'
start_date      = '2016-01-01'
end_date        = '2024-01-01'
max_cloud       = 0.05
udm_required    = True
clip_to_aoi     = True
harmonize_to_S2 = True
assets          = ["ortho_analytic_8b_sr","ortho_analytic_4b_sr"]
product_bundle  = "analytic_8b_sr_udm2,analytic_sr_udm2"    # second is fallback

# setup client
api_key = os.environ.get("PL_API_KEY")
client  = api.ClientV1(api_key)

# load area of interest
with open(aoi_file) as f:
    aoi = json.load(f)

# setup search filters
geometry_filter = api.filters.geom_filter(aoi, "geometry")
date_filter     = api.filters.date_range("acquired", gte = start_date + "T00:00:00.001Z", lte = end_date + 'T23:59:59.999Z')
cloud_filter    = api.filters.range_filter("cloud_cover", lte=max_cloud)
sr_filter       = api.filters.asset_filter(assets)
udm_filter      = api.filters.asset_filter(["ortho_udm2"])
if udm_required:
    comb_filter = api.filters.and_filter(geometry_filter, date_filter, cloud_filter, sr_filter, udm_filter)
else:
    comb_filter = api.filters.and_filter(geometry_filter, date_filter, cloud_filter, sr_filter)

# build request and get paginated search results
item_types = ['PSScene']
request    = api.filters.build_search_request(comb_filter, item_types)
results    = client.quick_search(request)

# put all pages in one string
output = io.StringIO()
results.json_encode(output, limit=int(1e5))
results_all = json.loads(output.getvalue())
no_of_features = len(results_all['features'])

# get total area in km2 for total order
polys_raw    = [Polygon(item['geometry']['coordinates'][0]) for item in results_all['features']]
polys_df     = gpd.GeoDataFrame(index=range(len(polys_raw)), crs='epsg:4326', geometry=polys_raw )
to_proj      = pyproj.Proj(proj='aea', lat_1=polys_df.total_bounds[1], lat_2=polys_df.total_bounds[2])
polys_df_aea = polys_df.to_crs(str(to_proj))
aoi_df_aea   = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[Polygon(aoi['coordinates'][0])]).to_crs(str(to_proj))
polys_in_aoi = polys_df_aea.clip(aoi_df_aea)
if clip_to_aoi:
    order_area   = sum(polys_in_aoi.area) * 1e-6
else:
    order_area   = sum(polys_df_aea.area) * 1e-6

# get ids and split into chunks of less than 500 items for api to work
ids       = [item['id'] for item in results_all['features']]
order_lim = 400
ids       = [ids[i:i + order_lim] for i in range(0, len(ids), order_lim)]

# set toolchain json according to settings
if clip_to_aoi & harmonize_to_S2:
    toolchain = [{"clip": {"aoi": aoi}},{"harmonize": {"target_sensor": "Sentinel-2"}}]
elif clip_to_aoi:
    toolchain = [{"clip": {"aoi": aoi}}]
elif harmonize_to_S2:
    toolchain = [{"harmonize": {"target_sensor": "Sentinel-2"}}]
else:
    toolchain = []

# show info
print('\nTotal items: ' + str(no_of_features))
print("Total order area: " + str(round(order_area,0)) + " km2 ("\
      + str(round(order_area / 3e7 * 100, 2)) + "% of UU DPG quota)\n")

# make separate orders for the chunks
for i in range(len(ids)):

    part_no = "{:02d}".format(i+1)
    print('Ordering: ' + order_name + " part " + part_no + ' (' + str(len(ids[i])) +' items)...')


    order_request = {  
    "name": order_name + " part " + part_no,
    "source_type": "scenes",
    "order_type": "partial",
    "products":[
        {  
            "item_ids": ids[i],
            "item_type": "PSScene",
            "product_bundle": product_bundle
        }
    ],
    "tools": toolchain
    }

    order_request_json = json.dumps(order_request)
    
    if perform_dryrun:
        print('Aborting order (dryrun)')
    else:
        client.create_order(order_request)

print('Finished')