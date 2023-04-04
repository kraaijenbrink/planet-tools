from planet import api
import os
import json
import time
import io
from datetime import datetime
from shapely.geometry import shape, GeometryCollection, Polygon
from pyproj import Proj

# setup client
api_key = os.environ.get("PL_API_KEY")
client = api.ClientV1(api_key)

# load area of interest
with open('filters/aoi_tight.json') as f:
    aoi = json.load(f)

# for year in range(2016,2023):
for year in range(2019,2023):

    # setup search filters
    geometry_filter = api.filters.geom_filter(aoi, "geometry")
    date_filter = api.filters.date_range("acquired", gte = str(year)+"-01-01T00:00:00.001Z", lte = str(year+1)+"-01-01T00:00:00.001Z")
    cloud_filter = api.filters.range_filter("cloud_cover", lte=0.2)
    sr_filter = api.filters.asset_filter(["ortho_analytic_8b_sr","ortho_analytic_4b_sr"])
    udm_filter   = api.filters.asset_filter(["ortho_udm2"])
    comb_filter = api.filters.and_filter(geometry_filter, date_filter, cloud_filter, sr_filter, udm_filter)

    # build request and get paginated search results
    item_types = ['PSScene']
    request = api.filters.build_search_request(comb_filter, item_types)
    results = client.quick_search(request)

    # put all pages in one string
    output = io.StringIO()
    results.json_encode(output, limit=int(1e4))
    results_all = json.loads(output.getvalue())

    # # print only the ids of scenes that fully contain the aoi
    # aoi_shape = shape(aoi)
    # for feature in results.items_iter(10000):
    #     g = shape(feature["geometry"])
    #     if g.contains(aoi_shape):
    #         print(feature["id"])

    # build order request in JSON
    ids = [item['id'] for item in results_all['features']]  # edit this so that you can split it for each 500 units

    # split into chunks of less than 500 items for api to work
    order_lim = 100
    ids = [ids[i:i + order_lim] for i in range(0, len(ids), order_lim)]

    for chunk in range(len(ids)):

        part_no = "{:02d}".format(chunk+1)

        order_request = {  
        "name": "Meretschi-Illgraben "+str(year)+' part '+part_no,
        "source_type": "scenes",
        "order_type": "partial",
        "products":[
            {  
                "item_ids": ids[chunk],
                "item_type": "PSScene",
                "product_bundle": "analytic_8b_sr_udm2,analytic_sr_udm2"    # fallback to 4 band
            }
        ],
        "tools": [
            {
            "clip": {
                "aoi": aoi
            }
            },
            {
            "harmonize": {
                "target_sensor": "Sentinel-2"
            }
            }
        ]
        }
        #order_request_json = json.dumps(order_request)

        # client.create_order(order_request)

