from planet import api
import os
import time
import requests
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool


# settings =================================================================

# list (multiple) orders by matching part of string
str_match = "Meretschi"

# root of output, additional subdirs will be created using order names
out_path = 'c:/workspace/temporary/'

# ==========================================================================


# setup client
api_key = os.environ.get("PL_API_KEY")
client = api.ClientV1(api_key)

# get order information
orders = client.get_orders()
order_ids = [x['id'] for x in orders.items_iter(1000) if str_match in x['name']]
order_names = [x['name'] for x in orders.items_iter(1000) if str_match in x['name']]

# prep output location
if not os.path.exists(out_path):
    os.mkdir(out_path)

# loop over orders and put in respective sub dirs, wait for finish of each order
for i in range(len(order_ids)):
    id = order_ids[i]
    nm = order_names[i]
    dirname = os.path.join(out_path, nm.replace(' ','_'))
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    # prepare data
    order_data = client.get_individual_order(id)
    order_results = order_data.get()['_links']['results']
    filenames = [item['name'] for item in order_results]
    basenames = [os.path.basename(fn) for fn in filenames]
    download_urls = [item['location'] for item in order_results]
    dest_urls = [os.path.join(dirname, fn) for fn in basenames]
    inputs = zip(download_urls, dest_urls)

    # download order
    print('Downloading ' + nm + ' (' + str(len(download_urls)) + ' files)', '...')
    callback = api.write_to_file(directory=dirname, overwrite=True)
    client.download_order(id, callback=callback)

    # monitor output dir size to check whether it is finished (HACK FOR NOW), should use proper async monitor
    # although client will continue downloading files to each folder one by one by itself
    dirsize = 1
    dirsize_old = 0
    while dirsize_old < dirsize:
        dirsize_old = dirsize
        time.sleep(30)
        dirsize = sum(os.path.getsize(os.path.join(dirname, f)) for f in os.listdir(dirname) if not f.endswith('.tmp'))

    print('Finished downloading', nm)