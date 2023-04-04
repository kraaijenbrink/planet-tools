from planet import api
import os
import time

# setup client
api_key = os.environ.get("PL_API_KEY")
client = api.ClientV1(api_key)

# get order information
orders = client.get_orders()
order_ids = [x['id'] for x in orders.items_iter(1000) if 'Meretschi-Illgraben ' in x['name']]
order_names = [x['name'] for x in orders.items_iter(1000) if 'Meretschi-Illgraben ' in x['name']]

# download output
out_path = 'c:/workspace/meretschi-data/planet'
if not os.path.exists(out_path):
    os.mkdir(out_path)

# loop over orders and put in respective dirs, wait for finish of each order
# for i in range(len(order_ids)):
for i in range(16,len(order_ids)):
    id = order_ids[i]
    nm = order_names[i]
    dirname = os.path.join(out_path, nm.replace(' ','_'))
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    order_data = client.get_individual_order(id)
    # order_data_names = order_data.get()['name']
    loc_urls = [item for item in order_data.items_iter(1000)]

    # download order, monitor output dir size to check whether it is finished (HACK FOR NOW)

    print('Downloading ' + nm + ' (' + str(len(loc_urls)) + ' files)', '...')
    callback = api.write_to_file(directory=dirname, overwrite=True)
    client.download_order(id, callback=callback)

    dirsize = 1
    dirsize_old = 0
    while dirsize_old < dirsize:
        dirsize_old = dirsize
        time.sleep(5)
        dirsize = sum(os.path.getsize(os.path.join(dirname, f)) for f in os.listdir(dirname) if not f.endswith('.tmp'))

    print('Finished downloading')






