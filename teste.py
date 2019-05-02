import json
import os
from pprint import pprint
from google.cloud import storage
import sys
sys.path.append(os.path.abspath('./gee_toolbox'))
import gee as gee_toolbox

# The location of the input data in Google Cloud Storage.
GCS_BUCKET = 'sistema_alertas_sccon'
GCS_PREFIX = 'tiles/'
GCS_IGNORE = ['init']

account = 1

jsonFile = os.path.join(os.getcwd(), 'grids.geojson')

ACCOUNTS = ['joao']


def getTiles(jsonFile, account=1):

    with open(jsonFile) as f:
        jsonData = json.load(f)

    features = jsonData['features']

    filtered = filter(
        lambda feature: feature['properties']['account'] == account, features)

    tiles = map(
        lambda feature: feature['properties']['PageNumber'], filtered)

    return tiles


bucket = storage.Client().get_bucket(GCS_BUCKET)
# blobs = bucket.list_blobs(prefix=GCS_PREFIX)

# tiles = getTiles(jsonFile, account=account)
blob = storage.Blob(
    'tiles/75004/20190326_124419_1034/20190326_124419_1034.xml',
    bucket=bucket)
# count = 1

print(blob.download_as_string())
# print(blob.name)

# for blob in blobs:

#     directory, filename = os.path.split(blob.name)

#     tile = int(directory.split('/')[1])

#     if tile in tiles:
#         print directory
#         print filename
# for i in range(0, len(ACCOUNTS)):
#     print i+1, ACCOUNTS[i]
