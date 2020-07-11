#!/usr/bin/env python
import ee
import os
import csv
import json
import time
import calendar
import iso8601
from pprint import pprint
from google.cloud import storage

METADATA_FILE = './skysat/skysat_metadata.txt'

LOG_FILE = './skysat/skysat_log.txt'

CREDENTIALS_FILE = "./skysat/credentials/ecostage.json"

# The location of the input data in Google Cloud Storage.
GCS_BUCKET = 'skysat'

# The uri prefix of each image
URI_PREFIX = "gs://{}/{}"

# The Earth Engine image collection to write to.
EE_COLLECTION_FOLDER = 'projects/nexgenmap/MapBiomas2/SKYSAT'

# The prefix for full-qualified Earth Engine resource names for legacy projects.
# (Right now all projects are legacy projects!)
EE_RESOURCE_NAME_PREFIX = 'projects/earthengine-legacy/assets'

INFO_LIST = [
    {
        "collection": "analytic",
        "bands": ["B0", "B1", "B2", "B3"]
    },
    {
        "collection": "analytic_dn",
        "bands": ["B0", "B1", "B2", "B3"]
    },
    {
        "collection": "analytic_udm",
        "bands": ["B0"]
    },
    {
        "collection": "ortho_panchromatic",
        "bands": ["B0"]
    },
    {
        "collection": "panchromatic_dn",
        "bands": ["B0"]
    },
    {
        "collection": "panchromatic_udm",
        "bands": ["B0"]
    },
    {
        "collection": "panchromatic_udm2",
        "bands": ["B0"]
    },
    {
        "collection": "pansharpened",
        "bands": ["B0", "B1", "B2", "B3"]
    },
    {
        "collection": "pansharpened_udm",
        "bands": ["B0"]
    },
    {
        "collection": "pansharpened_udm2",
        "bands": ["B0"]
    },
    {
        "collection": "udm2",
        "bands": ["B0"]
    },
    {
        "collection": "visual",
        "bands": ["B0", "B1", "B2", "B3"]
    },
]

def TimestampToSeconds(iso_time_string):
    
    iso_time_string = iso_time_string.split('.')[0].replace('Z','')

    timestamp = time.strptime(iso_time_string, '%Y-%m-%dT%H:%M:%S')

    return calendar.timegm(timestamp)

jsonFiles = open(METADATA_FILE, 'r').read().splitlines()

logFile = open(LOG_FILE,'w')

# acess storage
storageClient = storage.Client.from_service_account_json(CREDENTIALS_FILE)

bucket = storageClient.get_bucket(GCS_BUCKET)

ee.Initialize(credentials='persistent', use_cloud_api=True)

for jsonFile in jsonFiles:

    preffix = jsonFile \
            .replace('gs://{}/'.format(GCS_BUCKET), '') \
            .replace('_metadata.json', '')

    for info in INFO_LIST:

        name = "{}_{}.tif".format(preffix, info['collection'])
        
        stats = storage.Blob(bucket=bucket, name=name).exists(storageClient)

        print('[{}] {}'.format(stats, name))

        if stats:
            jsonName = "{}_metadata.json".format(preffix)

            # get bucket data as blob
            blob = bucket.get_blob(jsonName)

            # download as string
            jsonString = blob.download_as_string()

            # get metadata from json metafile
            metadata = json.loads(jsonString)
            
            # build manifest
            manifest = {
                "name": os.path.join(
                    EE_RESOURCE_NAME_PREFIX, 
                    EE_COLLECTION_FOLDER, 
                    info['collection'], 
                    preffix.replace('IMAGES/', "")
                ),
                "tilesets": [{
                    "sources": [{
                        "uris": [URI_PREFIX.format(GCS_BUCKET, name)],
                    }],
                }],
                "bands": [{"id": bid, "tileset_band_index": index} for (index, bid) in enumerate(info['bands'])],
                "properties": metadata['properties'],
                "start_time": {
                    "seconds": TimestampToSeconds(metadata['properties']['acquired']),
                },
            }

            # pprint(manifest)

            try:
                image = ee.Image(manifest['name']).getInfo()
                print(f'\033[93m[Image already exists]\033[0m', preffix)
            except:
                # try:
                # start ingesting task
                print(f'\033[92mIngerindo:\033[0m', preffix)
                task = ee.data.startIngestion(
                    ee.data.newTaskId()[0], manifest)
                # except:
                #     print(f'\033[91mErro ao ingerir: \033[0m', preffix)
                #     logFile.write(name)
        else:
            print('\033[91m[LOG]\033[0m')
            logFile.write(name)

logFile.close()
