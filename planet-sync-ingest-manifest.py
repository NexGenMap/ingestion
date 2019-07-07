#!/usr/bin/env python
import calendar
from collections import defaultdict
from cStringIO import StringIO
import ee
from google.cloud import storage
from oauth2client.client import GoogleCredentials
import os
import time
import xml.etree.ElementTree as ET
from pprint import pprint
import json
import sys
sys.path.append(os.path.abspath('./gee_toolbox'))
import gee as gee_toolbox
import time
import glob
import random

# The Earth Engine image collection to write to.
EE_COLLECTION = 'projects/nexgenmap/MapBiomas2/PLANET/tiles'

JSON_PATH = '/var/www/manifest'

ACCOUNTS = ['joao',
            'mapbiomas1', 'mapbiomas2',
            'mapbiomas3', 'mapbiomas4',
            # 'mapbiomas5', 'mapbiomas6',
            # 'mapbiomas7', 'mapbiomas8',
            # 'mapbiomas9', 'mapbiomas10'
            ]


def GetExistingAssetIds(collection_id, batch_size=10000):
    """Returns a list of existing asset IDs in an ImageCollection."""
    collection = ee.ImageCollection(collection_id)
    results = []
    start_time = time.time()
    page_token = ' '  # A token guaranteed to come before all asset ids.
    while True:
        batch = (collection.filter(ee.Filter.gt('system:index', page_token))
                 .limit(batch_size)
                 .reduceColumns(ee.Reducer.toList(), ['system:index'])
                 .get('list')
                 .getInfo())
        results.extend(batch)
        if len(batch) < batch_size:
            duration = time.time() - start_time
            return results
        else:
            page_token = batch[-1]


def Ingest(manifest):

    try:
        task = ee.data.startIngestion(
            ee.data.newTaskId()[0], manifest)

    except Exception as e:
        print 'error!', e


if __name__ == '__main__':
    while True:
        print('Initializing...')

        ee.Initialize(credentials='persistent', use_cloud_api=True)

        jsonFiles = glob.glob('{}/*.json'.format(JSON_PATH))
     
        assetids = GetExistingAssetIds(EE_COLLECTION)

        count = 1
        account = random.choice(ACCOUNTS)
        for jsonFile in jsonFiles:

            imageName = os.path.splitext(os.path.basename(jsonFile))[0]
            
            with open(jsonFile) as json_file:
                manifest = json.load(json_file)

            if imageName not in assetids:
                print('[{}] {} {}'.format(account, count, jsonFile))

                Ingest(manifest)

                if count > 500:
                    ee.Reset()

                    account = random.choice(ACCOUNTS)

                    gee_toolbox.switch_user(account)

                    try:
                        ee.Initialize(credentials='persistent',
                                      use_cloud_api=True)
                    except:
                        print 'Initialize error'

                    count = 1
                else:
                    count = count + 1

        print("Nap time! I'll be back in 1 hour. See you!")
        time.sleep(300)