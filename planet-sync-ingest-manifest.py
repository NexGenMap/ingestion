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

JSON_PATH = '/var/www/manifest'

ACCOUNTS = [
           'mapbiomas1', 'mapbiomas2',
           'mapbiomas3', 'mapbiomas4',
           # 'mapbiomas5', 'mapbiomas6',
           # 'mapbiomas7', 'mapbiomas8',
           # 'mapbiomas9', 'mapbiomas10'
            ]


def GetExistingAssetIds(collection_id, batch_size=10000):
    """Returns a list of existing asset IDs in an ImageCollection."""
    collection = ee.ImageCollection(collection_id)
    size = collection.size().getInfo()
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
        print(len(results), size)
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

        jsonFiles = glob.glob('{}/*/*.json'.format(JSON_PATH))
     
        print('{} manifests found.'.format(len(jsonFiles)))
        
        count = 1
        account = random.choice(ACCOUNTS)
        gee_toolbox.switch_user(account)

        ee.Initialize(credentials='persistent', use_cloud_api=True)
        
        imageList = GetExistingAssetIds('projects/nexgenmap/MapBiomas2/PLANET/tiles')

        for jsonFile in jsonFiles:

            imageName = os.path.splitext(os.path.basename(jsonFile))[0]
            
            if imageName not in imageList:
                print('[{}] {} {}'.format(account, count, jsonFile))

                try:
                    with open(jsonFile) as json_file:
                        manifest = json.load(json_file)
                        #manifest["name"] = manifest["name"].replace("tiles", "tiles-2")

                    Ingest(manifest)
                except Exception as e:
                    print(e)
                    continue

                if count > 500:
                    ee.Reset()

                    account = random.choice(ACCOUNTS)

                    gee_toolbox.switch_user(account)

                    try:
                        ee.Initialize(credentials='persistent', use_cloud_api=True)
                    except:
                        print 'Initialize error'
                        continue

                    count = 1
                else:
                    count = count + 1

            else:
                print('removing: ', jsonFile)
                os.remove(jsonFile)

        print("Nap time! I'll be back in 1 hour. See you!")
        time.sleep(300)
