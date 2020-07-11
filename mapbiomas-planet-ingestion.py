#!/usr/bin/env python
import ee
import os
import json
import sys
import random
from pprint import pprint

sys.path.append(os.path.abspath('./gee_toolbox'))
import gee as gee_toolbox

ACCOUNTS = [
    "joao"
    # 'mapbiomas-ingestion-1',
    # 'mapbiomas-ingestion-2',
    # 'mapbiomas-ingestion-3',
    # 'mapbiomas-ingestion-4',
    # 'mapbiomas-ingestion-5',
]


def Ingest(manifest):

    try:
        task = ee.data.startIngestion(
            ee.data.newTaskId()[0], manifest)
    except Exception as e:
        print 'error!', e


if __name__ == '__main__':

    ee.Initialize(credentials='persistent', use_cloud_api=True)

    account = random.choice(ACCOUNTS)
    gee_toolbox.switch_user(account)
    
    manifestFile = "./4157_20190601_140036_1006.json"

    with open(manifestFile) as manifestData:
        manifest = json.load(manifestData)
        pprint(manifest)
        Ingest(manifest)

    ee.Reset()
