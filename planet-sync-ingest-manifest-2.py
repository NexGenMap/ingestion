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

JSON_PATH = '/home/joao/Documents/projetos/ingestion/manifest.json'

ACCOUNTS = [
        #    'joao',
           'mapbiomas-ingestion-1',
            ]

def Ingest(manifest):

    try:
        task = ee.data.startIngestion(
            ee.data.newTaskId()[0], manifest)

    except Exception as e:
        print 'error!', e


if __name__ == '__main__':
    print('Initializing...')

    count = 1
    account = random.choice(ACCOUNTS)
    gee_toolbox.switch_user(account)

    ee.Initialize(credentials='persistent', use_cloud_api=True)
    
    imageName = os.path.splitext(os.path.basename(JSON_PATH))[0]
    
    print('[{}] {}'.format(account, JSON_PATH))

    try:
        with open(JSON_PATH) as json_file:
            manifest = json.load(json_file)

        Ingest(manifest)
    except Exception as e:
        print(e)
    
    gee_toolbox.switch_user('joao')
