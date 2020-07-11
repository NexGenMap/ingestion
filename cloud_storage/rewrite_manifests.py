#!/usr/bin/env python
import calendar
from collections import defaultdict
from cStringIO import StringIO
import ee
from google.cloud import storage
from oauth2client.client import GoogleCredentials
from pprint import pprint
import json
import sys
import time

# The location of the input data in Google Cloud Storage.
GCS_BUCKET = 'sistema_alertas_sccon'
GCS_PREFIX = 'manifests/pack-7/'
GCS_IGNORE = ['init']


def ScanAndUpload():

    bucket = storage.Client().get_bucket(GCS_BUCKET)

    blobs = bucket.list_blobs(prefix=GCS_PREFIX)

    count = int(1)
    nFilesPerCollection = 100000

    for blob in blobs:
        jsonData = blob.download_as_string()

        div = count / nFilesPerCollection
        remained = count % nFilesPerCollection

        print('[{}] [tiles-{}] {}'.format(count, str(div + 1), blob.name))

        try:
            filename = blob.name.replace('pack-7', 'pack-8')


            if div > 0 and remained == 0:
                bucket.blob(filename).upload_from_string(
                    jsonData.replace('PLANET/tiles', 'PLANET/tiles-'+ str(div + 1)))
            else:
                bucket.blob(filename).upload_from_string(
                    jsonData.replace('PLANET/tiles', 'PLANET/tiles-1'))
        except Exception as e:
            print(e)

        count += int(1)


if __name__ == '__main__':
    ScanAndUpload()
