#!/usr/bin/env python
import ee
import csv
from google.cloud import storage
# from oauth2client.client import GoogleCredentials
import os
from pprint import pprint
import json
import sys

ee.Initialize(credentials='persistent', use_cloud_api=True)

tableName = "/home/joao/Documentos/trabalho/mapbiomas2.0/csv/alertas-ingerir-27042020.csv"


def readParamsTable(tableName):

    table = []

    with open(tableName) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            table.append({
                'alert_id': row['alert_id'],
                'gcs_image': row['gcs_image'],
            })
            
    return table


# acess storage
storageClient = storage.Client()
bucketName = 'alerta-sccon'
# bucketName = 'alerta-storage'
bucket = storageClient.get_bucket(bucketName)

# read table
table = readParamsTable(tableName)

nImages = len(table)

images2ingest = []

alertIds = []

for i in range(nImages):
    alertIds.append(table[i]['alert_id'])

    uri = table[i]['gcs_image']
    
    name = uri.replace('gs://{}/'.format(bucketName), '')

    stats = storage.Blob(bucket=bucket, name=name).exists(storageClient)

    print('[{}/{}] {} | {}'.format(i, nImages, stats, uri))

    if stats:

        try:
            # get bucket data as blob
            blob = bucket.get_blob(name.replace('cut.tif', 'manifest.json'))
            # download as string
            jsonString = blob.download_as_string()
            # loads json data
            manifest = json.loads(jsonString)
            
            try:
                image = ee.Image(manifest['name']).getInfo()
                print('image already exists.')
            except:
                print('ingesting...')
                
                images2ingest.append(uri)

                task = ee.data.startIngestion(
                    ee.data.newTaskId()[0], manifest)
        except Exception as e:
            print(e)

print('{} images to ingest'.format(len(images2ingest)))
print('{} alerts'.format(len(list(set(t)))))

