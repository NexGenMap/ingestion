#!/usr/bin/env python
import ee
import csv
from google.cloud import storage
# from oauth2client.client import GoogleCredentials
import os
from pprint import pprint
import json
import sys
sys.path.append(os.path.abspath('./gee_toolbox'))
import gee as gee_toolbox

ee.Initialize(credentials='persistent', use_cloud_api=True)

tableName = "/home/joao/Documents/trabalho/mapbiomas2.0/csv/data-1586173799712.csv"


def readParamsTable(tableName):

    table = []

    with open(tableName) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            table.append(row)

    return table


# acess storage
storageClient = storage.Client()
bucketName = 'alerta-sccon'
# bucketName = 'alerta-storage'
bucket = storageClient.bucket(bucketName)

# read table
table = readParamsTable(tableName)
print(table[0].keys())
nImages = len(table)

with open(tableName.replace('.csv', '-atualizado.csv'), 'w') as outuputFile:

    writer = csv.DictWriter(outuputFile, fieldnames=table[0].keys())
    writer.writeheader()

    for i in range(nImages):

        uri = table[i]['gcs_image']
        
        name = uri.replace('gs://{}/'.format(bucketName), '')

        stats = storage.Blob(bucket=bucket, name=name).exists(storageClient)

        print('[{}/{}] {} | {}'.format(i, nImages, stats, uri))

        table[i]['gcs_exists'] = stats

        writer.writerow(table[i])

outuputFile.close()
