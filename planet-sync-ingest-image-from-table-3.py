#!/usr/bin/env python
import ee
import csv
from google.cloud import storage
# from oauth2client.client import GoogleCredentials
import os
from pprint import pprint
import json
import sys
import calendar
import time
from io import StringIO
import xml.etree.ElementTree as ET

ee.Initialize(credentials='persistent', use_cloud_api=True)

tableName = "/home/joao/Documentos/trabalho/mapbiomas2.0/csv/alertas-ingerir-27042020.csv"



# The names to use for the ingested bands in Earth Engine.
BAND_NAMES = ['B', 'G', 'R', 'N']

# The filename extensions for the TIF and corresponding XML files in Cloud Storage.
TIF_EXT = '_cut.tif'
XML_EXT = '.xml'
# alerta-sccon/PLANET/images/1334739/20181026_140032_1021

# The XML namespaces from which we will be extracting metadata.
XML_NAMESPACES = {
    'eop': 'http://earth.esa.int/eop',
    'opt': 'http://earth.esa.int/opt',
    'ps': 'http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level',
}

# The specification of the metadata properties that we will extract from the XML
# file, specified as triples with with the XML specifier, the corresponding name
# to use in Earth Engine, and a parsing function to decode the string to its value.
METADATA_SPECS = [
    ['.//eop:acquisitionType', 'acquisition_type', str],
    ['.//eop:productType', 'product_type', str],
    ['.//eop:Platform/eop:shortName', 'platform_short_name', str],
    ['.//eop:Platform/eop:serialIdentifier',
        'platform_serial_identifier', str],
    ['.//eop:Platform/eop:orbitType', 'platform_orbit_type', str],
    ['.//eop:Instrument/eop:shortName', 'instrument_short_name', str],
    ['.//eop:sensorType', 'sensor_type', str],
    ['.//eop:resolution', 'resolution', float],
    ['.//ps:scanType', 'scan_type', str],
    ['.//eop:orbitDirection', 'orbit_direction', str],
    ['.//eop:incidenceAngle', 'incidence_angle', float],
    ['.//opt:illuminationAzimuthAngle', 'illumination_azimuth_angle', float],
    ['.//opt:illuminationElevationAngle', 'illumination_elevation_angle', float],
    ['.//ps:azimuthAngle', 'azimuth_angle', float],
    ['.//ps:spaceCraftViewAngle', 'space_craft_view_angle', float],
    ['.//opt:cloudCoverPercentage', 'cloud_cover_percentage', float],
    ['.//ps:unusableDataPercentage', 'unusable_data_percentage', float],
    ['.//ps:acquisitionDateTime', 'acquisition_time', str],
]

# The XML spec for the property containing the image acquisition / start time.
XML_METADATA_TIME_SPEC = ''


def TimestampToSeconds(iso_time_string):
    """Parses a date string and returns a timestamp in millisconds.

    Args:
    date_string: A date string in the format used in Planet metadata, i.e.
            the form YYYY-mm-ddTHH-MM-SS+00:00 in GMT.

    Returns:
    The corresponding timestamp in milliseconds since the Unix epoch.
    """
    timestamp = time.strptime(iso_time_string, '%Y-%m-%dT%H:%M:%S+00:00')
    return calendar.timegm(timestamp)


def xmlToJsonMetadata(xmlString):

    root = ET.fromstring(xmlString)

    metadata = {}

    for specifier, name, parser in METADATA_SPECS:
        element = root.find(specifier, XML_NAMESPACES)
        metadata[name] = parser(element.text)

    return metadata


def readParamsTable(tableName):

    table = []

    with open(tableName) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            table.append({
                'alert_id': row['alert_id'],
                'image_id': row['image_id'],
                'gcs_image': row['gcs_image'],
                # 'token': row['token'],
            })

    return table


# acess storage
storageClient = storage.Client()

bucket = storageClient.get_bucket(GCS_BUCKET)

# read table
table = readParamsTable(tableName)

nImages = len(table)

for i in range(nImages):

    # build the image GCS uri
    tifUri = URI_PREFIX.format(
        GCS_BUCKET, table[i]['alert_id'], table[i]['image_id'], table[i]['image_id'])

    # tifUri = table[i]['gcs_image']

    # get the full image name
    name = tifUri.replace('gs://{}/'.format(GCS_BUCKET), '')
    # print(name)
    # check the image in storage
    stats = storage.Blob(bucket=bucket, name=name).exists(storageClient)

    # print('[{}/{}] {} | {}'.format(i, nImages, stats, tifUri))

    if stats:
        try:
            # get bucket data as blob
            blob = bucket.get_blob(name.replace(TIF_EXT, XML_EXT))

            # download as string
            xmlString = blob.download_as_string()

            # get metadata from xml metafile
            metadata = xmlToJsonMetadata(xmlString)

            # add alert_id as tile propertie
            metadata['tile'] = table[i]['alert_id']

            # build manifest
            manifest = {
                'name': os.path.join(EE_RESOURCE_NAME_PREFIX, EE_COLLECTION, table[i]['alert_id'] + '_' + table[i]['image_id']),
                "tilesets": [{
                    "sources": [{
                        "uris": [tifUri],
                    }],
                }],
                "bands": [{"id": bid, "tileset_band_index": index} for (index, bid) in enumerate(BAND_NAMES)],
                "properties": metadata,
                "start_time": {
                    "seconds": TimestampToSeconds(metadata['acquisition_time']),
                },
            }

            manifestName = name.replace(TIF_EXT, '_manifest.json')

            # check the image in storage
            manifestStats = storage.Blob(bucket=bucket, name=manifestName).exists(storageClient)
            
            if not manifestStats:
                # print('manifest exists: ', manifestStats)
                # print('uploading: ', manifestName)
                manifestBlob = storage.Blob(bucket=bucket, name=manifestName)
                manifestBlob.upload_from_string(json.dumps(manifest))

            # check an existent image in gee asset
            try:
                image = ee.Image(manifest['name']).getInfo()
                # print('image already exists.')
            except:
                # print('ingesting...')

                # start ingesting task
                task = ee.data.startIngestion(
                    ee.data.newTaskId()[0], manifest)
        except Exception as e:
            print(e)
    else:
        print('[{}/{}] {} | {}'.format(i, nImages, stats, tifUri))
