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


# The location of the input data in Google Cloud Storage.
GCS_BUCKET = 'sistema_alertas_sccon'
GCS_PREFIX = 'tiles/'
GCS_IGNORE = ['init']

# The Earth Engine image collection to write to.
EE_COLLECTION = 'projects/nexgenmap/MapBiomas2/PLANET/tiles'

# The prefix for full-qualified Earth Engine resource names for legacy projects.
# (Right now all projects are legacy projects!)
EE_RESOURCE_NAME_PREFIX = 'projects/earthengine-legacy/assets'

# The names to use for the ingested bands in Earth Engine.
BAND_NAMES = ['B', 'G', 'R', 'N']

# The filename extensions for the TIF and corresponding XML files in Cloud Storage.
TIF_EXT = '_cut.tif'
XML_EXT = '.xml'

# sccon json path
JSON_PATH = "/home/joao/Documents/trabalho/mapbiomas2.0/json/input"
JSON_OUTPUT_PATH = "/home/joao/Documents/trabalho/mapbiomas2.0/json/output"

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
    ['.//eop:acquisitionType', 'acquisition_type', unicode],
    ['.//eop:productType', 'product_type', unicode],
    ['.//eop:Platform/eop:shortName', 'platform_short_name', unicode],
    ['.//eop:Platform/eop:serialIdentifier',
        'platform_serial_identifier', unicode],
    ['.//eop:Platform/eop:orbitType', 'platform_orbit_type', unicode],
    ['.//eop:Instrument/eop:shortName', 'instrument_short_name', unicode],
    ['.//eop:sensorType', 'sensor_type', unicode],
    ['.//eop:resolution', 'resolution', float],
    ['.//ps:scanType', 'scan_type', unicode],
    ['.//eop:orbitDirection', 'orbit_direction', unicode],
    ['.//eop:incidenceAngle', 'incidence_angle', float],
    ['.//opt:illuminationAzimuthAngle', 'illumination_azimuth_angle', float],
    ['.//opt:illuminationElevationAngle', 'illumination_elevation_angle', float],
    ['.//ps:azimuthAngle', 'azimuth_angle', float],
    ['.//ps:spaceCraftViewAngle', 'space_craft_view_angle', float],
    ['.//opt:cloudCoverPercentage', 'cloud_cover_percentage', float],
    ['.//ps:unusableDataPercentage', 'unusable_data_percentage', float],
]

# The XML spec for the property containing the image acquisition / start time.
XML_METADATA_TIME_SPEC = './/ps:acquisitionDateTime'


def getJsonNames(path):
    names = []
    for root, dirs, files in os.walk(path):
        for file in files:
            names.append(os.path.join(root, file))
    return names


def getUniquePaths(json):

    uris = []

    for tile in json['tiles']:

        if tile['status'] == "FINISHED":

            for scene in tile['scenes']:

                if scene != []:

                    if scene['path'].find('visual') == -1:

                        uris.append(scene['path'].replace(
                            "https://storage.googleapis.com/{}/".format(GCS_BUCKET), ""))

    return uris


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


def BuildIngestManifest(path, bucket, tile, asset_id):
    """Builds an ingestion manifest for an asset given XML and TIF blobs."""

    blob = storage.Blob(path.replace(TIF_EXT, XML_EXT), bucket=bucket)
    xml = blob.download_as_string()
    root = ET.parse(StringIO(xml)).getroot()

    metadata = {
        'tile': tile
    }

    for specifier, name, parser in METADATA_SPECS:
        element = root.find(specifier, XML_NAMESPACES)
        metadata[name] = parser(element.text)

    tif_uri = "gs://{}/{}".format(GCS_BUCKET, path)
    time_string = root.find(XML_METADATA_TIME_SPEC, XML_NAMESPACES).text

    return {
        'name': os.path.join(EE_RESOURCE_NAME_PREFIX, EE_COLLECTION, tile + '_' + asset_id),
        "tilesets": [{
            "sources": [{
                "uris": [tif_uri],
            }],
        }],
        "bands": [{"id": bid, "tileset_band_index": index} for (index, bid) in enumerate(BAND_NAMES)],
        "properties": metadata,
        "start_time": {
            "seconds": TimestampToSeconds(time_string),
        },
    }


if __name__ == '__main__':

    ee.Initialize(credentials='persistent', use_cloud_api=True)

    bucket = storage.Client().get_bucket(GCS_BUCKET)

    # jsonNames = getJsonNames(JSON_PATH)
    jsonNames = [
        "01-amazonia.json",
        "01-caatinga.json",
        "01-cerrado.json",
        "01-mataatlantica.json",
        "01-pampa.json",
        "01-pantanal.json"
    ]

    for jsonName in jsonNames:
        with open(os.path.join(JSON_PATH, jsonName)) as json_file:
            data = json.load(json_file)

        paths = getUniquePaths(data)

        for path in paths:

            _, tile, asset_id, _ = path.split('/')
            outname = "{}/{}.json".format(JSON_OUTPUT_PATH, asset_id)

            if not os.path.isfile(outname):
                print(path)

                manifest = BuildIngestManifest(path, bucket, tile, asset_id)

                with open(outname, 'w') as outfile:
                    json.dump(manifest, outfile)
