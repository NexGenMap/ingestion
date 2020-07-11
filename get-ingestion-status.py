#!/usr/bin/env python
import ee
import os
import json
import sys
import time
import datetime

# The Earth Engine image collection to write to.
EE_COLLECTION = 'projects/nexgenmap/MapBiomas2/PLANET/tiles'
JSONFILE = 'grids-2.json'


def getTiles(jsonFile):
    """Get tiles by account"""

    with open(jsonFile) as f:
        jsonData = json.load(f)

    tiles = map(
        lambda feature:
            str(feature['id']), jsonData)

    return tiles


if __name__ == '__main__':

    ee.Initialize()

    jsonFileName = os.path.join(os.getcwd(), JSONFILE)

    tiles = getTiles(jsonFileName)

    while True:

        try:
            collection = ee.ImageCollection(
                EE_COLLECTION).filter(ee.Filter.inList('tile', tiles))

            print('[{}] imagens ingeridas: {}'.format(
                datetime.datetime.now(), collection.size().getInfo()))

            time.sleep(60)
        except:
            pass
