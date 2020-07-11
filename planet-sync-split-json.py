

import json

jsonfile = "/home/joao/Documents/trabalho/mapbiomas2.0/ingestao/04-05-06-2019/input/mata-atlantica_pantanal.json"

with open(jsonfile) as json_file:
    jsondata = json.load(json_file)

nparts = 5
ntiles = len(jsondata['tiles'])

ntilesparts = round(ntiles/float(nparts))

print(ntilesparts)

for i in range(0, nparts):
    i0 = int(i * ntilesparts)
    i1 = min(int((i+1) * ntilesparts - 1), ntiles)
    tilespart = jsondata['tiles'][i0: i1]

    jsonpart = {
        'check_download_token': jsondata['check_download_token'],
        'start_date': jsondata['start_date'],
        'end_date': jsondata['end_date'],
        'tiles': tilespart,
    }

    with open(jsonfile.replace('.json', str(i) + '.json'), 'w') as outfile:  
        json.dump(jsonpart, outfile)
