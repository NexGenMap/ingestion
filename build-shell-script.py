

fileName = "/home/joao/planet_bucket.txt"
outfileName = "/home/joao/move_manifest_files.sh"

destination = "gs://sistema_alertas_sccon/manifests/pack-7/"
comand = "gsutil -m mv -r"

file = open(fileName, "r")

outfile = open(outfileName, "w")

for line in file.readlines():

    newLine = "{} {}* {}".format(comand, line.rstrip("\n\r"), destination)
    print(newLine)

    outfile.write(newLine + "\n")

outfile.close()