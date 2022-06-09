import googlemaps
import csv

def pointGeneration(inputfile,outputfile,client_key= 'AIzaSyCYqiVOAFFPLYF97qDK5wpO7a-VCayxmyo'):
    iFilepath = inputfile
    oFilepath = outputfile
    line_count= 0
    station_id = 1

    # initialization of google maps function
    gmaps = googlemaps.Client(key=client_key)
    writeFile = open(oFilepath,'w+')
    csv_file = open(iFilepath, encoding="utf-8", errors="", newline="")
    inputFile = csv.reader(csv_file, delimiter=",")
    for line in inputFile:
        address = line[3]
        location  = gmaps.geocode(address)
        latitude = location[0]['geometry']['location'] ['lat']
        longitude=location[0]['geometry']['location']['lng']
        point ="Point("+str(longitude)+" "+str(latitude)+")"
        result =str(station_id)+ str(point)
        writeFile.write(result)
        station_id+=1
    writeFile.close()

if __name__ == '__main':
    pointGeneration("stationInfo.csv", 'stationAdd.txt')

