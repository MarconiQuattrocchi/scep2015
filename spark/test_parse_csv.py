import csv
import datetime
import re
import os
import fileinput
import collections
#from pyspark.sql import Row


def parseTaxiLogTime(s):
    """ Convert log time string format into a Python datetime object
    Args:
        s (str): date and time in log time format
    Returns:
        datetime: datetime object (ignore timezone)
    """
    return datetime.datetime(int(s[0:4]),
                             int(s[5:7]),
                             int(s[8:10]),
                             int(s[11:13]),
                             int(s[14:16]),
                             int(s[17:19]))


TAXI_LOG_PATTERN = '^(\S+),(\S+),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d+),(\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\S+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+)'


TOP_LEFT_CELL_CENTER = {'latitude' : 41.474937, 'longitude' : -74.913585}
LATITUDE_STEP = 0.004491556
LONGITUDE_STEP = 0.005986

def getLatitudeCell2(latitude):
    floatValue = (TOP_LEFT_CELL_CENTER['latitude'] + LATITUDE_STEP/2 - latitude)/LATITUDE_STEP
    if(floatValue>=0):
        return int(floatValue) + 1
    else:
        return -1

def getLongitudeCell2(longitude):
    floatValue = (longitude - (TOP_LEFT_CELL_CENTER['longitude'] - LONGITUDE_STEP/2))/LONGITUDE_STEP
    if(floatValue>=0):
        return int(floatValue) + 1
    else:
        return -1

def getLatitudeCell(latitude, resolution):
    step = LATITUDE_STEP/resolution
    floatValue = (TOP_LEFT_CELL_CENTER['latitude'] + LATITUDE_STEP/2 - latitude)/step
    if(floatValue>=0):
        return int(floatValue) + 1
    else:
        return -1

def getLongitudeCell(longitude, resolution):
    step = LONGITUDE_STEP/resolution
    floatValue = (longitude - (TOP_LEFT_CELL_CENTER['longitude'] - LONGITUDE_STEP/2))/step
    if(floatValue>=0):
        return int(floatValue) + 1
    else:
        return -1




#DICTIONARY VERSION (JUST FOR TESTING)
def parseTaxiLogLine(logline):

    match = re.search(TAXI_LOG_PATTERN, logline)

    if match is None:
        return { 'key':'fail'}
    else:
        return {
    'medallion'            : match.group(1),
    'hack_license'         : match.group(2),
    'pickup_datetime'      : parseTaxiLogLine(match.group(3)),
    'dropoff_datetime'     : parseTaxiLogLine(match.group(4)),
    'trip_time_in_secs'    : int(match.group(5)),
    'trip_distance'        : float(match.group(6)),
    'pickup_longitude'     : getLongitudeCell(float(match.group(7)),1),
    'pickup_latitude'      : getLatitudeCell(float(match.group(8)),1),
    'dropoff_longitude'    : getLongitudeCell(float(match.group(9)),1),
    'dropoff_latitude'     : getLatitudeCell(float(match.group(10)),1),
    'payment_type'         : match.group(11),
    'fare_amount'          : float(match.group(12)),
    'surcharge'            : float(match.group(13)),
    'mta_tax'              : float(match.group(14)),
    'tip_amount'           : float(match.group(15)),
    'tolls_amount'         : float(match.group(16)),
    'total_amount'         : float(match.group(17)),
    'key'                  : "%s_%s__%s_%s"%(getLatitudeCell(float(match.group(8)),1),getLongitudeCell(float(match.group(7)),1),getLatitudeCell(float(match.group(10)),1),getLongitudeCell(float(match.group(9)),1)),
    'pickup_cell'          : "%s.%s"%(getLatitudeCell(float(match.group(8)),1),getLongitudeCell(float(match.group(7)),1)),
    'dropoff_cell'         : "%s.%s"%(getLatitudeCell(float(match.group(10)),1),getLongitudeCell(float(match.group(9)),1))
         }

current_dir =  os.path.abspath(os.path.dirname(__file__))

file_path = os.path.abspath(current_dir + "/../dataset/little-2000.csv")


file = fileinput.input(file_path)

print "Reading file at path:", file_path,"\n"


print "Reading file at path:", file_path,"\n"


line = file.readline()

print parseTaxiLogLine(line)
counts = {}
while line:
    line = file.readline()
    x = parseTaxiLogLine(line)
    #print x
    counts[x['key']] = counts.get(x['key'], 0) + 1


d_view = [ (v,k) for k,v in counts.iteritems() ]
d_view.sort(reverse=True) # natively sort tuples by first element
for v,k in d_view:
    print "%s: %d" % (k,v)

#ranking = collections.OrderedDict(sorted(counts.items()))
#for k, v in ranking.iteritems(): print k, v






#
#
# #reader = csv.reader(scsv.split('\n'), delimiter=',')
# reader = scsv.split(',')
# pine =  TAXI_LOG_PATTERN.split(',')
# print pine
# a = zip(reader,pine)
#
# for (i,j) in a:
#     m = re.search(j,i)
#     print m.group()
# match = re.search(TAXI_LOG_PATTERN,scsv)
# print match.group()
# print match.group(17)
# parsedLine= parseTaxiLogLine(scsv)
# print parsedLine
# print getLatitudeCell(parsedLine['pickup_latitude'])
# print getLongitudeCell(parsedLine['pickup_longitude'])
# print getLatitudeCell(41.47437)

#print getLongitudeCell(TOP_LEFT_CELL_CENTER['longitude']-LONGITUDE_STEP/2.0-0.00000001)
#print getLongitudeCell2(TOP_LEFT_CELL_CENTER['longitude']-LONGITUDE_STEP/2-0.000000001,1)




#print getLongitudeCell2(TOP_LEFT_CELL_CENTER['longitude']+ 0.01, 2)
#print getLatitudeCell2(TOP_LEFT_CELL_CENTER['latitude'],2)
