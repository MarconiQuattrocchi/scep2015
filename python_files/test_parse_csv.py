import csv
import datetime
import re

#from pyspark.sql import Row


#datetime(year, month, day[, hour[, minute[, second[, microsecond[, tzinfo]]]]])
def parseTimeString(s):
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

scsv="4E3FCCEDD9149C3AE6073326DECFD5F0,A379ADB375426ED563CFA63F1622CE4F,2013-01-20 23:47:40,2013-01-21 00:01:26,826,4.40,-73.986412,40.726639,-73.951103,40.729916,CRD,16.00,0.50,0.50,4.25,0.00,21.25"

TOP_LEFT_CELL_CENTER = {'latitude' : 41.474937, 'longitude' : -74.913585}
LATITUDE_STEP = 0.004491556
LONGITUDE_STEP = 0.005986

#DICTIONARY VERSION (JUST FOR TESTING)
def parseTaxiLogLine(logline):

    match = re.search(TAXI_LOG_PATTERN, logline)

    if match is None:
        return "fail"
    else:
        return {
    'medallion'            : match.group(1),
    'hack_license'         : match.group(2),
    'pickup_datetime'      : parseTimeString(match.group(3)),
    'dropoff_datetime'     : parseTimeString(match.group(4)),
    'trip_time_in_secs'    : int(match.group(5)),
    'trip_distance'        : float(match.group(6)),
    'pickup_longitude'     : float(match.group(7)),
    'pickup_latitude'      : float(match.group(8)),
    'dropoff_longitude'    : float(match.group(9)),
    'dropoff_latitude'     : float(match.group(10)),
    'payment_type'         : match.group(11),
    'fare_amount'          : float(match.group(12)),
    'surcharge'            : float(match.group(13)),
    'mta_tax'              : float(match.group(14)),
    'tip_amount'           : float(match.group(15)),
    'tolls_amount'         : float(match.group(16)),
    'total_amount'         : float(match.group(17))
         }

#spark version
def parseTaxiLogLineSpark(logline):

    match = re.search(TAXI_LOG_PATTERN, logline)

    if match is None:
        return (logline,0)
    else:
        return (Row(
    medallion           = match.group(1),
    hack_license        = match.group(2),
    pickup_datetime     = parseTaxiLogTime(match.group(3)),
    dropoff_datetime    = parseTaxiLogTime(match.group(4)),
    trip_time_in_secs   = int(match.group(5)),
    trip_distance       = float(match.group(6)),
    pickup_longitude    = float(match.group(7)),
    pickup_latitude     = float(match.group(8)),
    dropoff_longitude   = float(match.group(9)),
    dropoff_latitude    = float(match.group(10)),
    payment_type        = match.group(11),
    fare_amount         = float(match.group(12)),
    surcharge           = float(match.group(13)),
    mta_tax             = float(match.group(14)),
    tip_amount          = float(match.group(15)),
    tolls_amount        = float(match.group(16)),
    total_amount        = float(match.group(17))
         ),1)

def getLatitudeCell(latitude):
#    print TOP_LEFT_CELL_CENTER['latitude'],LATITUDE_STEP/2, latitude
    return int((TOP_LEFT_CELL_CENTER['latitude'] + LATITUDE_STEP/2 - latitude)/LATITUDE_STEP) + 1

def getLongitudeCell(longitude):
#    print TOP_LEFT_CELL_CENTER['longitude'],LONGITUDE_STEP/2,  longitude
    return int((longitude - TOP_LEFT_CELL_CENTER['longitude'] - LONGITUDE_STEP/2 )/LONGITUDE_STEP) + 1


def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseTaxiLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs






#reader = csv.reader(scsv.split('\n'), delimiter=',')
reader = scsv.split(',')
pine =  TAXI_LOG_PATTERN.split(',')
print pine
a = zip(reader,pine)

for (i,j) in a:
    m = re.search(j,i)
    print m.group()
match = re.search(TAXI_LOG_PATTERN,scsv)
print match.group()
print match.group(17)
parsedLine= parseTaxiLogLine(scsv)
print parsedLine
print getLatitudeCell(parsedLine['pickup_latitude'])
print getLongitudeCell(parsedLine['pickup_longitude'])
print getLatitudeCell(41.47437)
print getLongitudeCell(TOP_LEFT_CELL_CENTER['longitude'])
