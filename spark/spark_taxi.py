#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in text encoded with UTF8 received from the network every second.

 Usage: recoverable_network_wordcount.py <hostname> <port> <checkpoint-directory> <output-file>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
   data. <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
   <output-file> file to which the word counts will be appended

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`

 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/recoverable_network_wordcount.py \
        localhost 9999 ~/checkpoint/ ~/out`

 If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
 a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
 checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from
 the checkpoint data.
"""
from __future__ import print_function

import os
import sys
#import ast
import itertools

import datetime
import re

from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


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

def max_by_dropoff_date((a_start,a_end), (b_start,b_end)):
    if a_end > b_end:
#        return ("%s"%a_start,"%s"%a_end)
        return (a_start,a_end)
    else:
#        return ("%s"%b_start,"%s"%b_end)
        return (b_start,b_end)


TAXI_LOG_PATTERN = '^(\S+),(\S+),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d+),(\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\S+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+)'


TOP_LEFT_CELL_CENTER = {'latitude' : 41.474937, 'longitude' : -74.913585}
LATITUDE_STEP = 0.004491556
LONGITUDE_STEP = 0.005986

def getLatitudeCell(latitude):
    floatValue = (TOP_LEFT_CELL_CENTER['latitude'] + LATITUDE_STEP/2 - latitude)/LATITUDE_STEP
    if(floatValue>=0):
        return int(floatValue) + 1
    else:
        return -1

def getLongitudeCell(longitude):
    floatValue = (longitude - TOP_LEFT_CELL_CENTER['longitude'] - LONGITUDE_STEP/2 )/LONGITUDE_STEP
    if(floatValue>=0):
        return int(floatValue) + 1
    else:
        return -1



def parseTaxiLogLine(logline):

    match = re.search(TAXI_LOG_PATTERN, logline)

    if match is None:
        return (logline,0)
    else:
        return (Row(
    medallion               = match.group(1),
    hack_license            = match.group(2),
    pickup_datetime         = parseTaxiLogTime(match.group(3)),
    dropoff_datetime        = parseTaxiLogTime(match.group(4)),
    trip_time_in_secs       = int(match.group(5)),
    trip_distance           = float(match.group(6)),
    pickup_longitude_cell   = getLongitudeCell(float(match.group(7))),
    pickup_latitude_cell    = getLatitudeCell(float(match.group(8))),
    dropoff_longitude_cell  = getLongitudeCell(float(match.group(9))),
    dropoff_latitude_cell   = getLatitudeCell(float(match.group(10))),
    payment_type            = match.group(11),
    fare_amount             = float(match.group(12)),
    surcharge               = float(match.group(13)),
    mta_tax                 = float(match.group(14)),
    tip_amount              = float(match.group(15)),
    tolls_amount            = float(match.group(16)),
    total_amount            = float(match.group(17))
         ), 1)



def parseLogs(loglines):
    """ Read and parse log file """
    parsed_logs = (loglines
                   .map(lambda x : parseTaxiLogLine(x))
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
#    failed_logs_count = failed_logs.count()
#    if failed_logs_count > 0:
#        print('Number of invalid logline: %d' % failed_logs.count())
#        for line in failed_logs.take(20):
#            print('Invalid logline: %s' % line)

#    print('Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs




def createContext(host, port, outputPath1, outputPath2):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    if os.path.exists(outputPath1):
        os.remove(outputPath1)
    sc = SparkContext(appName="PythonStreamingWindowedRecoverableNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    lines = ssc.socketTextStream(host, port)


    parsed_trips, well_formed_trips, failed_trips = parseLogs(lines)

    relevantTrips = well_formed_trips.filter(lambda trip: 0 < trip.pickup_latitude_cell < 300 and 0 < trip.pickup_longitude_cell < 300 and 0< trip.dropoff_latitude_cell < 300 and 0 < trip.dropoff_longitude_cell < 300 ).cache()

    queryOneFormat = relevantTrips.map(lambda trip: Row(
                                 pickup_datetime    = trip.pickup_datetime,
                                 dropoff_datetime   = trip.dropoff_datetime,
                                 start_cell_id      = "%s.%s"%(trip.pickup_latitude_cell, trip.pickup_longitude_cell),
                                 end_cell_id        = "%s.%s"%(trip.dropoff_latitude_cell, trip.dropoff_longitude_cell)))

    tripsByCellsAndWindows = queryOneFormat.map(lambda trip: ((trip.start_cell_id, trip.end_cell_id), (trip.pickup_datetime, trip.dropoff_datetime))).window(5,1).cache()

#    tripsByCellsAndWindows.checkpoint(120)

    tripCountsByCells = tripsByCellsAndWindows.transform(lambda rdd: rdd.map(lambda (k,v): (k, 1)).reduceByKey(lambda x, y: x + y))

    #TAKING INTO ACCOUNT THE LAST DROPOFF TIME
    tripMaxDropoffBycell = tripsByCellsAndWindows.transform(lambda rdd: rdd.reduceByKey(lambda a,b: max_by_dropoff_date(a,b)).map(lambda (k,v): (k,("%s"%v[0],"%s"%v[1]))))

    tripCountsByCells.pprint()
    tripMaxDropoffBycell.pprint()


#    tripMaxDropoffBycell.checkpoint(120)



#    tripCountsByCells.checkpoint(120)
    #
    # sortedTripCounts = tripCountsByCells.map(lambda (trip, count):(count,trip)).transform(lambda rdd: rdd.sortByKey(False).map(lambda (trip, count):(count,trip)))
    #
    # sortedTripCounts.pprint()
    #
    # completeSet = sortedTripCounts.join(tripMaxDropoffBycell)
    #

    completeSet = tripCountsByCells.join(tripMaxDropoffBycell).map(lambda (k, v): (v[0],(k, v))).transform(lambda rdd: rdd.sortBy(lambda (count, v): (count, v[0]) ,ascending=False).map(lambda (count, tuple):tuple))

    completeSet.pprint()
    #EXPECTED SOMETHING LIKE: (('169.152', '168.152'), ('2015-07-17 12:00:55', '2015-07-17 12:02:55'))

#    list of the top three words per window with their position as key (ex. [(1,'word1'),(2,'word2')])
    topTenTripsWithIndex = completeSet.transform(lambda rdd: rdd.zipWithIndex().filter(lambda (k,v): v<10).map(lambda (k,v):k))

    topTenTripsWithIndex.pprint()

    def updateFunc(new_values, last_list):
        if last_list is None:
            last_ranking = []
        else:
            last_ranking = last_list[2:] #ignore pickup_datetime and dropoff_datetime

#        print("NEW-VALUES: %s" % new_values)

        new_ranking = [k for (k,v) in new_values[0]]

        counts = "OLD: %s --> NEW: %s" % (last_ranking, new_ranking)

        new = "%s" % new_values
        print("updateFunc: "+counts)
        #
        paddedOldRanking =  [i for i,j in list(itertools.izip_longest(last_ranking, range(10)))]
        diff = [j for i, j in zip(paddedOldRanking, new_ranking) if i != j]
        if len(diff)>0:
            print("new elements: %s (new ranking: %s)" % (diff, new_ranking))
            new_datez = [v[1] for (k,v) in new_values[0] if k in diff]
            print("NEW_DATEZ: %s"%new_datez)
            new_dates = max(new_datez)
            new_result = [new_dates, new_ranking]
            #write the new ranking on file outputPath2 only when there is a change in any of the the top N positions
            new_string="%s"%new_result
            #TODO format output
            with open(outputPath2, 'a') as f:
                f.write(new_string + "\n")
        else:
            new_result=last_list
        # write the comparison between old and new status on file outputPath1 every time updateFunc is invoked
        with open(outputPath1, 'a') as f:
            f.write(counts + "\n")
        return new_result




    rankingTripCount = topTenTripsWithIndex.transform(lambda rdd:rdd.map(lambda x:('ranking',x)).groupByKey().map(lambda x : (x[0], list(x[1]))))

#u    rankingTripCount = sortedTripCounts.transform(lambda rdd:rdd.map(lambda (k,v):('ranking',v)).groupByKey().map(lambda x : (x[0], list(x[1]))))
    rankingTripCount.pprint()
    #invoke updateFunc for each time window
    status = rankingTripCount.updateStateByKey(updateFunc)
    status.pprint()


    return ssc


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: recoverable_network_wordcount.py <hostname> <port> "
              "<checkpoint-directory> <output-file1> <output-file2>", file=sys.stderr)
        exit(-1)
    ranking = [1,2,3]
    host, port, checkpoint, output1, output2 = sys.argv[1:]
    ssc = StreamingContext.getOrCreate(checkpoint,
                                       lambda: createContext(host, int(port), output1, output2))
    ssc.start()
    ssc.awaitTermination()
