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

import time
import datetime
import re

from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def median(lst):
    lst = sorted(lst)
    if len(lst) < 1:
            return None
    if len(lst) %2 == 1:
            return lst[((len(lst)+1)/2)-1]
    else:
            return float(sum(lst[(len(lst)/2)-1:(len(lst)/2)+1]))/2.0

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


def max_by_dropoff_date(a,b):
    a_start = a[0]
    a_end = a[1]
    b_start = b[0]
    b_end = b[1]
    if len(a)==3 and len(b)==3:
        if a_end > b_end:
            return (a_start,a_end, a[2])
        else:
            return (b_start,b_end, b[2])
    else:
        if a_end > b_end:
            return (a_start,a_end)
        else:
            return (b_start,b_end)

def max_by_dropoff_and_sum(a,b):
    a_start = a[0]
    a_end = a[1]
    b_start = b[0]
    b_end = b[1]
    sum = a[2] + b[2]

    if a_end > b_end:
        return (a_start, a_end, sum)
    else:
        return (b_start, b_end, sum)



WINDOW_LENGTH_1 = 15
SLIDING_INTERVAL_1 = 1
WINDOW_LENGTH_2 = 30
SLIDING_INTERVAL_2 = 1


TAXI_LOG_PATTERN = '^(\S+),(\S+),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d+),(\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\S+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+)'


TOP_LEFT_CELL_CENTER = {'latitude' : 41.474937, 'longitude' : -74.913585}
LATITUDE_STEP = 0.004491556
LONGITUDE_STEP = 0.005986

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
    pickup_longitude_cell   = getLongitudeCell(float(match.group(7)), 1),
    pickup_latitude_cell    = getLatitudeCell(float(match.group(8)), 1),
    dropoff_longitude_cell  = getLongitudeCell(float(match.group(9)), 1),
    dropoff_latitude_cell   = getLatitudeCell(float(match.group(10)), 1),
# FOR QUERY2
    pickup_longitude_cell_2  = getLongitudeCell(float(match.group(7)), 2),
    pickup_latitude_cell_2   = getLatitudeCell(float(match.group(8)), 2),
    dropoff_longitude_cell_2 = getLongitudeCell(float(match.group(9)), 2),
    dropoff_latitude_cell_2  = getLatitudeCell(float(match.group(10)), 2),

    payment_type            = match.group(11),
    fare_amount             = float(match.group(12)),
    surcharge               = float(match.group(13)),
    mta_tax                 = float(match.group(14)),
    tip_amount              = float(match.group(15)),
    tolls_amount            = float(match.group(16)),
    total_amount            = float(match.group(17)),
    read_time               = time.time()
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
    return parsed_logs, access_logs, failed_logs

def formatResult(resultList):
    #expected list like: [('2013-01-01 00:20:00', '2013-01-01 00:24:00'), [('158.156', '153.157'), ('169.155', '169.155'), ('164.155', '164.155'), ('164.155', '163.154'), ('164.152', '164.154'), ('162.155', '162.155'), ('160.158', '162.157'), ('169.154', '164.156'), ('169.152', '168.152'), ('168.154', '167.152')]]
    timestamps=resultList[0]
    flat_cells = [x for sublists in resultList[1] for x in sublists]
    res = "%s,%s"%(timestamps[0],timestamps[1])
    for i in flat_cells:
        res="%s,%s"%(res,i)
    if len(resultList)>2:
        delay=resultList[2]
        res="%s,%s"%(res,delay)
    return res



def createContext(host, port, outputPath1, outputPath2, windowLength=WINDOW_LENGTH_1, slidingInterval=SLIDING_INTERVAL_1):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    if os.path.exists(outputPath1):
        os.remove(outputPath1)

    print("PIPPO: %s,%s"%(windowLength,slidingInterval))

    with open(outputPath1, 'w+') as f:
        f.write("QUERY 1 OUTPUT: " + "\n\n")
    with open(outputPath2, 'w+') as f:
        f.write("QUERY 2 OUTPUT: " + "\n\n")

    sc = SparkContext(appName="SparkStreamingTaxiQueries")
    ssc = StreamingContext(sc, 1)

    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    lines = ssc.socketTextStream(host, port)


    parsed_trips, well_formed_trips, failed_trips = parseLogs(lines)

    relevantTrips = well_formed_trips.filter(lambda trip: 0 < trip.pickup_latitude_cell < 300 and 0 < trip.pickup_longitude_cell < 300 and 0< trip.dropoff_latitude_cell < 300 and 0 < trip.dropoff_longitude_cell < 300 ).cache()

    queryFormat = relevantTrips.map(lambda trip: Row(
                                 pickup_datetime    = trip.pickup_datetime,
                                 dropoff_datetime   = trip.dropoff_datetime,
                                 start_cell_id      = "%s.%s"%(trip.pickup_latitude_cell, trip.pickup_longitude_cell),
                                 end_cell_id        = "%s.%s"%(trip.dropoff_latitude_cell, trip.dropoff_longitude_cell),
                                 #QUERY2
                                 start_cell_id_2    = "%s.%s"%(trip.pickup_latitude_cell_2, trip.pickup_longitude_cell_2),
                                 end_cell_id_2      = "%s.%s"%(trip.dropoff_latitude_cell_2, trip.dropoff_longitude_cell_2),
                                 fare_amount        = trip.fare_amount,
                                 tip_amount         = trip.tip_amount,
                                 medallion          = trip.medallion,
                                 read_time          = trip.read_time
                                 ))



#    firstWindowedData = queryFormat.window(WINDOW_LENGTH_1, SLIDING_INTERVAL_1).cache()
#    secondWindowedData = queryFormat.window(WINDOW_LENGTH_2, SLIDING_INTERVAL_2).cache()

    firstWindowedData = queryFormat.window(windowLength, slidingInterval).cache()
    secondWindowedData = queryFormat.window(2*windowLength, slidingInterval).cache()


    firstWindowedTimes = (firstWindowedData.map(lambda trip: ((trip.start_cell_id, trip.end_cell_id),time.time()))
                                          .reduceByKey(lambda x,y: max(x,y)))

#    tripsByCellsAndWindows.checkpoint(120)

    ###QUERY1
    tripsByCellsAndWindowsForCount = firstWindowedData.map(lambda trip: ((trip.start_cell_id, trip.end_cell_id), (trip.pickup_datetime, trip.dropoff_datetime))).cache()

    tripCountsByCells = tripsByCellsAndWindowsForCount.transform(lambda rdd: rdd.map(lambda (k,v): (k, 1)).reduceByKey(lambda x, y: x + y))


    #TAKING INTO ACCOUNT THE LAST DROPOFF TIME
    tripMaxDropoffBycell = tripsByCellsAndWindowsForCount.transform(lambda rdd: rdd.reduceByKey(lambda a,b: max_by_dropoff_date(a,b)).map(lambda (k,v): (k,("%s"%v[0],"%s"%v[1]))))


#    tripCountsByCells.pprint()
#    tripMaxDropoffBycell.pprint()

    completeSet = tripCountsByCells.join(tripMaxDropoffBycell).map(lambda (k, v): (v[0],(k, v))).transform(lambda rdd: rdd.sortBy(lambda (count, v): (count, v[0]) ,ascending=False).map(lambda (count, tuple):tuple))


#    completeSet.map(lambda x: ('COMPLETE_SET',x)).pprint()
    #EXPECTED SOMETHING LIKE: (('169.152', '168.152'), ('2015-07-17 12:00:55', '2015-07-17 12:02:55'))

#   list of the top ten Routes per window
    topTenTrips = completeSet.transform(lambda rdd: rdd.zipWithIndex().filter(lambda (k,v): v<10).map(lambda (k,v):k))

#    topTenTrips.map(lambda x: ('WHAEE',x)).pprint()



    ####QUERY2
    tripsByCellsAndWindowsForProfit = (firstWindowedData.map(lambda trip: (
                                                        trip.start_cell_id_2, (trip.pickup_datetime, trip.dropoff_datetime, trip.fare_amount + trip.tip_amount)))
                                                        .cache())

    tripsByMedallionsAndWindowsForEmptyTaxies = secondWindowedData.map(lambda trip: (
                                                        trip.medallion, (trip.pickup_datetime, trip.dropoff_datetime, trip.end_cell_id_2))).cache()


    medianProfitByCell = tripsByCellsAndWindowsForProfit.transform(lambda rdd: rdd.map(lambda (k,v): (k,v[2])).groupByKey().map(lambda (k,v): (k,median(v))))
    lastProfitEventByCell = tripsByCellsAndWindowsForProfit.transform(lambda rdd: rdd.map(lambda (k,v): (k,(v[0],v[1]))).reduceByKey(lambda a,b: max_by_dropoff_date(a,b)).map(lambda (k,v): (k,("%s"%v[0],"%s"%v[1]))))

    profitCompleteByCell = lastProfitEventByCell.join(medianProfitByCell)#.transform(lambda rdd: rdd.sortBy(lambda (k,v):v[1], ascending=False))

#    profitCompleteByCell.map(lambda x: ('PROFIT_BY_CELL',x)).pprint()

    lastTripsByMedallion = tripsByMedallionsAndWindowsForEmptyTaxies.transform(lambda rdd: rdd.reduceByKey(lambda a,b: max_by_dropoff_date(a,b))) #eliminato sortByKey
#   lastTripsByMedallion.pprint()
    countEmptyTaxiesByCells = lastTripsByMedallion.transform(lambda rdd: rdd.map(lambda (k,v):(v[2],(v[0],v[1],1)))
                                                                            .reduceByKey(lambda a,b: max_by_dropoff_and_sum(a,b))
                                                                            .map(lambda (k,v):(k,("%s"%v[0],"%s"%v[1],v[2])))
                                                                            ) #.sortBy(lambda (k,v):v[2], ascending=False)

#    countEmptyTaxiesByCells.map(lambda x: ('EMPTY',x)).pprint()

    profitabilityByCell = profitCompleteByCell.join(countEmptyTaxiesByCells).transform(lambda rdd: rdd.map(lambda (cell,(((pickup_prof,dropoff_prof),prof),(pickup_empty,dropoff_empty,empty_count))): (cell, (empty_count, prof, prof/empty_count, pickup_empty, dropoff_empty, pickup_prof, dropoff_prof))))
    #.sortBy(lambda x: x[1][2], ascending=False)
#    profitabilityByCell = profitCompleteByCell.join(countEmptyTaxiesByCells)
#    profitabilityByCell.map(lambda x: ('PROFITABILITY',x)).pprint()

    topTenProfitableAreas = profitabilityByCell.transform(lambda rdd: rdd.zipWithIndex().filter(lambda (k,v): v<10).map(lambda (k,v):k))

#    tripMaxDropoffBycell.checkpoint(120)



    def updateFunc(new_values, last_list):
#        print("LAST-LIST: %s" % last_list)
        if last_list is None:
            last_ranking = []
        else:
            last_ranking = last_list[1] #ignore pickup_datetime and dropoff_datetime

        if len(new_values) > 0:
            new_ranking = [k for (k,v) in new_values[0]]
        else:
            new_ranking = []

        comparison = "OLD: %s --> NEW: %s" % (last_ranking, new_ranking)

        new = "%s" % new_values
#        print("updateFunc: "+comparison)
        #
        paddedOldRanking =  [i for i,j in list(itertools.izip_longest(last_ranking, range(10)))]
        diff = [j for i, j in zip(paddedOldRanking, new_ranking) if i != j]
        if len(diff)>0:
#            print("new elements: %s (new ranking: %s)" % (diff, new_ranking))
            new_datez = [(v[1][1],v[1][0], v[2]) for (k,v) in new_values[0] if k in diff]
#            print("NEW_DATEZ: %s"%new_datez)
            new_dates = max(new_datez)
#            print("NEW_DATES[2]: %s"% new_dates[2])
            new_result = [(new_dates[1],new_dates[0]), new_ranking, time.time() - new_dates[2]]
#            print("NEW_RESULT: %s"%new_result)

            #write the new ranking on file outputPath2 only when there is a change in any of the the top N positions
            new_string=formatResult(new_result)
            with open(outputPath1, 'a+') as f:
                f.write(new_string + "\n\n")
        else:
            new_result=last_list
        # write the comparison between old and new status on file outputPath1 every time updateFunc is invoked
#        with open(outputPath1, 'a') as f:
#            f.write(comparison + "\n")
        return new_result




    def updateFunc2(new_values, last_list):
        if last_list is None:
            last_ranking = []
        else:
            last_ranking = last_list[1] #ignore pickup_datetime and dropoff_datetime

#        print("NEW-VALUES: %s" % new_values)

        if len(new_values) > 0:
            new_ranking = [k for (k,v) in new_values[0]]
        else:
            new_ranking = []

#        comparison = "OLD: %s --> NEW: %s" % (last_ranking, new_ranking)

        new = "%s" % new_values
#        print("updateFunc: "+comparison)
        #
        paddedOldRanking =  [i for i,j in list(itertools.izip_longest(last_ranking, range(10)))]
        diff = [j for i, j in zip(paddedOldRanking, new_ranking) if i != j]
        if len(diff)>0:
#            print("new elements: %s (new ranking: %s)" % (diff, new_ranking))
            new_datez = [(v[4],v[3]) for (k,v) in new_values[0] if k in diff]
#            print("NEW_DATEZ: %s"%new_datez)
            new_dates = max(new_datez)
            new_ranking_complete = [(k, v[0], v[1], v[2]) for (k,v) in new_values[0]]
            new_result_print = [(new_dates[1],new_dates[0]), new_ranking_complete]
            new_result = [(new_dates[1],new_dates[0]), new_ranking]
#            print("NEW_RESULT: %s" % new_result)
            #write the new ranking on file outputPath2 only when there is a change in any of the the top N positions
            new_string=formatResult(new_result_print)
            with open(outputPath2, 'a+') as f:
                f.write(new_string + "\n\n")
        else:
            new_result=last_list
        # write the comparison between old and new status on file outputPath1 every time updateFunc is invoked
#        with open(outputPath1, 'a') as f:
#            f.write(comparison + "\n")
        return new_result




    topTenWithDelays = topTenTrips.join(firstWindowedTimes).map(lambda (k,v): (k, (v[0][0], v[0][1], v[1])))
    rankingTripCount = topTenWithDelays.transform(lambda rdd:rdd.map(lambda x:('ranking',x)).groupByKey().map(lambda x : (x[0], list(x[1]))))

    rankingProfitability = topTenProfitableAreas.transform(lambda rdd:rdd.map(lambda x:('ranking2',x)).groupByKey().map(lambda x : (x[0], list(x[1]))))

#    topTenWithDelays.map(lambda x: ('TOP-TEN-WITH-DELAYS', x)).pprint()

    status = rankingTripCount.updateStateByKey(updateFunc)
    status.pprint()
    status2 = rankingProfitability.updateStateByKey(updateFunc2)
    status2.pprint()


    return ssc


if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("Usage: spark_taxi.py <hostname> <port> "
              "<checkpoint-directory> <output-file1> <output-file2>", file=sys.stderr)
        exit(-1)

    host, port, checkpoint, output1, output2, windowDuration, slideDuration = sys.argv[1:]
    ssc = StreamingContext.getOrCreate(checkpoint,
                                       lambda: createContext(host, int(port), output1, output2, int(windowDuration), int(slideDuration)))
    ssc.start()
    ssc.awaitTermination()
