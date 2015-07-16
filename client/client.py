import socket
import os
import fileinput
import re
import datetime
import time

TAXI_LOG_PATTERN = '^(\S+),(\S+),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}:\d{2}),(\d+),(\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\-?\d+\.\d+),(\S+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+),(\d+\.\d+)'

def to_datetime(s):
    return datetime.datetime(int(s[0:4]),
                             int(s[5:7]),
                             int(s[8:10]),
                             int(s[11:13]),
                             int(s[14:16]),
                             int(s[17:19]))
def to_timestring(d):
	return str(d.year)+"-"+str(d.month).zfill(2) +"-"+str(d.day).zfill(2)+" "+str(d.hour).zfill(2)+":"+str(d.minute).zfill(2)+":"+str(d.second).zfill(2) 

def process_line(line):
	match = re.search(TAXI_LOG_PATTERN, line)
	if match is None:
		return None

	dict = {
	'medallion'            : match.group(1),
	'hack_license'         : match.group(2),
	'pickup_datetime'      : to_datetime(match.group(3)),
	'dropoff_datetime'     : to_datetime(match.group(4)),
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
	'total_amount'         : float(match.group(17))}

	delta = dict['dropoff_datetime'] - dict['pickup_datetime']
	new_dropoff = datetime.datetime.now()
	new_pickup = new_dropoff - delta
	new_line = match.group(1)
	for i in range(2,18):
		if i == 3:
			new_line += ","+to_timestring(new_pickup)
		elif i == 4:
			new_line += ","+to_timestring(new_dropoff)
		else:
			new_line += ","+match.group(i)
	return (new_line, dict['dropoff_datetime'])

print "Loading configuration"

config = {}
execfile("client.conf", config) 
host = config["ip"]
port = config["port"]

print "Initizialing connection to host:", host, "at port:",port

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
current_dir =  os.path.abspath(os.path.dirname(__file__))
file_path = os.path.abspath(current_dir + "/"+config["file_path"])

s.connect((host,port))
file = fileinput.input(file_path)

print "Reading file at path:", file_path,"\n"


line = file.readline()		
(entry, ts)  = process_line(line)

while line:
	print "Sending entry:\n", entry,"\n"
	s.send(entry) 
	data = ''
 	data = s.recv(1024).decode()
 	line = file.readline()
 	if line:
 		old_ts = ts
 		(entry, ts)  = process_line(line)
 		delta = (ts-old_ts).total_seconds()
 		if delta > 0:
 			print "Waiting", delta, "for the next entry\n"
 			time.sleep(delta)
 	else:
 		break

s.close ()