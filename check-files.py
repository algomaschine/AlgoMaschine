from __future__ import print_function
from threading import Thread

import time
import datetime
import os, fnmatch
import psutil
import csv
import sys
import mysql.connector

from datetime import datetime
 
table = "file_checks"
'''
  `pattern` tinytext,
  `file_count` integer,
  `timeframe` integer,
  `fda_period` integer,
  `rows` integer
''' 
 
def write2mysql(ss):
	mydb = mysql.connector.connect(
		host="localhost",
		user="root",
		passwd="xsjhk%123SD==-Ghk11",
		database="altreva"
	)

	mycursor = mydb.cursor()
	for s in ss:
	
		a = s.split(",")
	
		sql = "INSERT INTO " + table + " (pattern, file_count,timeframe, fda_period,rows_count) VALUES ('" + a[0] + "'," + a[1] + "," + a[2]+ "," +a [3] + "," + a[4] +")"
		#print(sql)
		mycursor.execute(sql)
	
	mydb.commit()
	mydb.close()
	#print('wrote to mysql')



if __name__ == "__main__":

	listOfFiles = os.listdir('.')  
	pattern = "*" + sys.argv[1] + "*export1.csv"  
	
	patterns = set([])		# pattern (first 10 letters of the filename)
	timeframes = set([])	# timeframe
	lines = set([]) # number of lines
	fda_periods = set([]) # fda period
	
	all = set([])	
	
	for entry in listOfFiles:  
		if fnmatch.fnmatch(entry, pattern):
			
			# 1. get pattern
			p = entry[0:10] 
			patterns.add( p )
			#print(p + " added")
			
			# get timeframe / lines counts / fda period
			
			ifile = open(entry, "rU")
			reader = csv.reader(ifile, delimiter="\n")
			a = []
			for row in reader:
				a.append(row)
			ifile.close()
			
			# 2. get lines count 
			l = len(a)-1 
			lines.add( l )
			
			# 3. get timeframe
			t1 = (a[1][0]).split(",")[2]
			t2 = (a[2][0]).split(",")[2]
			#set the date and time format
			date_format = "%H:%M:%S"
			#convert string to actual date and time
			time1  = datetime.strptime(t1, date_format)
			time2  = datetime.strptime(t2, date_format)
			#find the difference between two dates
			diff = time2 - time1
			#print( (diff.seconds) / 60)
			tf = int((diff.seconds) / 60)
			timeframes.add( tf )
			
			
			# 4. fda_period
			fda_p = a[0]
			fda_p = fda_p[0]
			fda_p = (fda_p.split(',')[15]).split(" ")  # FDA (Trailing <<<10>>> bars; Range applied)
			fda_p = fda_p[2]
			fda_periods.add(fda_p)

			all.add( str(p) +"," + str(tf) +","+ str(fda_p)+","+ str(l) )
			

	#for a in all:
	#	print(a)
	#print("---------------------------------------------")
			
	# now check how many of each files we have
	#print("Pattern,", "File Count,","Timeframe,","FDA Period,","Rows")
	
	sql=[]
	
	for a in all:
		e = a.split(",")
		_p = e[0]
		_tf = e[1]
		_fda_p = e[2]
		_l = e[3]
		#print ("initi: ", _p, _tf, _fda_p, _l)
		#continue
		
		
		# now count number of files with such parameters
		counter = 0
		for entry in listOfFiles:  
			if fnmatch.fnmatch(entry, "*" + _p + "*export1.csv"  ):
				#print(entry, _p)
				# 1. get pattern
				p = entry[0:10] 
				# get timeframe / lines counts / fda period
				ifile = open(entry, "rU")
				reader = csv.reader(ifile, delimiter="\n")
				a = []
				for row in reader:
					a.append(row)
				ifile.close()
				# 2. get lines count 
				l = len(a)-1 
				# 3. get timeframe
				t1 = (a[1][0]).split(",")[2]
				t2 = (a[2][0]).split(",")[2]
				#set the date and time format
				date_format = "%H:%M:%S"
				#convert string to actual date and time
				time1  = datetime.strptime(t1, date_format)
				time2  = datetime.strptime(t2, date_format)
				#find the difference between two dates
				diff = time2 - time1
				#print( (diff.seconds) / 60)
				tf = int((diff.seconds) / 60)
				# 4. fda_period
				fda_p = a[0]
				fda_p = fda_p[0]
				fda_p = (fda_p.split(',')[15]).split(" ")  # FDA (Trailing <<<10>>> bars; Range applied)
				fda_p = fda_p[2]
				
				if (_p == str(p) and _tf == str(tf) and _fda_p==str(fda_p) and _l==str(l)): 
					#print("matched pattern: ")
					counter +=1
				
		sql.append(_p+ ","+ str(counter)+","+ _tf+","+ fda_p+","+ _l)
	write2mysql(sql)
	