from __future__ import print_function
from threading import Thread

import time
from datetime import datetime
import numpy as pynum
import os, fnmatch
import psutil
import csv
import sys
import mysql.connector

import calendar

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler

from os import SEEK_END
import mmap

files = []
pattern = ""
out_pattern = ""

table = "pnl_test"

fda_period = 15
fda_min_threshold = 0.6
fda_max_threshold = 0.6
fdaa_threshold = 0.6
maj_threshold = 20

sig_file = "" # in bars of file
last_signal_bar = 0

upd = {} # a dictionary for updated
 

class MyHandler(FileSystemEventHandler):
	def on_modified(self, event):
		#print(f'event type: {event.event_type}  path : {event.src_path}')
		# it fires twice, but fuck it, should be OK on 4H period
		if fnmatch.fnmatch(str(event.src_path), pattern):
			#print(str(event) + " " + str(event.src_path) + " was modified")
			file = event.src_path[2:] # strip './'
			bar, signal, fda = read_last_signal(file) 
			#print(file, bar, signal, fda)
			
			'''
			 1. create a data structure

					data['bar_filename'][signal_fda]
			'''
			
			# if the value is not there, we add it
			key = str(bar) + "_" + str(file)
			val = str(signal) + "_" + str(fda)
			if ( upd.get(key)==None  ):
				upd[key]=val
				
			# check that we have keys for ALL files that we monitor
			all_present = True
			for f in files:
				key = str(bar) + "_" + str(f)
				if ( upd.get(key)==None  ): # if at least one is not there, not all are present
					all_present = False
					break
			
			signal = 0
			if (all_present): # here we will calculate the signal for this bar
				# generate signal as usual
				for f in files:
					key = str(bar) + "_" + str(file)
					v = upd.get(key)
					s = int(v.split("_")[0])
					fda = float(v.split("_")[1])
					if (fda >= fda_threshold):
						signal += s
				
				msg = "Bar" + bar + ": " + str(signal) + " / " + str(sign_threshold)
				ts(msg)
				log_signal(msg)
				

def write2mysql(pnl,dd):
	mydb = mysql.connector.connect(
		host="localhost",
		user="root",
		passwd="xsjhk%123SD==-Ghk11",
		database="altreva"
	)

	mycursor = mydb.cursor()
	'''
	0 05k_7btc_3-optimized.pkl,
	1 period
	2 0.55,
	3 0.55,
	4 1,
	5 0.5,
	6 -2855.340000000004,
	7 -18.049999999997453
	
	fda_period = 15
	fda_min_threshold = 0.6
	fda_max_threshold = 0.6
	fdaa_threshold = 0.6
	maj_threshold = 20
		
	'''

	sql = "INSERT INTO " + table + " (file, fda_period, fda_min, fda_max, maj_threshold, fda_all_min, dd,  PnL) VALUES ('" + out_pattern + "'," + str(fda_period) + "," + str(fda_min_threshold) + "," + str(fda_max_threshold) + "," + str(maj_threshold) + "," + str(fdaa_threshold)  + "," + str(dd) + "," + str(pnl) + ")"
	#print(sql)
	mycursor.execute(sql)
	
	mydb.commit()
	mydb.close()
	#print('wrote to mysql')
				
					
def ts(s):	
	now = datetime.datetime.now()
	#print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)
	

def read_last_signal(filename):	

	'''
	0 Bar,
	1 Date,
	2 Time,
	3 Price,
	4 Forecast,
	5 Signal,
	6 Wealth Distribution,
	7 Buy Orders,
	8 Sell Orders,
	9 VM Trades,
	10 VM Trades MA (100),
	11 Population Position,
	12 Avg Genome Size,
	13 Right Forecasted Price Changes,
	14 Wrong Forecasted Price Changes,
	15 FDA (Trailing 100 bars; Range applied
	'''
	
	Bar_Col = 0
	Date_Col = 1
	Time_Col = 2
	Signal_Col = 5
	Fda_Col = 15
	Fda_All = 21
	
	ifile = open(filename, "rU")
	reader = csv.reader(ifile, delimiter=",")
	a = []
	a.append["Bar,Signal,FDA,FDA Alltime,Date,Year,Month,Day,Weekday,Hour"]
	for row in reader:
	
		'''
		%b Month as locale’s abbreviated name(Jun)
		%d Day of the month as a zero-padded decimal number(1)
		%Y Year with century as a decimal number(2015)
		%I Hour (12-hour clock) as a zero-padded decimal number(01)
		%M Minute as a zero-padded decimal number(33)
		%p Locale’s equivalent of either AM or PM(PM)
		'''
		
		# 06/21/2007
		dt = datetime.strptime(row[Date_Col], '%b/%d/%Y')
		
		year = dt.year
		month = dt.month
		day = dt.day
		weekday = td.weekday
		
		hour = int((row[Time_Col]).split(":")[0])
		
		a.append ( [row[Bar_Col],row[Signal_Col],row[Fda_Col],row[Fda_All],row[Date_Col], year, month, day, weekday, hour] )
		
	ifile.close()

	
	# what are we doing here???
	last = len(a)-1
	line = last
	bar = a[last][0]
	fda = a[last][2]
	if ( a[line][1] == "" ):
		#scroll up
		while (line >=1): # do not include the header
			#print(sig(a[line][1]))
			if (a[line][1] != ""):
				break
			line -= 1
	s = sig(a[line][1])

	return bar, s, fda # what the fuck is that????

def readcsv(filename):	
	'''
	0 Bar,
	1 Date,
	2 Time,
	3 Price,
	4 Forecast,
	5 Signal,
	6 Wealth Distribution,
	7 Buy Orders,
	8 Sell Orders,
	9 VM Trades,
	10 VM Trades MA (100),
	11 Population Position,
	12 Avg Genome Size,
	13 Right Forecasted Price Changes,
	14 Wrong Forecasted Price Changes,
	15 FDA (Trailing 100 bars; Range applied
	'''
	
	Bar_Col = 0
	Date_Col = 1
	Time_Col = 2
	Price_Col = 3
	Signal_Col = 5
	Fda_Col = 15
	Fda_All = 21
	
	ifile = open(filename, "rU")
	reader = csv.reader(ifile, delimiter=",")
	a = []
	a.append(["Bar,Price,Signal,FDA,FDA Alltime,Date,Year,Month,Day,Weekday,Hour"])
	
	r = 0
	for row in reader:
		if (r==0): # don't process the header
			r =1
			continue
		'''
		%b Month as locale’s abbreviated name(Jun)
		%d Day of the month as a zero-padded decimal number(1)
		%Y Year with century as a decimal number(2015)
		'''
		# 06/21/2007
		dt = datetime.strptime(row[Date_Col], '%m/%d/%Y')
		
		year = dt.year
		month = dt.month
		day = dt.day
		wd= calendar.day_name[dt.weekday()]
		
		
		hour = int((row[Time_Col]).split(":")[0])
		minute = int((row[Time_Col]).split(":")[1])
		
		#print(row[Date_Col], row[Time_Col], year, month, day, wd, hour, minute)
		
		a.append ( [row[Bar_Col],row[Price_Col],row[Signal_Col],row[Fda_Col],row[Fda_All],row[Date_Col], year, month, day, wd, hour, minute] )
		
	ifile.close()
	return a


def sig(s):
	if (s=="Long"):
		return 1
	elif (s=="Short"):
		return -1
	else: # Cash or anything
		return 0
	
def write2file(lines):
	f = open(sig_file, 'w')
	for line in lines:
		f.write(line)
	f.close()
 
def realtime():
	
	# implement monitoring handlers for all files
	event_handler = MyHandler()
	observer = Observer()
	observer.schedule(event_handler, ".", recursive=False)
	observer.start()
	try:
		while True:
			time.sleep(60)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()
	
def historical():
	# check that files have the same number of lines
	lines_n = 0
	prev_file =""
	listOfFiles = os.listdir('.') 
	for report in listOfFiles:  
		if fnmatch.fnmatch(report, pattern ):
			a = readcsv(report)
			if (lines_n==0):
				lines_n = len(a)
			else:
				if (lines_n != len(a)):
					print("integrity check failed!")
					print(prev_file + ": " + str(lines_n) )
					print(report + ": " + str(len(a)))
					exit()
			prev_file = report
					
	#print("Number of lines in files: " + str(lines_n) + "     -> integrity check passed")
	#############################################
	
	# run a few apps with different periods as different processes
	day = 6 # 6 4-hour bars
	week = 5 * day
	two_weeks = 2 * week
	month = 4 * week
	
	###################### generate a file with array
	'''
	# a.append ( 	[row[Bar_Col],		0
					row[Price_Col],		1
					row[Signal_Col],	2
					row[Fda_Col],		3
					row[Fda_All],		4
					row[Date_Col], 		5
					year, 				6
					month, 				7
					day, 				8
					wd, 				9
					hour,				10
					minute] )			11	
	'''
	BarCol 		= 0
	PriceCol 	= 1
	SignalCol 	= 2
	FdaCol 		= 3
	FdaAll 		= 4
	DateCol 	= 5
	YearCol 	= 6
	MonthCol 	= 7
	DayCol 		= 8
	WeekdayCol 	= 9
	HourCol 	= 10
	MinuteCol 	= 11


	listOfFiles = os.listdir('.') 
	arrReports = [] # this will contain lists for every report
	for report in listOfFiles:  
		if fnmatch.fnmatch(report, pattern ):
			a = readcsv(report)
			r = 1 # 0 would be header
			# convert long/short/cash to 1,-1,0
			prev = 0
	
			# on row 1 fix Signal and Trail FDA (FDA All Time is always some number, because it took 100 bars)
			if (a[1][SignalCol]=="" or a[1][SignalCol]=="Cash"): 
				a[1][SignalCol]=0
				prev = 0
			elif (a[1][SignalCol]=="Long"):
				a[r][SignalCol]=1
				prev = 1
			elif (a[1][SignalCol]=="Short"):
				a[r][SignalCol] = -1
				prev = -1
				
			if (a[1][FdaCol]==""): a[r][FdaCol]=0

			while (r < len(a)):
				if ( a[r][SignalCol]=="" ): # the idea is to fill blank lines with the value of previous signal before the blank lines, even if it's blank )
					a[r][SignalCol]=prev
				else:
					# change cur row to digital and update prev
					if (a[r][SignalCol] == "Long"):
						a[r][SignalCol] = 1
						prev = 1
					elif (a[r][SignalCol] == "Short"):
						a[r][SignalCol] = -1
						prev = -1
					elif (a[r][SignalCol] == "Cash"):
						a[r][SignalCol] = 0
						prev = 0
			
				if (a[r][FdaCol]==""): a[r][FdaCol]=0			
				r += 1

			# 2. construct array of trading systems
			arrReports.append(a) # now it's arrReports[model_id][row][column]
			
	# now create the report and calculate PnL, then analyze this report in IBM Watson for best months/days/weekdays/hours of trading
	# -> check separately negative PnL rows and see if that has tendency related to date/month/weekday and can be eliminated
	pips = 0 # p&l in pips, start calculating from row 2
	dd = 0 # historical in pips 
	
	pips_max=0
	dd_pct =0 # drawdown immediate in percents
	
	#Now below we go across every row r, and then across fda columns for every model of the that row r
	
	lines =[] # PnL report file
	lines.append("Bar,Date,Price,Signal,Calculations,PnL,Drawdown,FDA,FDA Alltime,Year,Month,Day,Weekday,Hour,Minute\n")
	for r in range(len(arrReports[0])-1): # because we can't calculate PnL of the last row (we don't know values of the next one!)
		# that's header
		if (r < 2+fda_period): # we know the future price only on the next row of a signal, so row 1 has no future price
			continue
		else:
			row_signal = 0 # signal across a row with all models
			for model in arrReports:	
				fda_avg = 0.0
				# sum FDA forr previous bars of count period
				for row in pynum.arange(r-fda_period, r , 1):
					if (model[r][FdaCol] != ""): # it's null in the beginning of the file for some first lines
						fda_avg += float(model[r][FdaCol]) 
			
				fda_avg = fda_avg / fda_period # average
				
				if ( fda_avg >= fda_min_threshold and fda_avg <= fda_max_threshold and float(model[r][FdaAll]) >= fdaa_threshold): # make sure FDA is within needed range, then we count that signal
					row_signal += model[r][SignalCol]
	
			m = arrReports[0] # just take first model for calculations

			# actual cacl that has to be traced
			trace_calc = ""
			if ( abs(row_signal) >= maj_threshold): # it was a valid signal and we execute it
				if (row_signal > 0):
					pips += float(model[r+1][PriceCol]) - float(model[r][PriceCol])
					trace_calc = model[r+1][PriceCol] + " - " + model[r][PriceCol] + " = " + str(float(model[r+1][PriceCol]) - float(model[r][PriceCol]))
				elif (row_signal < 0):
					pips += float(model[r][PriceCol]) - float(model[r+1][PriceCol])
					trace_calc = model[r][PriceCol] + " - " + model[r+1][PriceCol] + " = " + str(float(model[r][PriceCol]) - float(model[r+1][PriceCol]))

			if (pips > pips_max):
				pips_max = pips
				
			# calculate immediate drawdown
			if (pips_max != 0): dd_pct = ((pips_max-pips)/pips_max)*100
			
			if (pips < 0 and pips < dd): # update hist max drawdown when we have immediate one bigger that historic max
				dd = pips

			#print(model[r][BarCol],",",model[r][DateCol],",",model[r][PriceCol],",",row_signal,",", pips,",", dd,",", model[r][FdaCol],",",model[r][FdaAll],",",model[r][YearCol],",",model[r][MonthCol],",",model[r][DayCol],",",model[r][WeekdayCol],",",model[r][HourCol])
			lines.append(str(model[r][BarCol])+","+str(model[r][DateCol])+","+str(model[r][PriceCol])+","+str(row_signal)+","+ trace_calc +","+ str(pips)+","+ str(dd_pct)+","+ str(model[r][FdaCol])+","+str(model[r][FdaAll])+","+str(model[r][YearCol])+","+str(model[r][MonthCol])+","+str(model[r][DayCol])+","+str(model[r][WeekdayCol])+","+str(model[r][HourCol])+","+str(model[r][MinuteCol])+"\n")
	
	write2file(lines)
	#print("PnL: ", pips, " / Drawdown: ", dd)
	write2mysql(pips,dd)
	print(out_pattern, ": PnL=", pips," Drawdown=", dd)

	
#def adaptive_historical():
def historical_adaptive():
	# check that files have the same number of lines
	lines_n = 0
	prev_file =""
	listOfFiles = os.listdir('.') 
	for report in listOfFiles:  
		if fnmatch.fnmatch(report, pattern ):
			a = readcsv(report)
			if (lines_n==0):
				lines_n = len(a)
			else:
				if (lines_n != len(a)):
					print("integrity check failed!")
					print(prev_file + ": " + str(lines_n) )
					print(report + ": " + str(len(a)))
					exit()
			prev_file = report
					
	#print("Number of lines in files: " + str(lines_n) + "     -> integrity check passed")
	#############################################
	

	
	###################### generate a file with array
	'''
	# a.append ( 	[row[Bar_Col],		0
					row[Price_Col],		1
					row[Signal_Col],	2
					row[Fda_Col],		3
					row[Fda_All],		4
					row[Date_Col], 		5
					year, 				6
					month, 				7
					day, 				8
					wd, 				9
					hour,				10
					minute] )			11	
	'''
	BarCol 		= 0
	PriceCol 	= 1
	SignalCol 	= 2
	FdaCol 		= 3
	FdaAll 		= 4
	DateCol 	= 5
	YearCol 	= 6
	MonthCol 	= 7
	DayCol 		= 8
	WeekdayCol 	= 9
	HourCol 	= 10
	MinuteCol 	= 11


	listOfFiles = os.listdir('.') 
	arrReports = [] # this will contain lists for every report
	for report in listOfFiles:  
		if fnmatch.fnmatch(report, pattern ):
			a = readcsv(report)
			r = 1 # 0 would be header
			# convert long/short/cash to 1,-1,0
			prev = 0
	
			# on row 1 fix Signal and Trail FDA (FDA All Time is always some number, because it took 100 bars)
			if (a[1][SignalCol]=="" or a[1][SignalCol]=="Cash"): 
				a[1][SignalCol]=0
				prev = 0
			elif (a[1][SignalCol]=="Long"):
				a[r][SignalCol]=1
				prev = 1
			elif (a[1][SignalCol]=="Short"):
				a[r][SignalCol] = -1
				prev = -1
				
			if (a[1][FdaCol]==""): a[r][FdaCol]=0

			while (r < len(a)):
				if ( a[r][SignalCol]=="" ): # the idea is to fill blank lines with the value of previous signal before the blank lines, even if it's blank )
					a[r][SignalCol]=prev
				else:
					# change cur row to digital and update prev
					if (a[r][SignalCol] == "Long"):
						a[r][SignalCol] = 1
						prev = 1
					elif (a[r][SignalCol] == "Short"):
						a[r][SignalCol] = -1
						prev = -1
					elif (a[r][SignalCol] == "Cash"):
						a[r][SignalCol] = 0
						prev = 0
			
				if (a[r][FdaCol]==""): a[r][FdaCol]=0			
				r += 1

			# 2. construct array of trading systems
			arrReports.append(a) # now it's arrReports[model_id][row][column]
			
	# now create the report and calculate PnL, then analyze this report in IBM Watson for best months/days/weekdays/hours of trading
	# -> check separately negative PnL rows and see if that has tendency related to date/month/weekday and can be eliminated
	pips = 0 # p&l in pips, start calculating from row 2
	dd = 0 # historical in pips 
	
	pips_max=0
	dd_pct =0 # drawdown immediate in percents
	
	#Now below we go across every row r, and then across fda columns for every model of the that row r
	
	lines =[] # PnL report file
	lines.append("Bar,Date,Price,Signal,Calculations,PnL,Drawdown,FDA,FDA Alltime,Year,Month,Day,Weekday,Hour,Minute\n")
	for r in range(len(arrReports[0])-1): # because we can't calculate PnL of the last row (we don't know values of the next one!)
		# that's header
		if (r < 2+fda_period): # we know the future price only on the next row of a signal, so row 1 has no future price
			continue
		else:
			row_signal = 0 # signal across a row with all models
			
			# generate pairs
			print("will be generating pairs here")
			exit()
			
			
			for model in arrReports:	
				fda_avg = 0.0
				# sum FDA forr previous bars of count period
				for row in pynum.arange(r-fda_period, r , 1):
					if (model[r][FdaCol] != ""): # it's null in the beginning of the file for some first lines
						fda_avg += float(model[r][FdaCol]) 
			
				fda_avg = fda_avg / fda_period # average
				
				if ( fda_avg >= fda_min_threshold and fda_avg <= fda_max_threshold and float(model[r][FdaAll]) >= fdaa_threshold): # make sure FDA is within needed range, then we count that signal
					row_signal += model[r][SignalCol]
	
			m = arrReports[0] # just take first model for calculations

			# actual cacl that has to be traced
			trace_calc = ""
			if ( abs(row_signal) >= maj_threshold): # it was a valid signal and we execute it
				if (row_signal > 0):
					pips += float(model[r+1][PriceCol]) - float(model[r][PriceCol])
					trace_calc = model[r+1][PriceCol] + " - " + model[r][PriceCol] + " = " + str(float(model[r+1][PriceCol]) - float(model[r][PriceCol]))
				elif (row_signal < 0):
					pips += float(model[r][PriceCol]) - float(model[r+1][PriceCol])
					trace_calc = model[r][PriceCol] + " - " + model[r+1][PriceCol] + " = " + str(float(model[r][PriceCol]) - float(model[r+1][PriceCol]))

			if (pips > pips_max):
				pips_max = pips
				
			# calculate immediate drawdown
			if (pips_max != 0): dd_pct = ((pips_max-pips)/pips_max)*100
			
			if (pips < 0 and pips < dd): # update hist max drawdown when we have immediate one bigger that historic max
				dd = pips

			#print(model[r][BarCol],",",model[r][DateCol],",",model[r][PriceCol],",",row_signal,",", pips,",", dd,",", model[r][FdaCol],",",model[r][FdaAll],",",model[r][YearCol],",",model[r][MonthCol],",",model[r][DayCol],",",model[r][WeekdayCol],",",model[r][HourCol])
			lines.append(str(model[r][BarCol])+","+str(model[r][DateCol])+","+str(model[r][PriceCol])+","+str(row_signal)+","+ trace_calc +","+ str(pips)+","+ str(dd_pct)+","+ str(model[r][FdaCol])+","+str(model[r][FdaAll])+","+str(model[r][YearCol])+","+str(model[r][MonthCol])+","+str(model[r][DayCol])+","+str(model[r][WeekdayCol])+","+str(model[r][HourCol])+","+str(model[r][MinuteCol])+"\n")
	
	write2file(lines)
	#print("PnL: ", pips, " / Drawdown: ", dd)
	write2mysql(pips,dd)
	print(out_pattern, ": PnL=", pips," Drawdown=", dd)
	
#def realtime_adaptive():
	
if __name__ == "__main__":


	'''
	Run examples 
	
		Historical reports: python sig-gen.py hist 05k_8btc_1 5 0.5 0.59 0.51 1 BTCUSD-4H
							python sig-gen.py hista 05k_sber_ 5 0.7 1 0.515 1
	
		Realtime reports: TODO! sig-gen.py real timeframe instrument ... this will be an adaptive method
	'''
	
	mode = sys.argv[1]
	out_pattern = sys.argv[2]
	pattern = "*" + sys.argv[2] + "*export1.csv"
	fda_period = float(sys.argv[3])
	fda_min_threshold = float(sys.argv[4]) # FDA lower boundary
	fda_max_threshold = float(sys.argv[5]) # FDA higher boundary
	fdaa_threshold = float(sys.argv[6]) #
	maj_threshold = int(sys.argv[7])
	
	
	'''
		todo:	calculate timeframe from files, make sure they all have the same number of lines
				the output name should be like instrument_timeframe_pattern_live.csv or the output name should be like instrument_timeframe_pattern_hist.csv
	'''
	
	listOfFiles = os.listdir('.')  
	
	# get the array of files
	for entry in listOfFiles: 
		if fnmatch.fnmatch(entry, pattern):
			files.append(entry) # now we've got a full list
	
	# informational notice
	print( "Files matching pattern: " + pattern)
	for e in files:
		print(e)
	print( "Total files monitored: " + str(len(files)))
	
	# optional - some unique string like "EURUSD-4h" or so
	f_name = "ptrn(" + sys.argv[2] + ")_p" + sys.argv[3] + "_min" + sys.argv[4] + "_max" + sys.argv[5] + "_" + sys.argv[6] + "_maj" + sys.argv[7] + ".csv"
	if (sys.argv[0] == "8"): 
		sig_file = "_" + sys.argv[8] + "_" + f_name
	else:
		sig_file =  "_" + f_name
	
	
	# now let's start handling them
	if (mode=="hist"):
		sig_file = "sig-hist" + sig_file
		historical()
	if (mode=="hista"):
		# python sig-gen.py hista 05k_sber 5period 0.7min 1max
		print("historic adaptive mode")
		sig_file = "sig-hista" + sig_file
		historical_adaptive()
	elif (mode=="real"):
		sig_file = "sig-real-real" + sig_file
		realtime()
	else:
		print("Check the first argument, only hist and real modes are supported") 
	