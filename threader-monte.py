from __future__ import print_function
from threading import Thread


import time
import datetime
import os, fnmatch
import psutil
import csv
import sys
import numpy as pynum
import pickle

from queue import LifoQueue
cmd = LifoQueue()

# now these are the new values in our array

PriceCol = 0
SignalCol = 1
FdaCol = 2
	

def run_prog():
	#now = datetime.datetime.now()
	c = cmd.get()
	#print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " >> " + c + " - STARTED")
	os.system(c)
	#now = datetime.datetime.now()
	#print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " << " + c + " - FINISHED")


def process_report_regex(regex):
	out_file = "optimus-" + regex + ".csv" # format of report:  min_fda, max_fda, opt_period, maj_threshold, profit_pips
	postfix = "*export1.csv"
	listOfFiles = os.listdir('.') 
	arrReports = [] # this will contain lists for every report
	DataRows = -777
	# for each file we need to create a list of rows
	# then add this list to arrReports
	for report in listOfFiles:  
		if fnmatch.fnmatch(report, regex+postfix ):
			a = readcsv(report)
			# 1. calculate rows from model_1 
			if (DataRows==-777): 
				DataRows=len(a) # and we do it only ONCE
			
			'''
			print("OLD ARRAY \n")
			r = 1
			while (r < len(a)):
				print(a[r][0],a[r][SignalCol])
				r += 1
			'''
			
			r = 1 # 1 would be header
			prev = ""
			while (r < len(a)):
				if ( a[r][SignalCol]=="" ): # the idea is to fill blank lines with the value of previous signal before the blank lines, even if it's blank )
					a[r][SignalCol]=prev
				else:
					prev = a[r][SignalCol]
				r += 1
			
			'''
			print("NEW NEW ARRAY \n")
			r = 1
			while (r < len(a)):
				print(a[r][0],a[r][SignalCol])
				r += 1
			'''
			
			#print("appending: " + report)
			# 2. construct array of trading systems
			arrReports.append(a) # now it's arrReports[model_id][row][column]
			
	# at this point arrReports contains all the signal files with Signals col filled correctly		
	# print(len(arrReports))
	pFile = regex  + '.pkl'
	if (os.path.isfile(pFile)==False):	
		output = open(pFile, 'wb')
		pickle.dump(arrReports, output)
		output.close()
	
	# fda thershold value
	low = 0.4
	up = 0.78
	fda_step = 0.02
	
	for fda_low in pynum.arange(low, up, fda_step):
		for fda_high in pynum.arange(fda_low+fda_step, up, fda_step):			
			cmd.put( "python ts-monte.py " + str(pFile) + " " + str(DataRows) + " " + str(fda_low) + " " + str(fda_high) )

	while (cmd.qsize()>0):
		#ts("CPU Utilization = " + str(cpu_util()) + "% // tasks in stack: " + str(cmd.qsize()) )

		# start new processes while CPU utilization is less than 89
		if (psutil.cpu_percent()<60 and ram_util()<50):
			thread = Thread(target = run_prog)
			thread.start()
			time.sleep(2)
			
		time.sleep(3) # checks once per minute

				
	
	# TODO: make an option to reproduce the report for this signals in the future for MT4


				
# Remove the print statements. They force your process to pause and do IO instead of using pure CPU. –
# CHECK THIS!


# print something with timestamp
def ts(s):	
	now = datetime.datetime.now()
	print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)
	

def ram_util():
	return ( float((str(psutil.virtual_memory()).split(",")[2]).replace(" ","").replace("percent=","")) )

def cpu_util():
	max_t = 2
	t = max_t # so many iterations to measure CPI Utilization
	util = 0
	while (t>0): # we do it a few times and calc avg to be sure
		util += psutil.cpu_percent()
		time.sleep(t)
		t-=1
	return (util/max_t)
	
				
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
	

	Price_Col = 3
	Signal_Col = 5
	Fda_Col = 15
	
	ifile = open(filename, "rU")
	reader = csv.reader(ifile, delimiter=",")

	a = []
	for row in reader:
		a.append ( [row[Price_Col],row[Signal_Col],row[Fda_Col],] )
    
	ifile.close()
	return a			

def analyze(reports):
	for f in reports:
		print(f)
	
			
	
if __name__ == "__main__":
	
	# python TS-Optimizer.py 01k_10j2_1
	r = 1
	while (r < len(sys.argv)):
		process_report_regex( sys.argv[r] )
		r += 1
	
	#print("Done execution, waiting for all threads to continue...")

