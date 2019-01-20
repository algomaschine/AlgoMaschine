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

table = ""

# now these are the new values in our array

PriceCol = 0
SignalCol = 1
FdaCol = 2
FdaAll = 3	

def run_prog():
	#now = datetime.datetime.now()
	c = cmd.get()
	#print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " >> " + c + " - STARTED")
	os.system(c)
	#now = datetime.datetime.now()
	#print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " << " + c + " - FINISHED")


def process_report_regex(regex):
	postfix = "*export1.csv"
	listOfFiles = os.listdir('.') 
	arrReports = [] # this will contain lists for every report
	pFile = regex  + '-optimized.pkl'
	if (os.path.isfile(pFile)==False):	
		for report in listOfFiles:  
			if fnmatch.fnmatch(report, regex+postfix ):
				a = readcsv(report)
			
				'''
				print("OLD ARRAY \n")
				r = 1
				while (r < len(a)):
					print(a[r][0],a[r][SignalCol])
					r += 1
				'''
			
				r = 1 # 1 would be header
				# convert long/short/cash to 1,-1,0
				
				# TODO: somethig is wrong here below, fix it!!!!!!!!
				
				prev = 0
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
				
					r += 1
			
				'''
				print("NEW NEW ARRAY \n")
				r = 1
				while (r < len(a)):
					print(a[r][0],a[r][SignalCol])
					r += 1
			
				exit()
				'''
			
				#print("appending: " + report)
				# 2. construct array of trading systems
				arrReports.append(a) # now it's arrReports[model_id][row][column]
			
		output = open(pFile, 'wb')
		pickle.dump(arrReports, output)
		output.close()
	
	#com = "python ts-simple.py " + str(pFile) + " " + table
	com = "python ts-simple-optimized.py " + str(pFile) + " " + table
	cmd.put(com)

	while (cmd.qsize()>0):
		#ts("CPU Utilization = " + str(cpu_util()) + "% // tasks in stack: " + str(cmd.qsize()) )

		# start new processes while CPU utilization is less than 89
		if (psutil.cpu_percent()<99 and ram_util()<80 ):
			thread = Thread(target = run_prog)
			thread.start()
			#time.sleep(2)
			
		time.sleep(1) # checks once per minute

				
	
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
	Fda_All = 21
	
	ifile = open(filename, "rU")
	reader = csv.reader(ifile, delimiter=",")

	a = []
	for row in reader:
		a.append ( [row[Price_Col],row[Signal_Col],row[Fda_Col],row[Fda_All]] )
    
	ifile.close()
	return a			

def analyze(reports):
	for f in reports:
		print(f)
	
			
	
if __name__ == "__main__":
	
	# python TS-Optimizer.py table 01k_10j2_1
	
	table = sys.argv[1]
	
	r = 2
	while (r < len(sys.argv)):
		process_report_regex( sys.argv[r] )
		r += 1
	
	
	#print ("table: " , sys.argv[2])
	#exit()
	
	#print("Done execution, waiting for all threads to continue...")

