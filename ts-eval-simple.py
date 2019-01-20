from __future__ import print_function

import time
import datetime
import os, fnmatch
import psutil
import csv
import sys
import numpy as pynum
import pickle
import sqlite3

pFile = ""

BarCol = 0
DateCol = 1
TimeCol = 2
PriceCol = 3
SignalCol = 4
FdaCol = 5

	
		
def write2sqlite(sql):
	con = sqlite3.connect(pFile+'.sqlite3')  
	cur = con.cursor() 
	cur.executescript(sql)
	con.commit()
	con.close()

def system_test(system_id, arrReports, rows, fda_low, fda_high, maj_threshold):

				#ts("processing model " + str(system_id))
				#print(arrReports)

				#ts("START system " + str(systems) + " // FDA range: " + str(fda_low) + " - " + str(fda_high) + str(maj))
				#continue # takes 8 seconds for each model, (8*17860)/3600 = 39 hours, so we need threading here!
				buys = 0
				sells = 0
				holds = 0
				pips = 0 # profit in pips
				prev_signal = 0
				signal = 0
				# count buys/sells
				sql = ""
				for rr in range(rows): # will go from 0 to DataRows-1
					if (rr==0):
						continue # row 0 is not counted
					
					else:
						# TODO: try random selecntion of N models
						'''
							for tries in range(1,1000,1): # 1000 times
								for sample in range(10,100,5): # various sample sizes, by len(arrReports)
									#do stuff: select random models
								
						'''
						fda = 0.0
						for model in range(len(arrReports)): # for each model
							if (arrReports[model][rr][FdaCol]==""):
								fda = 0
							else:
								fda = float(arrReports[model][rr][FdaCol])
							sig = arrReports[model][rr][SignalCol]
							
							if (fda >= fda_low and fda <= fda_high):
								if (sig=="Long"):
									buys+=1
								elif (sig =="Short"):
									sells-=1 # we get negative values here
							else:
								holds+=1
							
							#signal = 0 # hold by default
							sum = buys-sells
							if (sum >=maj_threshold):
								signal = sum
							
	
						
						#prev_price = arrReports[model][rr][PriceCol]

						# calculate trading decision and get P&L
						if (rr==1):
							# we initialize profit
							pips=0
							# but we save the signal
						else: # starting from row 2 and up
							if (prev_signal > 0):
								# we bought, subtract previous price from current price
								pips += float(arrReports[model][rr][PriceCol]) - float(arrReports[model][rr-1][PriceCol])

							elif (prev_signal < 0):
								# we sold, subtract current price from previous
								pips += float(arrReports[model][rr-1][PriceCol]) - float(arrReports[model][rr][PriceCol])
							
							
						# save current signal	
						prev_signal = signal
	
				# print results
				#ts("* << FINISHED Thread " + str(system_id))
				sql += pFile + "," + str(system_id) + ", "+ str(fda_low) + ", " + str(fda_high) + ", " + str(maj_threshold) + ", " + str(pips))
				time.sleep(2)
				write2sqlite(sql)

# print something with timestamp
def ts(s):	
	now = datetime.datetime.now()
	print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)

			
	
if __name__ == "__main__":
	
	# that's how it's called by TS-Optimizer.py
	# cmd.put( "python ts-eval.py " + str(pFile) + " " + str(systems) + " " +  str(DataRows) + " " + str(fda_low) + " " + str(fda_high) + " " + str(maj))
	# example: python ts-eval.py 01k_10j2_1.pkl 777 10000 0.55 0.7 1
	
	pFile = sys.argv[1]
	pkl_file = open(pFile, 'rb')
	arrReports = pickle.load(pkl_file)
	pkl_file.close()

	system_id = int(sys.argv[2])
	rows = int(sys.argv[3])
	fda_low = float(sys.argv[4])
	fda_high = float(sys.argv[5])
	threshold = int(sys.argv[6])
	
	# now we need an option to generate report here
	'''
	if (int(sys.argv[7])==1):
		GenerateReport = True
	else
		GenerateReport = False
	'''
	
	# system_test(system_id, arrReports, rows, fda_low, fda_high, maj_threshold)
	system_test(system_id, arrReports, rows, fda_low, fda_high, threshold )
