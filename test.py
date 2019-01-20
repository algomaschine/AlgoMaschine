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

PriceCol = 0
SignalCol = 1
FdaCol = 2
	


def system_test(arrReports, rows, fda_low, fda_high):

	n =  0
	for model in range(len(arrReports)): # for each model separately calculate pnl pips
		n += 1
		pips = 0 # profit in pips
		for rr in range(rows):
			if (rr==0 or rr==1 or arrReports[model][rr-1][FdaCol]=="" ):
				continue
			else:
				if ( float(arrReports[model][rr-1][FdaCol]) >= fda_low and float(arrReports[model][rr-1][FdaCol]) <= fda_high):
					if ( arrReports[model][rr-1][SignalCol] == "Long"):
						pips += float(arrReports[model][rr][PriceCol]) - float(arrReports[model][rr-1][PriceCol]) 
					elif ( arrReports[model][rr-1][SignalCol] == "Short"):
						pips += float(arrReports[model][rr-1][PriceCol]) - float(arrReports[model][rr][PriceCol]) 

		# print results
		# ts("* << FINISHED Thread " + str(system_id))
		# ts(pFile + ", " + str(n) + ", " + str(fda_low) + ", " + str(fda_high) + ", " + str(pips))
		txt = pFile + "," + str(n) + "," + str(fda_low) + "," + str(fda_high) + "," + str(pips)
		write2sqlite(txt)
		
def write2sqlite(txt):
	con = sqlite3.connect('fda-tests.sqlite3')  
	cur = con.cursor() 
	customer_sql = "INSERT INTO `Test_1k` (LN) VALUES ('" + txt + "');"
	cur.execute(customer_sql)
	con.commit()
	con.close()

# print something with timestamp
def ts(s):	
	now = datetime.datetime.now()
	print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)

			
	
if __name__ == "__main__":
	
	write2sqlite("eddyed !")
	exit()
	
	# cmd.put( "python ts-monte.py " + str(pFile) + " " + str(DataRows) + " " + str(fda_low) + " " + str(fda_high) )

	# example: python ts-eval.py 01k_10j2_1.pkl 10000 0.55 0.7 
	
	pFile = sys.argv[1]
	pkl_file = open(pFile, 'rb')
	arrReports = pickle.load(pkl_file)
	pkl_file.close()

	rows = int(sys.argv[2])
	fda_low = float(sys.argv[3])
	fda_high = float(sys.argv[4])
	
	# now we need an option to generate report here
	'''
	if (int(sys.argv[7])==1):
		GenerateReport = True
	else
		GenerateReport = False
	'''
	
	# system_test(system_id, arrReports, rows, fda_low, fda_high, maj_threshold)
	system_test(arrReports, rows, fda_low, fda_high)
