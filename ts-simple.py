from __future__ import print_function

import time
import datetime
import os, fnmatch
import psutil
import csv
import sys
import numpy as pynum
import pickle
#import sqlite3  
import mysql.connector

from secrets import randbelow


pFile = ""
table = ""

PriceCol = 0
SignalCol = 1
FdaCol = 2
FdaAll = 3	
	
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
	
		sql = "INSERT INTO " + table + " (file, fda_min, maj_threshold, fda_all_min, dd,  PnL) VALUES ('" + a[0] + "'," + a[1] + "," + a[2]+ "," +a [3] + "," + a[4] + "," + a[5] + ")"
		#print(sql)
		mycursor.execute(sql)
	
	mydb.commit()
	mydb.close()
	#print('wrote to mysql')

	

def system_test(arrReports):

	mmin = 1
	mmax = len(arrReports) # this gets us the amount of models in total
	mstep = 1
	sql =""
	
	rows = len(arrReports[0])
	s = []
	
	
	# fda_low should be put inside this cycle to avoid an extra I/O operation
	
	low = 0.54
	up = 0.75
	fda_step = 0.01
	for fda_low in pynum.arange(low, up, fda_step):	
		for fda_all_t in pynum.arange(0.5, 0.53, 0.005):
			# majority voting threshold value
			for maj_threshold in pynum.arange(mmin, mmax, mstep):
				try:
					pips = 0 # p&l in pips, start calculating from row 2
					dd = 0 # TODO: add drawdown calculations
					for r in range(rows-1): # because we can't calculate PnL of the last row (we don't know values of the next one!)
						# that's header
						if (r<2): # we know the future price only on the next row of a signal, so row 1 has no future price
							continue
						else:
							row_signal = 0 # signal across a row with all models
							m = 1
							for model in arrReports:	
								if ((model[r][FdaCol])==""): model[r][FdaCol]=0 # some of the initial values might have spaces
								if ( float(model[r][FdaCol]) >= fda_low and float(model[r][FdaAll]) >= fda_all_t): # make sure FDA is within needed range, then we count that signal
									row_signal += model[r][SignalCol]
								m+=1
			
							model = arrReports[0] # just take first model for calculations
							if ( abs(row_signal) >= maj_threshold): # it was a valid signal and we execute it
								if (row_signal > 0):
									pips += float(model[r+1][PriceCol]) - float(model[r][PriceCol])
								elif (row_signal < 0):
									pips += float(model[r][PriceCol]) - float(model[r+1][PriceCol])
									
							if (pips < 0 and pips < dd):
								dd = pips
	
					s.append ( pFile + "," + str(fda_low) + "," + str(maj_threshold) + "," + str(fda_all_t)  + ","+ str(dd) + "," + str(pips) )
						
				except:	
					print("Unexpected error:", sys.exc_info()[0])
					print("file: ", pFile)
					print("row: ", r)
					print("model: ", m)
					print("model[r][FdaCol])=", model[r][FdaCol], "/")
					raise
		
	write2mysql(s)

# print something with timestamp
def ts(s):	
	now = datetime.datetime.now()
	print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)

			
	
if __name__ == "__main__":
	
	pFile = sys.argv[1]
	ts("::>> started " + pFile)
	pkl_file = open(pFile, 'rb')
	arrReports = pickle.load(pkl_file)
	pkl_file.close()
	
	table = sys.argv[2]
	
	system_test(arrReports)
	ts("<<:: finished " + pFile)
