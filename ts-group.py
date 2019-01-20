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

PriceCol = 0
SignalCol = 1
FdaCol = 2
table =""
	
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
	
		sql = "INSERT INTO " + table + " (file, fda_min, fda_max, maj_threshold, fda_all_min, dd,  PnL) VALUES ('" + a[0] + "'," + a[1] + "," + a[2]+ "," +a [3] + "," + a[4] + "," + a[5] + ")"
		#print(sql)
		mycursor.execute(sql)
	
	mydb.commit()
	mydb.close()
	#print('wrote to mysql')


def system_test(arrReports, fda_low, fda_high):

	mmin = 1
	mmax = len(arrReports) 
	mstep = 1
	sql =""
	
	rows = len(arrReports[0])
	s = []
	
	# majority voting threshold value
	for maj_threshold in pynum.arange(mmin, mmax, mstep):
		pips = 0 # p&l in pips, start calculating from row 2
		for r in range(rows):
			# that's header
			if (r<2): # we know the future price only on the next row of a signal, so row 1 has no future price
				continue
			else:
				row_signal = 0 # signal across a row with all models
				dd = 0
				for model in arrReports:
					#print(model)
					#exit()
					if ((model[r][FdaCol])==""): model[r][FdaCol]=0
					if ( float(model[r][FdaCol]) >= fda_low and float(model[r][FdaCol]) <= fda_high): # make sure FDA is within needed range, then we count that signal
						row_signal += model[r][SignalCol]
			
				model = arrReports[0] # just take first model for calculations
				if ( abs(row_signal) >= maj_threshold): # it was a valid signal and we execute it
					if (row_signal > 0):
						pips += float(model[r][PriceCol]) - float(model[r-1][PriceCol])
					elif (row_signal < 0):
						pips += float(model[r-1][PriceCol]) - float(model[r][PriceCol])
						
				if (pips < 0 and pips < dd):
					dd = pips
	
		s.append ( pFile + "," + str(fda_low) + "," + str(fda_high) + "," + str(maj_threshold) + "," + str(pips) )
	
	write2mysql(s)

def write2sqlite(sql):
	con = sqlite3.connect(pFile+'.sqlite3')  
	print("writing to: " + pFile+'.sqlite3')
	cur = con.cursor() 
	cur.executescript(sql)
	con.commit()
	con.close()

# print something with timestamp
def ts(s):	
	now = datetime.datetime.now()
	print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)

			
	
if __name__ == "__main__":
	
	pFile = sys.argv[1]
	pkl_file = open(pFile, 'rb')
	arrReports = pickle.load(pkl_file)
	pkl_file.close()

	fda_low = float(sys.argv[2])
	fda_high = float(sys.argv[3])
	table = sys.argv[4]
	
	# system_test(system_id, arrReports, rows, fda_low, fda_high, maj_threshold)
	system_test(arrReports, fda_low, fda_high)
