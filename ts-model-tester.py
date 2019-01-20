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
	
def write2mysql(ss):
	mydb = mysql.connector.connect(
		host="localhost",
		user="root",
		passwd="xsjhk%123SD==-Ghk11",
		database="altreva"
	)

	mycursor = mydb.cursor()
	for s in ss:
		sql = "INSERT INTO altreva.testgroup_samples (fn) VALUES ('" + s + "')"	
		mycursor.execute(sql)
	
	mydb.commit()
	mydb.close()
	#print('wrote to mysql')


def system_test(arrReports, fda_low, fda_high):

	mmin = 1
	mmax = 95
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
	
		if (pips != 0):
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
	
	pFile = sys.argv[1] # regex for model files
	fda_low = float(sys.argv[2])
	fda_high = float(sys.argv[3])
	maj = int(sys.argv[4])
	
	# 1. it will calculate collective model's PnL, DD and output it to a file model-pFile-fda_low-fda_high-maj.csv in the format: Weekdays, Trades, TotalPnL, DD, Average Profit per trading day
	# 2. it will generate a signal file to be used in MT4 to test SL/TP values, lot, and other risk-related stuff.
	# system_test(system_id, arrReports, rows, fda_low, fda_high, maj_threshold)
	system_test(pFile, fda_low, fda_high, risk)
