from __future__ import print_function

import time


import psutil
import csv
import sys
import numpy as pynum
import pickle

import mysql.connector

from secrets import randbelow


from threading import Thread


#from datetime import datetime
from datetime import datetime, timedelta
import os, fnmatch


import calendar
	


def readcsv(filename):	

	f = open("watson-"+filename, 'w')

	#PnL Diff,Month,Day,Weekday,Hour
	pnl=0
	m=1
	d=2
	w=3
	h=4
	
	ifile = open(filename, "rU")
	reader = csv.reader(ifile, delimiter=",")

	f.write("PnL Diff,Year,Month,Day,Weekday,Hour,WeekOfYear,WeekOfMonth,PnL_Is_Positive\n")
	
	r = 0
	prev_pnl =0.0
	for row in reader:
		if (r<2):
			if (r>0): prev_pnl = float(row[4])
			r+=1
			continue
		'''
			Bar			0
			Date		1
			Price		2
			Signal		3
			PnL			4
			Drawdown	5	
			FDA			6
			FDA Alltime	7
			Year		8
			Month		9
			Day			10
			Weekday		11
			Hour		12	

		'''
		# construct this array: "PnL Diff,Year,Month,Day,Weekday,Hour,WeekOfYear, WeekOfMonth, PnL_Is_Positive\n"
		
		
		
		dt = datetime.strptime(row[1], "%m/%d/%Y")
		week_of_year = dt.isocalendar()[1]
		week_of_month = get_week_of_month( dt )
		
		pnl_diff = float(row[4]) - prev_pnl
		sex = str(pnl_diff) +","+ str(row[8]) +","+ str(row[9])  +","+ str(row[10]) +","+ str(row[11]) +","+ str(row[12])+","+ str(week_of_year)+ "," + str(week_of_month) +"," + str(pnl_diff>0)+"\n" 
		#print(sex)
		f.write(sex)
		prev_pnl = float(row[4])
    
	ifile.close()
	f.close()
	
	
def get_week_of_month(date):
	month = date.month
	week = 0
	while date.month == month:
		week += 1
		date -= timedelta(days=7)
		
	return week
	
if __name__ == "__main__":
	
	
	readcsv(sys.argv[1])
	
