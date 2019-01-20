from __future__ import print_function

import time
import datetime
import os, fnmatch
import psutil
import csv
import sys
import numpy as pynum
import pickle
from secrets import randbelow

arrReports=[]

def system_test():

	rows = len(arrReports)
	
	'''
	PnL Diff,		0
	Year,			1
	Month,			2	
	Day,			3	
	Weekday,		4
	Hour,			5
	WeekOfYear,		6
	WeekOfMonth,	7
	PnL_Is_Positive	8
	'''
	
	# majority voting threshold value
	#print(arrReports)
	raw_pnl = 0.0
	optimized_pnl=0.0
	for r in range(rows):

		
		#norm_pips += float(arrReports[r][0]) # pnl without optimization
		raw_pnl += float(arrReports[r][0])
		#days = [4,5,7,11,13,14,16,17,18,19,21,22,25,27,28] # sig-05k_10e2_2-hist.csv
		#days = [4,5,7,8,11,13,14,16,18,19,24,25]
		#weekdays = ["Monday", "Tuesday"]
		
		if (int(arrReports[r][6]) in [8,14,16,20,21,25,28,30,33,41,46]): 
			continue
		
		#print("counted: ",float(arrReports[r][0]))
		
		'''
		TODO Optimizations:
		
			by weekdays
			by weekdays + month
			by day+month
			by week of month
		
		'''
		
		#if ( (day==0 and (wk=="Tuesday" or wk=="Wednesday")) or (day==4 and wk=="Wednesday") or (day==8 and (wk=="Monday" or wk=="Friday")) or (day==12 and wk=="Monday") or (day==16 and (wk=="Monday" or wk=="Wednesday" or wk=="Thursday" or wk=="Friday")) or (day==29 and wk=="Friday") ): continue
		
		#if (mn==2 or mn==5 or mn==6 or mn==8): continue 
		
		#if (hs == 8): continue
		
		#if (hs==4 or hs==16): continue
		#sig-05k_10f1_4-hist.csv
		#before: 22.690999999999946 / optimized: 41.43799999999969
		
		'''
		todo:
			1) implement week of the month
			2) implement hash for complex rules
		'''
		
		''''
		weekday_day = {
			"Tuesday":1,
			"Friday":1,
			"Friday":2,
			"Monday":3,
			"Thursday":3,
			"Friday":3,
			"Monday":6,
			"Monday":7,
			"Wednesday":8,
			"Thursday":8,
			"Friday":8,
			"Thursday":9,
			"Monday":10,
			"Wednesday":10
		}
		for a, b in weekday_day.items():
			if (wk==a and day==b): 
				print (norm_pips, pips)
				continue
		'''
		
		
		#if (day in [3,8,12,20,24,27]): continue
		#sig-50k_77e2_8-hist.csv
		#before: 1.3108399999999911 / optimized: 1.462359999999994
		
		
		#month_filter = [1,6,8,9]
		#if (month in month_filter) : continue
		#sig-05k_10f1_4-hist.csv
		#before: 22.72600000000024 / optimized: 44.248000000000104
		
		
		#if (day in days): continue  # before: 42.673999999999666 / optimized: 75.95000000000003  |      sig-05k_10f1_4-hist.csv : before: 22.73600000000016 / optimized: 42.37100000000029
		
		#if ( (wk=="Monday" and month==1) or (wk=="Monday" and month==2) or (wk=="Monday" and month==6) or (wk=="Monday" and month==8) or (wk=="Tuesday" and month==1) or (wk=="Tuesday" and month==5) or (wk=="Tuesday" and month==6) or (wk=="Tuesday" and month==8) or (wk=="Tuesday" and month==11) or(wk=="Wednesday" and month==2) or (wk=="Wednesday" and month==5) or (wk=="Wednesday" and month==7) or (wk=="Wednesday" and month==8) or (wk=="Thursday" and month==3) or (wk=="Thursday" and month==9) or (wk=="Thursday" and month==10) ): continue # before: 42.673999999999666 / optimized: 71.93999999999997
		
		#if (wk in weekdays): continue
		
		#if (mn==2 or mn==5 or mn==6 or mn==8 or hs==16 or hs==20): continue 
		
		# 16,17,20,21
		#if (hs==1 or hs==2 or hs ==13 or hs ==14 or hs == 16 or hs ==17 or hs ==20 or hs ==21): continue # 52211
		
		
		#optimized pnl
		optimized_pnl += float(arrReports[r][0])
	
	print("before: " + str(raw_pnl) + " / optimized: " + str(optimized_pnl))

def readcsv(filename):	


	#PnL Diff,Month,Day,Weekday,Hour
	pnl=0
	m=1
	d=2
	w=3
	h=4

	
	ifile = open(filename, "rU")
	reader = csv.reader(ifile, delimiter=",")

	a = []
	r = 0
	prev_pnl =0.0
	for row in reader:
		if (r==0):
			r+=1
			continue
		
		'''
		PnL Diff,		0
		Year,			1
		Month,			2	
		Day,			3	
		Weekday,		4
		Hour,			5
		WeekOfYear,		6
		WeekOfMonth,	7
		PnL_Is_Positive	8
		'''

		#print(row[0], row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8])
		
		# construct this array: # 
		pnl_diff = float(row[0]) - prev_pnl
		a.append( [ pnl_diff, row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8] ] )
		prev_pnl = float(row[0])
    
	ifile.close()
	
	return a	
			
	
if __name__ == "__main__":
	

	
	print(sys.argv[1])
	arrReports = readcsv(sys.argv[1]) # input a Watson file here (no headers addumed)
	system_test()
