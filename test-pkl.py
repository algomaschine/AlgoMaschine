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



	
# print something with timestamp
def ts(s):	
	now = datetime.datetime.now()
	print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)

			
	
if __name__ == "__main__":
	
	
	#
	
	pFile = sys.argv[1]
	#ts("::>> started " + pFile)
	pkl_file = open(pFile, 'rb')
	arrReports = pickle.load(pkl_file)
	pkl_file.close()
	
	print(arrReports)
	
	m =0
	for model in arrReports:
		print("model ", m)
		ri =0
		for r in model:
			print("model ", m, " row ", ri, " | ", r )
			ri +=1
		m+=1
	
	
	