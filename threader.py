from __future__ import print_function
from threading import Thread

import time
import datetime
import os, fnmatch
import psutil
import csv
import sys

from queue import LifoQueue
cmd = LifoQueue()



def run_prog():
	now = datetime.datetime.now()
	c = cmd.get()
	print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " >> " + c + " - STARTED")
	os.system(c)
	now = datetime.datetime.now()
	print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " << " + c + " - FINISHED")
	"""
	if (cmd.qsize()==0):
		print("THAT'S ALL, FOLKS!")
	
	need to save treads IDs and check it	
	"""
	

if __name__ == "__main__":

	#listOfFiles = os.listdir('.')  
	#pattern = "*TZ0dt-1min.csv"  
	#for entry in listOfFiles:  
	#	if fnmatch.fnmatch(entry, pattern):
			#newf = entry.replace("1min.csv","TZ0dt-1min.csv")
			#cmd.put("python conv_tz.py " + entry + " 0 > " + newf)
	#		cmd.put("python conv_tf.py " + entry)

	
	n_models_to_generate = sys.argv[1]
	models_prefix = int(sys.argv[2])
	models_bat = sys.argv[3]	
	
	i= int(n_models_to_generate)
	
	while (i>0):
		cmd.put(models_bat + " " +str(models_prefix+i))
		i-=1
		
		
	n=1 # correct that to 10 later
	time.sleep(15)
	while (n>0): #run initial 30
		thread = Thread(target = run_prog)
		thread.start()
		time.sleep(5)
		n-=1
	
	
    # whil there is something in the stack and processor load <88%
	while (cmd.qsize()>0):
		time.sleep(5)
		t = 5
		util = 0
		while (t>0): # we do it a few times and calc avg to be sure
			util += psutil.cpu_percent()
			time.sleep(t)
			t-=1
		util /= 5
		
		now = datetime.datetime.now()
		print(now.strftime("%Y-%m-%d %H:%M:%S") + ": CPU Utilization = " + str(util) + "% // tasks in stack: " + str(cmd.qsize()))

		# start new processes while CPU utilization is less than 89
		if util<90:
			thread = Thread(target = run_prog)
			thread.start()
			
		time.sleep(10) # then do checks once per minute

		
	print("Done execution, waiting for all threads to continue...")
	
