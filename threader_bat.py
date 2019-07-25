from __future__ import print_function
from threading import Thread

import time
import datetime
import os, fnmatch
import psutil
import csv
import sys
import progressbar
import subprocess

from queue import LifoQueue
cmd = LifoQueue()

bat_with_commands = "main.bat"
progress = 0
bar = None

def run_prog():
	
	global progress
	global bar
		
	SW_MINIMIZE = 6
	info = subprocess.STARTUPINFO()
	info.dwFlags = subprocess.STARTF_USESHOWWINDOW
	info.wShowWindow = SW_MINIMIZE
	
	c = cmd.get()
	
	try:
		p = subprocess.Popen(c, startupinfo=info)
		now = datetime.datetime.now()
		print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " >> " + c + " - STARTED")
		p.communicate() # this makes it wait until finished
		progress += 1 # update the progress
		bar.update(progress) 
		print("* " + now.strftime("%Y-%m-%d %H:%M:%S") + " << " + c + " - FINISHED")
		
	except Exception as e:
		print("ERROR run_prog():", c)
		print(Exception)
		
# sometimes the prog goes crazy and starts loading too many processes
def under_control():
	prog1 = "Adaptive Modeler.exe"

	n_found = 0
	for process in psutil.process_iter():
		n = process.name()
		if (prog1 in n): n_found +=1

	if (n_found>99): # 100 is about max...
		return False
	else:
		return True
	
if __name__ == "__main__":


	with open(bat_with_commands, "r") as fileHandler:
		# Read each line in loop
		for line in fileHandler:
			if ("REM " not in line): cmd.put(line) # we don't execute comments


	bar = progressbar.ProgressBar(max_value=cmd.qsize(),widget_kwargs=dict(fill='█'))
	progress = 0
	bar.update(progress)
	
	n=50 
	while (n>0): #run initial 
		thread = Thread(target = run_prog)
		thread.start()
		time.sleep(1)
		n-=1
	
	time.sleep(20)
	
    # whil there is something in the stack and processor load <88%
	while (cmd.qsize()>0):

		util = psutil.cpu_percent()
		now = datetime.datetime.now()
		print("\n" + now.strftime("%Y-%m-%d %H:%M:%S") + ": CPU Utilization = " + str(util) + "% // tasks in stack: " + str(cmd.qsize()))
		
		# start new processes while CPU utilization is less than 89
		if util<98 and under_control()==True:
			thread = Thread(target = run_prog)
			thread.start()
			
		
		time.sleep(3) # then do checks once per minute
		
	
