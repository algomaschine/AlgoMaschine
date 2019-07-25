import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import os
import time

# tutorial https://colab.research.google.com/github/jakevdp/PythonDataScienceHandbook/blob/master/notebooks/04.00-Introduction-To-Matplotlib.ipynb#scrollTo=AuKBYgIn4Hmm

altreva_init_bars = 100 # we need to consider 100 bars for initialization
chunk_step = 500 # this is minimum chunk size in bars and also is the size of the step for sliding window
models_to_generate = 100
config_index_start = 1000

ratios = [0.9] # [0.7,0.8,0.9]

threader_bat = "threader_bat.py" # this bat will have {models_to_generate} lines to execute
main_bat = "all.bat"

model_config = "5k-bc1-ba1.acn"
model_style = "style-fda1.aps"
models_files = model_config + "_MODELS.csv" # this will be storing the seed

file = "USDCAD_240_1.csv" # this should actually be read from command line 
a = file.split("_")

short_file = a[0] +"_" + a[1]

filename_text_size = 35 # unify that for Altreva configs

def comb_chunk_ratio(min_bars, max_bars):
    pairs = []
    for b in range(min_bars, max_bars, chunk_step): # step is assumed to be min_bars
        for r in ratios:
            ##print(b,round(r,1)) # for some shitty reason need to round explicity...
            pairs.append([b,round(r,1)])

    return (pairs)
  

# chop-chop-chop into pieces
def calc_chunks(bmax, A, B):
  # we move left from the end by every B bars
  # but we take chunks of A+B bars
  n = 0
  #print("last_bar_index:",bmax)
  chunks = []
  while (bmax-n*B-A-altreva_init_bars >= B):
      start = bmax-n*B-A-B-altreva_init_bars
      end   = bmax-n*B
      ##print("..",n,start,end)
      chunks.append([start,end])
      n += 1
  return(chunks)
      
  
def write2bat(file, str):
    f = open(file,"a+")
    f.write(str)
    f.close()

if __name__ == "__main__":
  
  header = "Date,Time,Open,High,Low,Close\n"

  quotes = np.genfromtxt(file, delimiter=',', encoding="utf-8", dtype=str, skip_header=True,  usecols = (0,1,2,3,4,5))

  
  pairs = comb_chunk_ratio(3000,6000)
  print(pairs) # pairs of chunk
  #exit()
  
  bat = "main.bat"
  f_main_bat = open(bat,"w+") 
  
  bmax = len(quotes) # last bar
  for p in pairs: # step/ratio pairs
    size = p[0]
    ratio = p[1]
    #print("chunk size / ratio:",size, ratio)
    A = int (size*ratio)  # part A should have init bars
    B = int(size - A) 
    chunks = calc_chunks(bmax, A, B)
	  
    
	  
    n = len(chunks)
    for c in chunks:
        #print(c)
        start = c[0]
        end = c[1]

        f_chunk =  str(n)+"_"+str(start)+"-"+str(end) + "_" +  str(size) + "_" + str(ratio) + "_" + short_file  + ".csv"

        f = open(f_chunk,"w+")
        f.write(header)
        print(f_chunk)
        for line in range(start,end,1):
          l = ""
          for i in [0,1,2,3,4,5]:
            l += str(quotes[line,i])+","
          f.write(l[:-1]+"\n")
        #d = quotes[start:A+B+altreva_init_bars]
        ##print(d)
        f.close()

		
        r = 1000 # this will be the model's report index
        f_main_bat.write("REM -> START CHUNK: " + f_chunk + "\n")
        for line in range(1,models_to_generate+1,1): # add Altreva calling lines to the files
			# "C:\Program Files (x86)\Altreva\Adaptive Modeler\Bin\Adaptive Modeler.exe" /C:1 4_2003-4103_2000_0.7_USDCAD_1440_________.csv Standard.acn style.aps /RunToEnd /E:models.csv /r:104 /CloseAtEnd 
            r += 1 
            cmd = "\"C:\Program Files (x86)\Altreva\Adaptive Modeler\Bin\Adaptive Modeler.exe\" /C:1 " + f_chunk + " " + model_config + " " + model_style + " /RunToEnd " + models_files + " /r:" + str(r) + " /CloseAtEnd \n"
            f_main_bat.write(cmd)
		
        f_main_bat.write("REM <- END CHUNK: " + f_chunk + "\n")
        n -= 1
  
  f_main_bat.close()  


