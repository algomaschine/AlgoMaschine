from multiprocessing import Pool

import time
import timeit
from datetime import datetime, timedelta
import os, fnmatch
import psutil
import csv
import numpy as np
import itertools
import sys
import mysql.connector
import pandas as pd
import pymysql

from backtesting import Strategy
from backtesting import Backtest
from scipy.stats import linregress

import warnings
from statistics import mean
from matplotlib.pylab import plt #load plot library


table = ""
postfix = "*export1.csv" # that's added to regex to search for Altreva report files

# max amount of records to write into the database (saves time)
MaxResultRows2Database = 100
MaxPoolForModelsGroup = 15 # from how many models shall we take the pool of group selection

# fixed columns in the database
f_date=""
l_date=""

# columns in file
fBar_col = 0
fDate_col = 1
fTime_col = 2
fPrice_col = 3
fSignal_col = 5
fFDA_col = 15 # (Trailing 1 bars; Range applied)
fFDS_col = 16 # (Trailing 100 bars; Range applied)

# this will hold [Open,High,Low,Close,Buy,Sell] values for every row of the quotes
data_ohlcbs = None
data_ohlcbs_a = None
data_ohlcbs_b = None
A = None
B = None
AB = None
v_separator = 0

# will need those to make performance comparison in the database
slope_a = 0
slope_b = 0

# for the filename of the chart
chart_title = ""
chart_filename = ""


# columns in OHLC HISTORY
_d=0
_t=1
_o=2
_h=3
_l=4
_c=5
_b=6
_s=7
CHUNK_A = 0
CHUNK_B = 1


# THIS BELOW IS NOT THERE FOR CURRENT CONFIGS!
fFD_AUC_col = 17 # (Trailing 1 bars; Range applied)
fFDA_All = 21

# columns in numpy array
aDate_col = 0
aTime_col = 1
aPrice_col = 2
aSignal_col = 3

# model's parameters
iPeriods = range(5,10,1) 	# period values for FDA & FA_AUC calculations
FDA_threshold = 0.7
group_sizes = [1,2,3,4,5,6,7,8,9]


# will need below data to fill MySQL table
# integer bar number
start_a = 0
end_a = 0
start_b = 0
end_b = 0

# numpy array
price_a = 0
price_b = 0

# double - benchmark values for PnL (if a trade is better than longterm buy/sell strategy and best trade taken between all time chart extrenuma
buy_hold_a= 0
max_min_a = 0
buy_hold_b= 0
max_min_b= 0

# dates
dt_a = 0
dt_b = 0

start_a_date= 0
end_a_date= 0
start_b_date= 0
end_b_date= 0

# actual trading days to compute Performance (avg daily PnL) and Turnover (lots) per period of given days
trading_days_a = 0
trading_days_b = 0


def identity(values):
	"""
	fubction is used in strategy to calculate signals
	"""
	return pd.Series(values)


class MyStrategy(Strategy):

	def init(self):
		self.signals = self.I(identity, self.data.Signals)
		self.files_period = self.data.files_period[0]

	def next(self):
		signal = self.signals[-1]
		if signal > 0:
			# Let the strategy close any current position and use all available funds to buy the asset
			self.buy()
		elif signal < 0:
			# Let the strategy close any current position and use all available funds to short sell the asset
			self.sell()
		else:
			assert signal == 0
			self.position.close()



def check_table(table): # will create table or exit the software is table exists

	# todo: change logic
	# if does not exit, then create it, otherwise leave it

	mydb = mysql.connector.connect(
		host="localhost",
		user="root",
		passwd="xsjhk%123SD==-Ghk11",
		database="altreva"
	)

	cursor = mydb.cursor()

	stmt = "SHOW TABLES LIKE '" + table + "'"
	cursor.execute(stmt)
	result = cursor.fetchone()
	if result: return # there is a table named "tableName"

    # there are no tables named "tableName"

	# this should create the table OR exit with error
	_SQL = "CREATE TABLE `" + table + "` ("
	_SQL += "`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"

	_SQL += "`chunk` INT NULL,"
	_SQL += "`hist_bars` VARCHAR(45) NULL,"
	_SQL += "`length` INT NULL,"
	_SQL += "`ratio` FLOAT NULL,"
	_SQL += "`period_fda` INT NULL,"

	_SQL += "`pnl_a` DOUBLE NULL,"
	_SQL += "`dd_a` DOUBLE NULL,"
	_SQL += "`buy_hold_a` DOUBLE NULL,"
	_SQL += "`max_min_a` DOUBLE NULL,"

	_SQL += "`pnl_b` DOUBLE NULL,"
	_SQL += "`dd_b` DOUBLE NULL,"
	_SQL += "`buy_hold_b` DOUBLE NULL,"
	_SQL += "`max_min_b` DOUBLE NULL,"

	_SQL += "`trading_days_a` INT NULL,"
	_SQL += "`trading_days_b` INT NULL,"

	_SQL += "`trades_a` INT NULL,"
	_SQL += "`trades_b` INT NULL,"

	# this should be calculated automatically by an expression like "field_name double GENERATED ALWAYS AS (pnl_a/trading_days)"
	_SQL += "`avg_d_pnl_a` DOUBLE GENERATED ALWAYS AS (pnl_a/trading_days_a),"
	_SQL += "`avg_d_dd_a` DOUBLE GENERATED ALWAYS AS (dd_a/trading_days_a),"
	_SQL += "`avg_d_pnl_b` DOUBLE GENERATED ALWAYS AS (pnl_b/trading_days_b),"
	_SQL += "`avg_d_dd_b` DOUBLE GENERATED ALWAYS AS (dd_b/trading_days_b),"

	_SQL += "`avg_d_vol_a` DOUBLE GENERATED ALWAYS AS (trades_a/trading_days_a),"
	_SQL += "`avg_d_vol_b` DOUBLE GENERATED ALWAYS AS (trades_b/trading_days_b),"

	# log dates as well
	_SQL += "`start_a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
	_SQL += "`end_a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
	_SQL += "`start_b` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
	_SQL += "`end_b` TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"

	cursor.execute(_SQL)
	ts("Created table " + table)

def write2mysql(vofv):

	mydb = mysql.connector.connect(
		host="localhost",
		user="root",
		passwd="xsjhk%123SD==-Ghk11",
		database="altreva"
	)

	mycursor = mydb.cursor()

	'''
use altreva;
CREATE TABLE `1K_take_1_usdcad_1440` (

/* 0 */    `ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
/* 1 */    `chunk` int(11) DEFAULT NULL,
/* 2 */    `hist_bars` varchar(45) DEFAULT NULL,
/* 3 */    `length` int(11) DEFAULT NULL,
/* 4 */    `ratio` float DEFAULT NULL,
/* 5 */    `period_fda` int(11) DEFAULT NULL,
/* 6 */    `pnl_a` double DEFAULT NULL,
/* 7 */    `dd_a` double DEFAULT NULL,
/* 8 */    `buy_hold_a` double DEFAULT NULL,
/* 9 */    `max_min_a` double DEFAULT NULL,
/* 10 */    `pnl_b` double DEFAULT NULL,
/* 11 */    `dd_b` double DEFAULT NULL,
/* 12 */    `buy_hold_b` double DEFAULT NULL,
/* 13 */    `max_min_b` double DEFAULT NULL,
/* 14 */    `trading_days_a` int(11) DEFAULT NULL,
/* 15 */    `trading_days_b` int(11) DEFAULT NULL,
/* 16 */    `trades_a` int(11) DEFAULT NULL,
/* 17 */    `trades_b` int(11) DEFAULT NULL,
/* 18 */    `avg_d_pnl_a` double GENERATED ALWAYS AS ((`pnl_a` / `trading_days_a`)) VIRTUAL,
/* 19 */    `avg_d_dd_a` double GENERATED ALWAYS AS ((`dd_a` / `trading_days_a`)) VIRTUAL,
/* 20 */    `avg_d_pnl_b` double GENERATED ALWAYS AS ((`pnl_b` / `trading_days_b`)) VIRTUAL,
/* 21 */    `avg_d_dd_b` double GENERATED ALWAYS AS ((`dd_b` / `trading_days_b`)) VIRTUAL,
/* 22 */    `avg_d_vol_a` double GENERATED ALWAYS AS ((`trades_a` / `trading_days_a`)) VIRTUAL,
/* 23 */    `avg_d_vol_b` double GENERATED ALWAYS AS ((`trades_b` / `trading_days_b`)) VIRTUAL,
/* 24 */    `start_a` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
/* 25 */    `end_a` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
/* 26 */    `start_b` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
/* 27 */    `end_b` timestamp NULL DEFAULT CURRENT_TIMESTAMP

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

	'''

	print(vofv)

    #                                      0             1         2                3                  4                          5                       6
	#return [ str(group_str.count("/"))+" "+group_str, res["SQN"], res["# Trades"], res["Return [%]"], res["Max. Drawdown [%]"], res["Equity Final [$]"], res["Equity Peak [$]"] ]
	group_str = vofv[0]

	#### TODO: fill these
	chunk = ""
	ratio = 0
	bars = 0
	####

	sqn = vofv[1]
	trades = vofv[2]
	return_percent = vofv[3]
	max_dd = vofv[4]
	equity = vofv[5]
	equity_peak = vofv[6]


	'''
	first = True
	for index, row in top.iterrows():
		sql =""
		files_periods = row["files_periods"]
		trades = row["trades"]
		pnl = row["pnl"]
		dd = row["dd"]

		if (first):
			gen_signals_file(files_periods)
			first=False

		sql += "INSERT INTO " + table + " (files_periods, trades, pnl, dd, openclose_range, max_range,trading_days, first_date, last_date) VALUES ("
		sql += repr(files_periods) + "," + str(trades) + "," + str(pnl) + "," + str(dd) + "," + str(open_close_range) + "," + str(max_range) + "," + str(trading_days) + ","
		sql += repr(f_date) + "," + repr(l_date)
		sql += ");"
		#print(sql)
		mycursor.execute(sql)
	mydb.commit()
	mydb.close()
	'''


# we put 1 if avg is more than a threshold, so we can multiply by this number later
def fda_avg(fda,period):
	rslt = np.zeros(len(fda), dtype=float, order='C')
	for r in range(len(fda)):
		if (r+1-period >= 0):
			rslt[r] = 1 if fda[r+1-period:r+1].mean() >= FDA_threshold else 0

	return rslt

# File I/O function for multiprocessing
def files2sig_fda(filename):
	sig = np.genfromtxt(filename, delimiter=',', encoding="utf-8", dtype=str, skip_header=True, usecols = (fSignal_col))
	fda = np.genfromtxt(filename, delimiter=',', encoding="utf-8", dtype=float, skip_header=True, usecols = (fFDA_col))
	return([filename, sig, fda])


def digitize(sig_fda, period=-1): # used in miltiprocessing mode

	# TODO: the below should be taken out for I/O not to slow down multiprocessing
	#sig = np.genfromtxt(filename, delimiter=',', encoding="utf-8", dtype=str, skip_header=True, usecols = (fSignal_col))
	#fda = np.genfromtxt(filename, delimiter=',', encoding="utf-8", dtype=float, skip_header=True, usecols = (fFDA_col))

	filename = sig_fda[0]
	sig 	 = sig_fda[1]
	fda 	 = sig_fda[2]

	for r in range(0,len(sig),1):
		if (r==0 and sig[r]==''): # if the first row is empy, we don't know what signal was there, so we put 0 aka Cash
			sig[r]=0
		else:
			if (sig[r]==''):
				sig[r]=sig[r-1] # set to previous signal
			elif (sig[r]=='Cash'):
				sig[r]=0
			elif (sig[r]=='Long'):
				sig[r]=1
			elif (sig[r]=='Short'):
				sig[r]=-1

	sig = sig.astype(float)	# origianl signals

	v=[] # as many columns as we have periods, each contains signals filtered by FDA of the corresponding period
	if (period==-1): # do all periods
		for p in iPeriods:
			v.append ( [filename, p, np.multiply(sig, fda_avg(fda,p))] ) # scalar multiplication of signal and {1,0} based on fda
	else: # use specific period
		v.append ( [filename, period, np.multiply(sig, fda_avg(fda,p))] )

	return(v)

def drawdowns(equity_curve):
	i = np.argmax(np.maximum.accumulate(equity_curve) - equity_curve) # end of the period
	j = np.argmax(equity_curve[:i]) # start of period
	drawdown=abs(100.0*(equity_curve[i]-equity_curve[j]))

	return drawdown


# pnl_classic( [ filename, period, chunk_section, signals, calc_dd ]  )


#
def pnl_classic(model_infos):

	BUY_COL = 4
	SELL_COL = 5

	filename		= model_infos[0]
	period			= model_infos[1]
	ohlcbs 			= model_infos[2]
	signals 		= model_infos[3]
	calc_dd 		= model_infos[4]
	v_separator		= model_infos[5]

	#print(len(ohlcbs),len(signals))
	#exit()
	#print(ohlcbs)

	trades = 0
	pnl = 0
	max_dd_percent = 0

	dt_pnl = [0] # first element would be 0
	pnl_a  = 0
	pnl_b  = 0

	for r in range(1,len(ohlcbs),1): # signal on line N has to be traded on line N+1, thus no PnL on line 0
		signal = signals[r-1] # TODO: Are the signals themselves rigth? Check it out at some point!
		dpips  = 0 # It's really importtant to get this one nulled for rows that have 0 signal
		if ( signal != 0): # it was a valid signal and we execute it
			trades += 1
			if (signal>0):
				dpips = ohlcbs[r][BUY_COL]
			else:
				dpips = ohlcbs[r][SELL_COL]
			pnl += dpips # dpips was nulled, so it's OK to add it

		# DEBUG - TEST THIS NEXT TODO!
		#if (filename=="54_16055-19155_3000_0.9_USDCAD_240 1075_export1.csv" and period ==8):
		#	# maybe add DATE/time for the signals and prices columns to make sure we can see precisely the right rows
		#	print("DEBUG:", ohlcbs[r][0],ohlcbs[r][1],ohlcbs[r][2],ohlcbs[r][3], ohlcbs[r][4], ohlcbs[r][5], signals[r], dpips, pnl )

		dt_pnl.append(pnl) # this dataseries will be used for Drawdown calculation and also to return the PnL chart

	# dt_pnl[v_separator]  is the one we gonna be sorting the Pnl on A, an thus it will take less time
	pnl_a = dt_pnl[v_separator]
	pnl_b = pnl - pnl_a

	if (pnl_a>0): # we return everything because we need it...
		if (calc_dd == True): # do it here to save resources
			max_dd_percent = drawdowns(np.array(dt_pnl))
		else:
			max_dd_percent = 0 # initially we don't use that fot the sake of CPU resources

		#print(filename, period, trades, pnl, max_dd_percent)
		label = filename
		if period !=0:
			label += ":" + str(period)

		label += ",pnl_B=" + f'{pnl_b:.6f}'

		pnl_adjusted = []
		# adjust by the first close of the data series to make the Price and PnL curves start from the same value
		for n in dt_pnl: pnl_adjusted.append(n+ohlcbs[0][3])


		return [ filename, period, trades, pnl, pnl_a, pnl_b, max_dd_percent, label, signals, pnl_adjusted ]
	else:
		return None


def save_chart(data): #,  best_ga, label_ga): #, best_b,   label_b, \
#					best_gb, label_gb, best_ab, label_ab, best_gab, label_gab):

	# TODO:
	# 1. pips_per_week for each series, such that in differentiates between the timeframe in the report file, saving it somewhere in global constant
	# 2. put dates on the axis

	curves = []
	chart_close,	=	plt.plot(AB, label = "Price Close")
	curves.append(chart_close)

	for d in data:
		curve,	=	plt.plot(d[0], label = d[1])
		curves.append(curve)

	plt.axvline(x=v_separator)
	plt.legend(curves,loc='upper center',bbox_to_anchor=(0.5, -0.05), fancybox=True, shadow=True, ncol=2)
	#plt.legend(curves,loc='center left', bbox_to_anchor=(1, 0.5))
	plt.title(label=chart_title, fontdict=None, loc='center')

	try:
		os.mkdir(chart_filename.split("/")[0])
	except Exception:
		print("")

	fig = plt.gcf()
	fig.set_size_inches(17,10)
	plt.savefig(chart_filename)

# this will return the signals array defined by report file name m and perid p
#def get_specific_model(models,m,p):


# do combinatorics and PnL calculations
def process_reports(ReportNames): # we probably should pass separately the arrays here.
	# returns numpy arrays with digized signals adjusted by FDA, each column corresponds to an FDA value from iPeriods
	ts("building digitized vectors of signals adjusted by FDA values")
	# step 1: multiprocess-read all and return the vector of files
	with Pool() as p: sig_fda = p.map(files2sig_fda,ReportNames)
	# step 2: multiprocess-digitize the signals
	with Pool() as p: sig_fda_adjusted_grouped = p.map(digitize, sig_fda) # p.map return data in consequitive order, this was tested

	# IDEA: just select PROTITABLE & LEAST CORRELATED

	# sig_fda_adjusted_grouped contains arrays of arrays, so we need to make them arrays only
	sig_fda_adjusted = []
	for sfa in sig_fda_adjusted_grouped:
		for by_each_period in sfa: # there are arrays corresponding to various FDA periods applied on every single model
			sig_fda_adjusted.append(by_each_period)

	mp_a = [] # mp stands for Model:Period
	mp_b = []
	mp_ab = []

	# sig_fda_adjusted contains [model_name, period, [array of signals] ]
	for r in sig_fda_adjusted:
		filename = str(int(((r[0]).split(" ")[1]).replace("_export1.csv",""))-1000)
		period = r[1]
		''' doing like before
		data_ohlcbs_a = data_ohlcbs[start_a:end_a]
		data_ohlcbs_b = data_ohlcbs[start_b:end_b]
		'''
		signals_all = r[2]
		#signals_a = signals_all[start_a:end_a]
		#signals_b = signals_all[start_b:end_b]
		# TODO: fix the below for this very format: pnl_classic( [ filename, period, chunk_section, signals, calc_dd ]  )
		#mp_a.append(  [ filename, period, data_ohlcbs_a, signals_a, True] ) # note: price_a is used for the data
		#mp_b.append(  [ filename, period, data_ohlcbs_b, signals_b, True] )  # note: price_a is used for the data
		mp_ab.append( [ filename, period, data_ohlcbs,  signals_all, True, v_separator] )  # note: price_a is used for the data

	# calculate PnL
	#with Pool() as p: results_positive_pnl_a =  [l for l in p.map(pnl_classic, mp_a)  if l is not None]
	#with Pool() as p: results_positive_pnl_b =  [l for l in p.map(pnl_classic, mp_b)  if l is not None]
	with Pool() as p: results_pnl_ab = [l for l in p.map(pnl_classic, mp_ab) if l is not None]

	# this will show all the dataframe completely
	pd.set_option('display.max_columns', None)
	# to prevent splitting to the next rows
	pd.set_option('expand_frame_repr', False)

	#print(results_positive_pnl_a)
	#print(results_positive_pnl_b)
	#print(results_positive_pnl_ab)

	# headers: pnl_classic returns [ return [ filename, period, trades, pnl, pnl_a, pnl_b, max_dd_percent, label, signals <- array, dt_pnl <- array ]
	pnl_classic_cols 	   = ["Filename","Period","# Trades","PnL","PnL A", "PnL B", "Max. Drawdown","Label","Signals List","PnL List"]
	cols_without_sig_array = ["Filename","Period","# Trades","PnL","PnL A", "PnL B", "Max. Drawdown","Label"]

	df_ab = pd.DataFrame(results_pnl_ab)
	# add columns
	df_ab.columns = pnl_classic_cols



	# show resuls
	# lbl_without_array
##	print("TOP models with positive pnl_classic generated for A:",len(df_a))
##	print(top_a[cols_without_sig_array]) # f1 = df[['a','b']]
##	print("TOP models with positive pnl_classic generated for B:",len(df_b))
##	print(top_b[cols_without_sig_array])
##	print("TOP models with positive pnl_classic generated for AB:",len(df_ab))
##	print(top_ab[cols_without_sig_array])

	#print("top length",len(top))

	chartz = []
	#for index, row in top_ab.iterrows():
	#	chartz.append([row[col_pnl_list], row['Label']])

	# this will keep MaxResultRows2Database sorted desc by pnl (from 1 to avoid column names)
	top_a =  df_ab.sort_values(by=["PnL A"],ascending=False)[:MaxPoolForModelsGroup]
	#print("TOP models with positive pnl_classic generated for A")
	#print(top_a[cols_without_sig_array])


	# construct the groups of best on A
	models = range(0,len(top_a),1)
	best_groups_a = [] # array of comninations of models
	for n in group_sizes:
		comb = list(itertools.combinations( models, n))
		for c in comb:
			best_groups_a.append(c) # adding combination to the list of groups

	#print(best_groups_a)
	ts("Creating model groups permutations")
	mg = [] # models of groups
	for g in best_groups_a: # for group in groups
		#print(":: group",g)
		group_str = "" # string with the list of all modls
		group_sig = np.zeros(len(data_ohlcbs), dtype=float, order='C') #  CHECK!!! is the size correct? YES - we just all ALL signals
		for m_index in g:
			# m is index of model inside models
			model = top_a.iloc[m_index]
			f = model['Filename']
			#f = (f.split(" ")[1]).replace("_export1.csv","")
			p = str(model['Period'])
			s = model['Signals List']
			group_str += f + ":" + p + "/" # just a long string with list of model:period separated by '/' character
			group_sig = np.add(group_sig, s)

		#        [ filename, period,  data_ohlcbs,  signals_all, True, v_separator]
		mg.append( [ group_str[:-1],0, data_ohlcbs, group_sig, True, v_separator] ) # note: price_a is used for the data

	ts("Calculating PnL of each group")
	# execute pnl for each group in multiprocessing mode
	with Pool() as p: profitable_groups_a = [l for l in p.map(pnl_classic, mg) if l is not None]

	best_ga = pd.DataFrame(profitable_groups_a)
	##print("PART A: Total groups with positive Return [%] generated:",len(profitable_groups))
	best_ga.columns = pnl_classic_cols

	top_ga =  best_ga.sort_values(by=["PnL A"],ascending=False)[:MaxPoolForModelsGroup]
	print("TOP GROUP models with positive pnl_classic generated for A")
	#print(top_b[cols_without_sig_array])
	for index, row in top_ga[:1].iterrows(): # TODO: maybe we need MORE
		chartz.append([row['PnL List'], "best group A(" + str(row['Label'].count(":")) + "): " + row['Label']])

	top_b =  df_ab.sort_values(by=["PnL B"],ascending=False)[:MaxPoolForModelsGroup]
	print("TOP models with positive pnl_classic generated for B")
	print(top_b[cols_without_sig_array])
	for index, row in top_b[:1].iterrows():
		chartz.append([row['PnL List'], "best B: " + row['Label']])

	top_ab = df_ab.sort_values(by=["PnL"],ascending=False)[:MaxPoolForModelsGroup]
	print("TOP models with positive pnl_classic generated for AB")
	print(top_ab[cols_without_sig_array])
	for index, row in top_ab[:1].iterrows():
		chartz.append([row['PnL List'], "best AB: " + row['Label']])

	save_chart(chartz)




# print something with timestamp
def ts(s):
	now = datetime.now()
	print(now.strftime("%Y-%m-%d %H:%M:%S") + " // " + s)



if __name__ == "__main__":

	warnings.filterwarnings("ignore") # backtester rises warnings, we don't need them

	# python ts-combinator.py 1K_take_1 8_3253-4853_1500_0.7_USDCAD_1440 history_file_that_has_OHLC.csv
	start = time.time() # timing
	#print(sys.argv, len(sys.argv))
	if (len(sys.argv)!=4):
		ts("ERR: wrong number of input parmeters!")
		exit()

	#ts("________________________started________________________")
	tblPrefix = sys.argv[1]
	regex = sys.argv[2]
	DTOHLC_hist = sys.argv[3]

	                       # 0       1        2      3     4        5
	arr = regex.split("_") # 8 _ 3253-4853 _ 1500 _ 0.7 _ USDCAD _ 1440
	chunk = arr[0]
	history_bars = arr[1]
	length = int(arr[2])
	ratio = float(arr[3])
	tblCoreName = arr[4] + "_" + arr[5]
	table = tblPrefix + "_" + tblCoreName

	# TODO remove comment from the previous line for live version
	#check_table(table) # will create the table or stop executing if table exists


	ReportNames = [] # this will contain lists for every report, in-memory numpy
	listOfFiles = os.listdir('.')
	# build a list of report files
	for report in listOfFiles:
		if fnmatch.fnmatch(report, regex+postfix ):
			ReportNames.append(report)



	#debugging
	#print(len(ReportNames), ReportNames)
	#exit()

	try:
		# we assume that all the reports are of equal length
		# date,time array
		dt = np.genfromtxt(ReportNames[0], delimiter=',', encoding="utf-8", dtype=str, skip_header=True, usecols = (fDate_col, fTime_col))
		# date array
		datez = np.genfromtxt(ReportNames[0], delimiter=',', encoding="utf-8", dtype=str, skip_header=True, usecols = (fDate_col))
		timez = np.genfromtxt(ReportNames[0], delimiter=',', encoding="utf-8", dtype=str, skip_header=True, usecols = (fTime_col))
		# TODO: REMOVE, replace the uses for price_ohlc_bs
		# price array - WHERE ARE WE USING IT?
		price = np.genfromtxt(ReportNames[0], delimiter=',', encoding="utf-8", dtype=float, skip_header=True, usecols = (fPrice_col))
		# we need those for TP/SL and other things...
		price_ohlc = np.genfromtxt(DTOHLC_hist, delimiter=',', encoding="utf-8", dtype=float, skip_header=True, usecols = (_o,_h,_l,_c))
	except (OSError):
		print(OSError, "maybe this OHLC files is not found",DTOHLC_hist)
		exit()

	#print(price_ohlc)
	#print(price_ohlc[0][1])
	#exit()

	# pre-calculate the results of buy&sell actions to save speed on multiprocessing later
	OHLC_pnl = []
	line_to_pass = 100 # intial .csv quote file contains 100 more bars, which are not in the model's report,
	# 0 PnL on the first row, because it's the signal for the next row because Altreva uses them for "warming up"
	for r in range(line_to_pass,len(price_ohlc),1): # the signal is always for the next line, so we start from relative line 1, not line 0
		# calculate          OPEN                 HIGH             LOW               CLOSE             BUY = Close-prev_Close         /       SELL = prevClose-Close
		OHLC_pnl.append( [ price_ohlc[r][0], price_ohlc[r][1], price_ohlc[r][2], price_ohlc[r][3], price_ohlc[r][3]-price_ohlc[r-1][3] , price_ohlc[r-1][3]-price_ohlc[r][3] ] )

	data_ohlcbs = np.array(OHLC_pnl)
	# DEBUG - OK GOOD
	np.set_printoptions(precision=5,suppress=True) # this is to print numpy floats without e-notation
	#for r in range(0,len(data_ohlcbs),1):
	#	print(data_ohlcbs[r])
	#exit()
	# DEBUG


	# OK
	if (length != len(data_ohlcbs)):
		ts("ERR: Length=" + str(length) + " while " + str(len(data_ohlcbs)) + " rows in " + DTOHLC_hist )
		exit()
	else:
		ts("OK: PASSED CHECK, lines in filename string =" +str(length) + ", lines in " + DTOHLC_hist + " =" + str(len(data_ohlcbs)))

	#exit()
	# TODO: now double check this for having all the quotes and correct cuttage

	# calculate our limits for pnl_a and pnl_b
	start_a = 0
	end_a = int(length * ratio)-1
	start_b = end_a-1
	end_b = length+1
	print("variables: start_a / end_a / start_b / end_b:", start_a, end_a, start_b, end_b)

	data_ohlcbs_a = data_ohlcbs[start_a:end_a]
	data_ohlcbs_b = data_ohlcbs[start_b:end_b]

	#print("price A")
	#print(data_ohlcbs_a)
	#print("price B")
	#print(data_ohlcbs_b)
	#exit()

	# calculate roughly by 'Close' value since Altreva also uses Close-only internally
	#print('data_ohlcbs_a[3:]')
	#print(data_ohlcbs_a[:,3]) # all rows, Close column

	A = data_ohlcbs_a[:,3]
	B = data_ohlcbs_b[:,3]

	xa = range(1,len(A)+1,1)
	xb = range(1,len(B)+1,1)
	# will need those slopes in the future
	slope_a, intercept, r_value, p_value, std_err = linregress(xa, A)
	slope_b, intercept, r_value, p_value, std_err = linregress(xb, B)

	# for the chart filename to save
	chart_title = "Chunk file: " + (ReportNames[0]).split(" ")[0] + ".csv\nSlope A = " + f'{slope_a:.6f}' + " | " + "Slope B = " + f'{slope_b:.6f}' + " | A/B Slopes Ratio = " + f'{slope_a/slope_b:.6f}'
	chart_filename = str(length)+"/" + (ReportNames[0]).split(" ")[0] + ".png"

	#print(chart_title, " // ", chart_filename)

	'''
	def save_chart( close_price, v_separator, \
					best_a,  label_a,  best_ga, label_ga, best_b,   label_b, \
					best_gb, label_gb, best_ab, label_ab, best_gab, label_gab):

	'''

	# Use below code later in process_reports()
	AB = np.append(A,B)
	v_separator = len(A)
	#save_chart ( ab, len(A), ab, "ab", ab, "ab", ab, "ab", ab, "ab", ab, "ab", ab, "ab" )

	#exit()

	buy_hold_a	 = abs( float(A[0]) - float(A[len(A)-1]) )
	max_min_a 	 = np.max(A) - np.min(A) # roughly by Close value
	#print(buy_hold_a,max_min_a,np.max(A), np.min(A))
	#exit()
	buy_hold_b	 = abs( float(B[0]) - float(B[len(B)-1]) )
	max_min_b 	 = np.max(B) - np.min(B) # roughly by Close value

	trading_days_a = len(set(datez[start_a:end_a]))
	trading_days_b = len(set(datez[start_b:end_b]))

	#print(buy_hold_a,buy_hold_b,max_min_a,max_min_b,trading_days_a, trading_days_b)
	# ** ALL GOOD AND TESTED UNTIL HERE
	#exit()

	dt_a = dt[start_a:end_a]
	dt_b = dt[start_b:end_b]

	# do we need this date shit?
	start_a_date	= datetime.strptime(dt_a[0][0]					+ " " + dt_a[0][1], 			'%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
	end_a_date		= datetime.strptime(dt_a[len(dt_a)-1][0]		+ " " + dt_a[len(dt_a)-1][1],	'%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
	start_b_date	= datetime.strptime(dt_b[0][0]					+ " " + dt_b[0][1], 			'%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
	end_b_date		= datetime.strptime(dt_b[len(dt_b)-1][0]		+ " " + dt_b[len(dt_b)-1][1],	'%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

	#print(start_a_date,end_a_date, start_b_date, end_b_date)
	#exit()

	'''
	d1 = datetime.strptime(dt_a[0][0]				+ " " + dt_a[0][1], 			'%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
	d2 = datetime.strptime(dt[len(dt)-1][0]		+ " " + dt[len(dt)-1][1],	'%m/%d/%Y %H:%M:%S')
	# date periods - that's in MySQL format
	f_date = d1.strftime('%Y-%m-%d %H:%M:%S')
	l_date = d2.strftime('%Y-%m-%d %H:%M:%S')

	print("// -- some dataset stats ---")
	print("Start Open to Last Close range = ",open_close_range)
	print("Max Price to Min Price range =",max_range)
	print("Number of trading days is",trading_days)
	print("// -------------------------")
	'''

	# def process_reports(ReportNames, dt, datez, price ):
	process_reports( ReportNames )

	#ts("finished")
	t = time.time() - start
	print("time taken: ", t/60, "minutes")
	#ts("_______________________________________________________")
