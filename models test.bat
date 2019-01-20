CD "C:\Users\Administrator\Desktop\Terminals\EURUSD-1D\MQL4\Files"
cls

REM del *.pkl

timeout /t 10
EURCHF.acn-1000-.vbs
timeout /t 3600
EURGBP.acn-1000-.vbs
timeout /t 3600
EURJPY.acn-1000-.vbs
timeout /t 3600
EURNZD.acn-1000-.vbs
timeout /t 3600
EURUSD.acn-1000-.vbs
timeout /t 3600
GBPAUD.acn-1000-.vbs
timeout /t 3600
GBPCAD.acn-1000-.vbs
timeout /t 3600
GBPCHF.acn-1000-.vbs
timeout /t 3600
GBPJPY.acn-1000-.vbs
timeout /t 3600
GBPUSD.acn-1000-.vbs
timeout /t 3600
NZDJPY.acn-1000-.vbs
timeout /t 3600
NZDUSD.acn-1000-.vbs
timeout /t 3600
USDCAD.acn-1000-.vbs
timeout /t 3600
USDCHF.acn-1000-.vbs
timeout /t 3600
USDJPY.acn-1000-.vbs
timeout /t 3600
XAGUSD.acn-1000-.vbs
timeout /t 3600
XAUUSD.acn-1000-.vbs


exit

start python TS-runner-adaptive2.py 1D_cut1 05k_T130
start python TS-runner-adaptive2.py 1D_cut2 05k_T230

start python TS-runner-adaptive2.py 1D 50k_1D00
start python TS-runner-adaptive2.py 1D 50k_1D15
start python TS-runner-adaptive2.py 1D 50k_1D30

@pause