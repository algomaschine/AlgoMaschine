@echo off
setlocal enabledelayedexpansion
ECHO ### Checking the number of files under %1 ###
set numFiles=0
for %%x in (10k_1D15_6*.csv) do (
  set file[!numFiles!]=%%~nfx
  set /a numFiles+=1
 ) 
ECHO ### Number of files found: %numFiles%

set /a index=%numFiles%-1
for /L %%i in (0,1,%index%) do (
  rem echo !file[%%i]!
  find /v /c "" !file[%%i]!
  )
endlocal