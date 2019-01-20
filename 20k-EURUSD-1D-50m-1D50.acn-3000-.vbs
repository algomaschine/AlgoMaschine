' stuff from 'Files 20k 00-15-30' / cell 65:9
N_Agents = "20k"
AltrevaConfig = "20k-EURUSD-1D-50m-1D50.acn" 
UniqueConfPrefix = "1D50"
ModelsNamePrefix = 3000
NumberOfModels = 100
Style = "1FDA-style.aps"
' ------------------------------------------------------------------------
 


' stuff like below will be added automatically while forging the file
'N_Agents = "10K"
'AltrevaConfig = "10K-AUDNZD-240m-10d4.acn" 
'UniqueConfPrefix = "10d4"
'ModelsNamePrefix = 4000
'NumberOfModels = 100
'Style = "std-style.aps"
' ------------------------------------------------------------------------

ACN = "/AGN_CONF_NNNN/"
AGN_CONF_NNNN = N_Agents & "_" & UniqueConfPrefix


'cd "C:\Users\Administrator\Desktop\FXTM\t\MQL4\Files"
'copy 00K-AUDNZD-240m-10c4.acn 10c4%1.acn
'sfk191.exe replace 10c4%1.acn /MOD_FEED/00K_%1/ -yes
'timeout /t 5
'"C:\Program Files (x86)\Altreva\Adaptive Modeler\Bin\Adaptive Modeler.exe" /C:1 10c4%1.acn std-style.aps /RunToEnd  /E:00K_models_test4.csv  /CloseAtEnd 

ScriptDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)

' this starts generating models
Bat1 = ScriptDir & "\" & N_Agents & "_models_" & UniqueConfPrefix & "_" & ModelsNamePrefix & ".bat" 
' this starts Altreva models generation 
Bat3 = "threader" & UniqueConfPrefix & "_" & ModelsNamePrefix & ".bat"

line1 = "cd """ & ScriptDir &  """"
line2 = "copy " & AltrevaConfig & " " & UniqueConfPrefix  & "%1.acn"
line3 = "sfk191.exe replace " & UniqueConfPrefix & "%1.acn " & ACN & AGN_CONF_NNNN & "_%1/ -yes" ' make sure that Altreva contains AGN_CONF_NNNN where needed
line4 = "timeout /t 5"
line5 = """C:\Program Files (x86)\Altreva\Adaptive Modeler\Bin\Adaptive Modeler.exe"" /C:1 " & UniqueConfPrefix & "%1.acn " & Style & " /RunToEnd  /E:" & N_Agents & "_models_test_" & UniqueConfPrefix & ".csv  /CloseAtEnd "

Set objFSO=CreateObject("Scripting.FileSystemObject")
Set objFile = objFSO.CreateTextFile(Bat1,True)
objFile.Write line1 & vbCrLf
objFile.Write line2 & vbCrLf
objFile.Write line3 & vbCrLf
objFile.Write line4 & vbCrLf
objFile.Write line5 & vbCrLf
objFile.Close

Set objFSO=CreateObject("Scripting.FileSystemObject")
Set objFile = objFSO.CreateTextFile(Bat3,True)
objFile.Write line1 & vbCrLf
objFile.Write "python threader.py " & NumberOfModels & " " & ModelsNamePrefix & " """ &  Bat1 & """" & vbcrlf
objFile.Write "@pause" & vbcrlf
objFile.Close

CreateObject("WScript.Shell").Run Bat3