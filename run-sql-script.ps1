# $args[0] gets the 1 parameter passed in.  Parameter should be an 8 character date eg. 20161231 or can leave blank for today
# $s0=$args[0]
$ScriptPath = Split-Path $MyInvocation.InvocationName

#Mass loads data into the ISS database
get-pssnapin -Registered
Add-PSSnapin SqlServerCmdletSnapin100 # here lives Invoke-SqlCmd
Add-PSSnapin SqlServerProviderSnapin100
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-ManagerConsensus.sql" -serverinstance "ipc-vsql01" -database "DataAgg" # the parameter -database can be omitted based on what your sql script does.
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-PerformanceStatic.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-AUMOriginalCodeBlock.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-PricesOfSecuritiesFromGWP.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-PricesOfSecuritiesFromEVARE.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-returnsyahoo.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-WeightsOfIndexes.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 
invoke-sqlcmd -inputfile "$ScriptPath\mass-bulkinsert-to-PerformancePadPortBenchAsOf.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 

Get-Childitem -Path C:\Batches\AutomationProjects\Watcher\incoming_batch_scripts -Filter *.bat | % {& $_.FullName}
Move-Item C:\Batches\AutomationProjects\Watcher\incoming_batch_scripts\*.bat C:\Batches\AutomationProjects\Watcher\incoming_batch_scripts\archive\

#cmd.exe /c '\my-app\my-file.bat'

#invoke-sqlcmd -inputfile "$ScriptPath\send_email_of_results_sql_managerconsensus_imported.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 
#invoke-sqlcmd -inputfile "$ScriptPath\send_email_of_results_sql_aumoriginalcodeblock_imported.sql" -serverinstance "ipc-vsql01" -database "DataAgg" 

#add more here...	
