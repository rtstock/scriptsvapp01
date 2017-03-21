#[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | out-null 
#$sqlSrv = New-Object 'Microsoft.SqlServer.Management.Smo.Server' ($server)
#$DBParam1 = "Period=MTD"
#$DBParam2 = "MyName=Justin"
#$DBParam3 = "Dummy=Hello"
#$DBParams = $DBParam1, $DBParam2, $DBParam3
#Invoke-Sqlcmd -InputFile $DBScriptFile -Variable $DBParams -Serverinstance $DBServer -Database "$database"
############################################################################################################
# $args[0] gets the 1 parameter passed in.  Parameter should be an 8 character date eg. 20161231 or can leave blank for today
# $s0=$args[0]

$ScriptPath = Split-Path $MyInvocation.InvocationName

get-pssnapin -Registered
Add-PSSnapin SqlServerCmdletSnapin100 # here lives Invoke-SqlCmd
Add-PSSnapin SqlServerProviderSnapin100
# -----------------------------------------------------------------
# MTD
# Parameters to pass to the .sql script
$DBParam1 = "Period=MTD"
$DBParam2 = "MyName=Justin"
$DBParam3 = "DummyText=Hello"
$DBParams = $DBParam1, $DBParam2, $DBParam3
invoke-sqlcmd -inputfile "$ScriptPath\send-email-results-performance-strategies-top-and-bottom-10.sql" -Variable $DBParams -serverinstance "ipc-vsql01" -database "DataAgg" # the parameter -database can be omitted based on what your sql script does.

# -----------------------------------------------------------------
# QTD
# Parameters to pass to the .sql script
$DBParam1 = "Period=QTD"
$DBParam2 = "MyName=Justin"
$DBParam3 = "DummyText=Hello"
$DBParams = $DBParam1, $DBParam2, $DBParam3
invoke-sqlcmd -inputfile "$ScriptPath\send-email-results-performance-strategies-top-and-bottom-10.sql" -Variable $DBParams -serverinstance "ipc-vsql01" -database "DataAgg" # the parameter -database can be omitted based on what your sql script does.

# -----------------------------------------------------------------
# YTD
# Parameters to pass to the .sql script
$DBParam1 = "Period=YTD"
$DBParam2 = "MyName=Justin"
$DBParam3 = "DummyText=Hello"
$DBParams = $DBParam1, $DBParam2, $DBParam3
invoke-sqlcmd -inputfile "$ScriptPath\send-email-results-performance-strategies-top-and-bottom-10.sql" -Variable $DBParams -serverinstance "ipc-vsql01" -database "DataAgg" # the parameter -database can be omitted based on what your sql script does.

# -----------------------------------------------------------------


#SMALL SECTION OF CODE FROM ADDCITY.SQL

#INSERT #CITY VALUES
#	('$(CITY)', '$(STATE)', '$(COUNTRY)');