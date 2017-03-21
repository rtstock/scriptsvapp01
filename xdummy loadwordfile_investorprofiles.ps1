################################################################################################################################################################

#==============================================================
#PowerShell and SSRS
#http://www.sqlmusings.com / http://www.twitter.com/sqlbelle
#
# How to run:
# C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe "C:\Batches\AutomationProjects\AUMOriginalCodeBlock\code\run-sql-script.ps1"

 
#I am qualifying this because I have more than one version in my system
[void] [System.Reflection.Assembly]::Load("Microsoft.ReportViewer.WinForms, `
	Version=10.0.0.0, Culture=neutral, `
	PublicKeyToken=b03f5f7f11d50a3a")
#If you need webforms, use Microsoft.ReportViewer.WebForms
 
#Windows.Forms for viewing
[void][System.Reflection.Assembly]::LoadWithPartialName("System.Windows.Forms")
#System.Diagnostics because I want to open Acrobat
[void][System.Reflection.Assembly]::LoadWithPartialName("System.Diagnostics")
 

#--------------------------------------------------------------
#PDF
# C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe "C:\Batches\AutomationProjects\AUMOriginalCodeBlock\code\run-sql-script.ps1"
#--------------------------------------------------------------
#need these variables for PDF rendering
#http://www.csharpcourses.com/2008/06/how-to-pass-parameters-to-reporting.html
$mimeType = $null;
$encoding = $null;
$extension = $null;
$streamids = $null;
$warnings = $null;

####################################
# Here we open a SQL server instance 
####################################
get-pssnapin -Registered
Add-PSSnapin SqlServerCmdletSnapin100 # here lives Invoke-SqlCmd
Add-PSSnapin SqlServerProviderSnapin100


$ServerInstance = "ipc-vsql01"
$Database = "DataAgg"
$ConnectionTimeout = 230
$QueryTimeout = 120

$conn=new-object System.Data.SqlClient.SQLConnection
$ConnectionString = "Server={0};Database={1};Integrated Security=True;Connect Timeout={2}" -f $ServerInstance,$Database,$ConnectionTimeout
$conn.ConnectionString=$ConnectionString
$conn.Open()


####################################################
#  Here we read the Excel file on Company MISC drive
####################################################

#$FileName = "\\IPC-VFS01\company$\misc\INVESTMENTS\Investment Manager Performance Reporting\Centralized Performance\PSN Account-Benchmark Link.xlsx"

$OleDbConn = New-Object "System.Data.OleDb.OleDbConnection"
$OleDbCmd = New-Object "System.Data.OleDb.OleDbCommand"
$OleDbAdapter = New-Object "System.Data.OleDb.OleDbDataAdapter"
$DataTable = New-Object "System.Data.DataTable"

Copy-Item "\\IPC-VFS01\company\Marketing\Linda\LINDA\Profiles\Allen, Michael-LMAP Profile.doc" "\\IPC-VFS01\company\Marketing\Linda\LINDA\Profiles\Allen, Michael-LMAP Profile-1.doc"

#$OleDbConn.ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\Batches\AutomationProjects\QuarterEndManagerCharts\trials\psnbm.xlsx;Extended Properties=""Excel 12.0 Xml;HDR=YES"";"
#$OleDbConn.ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=""\\IPC-VFS01\company\Marketing\Linda\LINDA\Profiles\Allen, Michael-LMAP Profile-1.doc"";Extended Properties=""Word 12.0 Xml;HDR=YES"";"
$OleDbConn.ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=""\\IPC-VFS01\company\Marketing\Linda\LINDA\Profiles\Allen, Michael-LMAP Profile-1.doc"";"
Write-host $OleDbConn.ConnectionString
$OleDbConn.Open()

$OleDbCmd.Connection = $OleDbConn
$OleDbCmd.commandtext = "Select * from [DOCUMENT$]"
$OleDbAdapter.SelectCommand = $OleDbCmd

$RowsReturned = $OleDbAdapter.Fill($DataTable)

Write-host @RowsReturned
#$Query = "TRUNCATE TABLE PSNAccountBenchmarkLink_New"
#$cmd=new-object system.Data.SqlClient.SqlCommand($Query,$conn)
#$cmd.CommandTimeout=$QueryTimeout
#$da=New-Object system.Data.SqlClient.SqlDataAdapter($cmd)
#$dataset = New-Object System.Data.DataSet
#$da.Fill($dataSet) | Out-Null


ForEach ($DataRec in $DataTable) {
	#Write-host "value=$($DataRec[0])"
	ForEach ($DataCol in $DataRec) {
		Write-host "$($DataCol[0])","$($DataCol[1])","$($DataCol[2])","$($DataCol[3])"
		# $c0 = ($DataCol[0] -as [string])
		# $c1 = ($DataCol[1] -as [string])
		# $c2 = ($DataCol[2] -as [string])
		# $c3 = ($DataCol[3] -as [string])
		# $c4 = ($DataCol[4] -as [string])
		# $c5 = ($DataCol[5] -as [string])
		# $c6 = ($DataCol[6] -as [string])
		# $c7 = ($DataCol[7] -as [string])
		
		
		# $c0 = $($c0).Replace('''','''''')
		# $c1 = $($c1).Replace('''','''''')
		# $c2 = $($c2).Replace('''','''''')
		# $c3 = $($c3).Replace('''','''''')
		# $c4 = $($c4).Replace('''','''''')
		# $c5 = $($c5).Replace('''','''''')
		# $c6 = $($c6).Replace('''','''''')
		# $c7 = $($c7).Replace('''','''''')
		
		
		# $Query = "
					# INSERT INTO PSNAccountBenchmarkLink_New ( FirmName,ProductName,BenchmarkProductName,
						# StartingDate,AssetClass1,AssetClass2,AssetClass3,ClosedToNewMoney ) 
					# VALUES ('$c0','$c1','$c2','$c3','$c4','$c5','$c6','$c7')
				# "		
		# $cmd=new-object system.Data.SqlClient.SqlCommand($Query,$conn)
		# $cmd.CommandTimeout=$QueryTimeout
		# $da=New-Object system.Data.SqlClient.SqlDataAdapter($cmd)
		# $dataset = New-Object System.Data.DataSet
		# $da.Fill($dataSet) | Out-Null
    
		}
}

$OleDbConn.Close()
Remove-Item -Path "\\IPC-VFS01\company\Marketing\Linda\LINDA\Profiles\Allen, Michael-LMAP Profile-1.doc" -Force









# $ds.Tables["tblManagerStrategies"] | foreach {
    # write-host 'ProductName is :' $_.ProductName
    # $managerstrategy = $_.ProductName
	#################################################################
	# $params = $null;
	# Write-Output "|||||||||||| Getting BenchmarkName ||||||||||||||||||"
	# $managername = $managerstrategy.Split("|")[0]
	# $strategyname = $managerstrategy.Split("|")[1]
	# Write-Output $managername
	# Write-Output $strategyname

	# $sqlstring = "select BenchmarkName,StartPeriod from DataAgg.dbo.PSNAccountBenchmarkLink where ManagerName = '"+$managername+"' and StrategyName = '"+$strategyname+"';"
	# $myrecord = Invoke-Sqlcmd -Query $sqlstring -ServerInstance "ipc-vsql01"  
	# $benchmarkroot = $myrecord["BenchmarkName"]
	# Write-Output $benchmarkroot
	# $startperiod = $myrecord["StartPeriod"]
	# $sqlstring = "select ProductName from (select distinct ProductName from DataAgg.dbo.ProductValues where SourceName = 'PSN' ) A where DataAgg.dbo.SPLIT(ProductName,'|',2) = '"+$benchmarkroot+"';"
	# Write-Output $sqlstring
	# $myrecord = Invoke-Sqlcmd -Query $sqlstring -ServerInstance "ipc-vsql01"  
	# $benchmarkname = $myrecord["ProductName"]


	# Write-Output $benchmarkname
	# Write-Output $startperiod
	# "abcd".GetType().FullName
	# $benchmarkname.GetType().FullName
		
# }
