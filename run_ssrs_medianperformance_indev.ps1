

#==============================================================
#PowerShell and SSRS
#http://www.sqlmusings.com / http://www.twitter.com/sqlbelle
#
# How to run:
# C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe "C:\Batches\AutomationProjects\AUMOriginalCodeBlock\code\run-sql-script.ps1"


$outputextension = $args[0]
$maxasofdate = $args[1]

#I am qualifying this because I have more than one version in my system
[void] [System.Reflection.Assembly]::Load("Microsoft.ReportViewer.WinForms, `
	Version=10.0.0.0, Culture=neutral, `
	PublicKeyToken=b03f5f7f11d50a3a")
#If you need webforms, use Microsoft.ReportViewer.WebForms
 
#Windows.Forms for viewing
[void][System.Reflection.Assembly]::LoadWithPartialName("System.Windows.Forms")
#System.Diagnostics because I want to open Acrobat
[void][System.Reflection.Assembly]::LoadWithPartialName("System.Diagnostics")
 
$rv = New-Object Microsoft.Reporting.WinForms.ReportViewer;
$rv.ProcessingMode = "Remote";
$rv.ServerReport.ReportServerUrl = "http://ipc-vsql01/ReportServer";
#$rv.ServerReport.ReportPath = "/path/to/Financial Report";
$rv.ServerReport.ReportPath = "/Investment Strategy/Median Performance";

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


##############################################################################
 
get-pssnapin -Registered
Add-PSSnapin SqlServerCmdletSnapin100 # here lives Invoke-SqlCmd
Add-PSSnapin SqlServerProviderSnapin100

$ServerInstance = "ipc-vsql01"
$Database = "DataAgg"
$ConnectionTimeout = 30

$Query = "select max(AsOfDate) from "

$QueryTimeout = 120

$conn=new-object System.Data.SqlClient.SQLConnection
$ConnectionString = "Server={0};Database={1};Integrated Security=True;Connect Timeout={2}" -f $ServerInstance,$Database,$ConnectionTimeout
$conn.ConnectionString=$ConnectionString
$conn.Open()
$cmd=new-object system.Data.SqlClient.SqlCommand($Query,$conn)
$cmd.CommandTimeout=$QueryTimeout
$ds=New-Object system.Data.DataSet

$ds.Tables.Add("tblManagerStrategies")

$da=New-Object system.Data.SqlClient.SqlDataAdapter($cmd)
[void]$da.fill($ds.Tables["tblManagerStrategies"])
$conn.Close()
$ds.Tables["tblManagerStrategies"]
$csv_ManagerStrategies = "C:\Batches\AutomationProjects\QuarterEndManagerCharts\output\ManagerStrategies.csv"
$ds.Tables["tblManagerStrategies"] | export-csv $csv_ManagerStrategies -NoTypeInformation
##############################################################################

#here's a sample usage http://msdn.microsoft.com/en-us/library/microsoft.reporting.winforms.reportparameterinfo(v=vs.80).aspx
$params = new-object 'Microsoft.Reporting.WinForms.ReportParameter[]' 0
# $params[0] = new-Object Microsoft.Reporting.WinForms.ReportParameter("Measure", $measure, $false);
# $params[1] = new-Object Microsoft.Reporting.WinForms.ReportParameter("ManagerStrategyName", $managerstrategy, $false);
# $params[2] = new-Object Microsoft.Reporting.WinForms.ReportParameter("BenchmarkName",$benchmarkname, $false);
# $params[3] = new-Object Microsoft.Reporting.WinForms.ReportParameter("StartPeriod", $startperiod, $false);
# $params[4] = new-Object Microsoft.Reporting.WinForms.ReportParameter("EndPeriod", $endperiod, $false);
# $params[5] = new-Object Microsoft.Reporting.WinForms.ReportParameter("Rolling", $rolling, $false);


$rv.ServerReport.SetParameters($params);
$rv.ProcessingMode = [Microsoft.Reporting.WinForms.ProcessingMode]::Remote;

$rv.ShowParameterPrompts = $false;
$rv.ServerReport.Refresh();

$bytes = $null;	
$bytes = $rv.ServerReport.Render($outputextension.ToUpper(), $null, 
			[ref] $mimeType, 
			[ref] $encoding, 
			[ref] $extension, 
			[ref] $streamids, 
			[ref] $warnings);
			
$managerstrategyforpath = $managerstrategy + "" + $endperiod
$managerstrategyforpath = $managerstrategyforpath.Replace("|", "")
$managerstrategyforpath = $managerstrategyforpath.Replace("/", "")
$managerstrategyforpath = $managerstrategyforpath.Replace(" ", "")
$managerstrategyforpath = $managerstrategyforpath.Replace(".", "")
$managerstrategyforpath = $managerstrategyforpath.Replace("-", "")

$myfileid += 1

$file = "C:\Batches\AutomationProjects\Watcher\output\Median Performance " + $myfileid + "." + $outputextension;
#$file = "C:\Batches\AutomationProjects\QuarterEndManagerCharts\output\ExcessReturns.pdf";
Write-Output "-------------------------------------------------------"
Write-Output "Exporting... "
Write-Output $file
Write-Output $bytes.Length
$fileStream = New-Object System.IO.FileStream($file, [System.IO.FileMode]::OpenOrCreate);
$fileStream.Write($bytes, 0, $bytes.Length);
$fileStream.Close();

#if you want to open the PDF automatically, can uncomment the following
#	[System.Diagnostics.Process]::Start($file);

