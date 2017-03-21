##############################################################################
 
get-pssnapin -Registered
Add-PSSnapin SqlServerCmdletSnapin100 # here lives Invoke-SqlCmd
Add-PSSnapin SqlServerProviderSnapin100

$ServerInstance = "ipc-vsql01"
$Database = "DataAgg"
$ConnectionTimeout = 30
$Query = "select MAX(class_last_invested_date) maxdate from dbo.vanalysisofbenchmarks_padportasof_imported"
write-host $Query

$QueryTimeout = 120

$conn=new-object System.Data.SqlClient.SQLConnection
$ConnectionString = "Server={0};Database={1};Integrated Security=True;Connect Timeout={2}" -f $ServerInstance,$Database,$ConnectionTimeout
$conn.ConnectionString=$ConnectionString
$conn.Open()
$cmd_maxdate=new-object system.Data.SqlClient.SqlCommand($Query,$conn)
$cmd_maxdate.CommandTimeout=$QueryTimeout
$ds_maxdate=New-Object system.Data.DataSet

$ds_maxdate.Tables.Add("tblmaxdate")

$da_maxdate=New-Object system.Data.SqlClient.SqlDataAdapter($cmd_maxdate)
[void]$da_maxdate.fill($ds_maxdate.Tables["tblmaxdate"])
$conn.Close()
$ds_maxdate.Tables["tblmaxdate"]
$ds_maxdate.Tables["tblmaxdate"] | foreach {
	$maxdate = $_.maxdate
}
write-host $maxdate
#$newfile="C:\Batches\AutomationProjects\Watcher\output\Median_Performance_Full_"+$maxdate+".xls"
$reportname= "\\ipc-vsql01\data\Batches\prod\Performance\output\Median_Performance_Full_"+$maxdate+".xls"
#$newfile= "\\ipc-vsql01\data\Batches\prod\Performance\output\Median_Performance_Full_2017-01-10.xls"
write-host $reportname
##############################################################################

get-SPRSDatabase | select id, querytimeout,connectiontimeout, status, server, ServiceInstance   


$User = "IPCNET\justin.malinchak"
$PWord = ConvertTo-SecureString -String "zg14Bn*1082" -AsPlainText -Force
$c = New-Object –TypeName System.Management.Automation.PSCredential –ArgumentList $User, $PWord
	
$url = "http://ipc-vsql01/Reports/Pages/Report.aspx?ItemPath=%2fInvestment+Strategy%2fMedian+Performance+Full&ViewMode=Detail"
#Invoke-WebRequest $url -UseDefaultCredentials -OutFile $reportname -TimeoutSec 500
#Invoke-WebRequest $url -Credential $c -OutFile $reportname -TimeoutSec 500

$webresponse = Invoke-WebRequest $url -Credential $c -OutFile $reportname -TimeoutSec 500
$response = $webresponse.allelements[0].innertext
write-host $response


# $R = Invoke-WebRequest https://www.hedgefundresearch.com/user/login?current=node/1
# $R.Forms[0].Name = "MyName"
# $R.Forms[0].Password = "MyPassword"
# Invoke-RestMethod http://website.com/service.aspx -Body $R