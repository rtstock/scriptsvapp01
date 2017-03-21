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

##############################################################################