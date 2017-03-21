function GetRSConnection($server, $instance)
{
    #   Create a proxy to the SSRS server and give it the namespace of 'RS' to use for
    #   instantiating objects later.  This class will also be used to create a report
    #   object.

    $User = "IPCNET\justin.malinchak"
    $PWord = ConvertTo-SecureString -String "zg14Bn*1082" -AsPlainText -Force
    $c = New-Object –TypeName System.Management.Automation.PSCredential –ArgumentList $User, $PWord

    $reportServerURI = "http://" + $server + "/" + $instance + "/ReportExecution2005.asmx?WSDL"

    $RS = New-WebServiceProxy -Class 'RS' -NameSpace 'RS' -Uri $reportServerURI -Credential $c
    $RS.Url = $reportServerURI
    return $RS
}

function GetReport($RS, $reportPath)
{
    #   Next we need to load the report. Since Powershell cannot pass a null string
    #   (it instead just passses ""), we have to use GetMethod / Invoke to call the
    #   function that returns the report object.  This will load the report in the
    #   report server object, as well as create a report object that can be used to
    #   discover information about the report.  It's not used in this code, but it can
    #   be used to discover information about what parameters are needed to execute
    #   the report.
    $reportPath = "/" + $reportPath
    $Report = $RS.GetType().GetMethod("LoadReport").Invoke($RS, @($reportPath, $null))

    # initialise empty parameter holder
    $parameters = @()
    $RS.SetExecutionParameters($parameters, "nl-nl") > $null
    return $report
}

function AddParameter($params, $name, $val)
{
    $par = New-Object RS.ParameterValue
    $par.Name = $name
    $par.Value = $val
    $params += $par
    return ,$params
}

function GetReportInFormat($RS, $report, $params, $outputpath, $format)
{
    #   Set up some variables to hold referenced results from Render
    $deviceInfo = "<DeviceInfo><NoHeader>True</NoHeader></DeviceInfo>"
    $extension = ""
    $mimeType = ""
    $encoding = ""
    $warnings = $null
    $streamIDs = $null

    #   Report parameters are handled by creating an array of ParameterValue objects.
    #   Add the parameter array to the service.  Note that this returns some
    #   information about the report that is about to be executed.
    #   $RS.SetExecutionParameters($parameters, "en-us") > $null
    $RS.SetExecutionParameters($params, "nl-nl") > $null

    #    Render the report to a byte array.  The first argument is the report format.
    #    The formats I've tested are: PDF, XML, CSV, WORD (.doc), EXCEL (.xls),
    #    IMAGE (.tif), MHTML (.mhtml).
    $RenderOutput = $RS.Render($format,
        $deviceInfo,
        [ref] $extension,
        [ref] $mimeType,
        [ref] $encoding,
        [ref] $warnings,
        [ref] $streamIDs
    )

    #   Determine file name
    $parts = $report.ReportPath.Split("/")
    $filename = $parts[-1] + "."
    switch($format)
    {
        "EXCEL" { $filename = $filename + "xls" } 
        "WORD" { $filename = $filename + "doc" }
        "IMAGE" { $filename = $filename + "tif" }
        default { $filename = $filename + $format }
    }

    if($outputpath.EndsWith("\\"))
    {
        $filename = $outputpath + $filename
    } else
    {
        $filename = $outputpath + "\" + $filename
    }

    $filename

    # Convert array bytes to file and write
    $Stream = New-Object System.IO.FileStream($filename), Create, Write
    $Stream.Write($RenderOutput, 0, $RenderOutput.Length)
    $Stream.Close()
}

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

$newfile= "\\ipc-vsql01\data\Batches\prod\Performance\output\Performance_by_Strategy_"+$maxdate+".xls"

write-host $newfile
##############################################################################

$RS = GetRSConnection -server "ipc-vsql01" -instance "ReportServer"
$RS.Timeout = 1800000
$report = GetReport -RS $RS -reportPath "Daily Stack/Performance by Strategy"

$params = @()
$params = AddParameter -params $params -name "p_AsOfDate" -val $maxdate
$params = AddParameter -params $params -name "p_ShowComposite" -val 0

GetReportInformat -RS $RS -report $report -params $params -outputpath "C:\Batches\AutomationProjects\Watcher\output" -format "EXCEL"


Copy-Item "C:\Batches\AutomationProjects\Watcher\output\Performance by Strategy.xls" $newfile
Start-Sleep -s 10
Remove-Item "C:\Batches\AutomationProjects\Watcher\output\Performance by Strategy.xls" 

#############################################################################
#Sends an email
# $ScriptPath = Split-Path $MyInvocation.InvocationName
# get-pssnapin -Registered
# Add-PSSnapin SqlServerCmdletSnapin100 # here lives Invoke-SqlCmd
# Add-PSSnapin SqlServerProviderSnapin100
# $DBParam1 = 'myfilename='+$newfile+''
# $DBParam2 = 'Period=MTD'
# $DBParam3 = 'DummyText=Hello'
# $DBParams = $DBParam1, $DBParam2, $DBParam3
# Invoke-sqlcmd -inputfile "$ScriptPath\send-email_performancebystrategy.sql" -Variable $DBParams -serverinstance "ipc-vsql01" -database "DataAgg" 
#############################################################################



return $newfile