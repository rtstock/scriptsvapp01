$PathFile = "\\ipc-vsql01\DATA\Batches\prod\WatchFolder\incoming\PagesOutput_GetPadPortBenchAsOf_20161124_ADAROT.xls"
$PathFile1 = "\\ipc-vsql01\DATA\Batches\prod\WatchFolder\incoming\PagesOutput_GetPadPortBenchAsOf_20161124_ADAROT-1.xls"
Copy-Item $PathFile $PathFile1

$xlspath = $PathFile1
$mysheet = "PagesOutput_GetPadPortBenchAsOf"

#ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss

$xlspath = $PathFile1
$sheet = $mysheet

$mincol = "zzzz"
$maxcol = "a"
$minrow = [int]::MaxValue
$maxrow = 1
$shellApplication = new-object -com shell.application
$file = Get-Item $xlspath
$destination = Split-Path $xlspath
if(!(Test-Path "$destination\temp")){
    [VOID] (New-Item -Path $destination -Name temp -ItemType directory)
}
Rename-Item $xlspath "$xlspath.zip"
$zipPackage = $shellApplication.NameSpace("$xlspath.zip")
$destinationFolder = $shellApplication.NameSpace("$destination\temp")
$destinationFolder.CopyHere($zipPackage.Items().item(2))
break
$sharedstr = ([xml] (Get-Content "$destination\temp\xl\sharedStrings.xml" -Encoding utf8)).sst.si | 
    Select-Object -ExpandProperty t | %{
        if($_ -is [System.Xml.XmlElement]){$_."#text"}else{$_}
    }

$sh = [xml](Get-Content "$destination\temp\xl\worksheets\sheet1.xml" -Encoding utf8)

$basedata = $sh.worksheet.sheetData | %{$_.row} | %{$_.c} | %{
    $col = $_.r -replace "\d+",""
    if($col -gt $maxcol){$maxcol = $col}
    if($col -lt $mincol){$mincol = $col}
    $row = $_.r -replace "[a-z]+",""
    if($row -gt $maxrow){$maxrow = $row}
    if($row -lt $minrow){$minrow = $row}
    $value = if($_.t -eq "s"){$sharedstr[($_.v)]}elseif($_.t -ne "E"){$_.v}
    New-Object -TypeName PSObject -Property @{col = $col; row = $row; value = $value}
}

Remove-Item "$destination\temp" -Confirm:$false -Force -Recurse
Rename-Item "$xlspath.zip" $xlspath

$h = @{}
([int][char]$mincol)..([int][char]$maxcol) | %{[string][char]$_} | %{$h.$_ = ""}
$th = @{}
$minrow..$maxrow | %{
    $th.$_ = New-Object -TypeName psobject -Property $h
}
$basedata | %{
    ($th.([int]$_.row)).($_.col) = $_.value
} 

$th.keys | Sort-Object |%{$th.$_}| 
    Select-Object -Property (([int][char]$mincol)..([int][char]$maxcol) | %{[string][char]$_}) |
        Export-Csv -Path ($xlspath -replace 'xlsx$',"csv") -NoTypeInformation -UseCulture
