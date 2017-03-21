function Get-FlightStatus {
    param($query)

    $url = "http://bing.com?q=flight status for $query"

    $result = Invoke-WebRequest $url

    $result.AllElements | 
        Where Class -eq "ans" |
        Select -First 1 -ExpandProperty innerText    
}
Write-host "running"
$result = Get-FlightStatus('JB310')
Write-host $result
Write-host "done"