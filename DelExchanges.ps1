# Delete Exchanges In RabbitMQ
# Jon Truran, Paypoint 2018

function delete-exchange {
    param([String]$ExchangeName,
          [System.Management.Automation.PSCredential]
          $PSCredObject = [System.Management.Automation.PSCredential]::Empty
          )
    $url = "http://localhost:15672/api/exchanges/%2f/" + $ExchangeName
    Write-Host "Deleting: " $ExchangeName
    Invoke-RestMethod -Uri $url -Method Delete -Credential $PSCredObject -ContentType "application/json"
}

#user account for webapi    
$rusr = "guest"
#password for webapi
$rpwd = "guest"
#re
$rgex = '^SiteJM'

$rpwde = ConvertTo-SecureString -String $rpwd -AsPlainText -Force
$cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $rusr,$rpwde
$url = "http://localhost:15672/api/exchanges"
$rreq = Invoke-RestMethod -Uri $url -Method Get -Credential $cred
$rreq | Select-Object -ExpandProperty name| ForEach { if ($_ -match $rgex ) { delete-exchange -ExchangeName $_ -PSCredObject $cred } }
 
