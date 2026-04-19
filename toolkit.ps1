#===========================================================


# ICY Toolkit - PowerShell Menu


# Functies:


# - Verbinden met servers


# - Services bekijken, herstarten of stoppen


# - Dispatch MAC log search (grep/tail)


# - ICYCCAppAPI Log Browser (selectie + grep/tail + export)


#===========================================================





[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()





# Modules laden (optioneel)


Import-Module Terminal-Icons -ErrorAction SilentlyContinue


Import-Module PSReadLine -ErrorAction SilentlyContinue


$script:ToolkitRoot = if (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) { $PSScriptRoot } elseif ($MyInvocation.MyCommand.Path) { Split-Path -Parent $MyInvocation.MyCommand.Path } else { (Get-Location).Path }
$script:SharedEnvPath = $null

try { Set-Location -Path $script:ToolkitRoot } catch {}





# SSH sleutel


$sshKey = if ($env:SSH_KEY_PATH) { $env:SSH_KEY_PATH } elseif ($env:SSH_KEY) { $env:SSH_KEY } else { "C:\Path\To\ssh-key.pem" }





# Servers configuratie


$servers = @(


    @{ Name = "Ymir (Productie)"; HostName = "ymir.icy.nl"; User = "hnijdam" },


    @{ Name = "IcyCCCloud (Productie)"; HostName = "icycccloud.icy.nl"; User = "hnijdam" },


    @{ Name = "Dispatch (Productie)"; HostName = "dispatch.icy.nl"; User = "hnijdam" },


    @{ Name = "IcyCCAppAPI (Productie)"; HostName = "icyccappapi.icy.nl"; User = "hnijdam" }


)





#====================== Helpers ==========================





function Get-FreePort {
    param (
        [int]$StartPort = 8501,
        [int]$EndPort = 8520
    )

    for ($port = $StartPort; $port -le $EndPort; $port++) {
        $listener = $null
        try {
            $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
            $listener.Start()
            return $port
        }
        catch {
            continue
        }
        finally {
            if ($listener) {
                try { $listener.Stop() } catch {}
            }
        }
    }

    throw "Geen vrije localhost-poort gevonden tussen $StartPort en $EndPort."
}

function Show-Menu {


    param (


        [string]$Title,


        [string[]]$Options,


        [switch]$Filter


    )


    $selection = 0


    $scrollOffset = 0





    # Bepaal maximale hoogte voor de lijst (vensterhoogte min header ruimte)


    try {


        $windowHeight = $Host.UI.RawUI.WindowSize.Height


        $windowWidth = $Host.UI.RawUI.WindowSize.Width


    } catch {


        $windowHeight = 25


        $windowWidth = 80


    }


    $listHeight = $windowHeight - 5


    if ($listHeight -lt 5) { $listHeight = 10 }





    # Cursor verbergen om flikkeren te voorkomen


    try { [Console]::CursorVisible = $false } catch {}





    Clear-Host





    try {


        while ($true) {


            # Scroll logica


            if ($selection -lt $scrollOffset) {


                $scrollOffset = $selection


            }


            elseif ($selection -ge $scrollOffset + $listHeight) {


                $scrollOffset = $selection - $listHeight + 1


            }





            # Cursor resetten naar (0,0)


            if ([Console]::IsOutputRedirected -eq $false) {


                try { [Console]::SetCursorPosition(0, 0) } catch { Clear-Host }


            } else {


                Clear-Host


            }





            # Header tekenen (regel voor regel overschrijven)


            Write-Host ($Title.PadRight($windowWidth - 1)) -ForegroundColor Cyan


            if ($Filter) {


                if (-not $script:__menu_filter) { $script:__menu_filter = "" }


                Write-Host ("Filter: " + $script:__menu_filter).PadRight($windowWidth - 1) -ForegroundColor Yellow


                Write-Host "Gebruik pijltjes om te navigeren, Enter om te selecteren. Typ om te filteren.".PadRight($windowWidth - 1) -ForegroundColor Gray


            } else {


                Write-Host "Gebruik pijltjes om te navigeren, Enter om te selecteren.".PadRight($windowWidth - 1) -ForegroundColor Gray


            }


            Write-Host ("-" * ($windowWidth - 1)) -ForegroundColor Gray





            # Determine display list (apply filter if requested)


            if ($Filter -and $script:__menu_filter) {


                $display = $Options | Where-Object { $_ -match [regex]::Escape($script:__menu_filter) }


            } else {


                $display = $Options


            }





            # Lijst tekenen (alleen zichtbare deel)


            for ($i = 0; $i -lt $listHeight; $i++) {


                $index = $scrollOffset + $i





                if ($index -lt $display.Count) {


                    $prefix = "   "


                    $color = "White"


                    if ($index -eq $selection) {


                        $prefix = "-> "


                        $color = "Green"


                    }





                    # Tekst afkappen of aanvullen met spaties om de hele regel te wissen


                    $text = "$prefix$($display[$index])"


                    if ($text.Length -ge $windowWidth) {


                        $text = $text.Substring(0, $windowWidth - 1)


                    } else {


                        $text = $text.PadRight($windowWidth - 1)


                    }


                    Write-Host $text -ForegroundColor $color


                } else {


                    # Lege regels wissen


                    Write-Host "".PadRight($windowWidth - 1)


                }


            }





            $key = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")


            $vk = $key.VirtualKeyCode


            $ch = $key.Character





            # If filtering is enabled and user typed printable characters, update filter


            if ($Filter) {


                if ($vk -eq 8) {


                    # Backspace


                    if ($script:__menu_filter.Length -gt 0) { $script:__menu_filter = $script:__menu_filter.Substring(0, $script:__menu_filter.Length - 1) }


                    $selection = 0


                    continue


                }


                if ($vk -eq 27) {


                    # ESC -> cancel


                    return -1


                }


                if ($vk -ne 38 -and $vk -ne 40 -and $vk -ne 13) {


                    if ($ch -ne [char]0 -and $ch -ne "") {


                        $script:__menu_filter += $ch


                        $selection = 0


                        continue


                    }


                }


            }





            if ($vk -eq 38) { # Up


                $selection--


                if ($selection -lt 0) { $selection = $display.Count - 1 }


            }


            elseif ($vk -eq 40) { # Down


                $selection++


                if ($selection -ge $display.Count) { $selection = 0 }


            }


            elseif ($vk -eq 13) { # Enter


                # map selected displayed item back to original Options index


                if ($display.Count -eq 0) { return -1 }


                $sel = $display[$selection]


                $orig = [Array]::IndexOf($Options, $sel)


                if ($orig -ge 0) { return $orig } else { return -1 }


            }


        }


    } finally {


        # Cursor herstellen bij afsluiten of crash


        try { [Console]::CursorVisible = $true } catch {}


    }


}





function Invoke-Export {


    param(


        $server,


        [Alias('logFile')]


        [string[]]$logFiles,


        [string]$pattern = "",


        [string]$timeFilter = "",


        [switch]$json,


        [switch]$excel


    )





    $serverName = $server.Name





    # Maak export folder aan


    $exportFolder = Join-Path $env:USERPROFILE "Documents\ICY-Logs"


    if (-not (Test-Path $exportFolder)) {


        New-Item -Path $exportFolder -ItemType Directory | Out-Null


    }





    # Bepaal bestandsnaam


    if ($logFiles.Count -gt 1) {


        $mainLog = "Combined-7Days"


    } else {


        $mainLog = $logFiles[0]


    }





    # Bestandsnaam ISO stijl


    $timestamp = Get-Date -Format "yyyy-MM-ddTHH-mm"


    $patternSafe = if ($pattern) { "_$($pattern -replace '[^a-zA-Z0-9]', '_')" } else { "" }


    $baseName = "$($mainLog)$patternSafe"


    $exportFile = Join-Path $exportFolder "$serverName-$baseName-$timestamp.log"





    # Bouw grep commando


    $grepCmd = ""


    if ($pattern) {


        $grepCmd = "grep -Eih '$pattern'" # -h onderdrukt bestandsnamen bij multiple files


    } else {


        $grepCmd = "cat"


    }





    # Tijdfilter commando


    $timeCmd = ""


    if ($timeFilter) {


        $timeCmd = "| grep '$timeFilter'"


    }





    # Paden samenstellen


    $remotePaths = $logFiles | ForEach-Object { "/var/lib/wildfly/production/standalone/log/$_" }


    $remotePathsStr = $remotePaths -join " "





    # Combineer commando (2>/dev/null om fouten over missende bestanden te negeren)


    $cmd = "$grepCmd $remotePathsStr 2>/dev/null $timeCmd"





    Write-Host "Export gestart naar $exportFile..." -ForegroundColor Cyan





    try {


        $remoteTemp = "/tmp/icy_export_$(Get-Random).log"


        Write-Host "Data verzamelen op server (voer sudo wachtwoord in indien nodig)..." -ForegroundColor Cyan





        # Stap 1: Remote uitvoeren naar temp file (zodat sudo prompt niet in de file komt)


        ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "sudo $cmd > $remoteTemp"





        # Stap 2: Downloaden


        scp -q -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName):$remoteTemp" $exportFile





        # Stap 3: Opruimen


        ssh -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "rm $remoteTemp"





        Write-Host "Export voltooid!" -ForegroundColor Green





        if ($json -or $excel) {


            if ((Get-Item $exportFile).Length -eq 0) {


                Write-Host "Waarschuwing: Het gedownloade bestand is leeg." -ForegroundColor Yellow


            }





            $lines = Get-Content $exportFile


            if ($lines -is [string]) { $lines = @($lines) } # Force array if single line





            $maxRows = 1000000 # Excel limiet veiligheid (max is 1.048.576)





            # Meldingen direct tonen


            if ($excel) {


                if ($lines.Count -gt $maxRows) {


                    Write-Host "Waarschuwing: Export gelimiteerd tot $maxRows rijen vanwege Excel limieten." -ForegroundColor Yellow


                }


                Write-Host "Excel bestand genereren (dit kan even duren)..." -ForegroundColor Cyan


            }





            Write-Host "Debug: $($lines.Count) regels ingelezen voor verwerking." -ForegroundColor DarkGray





            # Gebruik List voor performance (array += is te traag voor miljoenen regels)


            $parsedData = [System.Collections.Generic.List[PSCustomObject]]::new()


            $rowCount = 0


            $totalLines = $lines.Count





            for ($i = 0; $i -lt $totalLines; $i++) {


                $line = $lines[$i]





                # Progress bar update elke 5000 regels


                if ($i % 5000 -eq 0) {


                    Write-Progress -Activity "Logregels verwerken" -Status "$i / $totalLines" -PercentComplete (($i / $totalLines) * 100)


                }





                if ([string]::IsNullOrWhiteSpace($line)) { continue }





                if ($excel -and $rowCount -ge $maxRows) {


                    # Stop alleen als we Excel doen en de limiet bereikt is


                    # Als we ook JSON doen, zouden we eigenlijk door moeten gaan, maar voor nu kappen we af om consistent te blijven


                    break


                }





                # Probeer timestamp te matchen (Level is optioneel)


                if ($line -match '^(?<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:,\d+)?)\s+(?:(?<lvl>[A-Z]+)\s+)?(?<msg>.*)') {


                    $msg = $matches.msg


                    $level = if ($matches.lvl) { $matches.lvl } else { "" }


                    $account = ""


                    $req = ""





                    # Timestamp normaliseren (komma naar punt) en parsen naar DateTime object voor Excel


                    $tsString = $matches.ts -replace ',', '.'


                    $tsObj = $tsString # Fallback


                    try {


                        $tsObj = [DateTime]::Parse($tsString)


                    } catch {


                        # Als parsen mislukt, behoud string


                    }





                    # Extract Account (tussen ')' en 'request') en Request (alles na 'request')


                    # Voorbeeld: ... (default task-211818) fastdata@marinavolendam.nl request PUT ...


                    if ($msg -match '\)\s+(?<acc>[^\s]+)\s+request\s+(?<req>.*)') {


                        $account = $matches.acc


                        $req = $matches.req


                    }





                    $parsedData.Add([PSCustomObject]@{


                        timestamp = $tsObj


                        level     = $level


                        account   = $account


                        request   = $req


                        message   = $msg


                    })


                } else {


                    $parsedData.Add([PSCustomObject]@{


                        timestamp = ""


                        level     = ""


                        account   = ""


                        request   = ""


                        message   = $line


                    })


                }


                $rowCount++


            }


            Write-Progress -Activity "Logregels verwerken" -Completed


        }





        if ($json) {


            $jsonFile = [System.IO.Path]::ChangeExtension($exportFile, ".json")


            $parsedData | ConvertTo-Json -Depth 10 | Set-Content $jsonFile


            Write-Host "JSON export voltooid: $jsonFile" -ForegroundColor Green





            $open = Read-Host "JSON bestand openen? (J/N)"


            if ($open -eq "J") { Invoke-Item $jsonFile }


        }





        if ($excel) {


            $excelFile = [System.IO.Path]::ChangeExtension($exportFile, ".xlsx")


            # Melding is al eerder getoond





            try {


                $excelApp = New-Object -ComObject Excel.Application


                $excelApp.Visible = $false


                $excelApp.DisplayAlerts = $false


                $workbook = $excelApp.Workbooks.Add()


                $sheet = $workbook.Worksheets.Item(1)





                # Headers


                $sheet.Cells.Item(1, 1) = "Timestamp"


                $sheet.Cells.Item(1, 2) = "Level"


                $sheet.Cells.Item(1, 3) = "Account"


                $sheet.Cells.Item(1, 4) = "Request"


                $sheet.Cells.Item(1, 5) = "Message"





                # Styling Headers


                $headerRange = $sheet.Range("A1", "E1")


                $headerRange.Font.Bold = $true


                $headerRange.Font.Size = 12


                # $headerRange.Interior.ColorIndex = 37 # Kleur verwijderd op verzoek


                $headerRange.Borders.LineStyle = 0 # Continuous


                $headerRange.AutoFilter() # Filters toevoegen





                # Data invullen (Snelle methode via Array ipv cell-by-cell)


                $dataCount = $parsedData.Count


                if ($dataCount -gt 0) {


                    Write-Host "Debug: $dataCount rijen schrijven naar Excel..." -ForegroundColor DarkGray





                    # Converteer naar 2D array voor snelle insert


                    $dataArray = [object[,]]::new($dataCount, 5)


                    for ($i = 0; $i -lt $dataCount; $i++) {


                        $item = $parsedData[$i]


                        $dataArray[$i, 0] = $item.timestamp


                        $dataArray[$i, 1] = $item.level


                        $dataArray[$i, 2] = $item.account


                        $dataArray[$i, 3] = $item.request


                        # Excel cel limiet is 32767 chars, truncate voor zekerheid


                        $msg = $item.message


                        if ($msg.Length -gt 32000) { $msg = $msg.Substring(0, 32000) + "..." }


                        $dataArray[$i, 4] = $msg


                    }





                    # Schrijf in één keer naar range


                    $range = $sheet.Range("A2").Resize($dataCount, 5)


                    $range.Value2 = $dataArray





                    # Format Timestamp column (A) specifically for the data range


                    $timeRange = $sheet.Range("A2").Resize($dataCount, 1)





                    # Fix voor Nederlandse Excel (yyyy -> jjjj, hh -> uu) en komma/punt separator


                    $culture = Get-Culture


                    $decimalSep = $culture.NumberFormat.NumberDecimalSeparator





                    if ($culture.TwoLetterISOLanguageName -eq "nl") {


                        try {


                            # Nederlands: jjjj-mm-dd uu:mm:ss,000 (of .000 afhankelijk van separator)


                            $localFmt = "jjjj-mm-dd uu:mm:ss" + $decimalSep + "000"


                            $timeRange.NumberFormatLocal = $localFmt


                        } catch {


                            # Fallback 1: Probeer punt hardcoded (soms wijkt Excel af van OS)


                            try { $timeRange.NumberFormatLocal = "jjjj-mm-dd uu:mm:ss.000" } catch {


                                # Fallback 2: Zonder milliseconden


                                try { $timeRange.NumberFormatLocal = "jjjj-mm-dd uu:mm:ss" } catch {}


                            }


                        }


                    } else {


                        try {


                            $timeRange.NumberFormat = "yyyy-mm-dd hh:mm:ss.000"


                        } catch {


                            try { $timeRange.NumberFormat = "yyyy-mm-dd hh:mm:ss" } catch {}


                        }


                    }


                }





                # AutoFit Columns


                $sheet.Columns.Item(1).AutoFit() | Out-Null


                $sheet.Columns.Item(2).AutoFit() | Out-Null


                $sheet.Columns.Item(3).AutoFit() | Out-Null


                $sheet.Columns.Item(4).AutoFit() | Out-Null


                # Message kolom niet te breed maken


                $sheet.Columns.Item(5).ColumnWidth = 100


                $sheet.Columns.Item(5).WrapText = $true





                # Freeze Top Row


                $excelApp.ActiveWindow.SplitRow = 1


                $excelApp.ActiveWindow.FreezePanes = $true | Out-Null





                # Save


                $workbook.SaveAs($excelFile)


                $workbook.Close()


                $excelApp.Quit()





                [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excelApp) | Out-Null





                Write-Host "Excel export voltooid: $excelFile" -ForegroundColor Green





                $open = Read-Host "Excel bestand openen? (J/N)"


                if ($open -eq "J") { Invoke-Item $excelFile }


            } catch {


                Write-Host "Excel generatie mislukt: $_" -ForegroundColor Red


                if ($excelApp) { $excelApp.Quit() }


            }


        }





        # Opruimen origineel logbestand als er een conversie is gedaan


        if (($json -or $excel) -and (Test-Path $exportFile)) {


            Remove-Item $exportFile


        } elseif (-not ($json -or $excel)) {


            $open = Read-Host "Log bestand openen? (J/N)"


            if ($open -eq "J") { Invoke-Item $exportFile }


        }





    } catch {


        Write-Host "Export mislukt: $_" -ForegroundColor Red


    }


}





#====================== Dispatch MAC Search ==========================





function Show-DispatchSearch($server) {


    while ($true) {


        Clear-Host


        Write-Host "Dispatch Log Search - $($server.Name)" -ForegroundColor Green


        Write-Host "==================================" -ForegroundColor White


        $searchTerm = Read-Host "Voer zoekterm in (bv. laatste 4 MAC tekens '21:A4', of 'PEERED', of Regex)"


        if (-not $searchTerm) { return }





        # Check of het een MAC-adres fragment is (hexadecimaal, 2-4 tekens)


        if ($searchTerm -match '^[0-9A-Fa-f]{2}[:\-]?[0-9A-Fa-f]{2}$') {


             $searchQuery = "2c971703" + ($searchTerm -replace "[:\-]", "")


             Write-Host "Gedetecteerd als MAC-fragment. Zoeken naar volledig MAC: $searchQuery" -ForegroundColor Yellow


        } else {


             $searchQuery = $searchTerm


             Write-Host "Zoeken naar tekst/regex: $searchQuery" -ForegroundColor Yellow


        }





        $options = @("Eenmalig zoeken", "Live volgen (tail -f)", "Terug")


        $selection = Show-Menu -Title "Kies modus voor '$searchQuery'" -Options $options





        if ($selection -eq 2) { return }





        # Gebruik -E voor Extended Regex support


        if ($selection -eq 0) {


            ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "grep -Ei '$searchQuery' /var/log/dispatch/dispatch.log | tail -n 20"


            Pause


        }


        elseif ($selection -eq 1) {


                    Write-Host "Live volgen gestart, druk CTRL+C om te stoppen..." -ForegroundColor Cyan


            ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "tail -f /var/log/dispatch/dispatch.log | grep -Ei --line-buffered --color=always '$searchQuery'"


        }


    }


}





#====================== ICYCCAppAPI Log Browser ==========================





function Show-IcyCCAppAPILog($server) {


    while ($true) {


        Clear-Host


        Write-Host "ICYCCAppAPI Log Browser - $($server.Name)" -ForegroundColor Green


        Write-Host "==================================" -ForegroundColor White





        $files = ssh -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "ls -1 /var/lib/wildfly/production/standalone/log/" 2>$null


        if (-not $files) {


            Write-Host "Geen logbestanden gevonden!" -ForegroundColor Red


            Pause


            return


        }





        $menuOptions = @()


        $menuOptions += $files


        $menuOptions += "Terug"





        $selection = Show-Menu -Title "Kies een logbestand" -Options $menuOptions -Filter





        if ($selection -eq $files.Count) { return }





        $selectedFile = $files[$selection]


        Write-Host "Geselecteerd: $selectedFile" -ForegroundColor Cyan





        while ($true) {


            $actionOptions = @("Live volgen (tail -f)", "Zoeken (multi-keyword regex)", "Export log", "Terug")


            $actionSelection = Show-Menu -Title "Acties voor $selectedFile" -Options $actionOptions





            if ($actionSelection -eq 3) { break }





            switch ($actionSelection) {


                0 {


                    $keyword = Read-Host "Optioneel zoekfilter voor live (spatie gescheiden)"


                    $pattern = if ($keyword) { ($keyword -split '\s+') -join '|' } else { "" }


                    $tailCmd = "tail -f /var/lib/wildfly/production/standalone/log/$selectedFile"


                    if ($pattern) { $tailCmd += " | grep -Ei --line-buffered --color=always '$pattern'" }


                    Write-Host "Live volgen gestart, CTRL+C om te stoppen..." -ForegroundColor Cyan


                    ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "sudo $tailCmd"


                }


                1 {


                    $keyword = Read-Host "Voer zoekwoorden in (spatie gescheiden)"


                    if (-not $keyword) { continue }


                    $pattern = ($keyword -split '\s+') -join '|'


                    Write-Host "Zoeken naar '$pattern' in $selectedFile..." -ForegroundColor Yellow


                    $remoteFile = "/var/lib/wildfly/production/standalone/log/$selectedFile"


                        # Gebruik variabele om quoting issues te voorkomen


                        # Eenvoudiger pipeline zonder complexe kleurcodes/echo-escapes


                        $cmd = "sudo grep -Ei --color=always '$pattern' $remoteFile | tail -n 100"


                    ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" $cmd


                    $exportChoice = Read-Host "Wil je deze zoekresultaten exporteren? (J/N)"


                    if ($exportChoice -eq "J") {


                        $formatOptions = @("Standaard (.log)", "JSON (.json)", "Excel (.xlsx) - Geformatteerd", "Terug")


                        $formatSelection = Show-Menu -Title "Kies export formaat" -Options $formatOptions





                        if ($formatSelection -eq 3) { continue }





                        $json = $false


                        $excel = $false





                        if ($formatSelection -eq 1) { $json = $true }


                        if ($formatSelection -eq 2) { $excel = $true }





                        Invoke-Export -server $server -logFiles @($selectedFile) -pattern $pattern -json:$json -excel:$excel


                    }


                    Pause


                }


                2 {


                    $timeOptions = @("Laatste 10 minuten", "Laatste 1 uur", "Vandaag", "Afgelopen 7 dagen", "Geen tijdsfilter (hele bestand)", "Terug")


                    $timeSelection = Show-Menu -Title "Kies tijdsfilter" -Options $timeOptions





                    if ($timeSelection -eq 5) { continue }





                    $logFilesToExport = @($selectedFile)





                    switch ($timeSelection) {


                        0 { $timeFilter = (Get-Date).AddMinutes(-10).ToString("yyyy-MM-dd") }


                        1 { $timeFilter = (Get-Date).AddHours(-1).ToString("yyyy-MM-dd") }


                        2 { $timeFilter = (Get-Date).ToString("yyyy-MM-dd") }


                        3 {


                            $timeFilter = ""


                            # Bepaal basisnaam (strip datum indien aanwezig)


                            $baseName = $selectedFile -replace '\.\d{4}-\d{2}-\d{2}$', ''





                            $logFilesToExport = @()


                            # Ophalen van oud naar nieuw (6 dagen geleden t/m vandaag)


                            for ($d = 6; $d -ge 1; $d--) {


                                $dateStr = (Get-Date).AddDays(-$d).ToString("yyyy-MM-dd")


                                $logFilesToExport += "$baseName.$dateStr"


                            }


                            $logFilesToExport += $baseName


                        }


                        4 { $timeFilter = "" }


                        default { $timeFilter = "" }


                    }





                    $formatOptions = @("Standaard (.log)", "JSON (.json)", "Excel (.xlsx) - Geformatteerd", "Terug")


                    $formatSelection = Show-Menu -Title "Kies export formaat" -Options $formatOptions





                    if ($formatSelection -eq 3) { continue }





                    $json = $false


                    $excel = $false





                    if ($formatSelection -eq 1) { $json = $true }


                    if ($formatSelection -eq 2) { $excel = $true }





                    Invoke-Export -server $server -logFiles $logFilesToExport -timeFilter $timeFilter -json:$json -excel:$excel


                    Pause


                }


            }


        }


    }


}





#====================== Service Menu ==========================





function Show-ServiceMenu($server) {


    while ($true) {


        # Services ophalen


        $rawCommand = 'systemctl list-units --type=service --no-pager --no-legend "icycc-*.service"'


        $rawServices = ssh -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" $rawCommand


        $services = $rawServices |


            ForEach-Object {


                $_ -replace '^[●\s]+', '' |


                ForEach-Object { ($_ -split '\s+')[0] }


            } |


            Where-Object { $_ } |


            Sort-Object -Unique





        $menuOptions = @()


        $menuOptions += $services





        $hasDispatch = $false


        $hasAppApi = $false





        if ($server.Name -like "*Dispatch*") {


            $menuOptions += "MAC Log Search (dispatch.log)"


            $hasDispatch = $true


        }


        if ($server.Name -like "*IcyCCAppAPI*") {


            $menuOptions += "Log Browser (ICYCCAppAPI)"


            $hasAppApi = $true


        }


        $menuOptions += "Terug naar servermenu"





        $selection = Show-Menu -Title "Service Menu - $($server.Name)" -Options $menuOptions -Filter





        if ($selection -lt $services.Count) {


            $svcName = $services[$selection]


            Show-ActionMenu $server $svcName


        } else {


            $remainingIndex = $selection - $services.Count


            $currentOptionIndex = 0





            if ($hasDispatch) {


                if ($remainingIndex -eq $currentOptionIndex) {


                    Show-DispatchSearch $server


                    continue


                }


                $currentOptionIndex++


            }





            if ($hasAppApi) {


                if ($remainingIndex -eq $currentOptionIndex) {


                    Show-IcyCCAppAPILog $server


                    continue


                }


                $currentOptionIndex++


            }





            if ($remainingIndex -eq $currentOptionIndex) {


                return


            }


        }


    }


}





#====================== Database Menu Integration ==========================


function Get-PythonExePath {


    # Prefer workspace virtualenvs, fallback to the Python launcher or system python


    $root = if (-not [string]::IsNullOrWhiteSpace($script:ToolkitRoot)) { $script:ToolkitRoot } elseif (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) { $PSScriptRoot } else { (Get-Location).Path }

    $candidates = @(
        (Join-Path $root "python\DBscript\venv\Scripts\python.exe"),
        (Join-Path $root "python\DBscript\.venv\Scripts\python.exe"),
        (Join-Path $root "python\DBscript\virt-dahs\Scripts\python.exe"),
        (Join-Path $root "venv\Scripts\python.exe"),
        (Join-Path $root ".venv\Scripts\python.exe"),
        (Join-Path $root "virt-dahs\Scripts\python.exe")
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) { return $candidate }
    }

    $releaseVenv = Join-Path $root ".venv\Scripts\python.exe"
    if (Test-Path $releaseVenv) { return $releaseVenv }

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher) { return $pyLauncher.Source }

    $py = Get-Command python -ErrorAction SilentlyContinue
    if ($py) { return $py.Source }

    return $null


}

function Get-DbEnvPath {
    $root = if (-not [string]::IsNullOrWhiteSpace($script:ToolkitRoot)) { $script:ToolkitRoot } elseif (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) { $PSScriptRoot } else { (Get-Location).Path }
    $parentRoot = Split-Path -Parent $root
    $candidates = @(
        $script:SharedEnvPath,
        (Join-Path $root "python\DBscript\.env"),
        (Join-Path $root "DBscript\.env"),
        (Join-Path $root ".env"),
        (Join-Path $parentRoot "python\DBscript\.env"),
        (Join-Path $parentRoot "DBscript\.env"),
        (Join-Path $parentRoot ".env")
    ) | Select-Object -Unique

    foreach ($candidate in $candidates) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path $candidate)) {
            return $candidate
        }
    }

    return $null
}

function Import-ToolkitEnv {
    param([switch]$Quiet)

    $envPath = Get-DbEnvPath
    if (-not $envPath) {
        if (-not $Quiet) {
            Write-Host "Geen .env gevonden voor de DB scripts." -ForegroundColor Yellow
        }
        return $false
    }

    foreach ($line in Get-Content -Path $envPath) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) { continue }

        $parts = $trimmed.Split("=", 2)
        $key = $parts[0].Trim()
        $value = $parts[1].Trim().Trim('"').Trim("'")

        if (-not [string]::IsNullOrWhiteSpace($key)) {
            [Environment]::SetEnvironmentVariable($key, $value, 'Process')
        }
    }

    if (-not $Quiet) {
        Write-Host "DB .env geladen vanaf $envPath" -ForegroundColor DarkGray
    }

    return $true
}


function Show-BridgeComlogMenu {
    $py = Get-PythonExePath
    if (-not $py) {
        Write-Host "Python executable not found. Please install Python or adjust the venv path." -ForegroundColor Red
        Pause
        return
    }

    $scriptPath = Join-Path $PSScriptRoot "Bridge TX\Bridge_Comlog_Viewer.py"
    if (-not (Test-Path $scriptPath)) {
        $scriptPath = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "Bridge TX\Bridge_Comlog_Viewer.py"
    }
    if (-not (Test-Path $scriptPath)) {
        Write-Host "Bridge_Comlog_Viewer.py not found at $scriptPath" -ForegroundColor Red
        Pause
        return
    }
    try { $scriptFull = (Resolve-Path $scriptPath).Path } catch { $scriptFull = $scriptPath }

    $dbOptions = @(
        "mysql",
        "mariadb",
        "Cancel"
    )
    $dbSel = Show-Menu -Title "Bridge Comlog - Kies database" -Options $dbOptions
    if ($dbSel -lt 0 -or $dbSel -eq ($dbOptions.Count - 1)) { return }
    $database = $dbOptions[$dbSel]

    $klantDb = Read-Host "Klant DB naam (bijv. nl_voorbeeld)"
    if ([string]::IsNullOrWhiteSpace($klantDb)) {
        Write-Host "Klant DB naam is verplicht." -ForegroundColor Red
        Pause
        return
    }

    $minutesRaw = Read-Host "Aantal minuten terug (default 1440)"
    if ([string]::IsNullOrWhiteSpace($minutesRaw)) { $minutesRaw = "1440" }
    $minutes = 0
    if (-not [int]::TryParse($minutesRaw, [ref]$minutes)) {
        Write-Host "Ongeldig aantal minuten." -ForegroundColor Red
        Pause
        return
    }

    $envLoaded = Import-ToolkitEnv -Quiet
    $envPath = Get-DbEnvPath
    if (-not $envLoaded -or -not $envPath) {
        Write-Host ".env niet gevonden voor Bridge Comlog Viewer." -ForegroundColor Red
        Pause
        return
    }

    $dbUser = [Environment]::GetEnvironmentVariable('DB_USER', 'Process')
    $dbPassword = [Environment]::GetEnvironmentVariable('DB_PASSWORD', 'Process')

    if ([string]::IsNullOrWhiteSpace($dbUser) -or [string]::IsNullOrWhiteSpace($dbPassword)) {
        Write-Host "DB_USER/DB_PASSWORD ontbreken in $envPath" -ForegroundColor Red
        Pause
        return
    }

    $filterInput = Read-Host "Filter (default: ab abab)"
    if ([string]::IsNullOrWhiteSpace($filterInput)) { $filterInput = "" }

    $sortOptions = @(
        "bridge_id (default)",
        "restart",
        "filtered",
        "unfiltered",
        "poll_pct",
        "fail_pct",
        "bridgetype",
        "swversion",
        "Cancel"
    )
    $sortSel = Show-Menu -Title "Sorteer op" -Options $sortOptions
    if ($sortSel -lt 0 -or $sortSel -eq ($sortOptions.Count - 1)) { return }
    if ($sortSel -eq 0) { $sortBy = "bridge_id" } else { $sortBy = $sortOptions[$sortSel] }

    $args = @(
        $database,
        $klantDb,
        $minutes,
        $dbUser,
        $dbPassword,
        $filterInput,
        $sortBy
    )

    Write-Host "Starting Bridge Comlog Viewer..." -ForegroundColor Yellow
    Write-Host "Python: $py" -ForegroundColor Cyan
    Write-Host "Script: $scriptFull" -ForegroundColor Cyan
    Write-Host ("Run: database={0}, klant={1}, minutes={2}, sort={3}" -f $database, $klantDb, $minutes, $sortBy) -ForegroundColor Cyan
    & $py $scriptFull @args
    Pause
}


function Start-PulseCounterOffsetTool {
    $scriptRoot = $PSScriptRoot
    if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
        $scriptRoot = (Get-Location).Path
    }

    $launcherPath = Join-Path $scriptRoot "python\Pulse Counter Offset Tool\start_standalone.ps1"
    if (-not (Test-Path $launcherPath)) {
        Write-Host "start_standalone.ps1 not found at $launcherPath" -ForegroundColor Red
        Pause
        return
    }

    try {
        Write-Host "Start Pulse Counter Offset Tool vanuit de Toolkit..." -ForegroundColor Yellow
        & $launcherPath
        Write-Host "Pulse Counter Offset Tool afgesloten." -ForegroundColor Green
        Pause
    }
    catch {
        Write-Host "Fout bij starten van Pulse Counter Offset Tool: $_" -ForegroundColor Red
        Pause
    }
}

function Show-BridgeScriptsMenu {
    while ($true) {
        $options = @(
            "Bridgebeheer",
            "Bridge health scan",
            "Poll fails scan",
            "Bridge Comlog Viewer",
            "Pulse Counter Offset Tool",
            "Log backup (laatste 14 dagen)",
            "Terug"
        )

        $selection = Show-Menu -Title "Python Scripts" -Options $options

        if ($selection -ge 0 -and $selection -le 5) {
            Import-ToolkitEnv -Quiet | Out-Null
            $py = Get-PythonExePath
            if (-not $py) {
                Write-Host "Python executable not found. Please install Python or adjust the venv path." -ForegroundColor Red
                Pause
                continue
            }
        }

        if ($selection -eq 0) {
            $scriptPath = Join-Path $PSScriptRoot "python\DBscript\db_menu.py"
            if (-not (Test-Path $scriptPath)) {
                Write-Host "db_menu.py not found at $scriptPath" -ForegroundColor Red
                Pause
                continue
            }

            Write-Host "Starting bridge management menu..." -ForegroundColor Yellow
            & $py $scriptPath --manage-bridges
            Pause
        }
        elseif ($selection -eq 1) {
            $scriptPath = Join-Path $PSScriptRoot "python\DBscript\list_bridges_prompt.py"
            if (-not (Test-Path $scriptPath)) {
                Write-Host "list_bridges_prompt.py not found at $scriptPath" -ForegroundColor Red
                Pause
                continue
            }

            Write-Host "Bridge health scan wordt gestart..." -ForegroundColor Yellow
            Push-Location (Split-Path -Parent $scriptPath)
            try {
                & $py $scriptPath --action all --export "./bridge_scan_menu_output" --gap-minutes 20 --window-days 4 --restart-window-threshold 20
            }
            finally {
                Pop-Location
            }
            Pause
        }
        elseif ($selection -eq 2) {
            $scriptPath = Join-Path $PSScriptRoot "python\DBscript\list_bridges_prompt.py"
            if (-not (Test-Path $scriptPath)) {
                Write-Host "list_bridges_prompt.py not found at $scriptPath" -ForegroundColor Red
                Pause
                continue
            }

            Write-Host "Poll fails scan wordt gestart..." -ForegroundColor Yellow
            Push-Location (Split-Path -Parent $scriptPath)
            try {
                & $py $scriptPath --action pollall --export "./pollfail_menu_output" --poll-threshold 15
            }
            finally {
                Pop-Location
            }
            Pause
        }
        elseif ($selection -eq 3) {
            Show-BridgeComlogMenu
        }
        elseif ($selection -eq 4) {
            Start-PulseCounterOffsetTool
        }
        elseif ($selection -eq 5) {
            $scriptPath = Join-Path $PSScriptRoot "python\DBscript\backup_recent_logs.py"
            if (-not (Test-Path $scriptPath)) {
                Write-Host "backup_recent_logs.py not found at $scriptPath" -ForegroundColor Red
                Pause
                continue
            }

            Write-Host "Start backup van database logs over de laatste 14 dagen..." -ForegroundColor Yellow
            & $py $scriptPath --days 14
            Pause
        }
        else {
            return
        }
    }
}


# Database integration: call `db_menu.py` directly from the main menu.





# Database integration: call `db_menu.py` directly from the main menu.





#====================== Action Menu ==========================





function Show-ActionMenu($server, $serviceName) {


    while ($true) {


        $options = @("Status bekijken", "Live logs bekijken", "Herstarten", "Stoppen", "Terug")


        $selection = Show-Menu -Title "Acties voor $serviceName op $($server.Name)" -Options $options





        switch ($selection) {


            0 {


                [Console]::Clear(); try { [Console]::SetCursorPosition(0,0) } catch {}


                Write-Host "Verbinden met $($server.Name)..." -ForegroundColor Yellow


                ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "sudo systemctl status $serviceName --no-pager -l"


                Pause


            }


            1 {


                [Console]::Clear(); try { [Console]::SetCursorPosition(0,0) } catch {}


                # Live-tail met kleur en regex support


                $keyword = Read-Host "Optioneel zoekfilter (spatie gescheiden)"


                Write-Host "Verbinden met $($server.Name)..." -ForegroundColor Yellow


                $pattern = if ($keyword) { ($keyword -split '\s+') -join '|' } else { "" }


                $tailCmd = "journalctl -u $serviceName -f --no-pager"


                if ($pattern) { $tailCmd += " | grep -Ei --line-buffered --color=always '$pattern'" }


                ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "sudo $tailCmd"


            }


            2 {


                [Console]::Clear(); try { [Console]::SetCursorPosition(0,0) } catch {}


                Write-Host "Herstarten $serviceName..." -ForegroundColor Yellow


                ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "sudo systemctl restart $serviceName"


                Pause


            }


            3 {


                [Console]::Clear(); try { [Console]::SetCursorPosition(0,0) } catch {}


                Write-Host "Stoppen $serviceName..." -ForegroundColor Yellow


                ssh -t -o GSSAPIAuthentication=no -i $sshKey "$($server.User)@$($server.HostName)" "sudo systemctl stop $serviceName"


                Pause


            }


            4 { return }


        }


    }


}





#====================== Thermostat Diagnostics ==========================





function Show-ThermostatDiagnostics {


    while ($true) {


        Clear-Host


        Write-Host "Diagnostiek Thermostaat" -ForegroundColor Green


        Write-Host "==================================" -ForegroundColor White


        Write-Host "Vul de parameters in (laat leeg voor standaardwaarde)"





        $db = Read-Host "Database naam (bv. nl_strabrechtsevennen) [Verplicht]"


        if (-not $db) {


            $retry = Read-Host "Database naam is verplicht. Opnieuw proberen? (J/N)"


            if ($retry -eq "N") { return }


            continue


        }





        $loc = Read-Host "Locatie ID (loc) [Standaard: 1]"


        if (-not $loc) { $loc = "1" }





        $locdevice = Read-Host "Buildingtype ID (locdevice) [Standaard: 1]"


        if (-not $locdevice) { $locdevice = "1" }





        # Vaste waarden (devid=2, devtype=20)


        $devid = "2"


        $devtype = "20"





        $deviceid = Read-Host "Device ID (deviceid) [Standaard: 1]"


        if (-not $deviceid) { $deviceid = "1" }





        $dev = Read-Host "Dev (dev) [Standaard: 1]"


        if (-not $dev) { $dev = "1" }





        $url = "https://succurro.icy.nl/mod/rapportage/diagnostiekThermostaat.php?loc=$loc&locdevice=$locdevice&devid=$devid&deviceid=$deviceid&devtype=$devtype&dev=$dev&db=$db"





        Write-Host "Downloading....." -ForegroundColor Cyan





        $exportFolder = Join-Path $env:USERPROFILE "Downloads"


        $fileName = "Diagnostiek_$db_devid$devid.xls"


        $outputPath = Join-Path $exportFolder $fileName





        try {


            Write-Progress -Activity "Diagnostiek Rapport" -Status "Bezig met downloaden..." -PercentComplete 50


            $response = Invoke-WebRequest -Uri $url -OutFile $outputPath -ErrorAction Stop -PassThru


            Write-Progress -Activity "Diagnostiek Rapport" -Status "Voltooid" -PercentComplete 100 -Completed





            Write-Host "[OK] HTTP $([int]$response.StatusCode) - Bestand opgeslagen in: $outputPath" -ForegroundColor Green





            $open = Read-Host "Bestand openen? (J/N)"


            if ($open -eq "J") {


                Invoke-Item $outputPath


            }


        }


        catch {


            Write-Progress -Activity "Diagnostiek Rapport" -Status "Mislukt" -Completed


            $status = "Onbekend"


            if ($_.Exception.Response) {


                $status = [int]$_.Exception.Response.StatusCode


            }


            Write-Host "[ERROR] Fout bij downloaden (HTTP $status): $_" -ForegroundColor Red


        }





        $again = Read-Host "Nog een rapport downloaden? (J/N)"


        if ($again -ne "J") { return }


    }


}





#====================== Location & Devices Report ==========================





function Show-LocationDevicesReport {


    while ($true) {


        Clear-Host


        Write-Host "Rapportage Locaties & Devices" -ForegroundColor Green


        Write-Host "==================================" -ForegroundColor White





        $db = Read-Host "Database naam (bv. nl_wedderbergen) [Verplicht]"


        if (-not $db) {


            $retry = Read-Host "Database naam is verplicht. Opnieuw proberen? (J/N)"


            if ($retry -eq "N") { return }


            continue


        }





        $url = "https://succurro.icy.nl/mod/rapportage/locatiesDevices.php?db=$db"





        Write-Host "Downloading....." -ForegroundColor Cyan





        $exportFolder = Join-Path $env:USERPROFILE "Downloads"


        $baseName = "LocatiesDevices_$db"


        $extension = ".xls"


        $fileName = "$baseName$extension"


        $outputPath = Join-Path $exportFolder $fileName





        # Unieke bestandsnaam genereren als bestand al bestaat (voorkomt lock errors)


        $counter = 1


        while (Test-Path $outputPath) {


            $fileName = "${baseName}_$counter${extension}"


            $outputPath = Join-Path $exportFolder $fileName


            $counter++


        }





        try {


            Write-Progress -Activity "Locaties & Devices Rapport" -Status "Bezig met downloaden..." -PercentComplete 50


            $response = Invoke-WebRequest -Uri $url -OutFile $outputPath -ErrorAction Stop -PassThru


            Write-Progress -Activity "Locaties & Devices Rapport" -Status "Voltooid" -PercentComplete 100 -Completed





            Write-Host "[OK] HTTP $([int]$response.StatusCode) - Bestand opgeslagen in: $outputPath" -ForegroundColor Green





            # Post-processing: Headers opschonen (# weg), dikgedrukt en filters


            Write-Host "Excel bestand nabewerken (Headers opschonen & filters)..." -ForegroundColor Cyan


            try {


                $excelApp = New-Object -ComObject Excel.Application


                $excelApp.Visible = $false


                $excelApp.DisplayAlerts = $false


                $workbook = $excelApp.Workbooks.Open($outputPath)


                $sheet = $workbook.Worksheets.Item(1)





                # Headers staan op rij 5


                $usedRange = $sheet.UsedRange


                $colCount = $usedRange.Columns.Count





                # Loop door headers (rij 5) om # te verwijderen


                for ($c = 1; $c -le $colCount; $c++) {


                    $cell = $sheet.Cells.Item(5, $c)


                    if ($cell.Value2 -ne $null) {


                        $newValue = $cell.Value2.ToString().Replace("#", "").Trim()


                        $cell.Value2 = $newValue


                    }


                }





                # Styling: Dikgedrukt en AutoFilter


                $headerRange = $sheet.Range($sheet.Cells.Item(5, 1), $sheet.Cells.Item(5, $colCount))


                $headerRange.Font.Bold = $true


                $headerRange.AutoFilter() | Out-Null





                # Kolommen aanpassen


                $usedRange.Columns.AutoFit() | Out-Null





                $workbook.Save()


                $workbook.Close()


                $excelApp.Quit()


                [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excelApp) | Out-Null


            } catch {


                Write-Host "Waarschuwing: Kon Excel nabewerking niet uitvoeren (is Excel geïnstalleerd?): $_" -ForegroundColor Yellow


                if ($excelApp) { $excelApp.Quit() }


            }





            $open = Read-Host "Bestand openen? (J/N)"


            if ($open -eq "J") {


                Invoke-Item $outputPath


            }


        }


        catch {


            Write-Progress -Activity "Locaties & Devices Rapport" -Status "Mislukt" -Completed


            $status = "Onbekend"


            if ($_.Exception.Response) {


                $status = [int]$_.Exception.Response.StatusCode


            }


            Write-Host "[ERROR] Fout bij downloaden (HTTP $status): $_" -ForegroundColor Red


        }





        $again = Read-Host "Nog een rapport downloaden? (J/N)"


        if ($again -ne "J") { return }


    }


}





#====================== Node.js 4850CM Tools Menu ==========================





function Show-NodeToolsMenu {


    $nodeScriptDir = Join-Path $script:ToolkitRoot "Nodejs 4850cm database tools"
    if (-not (Test-Path $nodeScriptDir)) {
        $nodeScriptDir = "c:\Users\h.nijdam\OneDrive - I.C.Y. B.V\Scripts\Nodejs 4850cm database tools"
    }


    $nodeScript = Join-Path $nodeScriptDir "index.js"





    if (-not (Test-Path $nodeScript)) {


        Write-Error "Node.js script niet gevonden op: $nodeScript"


        Pause


        return


    }





    while ($true) {


        # Hoofdmenu database opties
        $dbOptions = @(


            "1. MySQL Database (icyccdb.icy.nl)",


            "2. MariaDB Database (icyccdb02.icy.nl)",


            "Q. Terug naar Hoofdmenu"

        )





        $dbIndex = Show-Menu -Title "************ 4850CM DB TOOLS TOOLKIT (PowerShell) ************" -Options $dbOptions -Filter





        if ($dbIndex -eq 2) { return } # Q





        $dbArg = ""


        $dbName = ""





        if ($dbIndex -eq 0) { $dbArg = "A"; $dbName = "MySQL" }


        elseif ($dbIndex -eq 1) { $dbArg = "B"; $dbName = "MariaDB" }





        while ($true) {


            $actionOptions = @(


                "A. (!) Timedtask toevoegen alle organisaties",


                "B. (!) Settings toevoegen alle organisaties",


                "C. (!) Modules omzetten naar 60 seconden schakeltijd (sendlist)",


                "D. Check de timedtask van setting ICY4850HARDWARECHECK",


                "E. Rapport & huidige status ICY4850HARDWAREISSUE",


                "F. Rapport & huidige status (min & max) ICY4850CM",


                "G. Zoek organisatie (Regex)",


                "H. Check & wijzig schakeltijden (alle organisaties)",
                "Q. Terug naar Database Selectie"


            )





            $actionIndex = Show-Menu -Title "Geselecteerde Database: $dbName" -Options $actionOptions

            # Map menu index to action letter including new H option
            $letters = @("A","B","C","D","E","F","G","H","Q")
            if ($actionIndex -ge 0 -and $actionIndex -lt $letters.Count) { $actionInput = $letters[$actionIndex] } else { $actionInput = "Q" }












            # Map index to Action Letter


            $actions = @("A", "B", "C", "D", "E", "F", "G", "H")


            $actionInput = $actions[$actionIndex]
            # Override mapping to include H and Q from $letters (ensures H works)
            if ($letters -and $actionIndex -ge 0 -and $actionIndex -lt $letters.Count) { $actionInput = $letters[$actionIndex] }
            if ($actionInput -eq 'Q') { break }





            $extraArg = ""


            $extraArg2 = ""





            if ($actionInput -eq 'C') {


                Write-Host "Je hebt Optie C geselecteerd: Schakeltijden aanpassen." -ForegroundColor Yellow


                $extraArg = Read-Host "Voer Database Schema (Organisatie) in"


                if ([string]::IsNullOrWhiteSpace($extraArg)) {


                    Write-Host "Schema is verplicht!" -ForegroundColor Red


                    Start-Sleep -Seconds 2


                    continue


                }





                # Dry Run Menu


                $dryRunOptions = @("Ja (Dry-Run - Veilig)", "Nee (Live - Wijzigingen doorvoeren)", "Annuleren")


                $dryRunIndex = Show-Menu -Title "Dry-Run modus inschakelen?" -Options $dryRunOptions





                if ($dryRunIndex -eq 2) { continue }





                if ($dryRunIndex -eq 0) {


                    $extraArg2 = "true"


                    Write-Host "Dry-Run Modus INGESCHAKELD. Er worden geen wijzigingen gemaakt." -ForegroundColor Magenta


                } else {


                    Write-Host "Dry-Run Modus UITGESCHAKELD. Wijzigingen WORDEN doorgevoerd!" -ForegroundColor Red


                }





                $confirm = Read-Host "Weet je zeker dat je door wilt gaan? (type 'ja' om te bevestigen)"


                if ($confirm -ne 'ja') {


                    Write-Host "Geannuleerd." -ForegroundColor Yellow


                    Start-Sleep -Seconds 1


                    continue


                }


            }


            elseif ($actionInput -eq 'G') {


                $extraArg = Read-Host "Voer Zoekterm in (Regex)"


            }


            elseif ($actionInput -in 'A','B') {


                 $confirm = Read-Host "Deze actie wijzigt data. Type 'ja' om door te gaan"


                 if ($confirm -ne 'ja') { continue }


            }





            # Execute Node Script


            Write-Host "Node.js script uitvoeren..." -ForegroundColor Cyan





            Push-Location -Path $nodeScriptDir


            try {


                # Construct args for Node worker-mode: always pass DB + action.
                $argsList = @($dbArg, $actionInput)
                if ($actionInput -eq 'H') {
                    $argsList += '-i'
                }
                if (![string]::IsNullOrEmpty($extraArg)) { $argsList += $extraArg }
                if (![string]::IsNullOrEmpty($extraArg2)) { $argsList += $extraArg2 }






                Write-Host "Uitvoeren: $nodeScript (CWD: $nodeScriptDir)" -ForegroundColor Cyan
                Write-Host ("Node versie: " + (& node -v)) -ForegroundColor Cyan
                Write-Host ("Args: " + ($argsList -join ' ')) -ForegroundColor Cyan

                & node index.js $argsList


            } finally {


                Pop-Location


            }





            Write-Host "Klaar. Druk op een toets om door te gaan..."


            $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")


        }


    }


}





#====================== Main Menu ==========================





function Show-MainMenu {


    while ($true) {


        $menuOptions = @()


        foreach ($srv in $servers) {


            $menuOptions += $srv.Name


        }


        $menuOptions += "Diagnostiek Thermostaat (HTTP)"


        $menuOptions += "Rapportage Locaties & Devices (HTTP)"


        $menuOptions += "ICY4850CM Database Tools (Node.js)"


        $menuOptions += "Python Scripts"


        $menuOptions += "Afsluiten"





        $selection = Show-Menu -Title "ICY Toolkit - Hoofdmenu" -Options $menuOptions -Filter





        if ($selection -lt $servers.Count) {


            Show-ServiceMenu $servers[$selection]


        }


        elseif ($selection -eq $servers.Count) {


            Show-ThermostatDiagnostics


        }


        elseif ($selection -eq ($servers.Count + 1)) {


            Show-LocationDevicesReport


        }


        elseif ($selection -eq ($servers.Count + 2)) {


            Show-NodeToolsMenu


        }


        elseif ($selection -eq ($servers.Count + 3)) {
            Show-BridgeScriptsMenu
        }


        elseif ($selection -eq ($servers.Count + 4)) {


            break


        }


    }


}





<#
# -----------------------------------------------------------------


# ICY decode helper: Invoke-ICYDecode


# Adds a convenient wrapper to call the Python `icy_gui.py` decoder


# Usage examples:


#   Invoke-ICYDecode -Hex 20030C0C0A10131515


#   Invoke-ICYDecode -DbPrompt -NoGui


#   icy-decode 20030C0C0A10131515


# -----------------------------------------------------------------


function Invoke-ICYDecode {


    [CmdletBinding()]


    param(


        [Parameter(Position=0, ValueFromPipeline=$true)]


        [CmdletBinding()]
        param(
            [Parameter(Position=0, ValueFromPipeline=$true)]
            [string]$Hex,
            [switch]$DbPrompt,
            [switch]$NoGui,
            [switch]$Copy,
            [switch]$SummaryOnly,
            [string]$Device
        )
        # Prefer toolkit helper to locate Python; fall back to 'python' in PATH
        $py = Get-PythonExePath
        if (-not $py) { $py = 'python' }
        # Script path relative to the Toolkit folder
        $scriptPath = Join-Path $PSScriptRoot "..\python\icy_gui.py"
        if (-not (Test-Path $scriptPath)) {
            $scriptPath = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) '..\python\icy_gui.py'
        }
        try { $scriptFull = (Resolve-Path $scriptPath).Path } catch { $scriptFull = $scriptPath }
        $args = @()
        if ($DbPrompt) { $args += '--db-prompt' }
        if ($NoGui) { $args += '--nogui' }
        if ($Copy) { $args += '--copy' }
        if ($SummaryOnly) { $args += '--summary-only' }
        if ($Device) {
            $args += '--device'
            $args += $Device
        }
        if ($Hex) { $args += $Hex }
        Write-Host "Invoking: $py $scriptFull $($args -join ' ')" -ForegroundColor Cyan
        try {
            & $py $scriptFull @args
        } catch {
            Write-Host "Fout bij uitvoeren van python decoder: $_" -ForegroundColor Red
        }
        }
}

    if (-not (Test-Path $scriptPath)) {


        $scriptPath = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) '..\python\icy_gui.py'


    }


    try { $scriptFull = (Resolve-Path $scriptPath).Path } catch { $scriptFull = $scriptPath }





    $args = @()


    if ($DbPrompt) { $args += '--db-prompt' }


    if ($NoGui) { $args += '--nogui' }


    if ($Copy) { $args += '--copy' }


    if ($SummaryOnly) { $args += '--summary-only' }


    if ($Device) {


        $args += '--device'


        $args += $Device


    }


    if ($Hex) { $args += $Hex }





    Write-Host "Invoking: $py $scriptFull $($args -join ' ')" -ForegroundColor Cyan


    try {


        & $py $scriptFull @args


    } catch {


        Write-Host "Fout bij uitvoeren van python decoder: $_" -ForegroundColor Red


    }


}





# Short aliases


Set-Alias -Name icy-decode -Value Invoke-ICYDecode -Scope Global -ErrorAction SilentlyContinue


Set-Alias -Name icy -Value Invoke-ICYDecode -Scope Global -ErrorAction SilentlyContinue





#>
# Start menu

Import-ToolkitEnv -Quiet | Out-Null
if ($env:SSH_KEY_PATH) {
    $sshKey = $env:SSH_KEY_PATH
}
elseif ($env:SSH_KEY) {
    $sshKey = $env:SSH_KEY
}

Show-MainMenu


Write-Host "Bedankt voor het gebruiken van de ICY Toolkit!" -ForegroundColor Green


Start-Sleep -Seconds 1


[Environment]::Exit(0)
    # Prefer toolkit helper to locate Python; fall back to 'python' in PATH

    $py = Get-PythonExePath

    if (-not $py) { $py = 'python' }

