# Fresh start for the demo: wipe all data, start the stack, fill the history at high speed.
#
#   powershell -ExecutionPolicy Bypass -File .\demo_reset.ps1
#   powershell -ExecutionPolicy Bypass -File .\demo_reset.ps1 -FillEnd 2018-08-01
#
# Afterwards start the LIVE part in front of the examiners (1 simulated day = 1 real minute):
#   docker compose run --rm --name producer producer --start 2018-07-20

param(
    [string]$FillStart = "2018-05-20",
    [string]$FillEnd   = "2018-07-20"
)

Write-Host "1/4  Removing old containers and ALL data (Kafka topics, database, checkpoints)..." -ForegroundColor Cyan
docker compose down -v

Write-Host "2/4  Building the image (fast if nothing changed)..." -ForegroundColor Cyan
docker compose build
if ($LASTEXITCODE -ne 0) { Write-Host "Build failed - stop here and check the error above." -ForegroundColor Red; exit 1 }

Write-Host "3/4  Starting all services..." -ForegroundColor Cyan
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Host "docker compose up failed." -ForegroundColor Red; exit 1 }

Write-Host "     Waiting until the 3 scorers have joined their consumer group..." -ForegroundColor Cyan
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    $logs = docker compose logs scorer-1 scorer-2 scorer-3 2>$null | Out-String
    $n = ([regex]::Matches($logs, "ASSIGNED")).Count
    if ($n -ge 3) { $ready = $true; break }
    Start-Sleep -Seconds 2
}
if (-not $ready) { Write-Host "Scorers not ready after 2 minutes - check: docker compose logs scorer-1" -ForegroundColor Red; exit 1 }
Start-Sleep -Seconds 5

Write-Host "4/4  Filling history $FillStart -> $FillEnd at 1 simulated day per second..." -ForegroundColor Cyan
docker compose run --rm --name producer producer --start $FillStart --end $FillEnd --speed 86400

Write-Host ""
Write-Host "Ready. Open Grafana: http://localhost:3000  (admin / admin)" -ForegroundColor Green
Write-Host "Start the LIVE stream now with:" -ForegroundColor Green
Write-Host "  docker compose run --rm --name producer producer --start $FillEnd" -ForegroundColor Yellow
