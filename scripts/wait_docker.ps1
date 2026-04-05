for ($i = 0; $i -lt 12; $i++) {
    Start-Sleep -Seconds 10
    $result = docker info 2>&1 | Out-String
    if ($result -match 'Server Version') {
        Write-Host "Docker is ready!" -ForegroundColor Green
        exit 0
    }
    Write-Host "Waiting for Docker... ($($i * 10)s)"
}
Write-Host "Docker failed to start after 120s" -ForegroundColor Red
exit 1
