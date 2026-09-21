[CmdletBinding()]
param([string]$MasterHost = '32.236.94.251', [int]$Port = 5000, [switch]$NoPause)
$ErrorActionPreference = 'Stop'
$exitCode = 1
try {
    $javaName = if ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { 'java.exe' } else { 'java' }
    $candidates = @()
    if ($env:JAVA_HOME) { $candidates += Join-Path (Join-Path $env:JAVA_HOME 'bin') $javaName }
    $candidates += @(Get-Command $javaName -CommandType Application -All -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source)
    $java = $null
    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) { continue }
        $savedPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            $version = (& $candidate -version 2>&1 | Out-String)
            $ok = $LASTEXITCODE -eq 0
        } finally { $ErrorActionPreference = $savedPreference }
        if ($ok -and $version -match 'version\s+"?(\d+)' -and [int]$Matches[1] -ge 17) { $java = $candidate; break }
    }
    if (-not $java) { throw 'Java 17 이상이 필요합니다. Java 설치 및 JAVA_HOME/PATH를 확인하세요. 실행에는 javac가 필요하지 않습니다.' }
    $launcher = Join-Path $PSScriptRoot 'worker-launcher.jar'
    if (-not (Test-Path -LiteralPath $launcher -PathType Leaf)) { throw 'worker-launcher.jar가 없습니다. README의 도우미 빌드 방법을 확인하세요.' }
    Write-Host "Java: $java"
    $savedEncoding = [Console]::OutputEncoding
    try {
        [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
        & $java '-Dfile.encoding=UTF-8' -jar $launcher $MasterHost $Port
        $exitCode = $LASTEXITCODE
    } finally { [Console]::OutputEncoding = $savedEncoding }
} catch {
    Write-Host ('[오류] ' + $_.Exception.Message) -ForegroundColor Red
    $exitCode = 1
} finally {
    if (-not $NoPause) { [void](Read-Host '창을 닫으려면 Enter 키를 누르세요') }
}
exit $exitCode
