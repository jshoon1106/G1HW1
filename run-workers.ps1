[CmdletBinding()]
param(
    [switch]$NoPause
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = $PSScriptRoot
. (Join-Path $projectRoot 'run-workers-ui.ps1')
$masterHost = "32.236.94.251"
$masterPort = 5000
$currentJarPath = Join-Path $projectRoot "distributed-kv.jar"
$releaseId = "{0}-{1}" -f (Get-Date -Format "yyyyMMdd-HHmmss"), ([Guid]::NewGuid().ToString("N").Substring(0, 8))

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList
    )

    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "명령 실행 실패(exit=$LASTEXITCODE): $FilePath"
    }
}

function Get-NativeCommandResult {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = (& $FilePath @ArgumentList 2>&1 | Out-String).Trim()
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    [pscustomobject]@{ Output = $output; ExitCode = $exitCode }
}

function Get-JavaRuntime {
    $javaName = if ([System.Environment]::OSVersion.Platform -eq [System.PlatformID]::Win32NT) {
        "java.exe"
    } else {
        "java"
    }
    if ($env:JAVA_HOME) {
        $java = Join-Path (Join-Path $env:JAVA_HOME "bin") $javaName
        if (Test-Path -LiteralPath $java) { return $java }
    }

    $javaCommand = Get-Command java -ErrorAction SilentlyContinue
    if ($javaCommand) { return $javaCommand.Source }
    throw "Java 17 이상을 찾지 못했습니다."
}

function Test-TcpEndpoint {
    param([string]$HostName, [int]$TargetPort)

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $connection = $client.ConnectAsync($HostName, $TargetPort)
        if (-not $connection.Wait(1000)) { return $false }
        if ($connection.IsFaulted) { return $false }
        return $client.Connected
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

function Test-LocalListenPortAvailable {
    param([int]$TargetPort)

    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback, $TargetPort)
    try {
        $listener.Start()
        return $true
    } catch {
        return $false
    } finally {
        $listener.Stop()
    }
}

function Copy-RunLogs {
    param(
        [string]$Destination,
        [switch]$RemoveSource
    )

    $sources = @(@("Master.txt", "Worker1.txt", "Worker2.txt", "Worker3.txt", "Worker4.txt") |
        ForEach-Object { Join-Path $projectRoot $_ } |
        Where-Object { Test-Path -LiteralPath $_ })
    if ($sources.Count -eq 0) { return }

    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    foreach ($source in $sources) {
        Copy-Item -LiteralPath $source -Destination $Destination -Force
        if ($RemoveSource) { Remove-Item -LiteralPath $source -Force }
    }
}

function Invoke-WorkerMain {
    Write-Host "[1/6] Java 17 이상을 확인합니다."
    $java = Get-JavaRuntime
    $javaVersionResult = Get-NativeCommandResult -FilePath $java -ArgumentList @("-version")
    $javaVersion = $javaVersionResult.Output
    if ($javaVersionResult.ExitCode -ne 0 -or $javaVersion -notmatch 'version\s+"?(\d+)' -or [int]$Matches[1] -lt 17) {
        throw "Java 17 이상이 필요합니다: $javaVersion"
    }
    Write-Host "      Java 실행 파일: $java"

    Write-Host "[2/6] distributed-kv.jar 파일을 확인합니다."
    if (-not (Test-Path -LiteralPath $currentJarPath -PathType Leaf)) {
        throw "현재 JAR 파일을 찾을 수 없습니다: $currentJarPath"
    }

    Write-Host "[3/6] Worker P2P 포트 6001~6004를 확인합니다."
    foreach ($workerPort in 6001..6004) {
        if (-not (Test-LocalListenPortAvailable -TargetPort $workerPort)) {
            throw "로컬 Worker 포트 $workerPort 를 이미 사용 중입니다."
        }
    }

    Write-Host "[4/6] Master $masterHost`:$masterPort 연결을 기다립니다(최대 30초)."
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    $nextNotice = [DateTime]::UtcNow
    while (-not (Test-TcpEndpoint -HostName $masterHost -TargetPort $masterPort)) {
        if ([DateTime]::UtcNow -ge $deadline) {
            throw "Master $masterHost`:$masterPort 에 연결할 수 없습니다. Master 실행 상태와 방화벽을 확인하세요."
        }
        if ([DateTime]::UtcNow -ge $nextNotice) {
            $remaining = [Math]::Max(0, [Math]::Ceiling(($deadline - [DateTime]::UtcNow).TotalSeconds))
            Write-Host "      연결 대기 중... 남은 시간 약 $remaining 초"
            $nextNotice = [DateTime]::UtcNow.AddSeconds(5)
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Host "      Master 연결 확인 완료"

    $detailed = Read-DetailedOutputChoice
    Write-Host ('화면 모드: ' + $(if ($detailed) { '상세 출력' } else { '진행 상태 + 최종 요약' }))

    Write-Host "[5/6] 기존 실행 로그를 보관합니다."
    $logsRoot = Join-Path $projectRoot "logs"
    $previousLogDir = Join-Path $logsRoot "before-$releaseId"
    Copy-RunLogs -Destination $previousLogDir -RemoveSource

    Write-Host "[6/6] Worker 1~4를 실행합니다."
    $completedLogDir = Join-Path $logsRoot $releaseId
    $runResult = [pscustomobject]@{ ExitCode = 1; HadErrors = $true; Seconds = 0.0 }
    try {
        $runResult = Invoke-WorkerConsole -Java $java -JarPath $currentJarPath `
            -WorkingDirectory $projectRoot -MasterAddress $masterHost -Port $masterPort `
            -Detailed $detailed -Destination $completedLogDir
    } finally {
        Copy-RunLogs -Destination $completedLogDir
        $verification = Write-WorkerSummary -Directory $completedLogDir -ExitCode $runResult.ExitCode `
            -HadErrors $runResult.HadErrors -Seconds $runResult.Seconds
    }
    if ($verification -eq 'FAILED') { throw "실행 결과 확인 실패. $completedLogDir 의 로그와 summary.txt를 확인해주세요." }
    if ($verification -eq 'PARTIAL') { Write-Host '[확인 필요] 전체 완료를 확인하지 못했습니다. Master.txt 수신 및 내용을 확인해주세요.' }
}

try {
    Invoke-WorkerMain
    if (-not $NoPause) {
        [void](Read-Host "실행이 완료되었습니다. 창을 닫으려면 Enter 키를 누르세요")
    }
} catch {
    Write-Host ""
    Write-Host "[오류] Worker 실행에 실패했습니다." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    if (-not $NoPause) {
        [void](Read-Host "창을 닫으려면 Enter 키를 누르세요")
    }
    exit 1
}
