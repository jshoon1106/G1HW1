$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = $PSScriptRoot
$ec2Host = "32.236.94.251"
$ec2User = "ec2-user"
$port = 5000
$remoteRoot = "/opt/distributed-kv"
$serviceName = "distributed-kv-master"
$keyPath = Join-Path $projectRoot "temp\master-node-key.pem"
$currentJarPath = Join-Path $projectRoot "distributed-kv.jar"
$releaseId = "{0}-{1}" -f (Get-Date -Format "yyyyMMdd-HHmmss"), ([Guid]::NewGuid().ToString("N").Substring(0, 8))
$buildRoot = Join-Path $projectRoot "tmp\deploy-$releaseId"
$classesDir = Join-Path $buildRoot "classes"
$builtJarPath = Join-Path $buildRoot "distributed-kv-$releaseId.jar"

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

function Get-JavaToolchain {
    param([switch]$RequireCompiler)

    $homes = [System.Collections.Generic.List[string]]::new()
    if ($env:JAVA_HOME) { $homes.Add($env:JAVA_HOME) }
    foreach ($root in @(
        "C:\Program Files\Eclipse Adoptium",
        "C:\Program Files\Java",
        "C:\Program Files\Microsoft",
        "C:\Program Files\Amazon Corretto"
    )) {
        if (Test-Path -LiteralPath $root) {
            Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue |
                Sort-Object Name -Descending |
                ForEach-Object { $homes.Add($_.FullName) }
        }
    }

    foreach ($jdkHome in $homes) {
        $java = Join-Path $jdkHome "bin\java.exe"
        $javac = Join-Path $jdkHome "bin\javac.exe"
        $jar = Join-Path $jdkHome "bin\jar.exe"
        if ((Test-Path -LiteralPath $java) -and
            (-not $RequireCompiler -or
                ((Test-Path -LiteralPath $javac) -and (Test-Path -LiteralPath $jar)))) {
            return [pscustomobject]@{ Java = $java; Javac = $javac; Jar = $jar }
        }
    }

    $javaCommand = Get-Command java -ErrorAction SilentlyContinue
    $javacCommand = Get-Command javac -ErrorAction SilentlyContinue
    $jarCommand = Get-Command jar -ErrorAction SilentlyContinue
    if ($javaCommand -and
        (-not $RequireCompiler -or ($javacCommand -and $jarCommand))) {
        return [pscustomobject]@{
            Java = $javaCommand.Source
            Javac = if ($javacCommand) { $javacCommand.Source } else { $null }
            Jar = if ($jarCommand) { $jarCommand.Source } else { $null }
        }
    }

    if ($RequireCompiler) { throw "Java JDK 17 이상을 찾지 못했습니다." }
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

    $sources = @("Master.txt", "Worker1.txt", "Worker2.txt", "Worker3.txt", "Worker4.txt") |
        ForEach-Object { Join-Path $projectRoot $_ } |
        Where-Object { Test-Path -LiteralPath $_ }
    if ($sources.Count -eq 0) { return }

    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    foreach ($source in $sources) {
        Copy-Item -LiteralPath $source -Destination $Destination -Force
        if ($RemoveSource) { Remove-Item -LiteralPath $source -Force }
    }
}

function Invoke-LogVerification {
    param([string]$LogDirectory)

    $masterPath = Join-Path $LogDirectory "Master.txt"
    $workerPaths = 1..4 | ForEach-Object { Join-Path $LogDirectory "Worker$_.txt" }

    if (-not (Test-Path -LiteralPath $masterPath)) {
        throw "Master.txt not found: $masterPath"
    }
    foreach ($path in $workerPaths) {
        if (-not (Test-Path -LiteralPath $path)) {
            throw "Worker log not found: $path"
        }
    }

    $successMatches = Select-String -Path $masterPath -Pattern '\| RESULT \| SUCCESS \| KV\[(\d{4})\]'
    $successIds = @($successMatches | ForEach-Object { $_.Matches[0].Groups[1].Value })
    $uniqueSuccessIds = @($successIds | Sort-Object -Unique)
    $kvDumpCount = @(Select-String -Path $masterPath -Pattern '\| KV \| SUCCESS \| Key=[0-9a-f]{4}, Value=\d+$').Count
    $p2pEventLine = Select-String -Path $masterPath -Pattern 'P2P 부하 분산 수: (\d+)' |
        Select-Object -Last 1

    $p2pSent = 0
    $p2pReceived = 0
    $p2pSentEvents = 0
    $p2pReceivedEvents = 0
    $workerSuccess = 0
    $maximumQueue = 0
    $requiredWorkerStats = $true
    foreach ($path in $workerPaths) {
        $tail = Get-Content -LiteralPath $path -Tail 30
        $tailText = $tail -join "`n"
        foreach ($line in $tail) {
            if ($line -match 'P2P 작업 전송/수신: (\d+) / (\d+)') {
                $p2pSent += [int]$Matches[1]
                $p2pReceived += [int]$Matches[2]
            }
            if ($line -match 'P2P 전송 이벤트\(해당 Worker\): (\d+)') {
                $p2pSentEvents += [int]$Matches[1]
            }
            if ($line -match 'P2P 수신 이벤트\(해당 Worker\): (\d+)') {
                $p2pReceivedEvents += [int]$Matches[1]
            }
            if ($line -match '\| STAT \| SUCCESS \| 성공: (\d+)') {
                $workerSuccess += [int]$Matches[1]
            }
        }
        $requiredWorkerStats = $requiredWorkerStats -and
            $tailText.Contains('성공:') -and
            $tailText.Contains('실패:') -and
            $tailText.Contains('평균 대기 시간:') -and
            $tailText.Contains('장애 재할당 발생(해당 Worker):') -and
            $tailText.Contains('P2P 전송 이벤트(해당 Worker):') -and
            $tailText.Contains('P2P 수신 이벤트(해당 Worker):') -and
            $tailText.Contains('전체 수행 시간:')
        $queueMatches = Select-String -Path $path -Pattern '대기열=(\d+)/10|크기=(\d+)/10'
        foreach ($match in $queueMatches) {
            $value = if ($match.Matches[0].Groups[1].Value) {
                [int]$match.Matches[0].Groups[1].Value
            } else {
                [int]$match.Matches[0].Groups[2].Value
            }
            if ($value -gt $maximumQueue) { $maximumQueue = $value }
        }
    }

    $lastFailureWorker = @{}
    $sameWorkerRetryCount = 0
    foreach ($line in Get-Content -LiteralPath $masterPath) {
        if ($line -match '\| RESULT \| FAIL \| KV\[(\d{4})\] 워커(\d+) 처리 실패') {
            $lastFailureWorker[$Matches[1]] = [int]$Matches[2]
            continue
        }
        if ($line -match '\| DISTRIB \| INFO \| KV\[(\d{4})\] -> 워커(\d+) 배정.*우선 재시도') {
            $taskId = $Matches[1]
            $workerId = [int]$Matches[2]
            if ($lastFailureWorker.ContainsKey($taskId)) {
                if ($lastFailureWorker[$taskId] -eq $workerId) {
                    $sameWorkerRetryCount++
                }
                $lastFailureWorker.Remove($taskId)
            }
        }
    }

    $masterTailText = (Get-Content -LiteralPath $masterPath -Tail 40) -join "`n"
    $requiredMasterStats =
        $masterTailText.Contains('총 성공:') -and
        $masterTailText.Contains('총 실패(재시도 전):') -and
        $masterTailText.Contains('평균대기=') -and
        $masterTailText.Contains('장애 재할당 수:') -and
        $masterTailText.Contains('P2P 부하 분산 수:') -and
        $masterTailText.Contains('전체 수행 시간:')

    $p2pEvents = if ($p2pEventLine) {
        [int]$p2pEventLine.Matches[0].Groups[1].Value
    } else {
        0
    }

    $checks = [ordered]@{
        SuccessLines = $successIds.Count -eq 5000
        UniqueSuccessIds = $uniqueSuccessIds.Count -eq 5000
        KvDumpLines = $kvDumpCount -eq 5000
        WorkerSuccessTotal = $workerSuccess -eq 5000
        DifferentWorkerRetry = $sameWorkerRetryCount -eq 0
        RequiredStatistics = $requiredWorkerStats -and $requiredMasterStats
        P2PEventsOccurred = $p2pEvents -gt 0
        P2PEventSentReceivedMatch = $p2pSentEvents -eq $p2pReceivedEvents
        P2PEventMasterMatch = $p2pSentEvents -eq $p2pEvents
        P2PSentReceivedMatch = $p2pSent -eq $p2pReceived
        QueueLimitRespected = $maximumQueue -le 10
    }

    $checks.GetEnumerator() | ForEach-Object {
        "{0}: {1}" -f $_.Key, $(if ($_.Value) { "PASS" } else { "FAIL" })
    }
    "P2P tasks: $p2pSent sent / $p2pReceived received"
    "P2P events: $p2pSentEvents sent / $p2pReceivedEvents received / $p2pEvents master"
    "Same-worker retries: $sameWorkerRetryCount"
    "Maximum observed queue: $maximumQueue"

    if ($checks.Values -contains $false) {
        throw "통합 실행 로그 검증에 실패했습니다."
    }
}

$remoteDeployScript = @'
#!/usr/bin/env bash
set -euo pipefail

UPLOADED_JAR=$1
INSTALL_ROOT=$2
SERVICE_NAME=$3
SERVICE_USER=$4
PORT=$5
RELEASE_ID=$6

if [[ ! -f "$UPLOADED_JAR" ]]; then
    echo "uploaded JAR not found: $UPLOADED_JAR" >&2
    exit 2
fi
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
    echo "service user does not exist: $SERVICE_USER" >&2
    exit 2
fi
if ! command -v java >/dev/null 2>&1; then
    sudo dnf install -y java-17-amazon-corretto-headless
fi

JAVA_BIN=$(command -v java)
JAVA_VERSION=$("$JAVA_BIN" -version 2>&1 | head -n 1)
if [[ ! "$JAVA_VERSION" =~ \"([0-9]+) ]] || (( BASH_REMATCH[1] < 17 )); then
    echo "Java 17 or newer is required: $JAVA_VERSION" >&2
    exit 1
fi

SERVICE_GROUP=$(id -gn "$SERVICE_USER")
RELEASE_DIR="$INSTALL_ROOT/releases/$RELEASE_ID"
RUNTIME_DIR="$INSTALL_ROOT/runtime"
CURRENT_LINK="$INSTALL_ROOT/current"
UNIT_PATH="/etc/systemd/system/$SERVICE_NAME.service"
UNIT_TEMP="/tmp/$SERVICE_NAME-$RELEASE_ID.service"
PREVIOUS_RELEASE=""
if [[ -L "$CURRENT_LINK" ]]; then
    PREVIOUS_RELEASE=$(readlink -f "$CURRENT_LINK" || true)
fi

cleanup() {
    rm -f "$UPLOADED_JAR" "$UNIT_TEMP" "$0"
}
trap cleanup EXIT

sudo install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0755 "$RELEASE_DIR" "$RUNTIME_DIR"
sudo install -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0644 \
    "$UPLOADED_JAR" "$RELEASE_DIR/distributed-kv.jar"

cat > "$UNIT_TEMP" <<EOF
[Unit]
Description=Distributed KV Master
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$RUNTIME_DIR
Environment=LANG=C.UTF-8
ExecStart=$JAVA_BIN -Dfile.encoding=UTF-8 -jar $CURRENT_LINK/distributed-kv.jar master $PORT
Restart=no
TimeoutStopSec=10
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
EOF

sudo install -o root -g root -m 0644 "$UNIT_TEMP" "$UNIT_PATH"
sudo ln -sfn "$RELEASE_DIR" "$CURRENT_LINK"
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE_NAME.service"

ready=false
for _ in $(seq 1 40); do
    if ! sudo systemctl is-active --quiet "$SERVICE_NAME.service"; then
        break
    fi
    if ss -ltnH "sport = :$PORT" 2>/dev/null | grep -q .; then
        ready=true
        break
    fi
    sleep 0.25
done

if [[ "$ready" != true ]]; then
    sudo journalctl -u "$SERVICE_NAME.service" -n 40 --no-pager >&2 || true
    if [[ -n "$PREVIOUS_RELEASE" && -d "$PREVIOUS_RELEASE" ]]; then
        sudo ln -sfn "$PREVIOUS_RELEASE" "$CURRENT_LINK"
        sudo systemctl restart "$SERVICE_NAME.service" || true
    fi
    exit 1
fi
'@

do {
    $choice = (Read-Host "현재 디렉토리의 distributed-kv.jar 파일로 진행할까요? (Y: 그대로 실행, N: 컴파일 후 EC2 전송)").Trim().ToUpperInvariant()
} while ($choice -notin @("Y", "N"))

try {
    $toolchain = Get-JavaToolchain -RequireCompiler:($choice -eq "N")
    $javaVersion = (& $toolchain.Java -version 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $javaVersion -notmatch 'version\s+"?(\d+)' -or [int]$Matches[1] -lt 17) {
        throw "Java 17 이상이 필요합니다: $javaVersion"
    }

    if ($choice -eq "Y") {
        if (-not (Test-Path -LiteralPath $currentJarPath -PathType Leaf)) {
            throw "현재 JAR 파일을 찾을 수 없습니다: $currentJarPath"
        }
    } else {
        $javacVersion = (& $toolchain.Javac -version 2>&1 | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or $javacVersion -notmatch 'javac\s+(\d+)' -or [int]$Matches[1] -lt 17) {
            throw "Java JDK 17 이상이 필요합니다: $javacVersion"
        }

        New-Item -ItemType Directory -Path $classesDir -Force | Out-Null
        $sourceFiles = @(Get-ChildItem -LiteralPath (Join-Path $projectRoot "src") -Filter "*.java" -File |
            Sort-Object Name |
            ForEach-Object { $_.FullName })
        if ($sourceFiles.Count -eq 0) { throw "src 폴더에서 Java 소스를 찾지 못했습니다." }

        Invoke-CheckedCommand -FilePath $toolchain.Javac -ArgumentList (@(
            "--release", "17", "-encoding", "UTF-8", "-d", $classesDir
        ) + $sourceFiles)
        Invoke-CheckedCommand -FilePath $toolchain.Jar -ArgumentList @(
            "cfe", $builtJarPath, "DistributedKvApp", "-C", $classesDir, "."
        )
        Copy-Item -LiteralPath $builtJarPath -Destination $currentJarPath -Force
    }

    foreach ($workerPort in 6001..6004) {
        if (-not (Test-LocalListenPortAvailable -TargetPort $workerPort)) {
            throw "로컬 Worker 포트 $workerPort 를 이미 사용 중입니다."
        }
    }

    $keyFile = (Resolve-Path -LiteralPath $keyPath -ErrorAction Stop).Path
    $ssh = (Get-Command ssh -ErrorAction Stop).Source
    $sshBaseArgs = @(
        "-i", $keyFile,
        "-o", "ConnectTimeout=10",
        "-o", "StrictHostKeyChecking=accept-new"
    )
    $remoteTarget = "$ec2User@$ec2Host"

    if ($choice -eq "Y") {
        Invoke-CheckedCommand -FilePath $ssh -ArgumentList ($sshBaseArgs + @(
            $remoteTarget, "sudo systemctl restart $serviceName.service"
        ))
    } else {
        $scp = (Get-Command scp -ErrorAction Stop).Source
        $localDeployScript = Join-Path $buildRoot "ec2-deploy.sh"
        [System.IO.File]::WriteAllText(
            $localDeployScript, $remoteDeployScript, [System.Text.UTF8Encoding]::new($false))
        $remoteJar = "/tmp/distributed-kv-$releaseId.jar"
        $remoteScript = "/tmp/distributed-kv-deploy-$releaseId.sh"
        Invoke-CheckedCommand -FilePath $scp -ArgumentList ($sshBaseArgs + @(
            $currentJarPath, "${remoteTarget}:$remoteJar"
        ))
        Invoke-CheckedCommand -FilePath $scp -ArgumentList ($sshBaseArgs + @(
            $localDeployScript, "${remoteTarget}:$remoteScript"
        ))
        Invoke-CheckedCommand -FilePath $ssh -ArgumentList ($sshBaseArgs + @(
            $remoteTarget,
            "bash $remoteScript $remoteJar $remoteRoot $serviceName $ec2User $port $releaseId"
        ))
    }

    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while (-not (Test-TcpEndpoint -HostName $ec2Host -TargetPort $port)) {
        if ([DateTime]::UtcNow -ge $deadline) {
            throw "EC2 $ec2Host`:$port 에 연결할 수 없습니다."
        }
        Start-Sleep -Milliseconds 500
    }

    $previousLogDir = Join-Path $projectRoot "logs\before-$releaseId"
    Copy-RunLogs -Destination $previousLogDir -RemoveSource

    $completedLogDir = Join-Path $projectRoot "logs\$releaseId"
    try {
        Push-Location $projectRoot
        try {
            Invoke-CheckedCommand -FilePath $toolchain.Java -ArgumentList @(
                "-Dfile.encoding=UTF-8", "-jar", $currentJarPath, "workers", $ec2Host, "$port"
            )
        } finally {
            Pop-Location
        }

        Invoke-LogVerification -LogDirectory $projectRoot
    } finally {
        Copy-RunLogs -Destination $completedLogDir
    }
} finally {
    if (Test-Path -LiteralPath $buildRoot) {
        Remove-Item -LiteralPath $buildRoot -Recurse -Force
    }
}
