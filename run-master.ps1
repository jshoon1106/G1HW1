[CmdletBinding()]
param(
    [switch]$NoPause
)

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

function Get-JavaToolchain {
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
            (Test-Path -LiteralPath $javac) -and
            (Test-Path -LiteralPath $jar)) {
            return [pscustomobject]@{ Java = $java; Javac = $javac; Jar = $jar }
        }
    }

    $javaCommand = Get-Command java -ErrorAction SilentlyContinue
    $javacCommand = Get-Command javac -ErrorAction SilentlyContinue
    $jarCommand = Get-Command jar -ErrorAction SilentlyContinue
    if ($javaCommand -and $javacCommand -and $jarCommand) {
        return [pscustomobject]@{
            Java = $javaCommand.Source
            Javac = $javacCommand.Source
            Jar = $jarCommand.Source
        }
    }
    throw "Java JDK 17 이상을 찾지 못했습니다."
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
Restart=always
RestartSec=1
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
sudo systemctl enable "$SERVICE_NAME.service"
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

function Invoke-MasterMain {
    do {
        $choice = (Read-Host "EC2에 설치된 Master를 그대로 재시작할까요? (Y: 재시작만, N: 컴파일 후 배포)").Trim().ToUpperInvariant()
    } while ($choice -notin @("Y", "N"))

    try {
        if ($choice -eq "N") {
            $toolchain = Get-JavaToolchain
            $javaVersionResult = Get-NativeCommandResult -FilePath $toolchain.Java -ArgumentList @("-version")
            $javaVersion = $javaVersionResult.Output
            if ($javaVersionResult.ExitCode -ne 0 -or $javaVersion -notmatch 'version\s+"?(\d+)' -or [int]$Matches[1] -lt 17) {
                throw "Java JDK 17 이상이 필요합니다: $javaVersion"
            }
            $javacVersionResult = Get-NativeCommandResult -FilePath $toolchain.Javac -ArgumentList @("-version")
            $javacVersion = $javacVersionResult.Output
            if ($javacVersionResult.ExitCode -ne 0 -or $javacVersion -notmatch 'javac\s+(\d+)' -or [int]$Matches[1] -lt 17) {
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
        "Master 준비 완료: $ec2Host`:$port"
    } finally {
        if (Test-Path -LiteralPath $buildRoot) {
            Remove-Item -LiteralPath $buildRoot -Recurse -Force
        }
    }
}

try {
    Invoke-MasterMain
    if (-not $NoPause) {
        [void](Read-Host "실행이 완료되었습니다. 창을 닫으려면 Enter 키를 누르세요")
    }
} catch {
    Write-Host ""
    Write-Host "[오류] Master 실행에 실패했습니다." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    if (-not $NoPause) {
        [void](Read-Host "창을 닫으려면 Enter 키를 누르세요")
    }
    exit 1
}
