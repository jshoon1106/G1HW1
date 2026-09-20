# Presentation helpers only. Java networking, clocks and log writing remain unchanged.
function Read-DetailedOutputChoice {
    while ($true) {
        $answer = Read-Host '상세 로그를 화면에 표시할까요? [y/N]'
        if ([string]::IsNullOrWhiteSpace($answer) -or $answer.Trim() -match '^(?i)n$') { return $false }
        if ($answer.Trim() -match '^(?i)y$') { return $true }
        Write-Host 'y 또는 n을 입력해주세요. Enter는 기본 화면입니다.'
    }
}

function Invoke-WorkerConsole {
    param([string]$Java, [string]$JarPath, [string]$WorkingDirectory,
        [string]$MasterAddress, [int]$Port, [bool]$Detailed, [string]$Destination)

    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    $utf8 = [System.Text.UTF8Encoding]::new($false)
    $capture = [System.IO.StreamWriter]::new((Join-Path $Destination 'console.txt'), $false, $utf8)
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Java
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = $utf8
    $startInfo.StandardErrorEncoding = $utf8
    # These properties control redirected console encoding, not the simulation.
    $startInfo.Arguments = '-Dfile.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8 -Dsun.stderr.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8 -jar "' + $JarPath + '" workers ' + $MasterAddress + ' ' + $Port
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    $successIds = [System.Collections.Generic.HashSet[string]]::new()
    $connections = [System.Collections.Generic.HashSet[string]]::new()
    $nextUpdate = 0L
    $lastProgress = ''
    $hadErrors = $false
    $started = $false
    try {
        $started = $process.Start()
        $reads = @($process.StandardOutput.ReadLineAsync(), $process.StandardError.ReadLineAsync())
        while ($null -ne $reads[0] -or $null -ne $reads[1]) {
            $consumed = $false
            for ($stream = 0; $stream -lt 2; $stream++) {
                if ($null -eq $reads[$stream] -or -not $reads[$stream].IsCompleted) { continue }
                $line = $reads[$stream].GetAwaiter().GetResult()
                if ($null -eq $line) { $reads[$stream] = $null; continue }
                $consumed = $true
                $capture.WriteLine($line)
                if ($line -match 'WORKER(\d+) \| CONNECT \| SUCCESS') { [void]$connections.Add($Matches[1]) }
                if ($line -match '\| PROC \| SUCCESS \| KV\[(\d+)\]') { [void]$successIds.Add($Matches[1]) }
                $isError = $line -match 'Exception|Caused by:|\| CONNECT \| FAIL|\| TERMINATE \| FAIL'
                if ($isError) { $hadErrors = $true }
                if ($Detailed -or $stream -eq 1 -or $isError) { Write-Host $line }
                if ($stream -eq 0) { $reads[0] = $process.StandardOutput.ReadLineAsync() }
                else { $reads[1] = $process.StandardError.ReadLineAsync() }
            }
            if (-not $Detailed -and $watch.ElapsedMilliseconds -ge $nextUpdate) {
                $message = 'Worker 연결: {0}/4 | 성공 처리 확인: {1}/5000' -f $connections.Count, $successIds.Count
                if ($message -ne $lastProgress) { Write-Host $message; $lastProgress = $message }
                $nextUpdate = $watch.ElapsedMilliseconds + 500
            }
            if (-not $consumed) { Start-Sleep -Milliseconds 10 }
        }
        $process.WaitForExit()
        $watch.Stop()
        return [pscustomobject]@{ ExitCode = $process.ExitCode; HadErrors = $hadErrors; Seconds = $watch.Elapsed.TotalSeconds }
    } finally {
        # If the launcher itself is interrupted, do not leave its child running invisibly.
        if ($started -and -not $process.HasExited) { $process.Kill(); $process.WaitForExit() }
        $capture.Dispose()
        $process.Dispose()
    }
}

function Get-RunMetric {
    param([string]$Text, [string]$Label)
    $match = [regex]::Match($Text, '\| STAT \| (?:INFO|SUCCESS|FAIL|WARN) \| ' + [regex]::Escape($Label) + ': ([0-9.]+)')
    if ($match.Success) { return $match.Groups[1].Value }
    return '-'
}

function Write-WorkerSummary {
    param([string]$Directory, [int]$ExitCode, [bool]$HadErrors, [double]$Seconds)
    $utf8 = [System.Text.UTF8Encoding]::new($false)
    $rows = @()
    $workerComplete = $true
    $workerSuccess = 0L
    $workerFail = 0L
    $workerIds = [System.Collections.Generic.HashSet[string]]::new()
    $workerSuccessEvents = 0
    $metrics = @('작업 처리량', '성공 횟수', '실패 횟수', '평균 대기 시간', 'P2P 부하 분산 이벤트 횟수', '장애 재할당 횟수', '전체 수행 시간')
    for ($i = 1; $i -le 4; $i++) {
        $path = Join-Path $Directory "Worker$i.txt"
        $data = if (Test-Path -LiteralPath $path) { [System.IO.File]::ReadAllText($path, $utf8) } else { '' }
        $values = @{}
        foreach ($label in $metrics) {
            $values[$label] = Get-RunMetric $data $label
            if ($values[$label] -eq '-') { $workerComplete = $false }
        }
        if ($data -notmatch '\| TERMINATE \| SUCCESS') { $workerComplete = $false }
        foreach ($match in [regex]::Matches($data, '\| PROC \| SUCCESS \| KV\[(\d+)\]')) {
            [void]$workerIds.Add($match.Groups[1].Value); $workerSuccessEvents++
        }
        if ($values['성공 횟수'] -ne '-') { $workerSuccess += [long]$values['성공 횟수'] }
        if ($values['실패 횟수'] -ne '-') { $workerFail += [long]$values['실패 횟수'] }
        $rows += ('{0,-8} {1,6} {2,6} {3,12} {4,8} {5,8}' -f "Worker$i", $values['성공 횟수'], $values['실패 횟수'], $values['평균 대기 시간'], $values['P2P 부하 분산 이벤트 횟수'], $values['장애 재할당 횟수'])
    }
    $workerComplete = $workerComplete -and $workerSuccess -eq 5000 -and $workerIds.Count -eq 5000 -and $workerSuccessEvents -eq 5000
    $masterPath = Join-Path $Directory 'Master.txt'
    $master = if (Test-Path -LiteralPath $masterPath) { [System.IO.File]::ReadAllText($masterPath, $utf8) } else { '' }
    $ids = [System.Collections.Generic.HashSet[string]]::new()
    $results = [regex]::Matches($master, '\| RESULT \| SUCCESS \| KV\[(\d+)\]')
    foreach ($match in $results) { [void]$ids.Add($match.Groups[1].Value) }
    $entries = [regex]::Matches($master, '\| KV \| SUCCESS \| Key=([0-9a-fA-F]{4}), Value=(\d+)')
    $keys = [System.Collections.Generic.HashSet[string]]::new()
    $validValues = $true
    foreach ($entry in $entries) {
        [void]$keys.Add($entry.Groups[1].Value.ToLowerInvariant())
        $v = [long]$entry.Groups[2].Value
        if ($v -lt 1 -or $v -gt 100) { $validValues = $false }
    }
    $masterComplete = $master -match '\| TERMINATE \| SUCCESS' -and $results.Count -eq 5000 -and $ids.Count -eq 5000 -and $entries.Count -eq 5000 -and $keys.Count -eq 5000 -and $validValues
    for ($i = 1; $i -le 5000; $i++) { if (-not $ids.Contains(('{0:D4}' -f $i))) { $masterComplete = $false; break } }
    foreach ($label in $metrics) { if ((Get-RunMetric $master $label) -eq '-') { $masterComplete = $false } }
    $masterComplete = $masterComplete -and $ids.SetEquals($workerIds) -and (Get-RunMetric $master '성공 횟수') -eq "$workerSuccess" -and (Get-RunMetric $master '실패 횟수') -eq "$workerFail"
    $status = if ($ExitCode -ne 0 -or $HadErrors -or -not $workerComplete) { 'FAILED' } elseif (-not $masterComplete) { 'PARTIAL' } else { 'COMPLETED' }
    $description = switch ($status) {
        'COMPLETED' { '정상 완료: 고유 작업 5,000개·KV 출력·통계·종료 기록 확인' }
        'PARTIAL' { '일부 확인 불가: Worker 완료, Master 로그 누락 또는 검증 불일치' }
        'FAILED' { '실패 또는 미완료: 실행 오류나 Worker 완료 조건을 확인해주세요' }
    }
    $p2p = Get-RunMetric $master 'P2P 부하 분산 이벤트 횟수'
    $lines = @('', '========== 통합 실행 결과 ==========', "$status | $description", '',
        'Worker     성공   실패   평균대기(초) P2P이벤트 재할당', $rows, '',
        "고유 성공 확인: $($workerIds.Count)/5000", "Master P2P 이벤트: $p2p 회",
        ('Master 장애 재할당: {0} 회' -f (Get-RunMetric $master '장애 재할당 횟수')),
        ('실제 실행 시간: {0:F2}초' -f $Seconds),
        ('가상 수행 시간: {0}초' -f (Get-RunMetric $master '전체 수행 시간')),
        '※ Worker P2P는 송신+수신 참여 횟수이며, 합계를 Master 이벤트 수와 동일하게 보지 않습니다.',
        "로그 위치: $Directory")
    if ($p2p -eq '0') { $lines += 'P2P가 이번 실행에서 발생하지 않았습니다. 시연용 동작 확인은 별도로 필요합니다.' }
    $flat = @($lines | ForEach-Object { $_ })
    New-Item -ItemType Directory -Path $Directory -Force | Out-Null
    [System.IO.File]::WriteAllLines((Join-Path $Directory 'summary.txt'), [string[]]$flat, $utf8)
    foreach ($line in $flat) { Write-Host $line }
    return $status
}
