"""Script-only UX regression checks. No AWS connections; temporary fixtures only."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def main():
    shells = [shutil.which("pwsh")]
    if os.name == "nt":
        shells.append(str(Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"))
    shells = list(dict.fromkeys(s for s in shells if s))
    assert shells
    java, javac, jar = (shutil.which(tool) for tool in ("java", "javac", "jar"))
    assert java and javac and jar
    with tempfile.TemporaryDirectory(prefix=".tmp.worker-ui-", dir=ROOT) as folder:
        base = Path(folder).resolve()
        assert base.is_relative_to(ROOT)
        fixture = base / "logs with spaces"
        fixture.mkdir()
        master_lines = []
        for i in range(1, 5001):
            master_lines += [f"[1.00] MASTER | RESULT | SUCCESS | KV[{i:04}] stored",
                             f"[1.00] MASTER | KV | SUCCESS | Key={i:04x}, Value=42"]
        def stats(node, successes):
            metrics = [("작업 처리량", successes), ("성공 횟수", successes), ("실패 횟수", 0),
                       ("평균 대기 시간", 0), ("P2P 부하 분산 이벤트 횟수", 0),
                       ("장애 재할당 횟수", 0), ("전체 수행 시간", 1)]
            return [f"[1.00] {node} | STAT | INFO | {k}: {v}" for k, v in metrics] + [f"[1.00] {node} | TERMINATE | SUCCESS | done"]
        master_file = fixture / "Master.txt"
        master_text = "\n".join(master_lines + stats("MASTER", 5000))
        master_file.write_text(master_text, encoding="utf-8")
        for worker in range(1, 5):
            lines = [f"[1.00] WORKER{worker} | PROC | SUCCESS | KV[{i:04}] stored" for i in range((worker-1)*1250+1, worker*1250+1)]
            (fixture / f"Worker{worker}.txt").write_text("\n".join(lines + stats(f"WORKER{worker}", 1250)), encoding="utf-8")
        source = base / "Emitter.java"
        source.write_text('''public class Emitter {
 public static void main(String[] args) {
  for (int i=1;i<=4;i++) System.out.println("[0] WORKER"+i+" | CONNECT | SUCCESS | connected");
  for (int i=1;i<=5000;i++) {
   System.out.printf("[1] WORKER1 | PROC | SUCCESS | KV[%04d] 저장 완료%n",i);
   if(i%500==0) System.err.println("diagnostic "+i);
  }
  if(args.length>1 && args[1].equals("error")) {
   System.err.println("java.lang.RuntimeException: test failure"); System.exit(7);
  }
 }
}''', encoding="utf-8")
        subprocess.run([javac, "--release", "17", "-encoding", "UTF-8", str(source)], check=True, capture_output=True)
        fake_jar = base / "emitter with spaces.jar"
        subprocess.run([jar, "cfe", str(fake_jar), "Emitter", "-C", str(base), "Emitter.class"], check=True, capture_output=True)

        for shell in shells:
            def ps(code):
                script = base / "check.ps1"
                script.write_text("$ErrorActionPreference='Stop'\nSet-StrictMode -Version Latest\n. " + quote(ROOT / "run-workers-ui.ps1") + "\n" + code, encoding="utf-8-sig")
                result = subprocess.run([shell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)], capture_output=True, timeout=90)
                assert result.returncode == 0, (shell, result.stdout, result.stderr)
                return result
            for answer, expected in [("", "$false"), ("n", "$false"), ("Y", "$true")]:
                ps("$script:answers=[System.Collections.Generic.Queue[string]]::new(); $script:answers.Enqueue('bad'); $script:answers.Enqueue("+quote(answer)+"); function Read-Host { param($Prompt) return $script:answers.Dequeue() }; if ((Read-DetailedOutputChoice) -ne "+expected+") { throw 'choice failed' }")
            for detailed in (False, True):
                destination = base / ("console-" + Path(shell).stem + str(detailed))
                result_path = base / "result.json"
                result = ps("$r=Invoke-WorkerConsole -Java " + quote(java) + " -JarPath " + quote(fake_jar) + " -WorkingDirectory " + quote(base) + " -MasterAddress localhost -Port 5000 -Detailed $" + str(detailed).lower() + " -Destination " + quote(destination) + "; $r | ConvertTo-Json | Set-Content -Encoding UTF8 " + quote(result_path))
                payload = json.loads(result_path.read_text(encoding="utf-8-sig"))
                assert payload["ExitCode"] == 0 and not payload["HadErrors"]
                console = (destination / "console.txt").read_text(encoding="utf-8")
                assert console.count("| PROC | SUCCESS") == 5000 and console.count("diagnostic") == 10
                assert "저장 완료" in console
                assert (result.stdout.count(b"| PROC | SUCCESS") == 5000) == detailed
                assert result.stdout.count(b"diagnostic") == 10
            ps("$r=Invoke-WorkerConsole -Java " + quote(java) + " -JarPath " + quote(fake_jar) + " -WorkingDirectory " + quote(base) + " -MasterAddress error -Port 5000 -Detailed $false -Destination " + quote(base / "error") + "; if ($r.ExitCode -ne 7 -or -not $r.HadErrors) { throw 'error not detected' }")
            def summary(expected, exit_code=0):
                ps("$s=Write-WorkerSummary -Directory " + quote(fixture) + " -ExitCode " + str(exit_code) + " -HadErrors $false -Seconds 1.2; if($s -ne '"+expected+"') { throw ('Unexpected status: '+$s) }")
                assert expected in (fixture / "summary.txt").read_text(encoding="utf-8")
            summary("COMPLETED")
            master_file.unlink(); summary("PARTIAL")
            master_file.write_text(master_text.replace("KV[5000]", "KV[0001]"), encoding="utf-8"); summary("PARTIAL")
            master_file.write_text(master_text, encoding="utf-8"); summary("FAILED", 7)
            print("PASS", shell, "choice/quiet/detailed/UTF8/stdout+stderr/errors/complete/partial/duplicate")


if __name__ == "__main__":
    main()
