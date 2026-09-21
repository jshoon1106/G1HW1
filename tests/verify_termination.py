"""Isolated socket regressions; shorten timeouts only in temporary source copies."""
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    with tempfile.TemporaryDirectory(prefix="kv-termination-") as temp:
        base = Path(temp)
        classes = base / "classes"
        classes.mkdir()
        sources = []
        for source in (ROOT / "src").glob("*.java"):
            text = source.read_text(encoding="utf-8-sig")
            text = text.replace("TASK_COUNT = 5000", "TASK_COUNT = 20")
            text = text.replace("COMPLETION_TIMEOUT_MINUTES = 5", "COMPLETION_TIMEOUT_MINUTES = 1")
            text = text.replace("COMPLETION_TIMEOUT_MINUTES, TimeUnit.MINUTES", "COMPLETION_TIMEOUT_MINUTES, TimeUnit.SECONDS")
            text = text.replace("TERMINATION_TIMEOUT_SECONDS = 30", "TERMINATION_TIMEOUT_SECONDS = 1")
            text = text.replace("TERMINATION_CLOCK_TIMEOUT_SECONDS = 30", "TERMINATION_CLOCK_TIMEOUT_SECONDS = 1")
            target = base / source.name
            target.write_text(text, encoding="utf-8")
            sources.append(str(target))
        subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-d", str(classes), *sources], check=True)

        def launch(directory, *args):
            directory.mkdir()
            return subprocess.Popen(["java", "-cp", str(classes), "DistributedKvApp", *args], cwd=directory,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def connect(port):
            for _ in range(100):
                try:
                    return socket.create_connection(("127.0.0.1", port), timeout=5)
                except ConnectionRefusedError:
                    time.sleep(.05)
            raise AssertionError("Master did not start")

        for complete in (False, True):
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            directory = base / ("ack-timeout" if complete else "completion-timeout")
            process = launch(directory, "master", str(port))
            peers = [connect(port) for _ in range(4)]
            def worker(sock, index):
                with sock, sock.makefile("r", encoding="utf-8") as reader:
                    sock.sendall(f"HELLO|{index}|{6000+index}\n".encode())
                    for line in reader:
                        p = line.strip().split("|")
                        if p[0] == "TASK" and complete:
                            task = p[1].split(",")
                            sock.sendall(f"RESULT|{task[0]}|{task[3]}|SUCCESS|1.00|0.00|0|PROCESS\n".encode())
                        if p[0] == "TERMINATE":
                            if not complete:
                                sock.sendall(b"TERMINATE_ACK\n")
                            return
            threads = [threading.Thread(target=worker, args=(s, i+1), daemon=True) for i, s in enumerate(peers)]
            try:
                for thread in threads:
                    thread.start()
                process.wait(timeout=15)
                log = (directory / "Master.txt").read_text(encoding="utf-8")
                assert "| TERMINATE | SUCCESS" not in log, directory
                assert "불완전 종료" in log
                assert ("종료 ACK 제한시간 초과" if complete else "전체 처리 제한시간 초과") in log
                if complete:
                    assert "성공 횟수: 20" in log
                print("PASS", directory.name)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

        for send_clock in (False, True):
            with socket.socket() as server:
                server.bind(("127.0.0.1", 0))
                server.listen()
                server.settimeout(10)
                directory = base / ("worker-normal" if send_clock else "worker-clock-timeout")
                process = launch(directory, "workers", "127.0.0.1", str(server.getsockname()[1]))
                sockets = []
                try:
                    for _ in range(4):
                        peer, _ = server.accept()
                        sockets.append(peer)
                        peer.sendall(b"WELCOME|0\nTERMINATE|1000|0|0\n")
                    if send_clock:
                        for peer in sockets:
                            peer.sendall(b"FINAL_CLOCK|2000\n")
                    process.wait(timeout=10)
                    for i in range(1, 5):
                        log = (directory / f"Worker{i}.txt").read_text(encoding="utf-8")
                        assert ("| TERMINATE | SUCCESS" in log) == send_clock
                        assert ("| TERMINATE | FAIL" in log) != send_clock
                    print("PASS", directory.name)
                finally:
                    for peer in sockets:
                        peer.close()
                    if process.poll() is None:
                        process.kill()
                        process.wait()


if __name__ == "__main__":
    main()
