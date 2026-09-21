import java.nio.file.*;
import java.util.*;
import java.io.*;

public class LauncherTest {
    static String stats(int count) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < WorkerLauncher.METRICS.length; i++)
            s.append("[1] NODE | STAT | INFO | ").append(WorkerLauncher.METRICS[i]).append(": ").append(i < 2 ? count : 0).append('\n');
        return s + "[1] NODE | TERMINATE | SUCCESS | done\n";
    }
    static void check(boolean ok) { if (!ok) throw new AssertionError(); }
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        Path logs = root.resolve("fixture"); Files.createDirectories(logs);
        StringBuilder master = new StringBuilder();
        for (int i = 1; i <= 5000; i++) {
            master.append(String.format("[1] MASTER | RESULT | SUCCESS | KV[%04d]\n", i));
            master.append(String.format("[1] MASTER | KV | SUCCESS | Key=%04x, Value=42\n", i));
        }
        String full = master + stats(5000);
        Files.writeString(logs.resolve("Master.txt"), full);
        for (int w = 1; w <= 4; w++) {
            StringBuilder text = new StringBuilder();
            for (int i = (w-1)*1250+1; i <= w*1250; i++) text.append(String.format("[1] WORKER%d | PROC | SUCCESS | KV[%04d]\n", w, i));
            Files.writeString(logs.resolve("Worker" + w + ".txt"), text + stats(1250));
        }
        WorkerLauncher.Result ok = new WorkerLauncher.Result(0, false, 1);
        check(WorkerLauncher.summary(logs, ok).equals("COMPLETED"));
        Files.delete(logs.resolve("Master.txt"));
        check(WorkerLauncher.summary(logs, ok).equals("PARTIAL"));
        Files.writeString(logs.resolve("Master.txt"), full.replace("KV[5000]", "KV[0001]"));
        check(WorkerLauncher.summary(logs, ok).equals("PARTIAL"));
        Files.writeString(logs.resolve("Master.txt"), full + "[1] MASTER | TERMINATE | FAIL | timeout\n");
        check(WorkerLauncher.summary(logs, ok).equals("FAILED"));
        Files.writeString(logs.resolve("Master.txt"), full);
        check(WorkerLauncher.summary(logs, new WorkerLauncher.Result(7, false, 1)).equals("FAILED"));
        Path saved = root.resolve("saved");
        WorkerLauncher.copyLogs(logs, saved, true);
        check(!Files.exists(logs.resolve("Master.txt")) && Files.readString(saved.resolve("Master.txt")).equals(full));
        WorkerLauncher.copyLogs(saved, logs, false);
        check(Files.exists(saved.resolve("Master.txt")) && Files.exists(logs.resolve("Master.txt")));
        PrintStream output = System.out;
        for (boolean detailed : new boolean[]{false, true}) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            WorkerLauncher.Result result;
            try {
                System.setOut(new PrintStream(bytes, true, java.nio.charset.StandardCharsets.UTF_8));
                result = WorkerLauncher.console(root, "localhost", 5000, detailed, root.resolve("console-" + detailed));
            } finally { System.setOut(output); }
            check(result.code() == 0 && !result.errors());
            String shown = bytes.toString(java.nio.charset.StandardCharsets.UTF_8);
            check(shown.contains("| PROC | SUCCESS") == detailed);
            check(shown.contains("diagnostic"));
            String captured = Files.readString(root.resolve("console-" + detailed + "/console.txt"));
            check(WorkerLauncher.matches(captured, "KV\\[(\\d+)\\]").size() == 5000 && captured.contains("저장 완료"));
        }
        check(WorkerLauncher.console(root, "error", 5000, false, root.resolve("error")).errors());
        System.out.println("PASS launcher summary/duplicates/timeouts/backup/quiet/detailed/UTF8/stdout+stderr/errors");
    }
}
