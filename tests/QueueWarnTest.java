import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;

// Run in an isolated working directory, with the application classes on the classpath.
public class QueueWarnTest {
    static Object field(Object w, String name) throws Exception {
        Field f = WorkerNode.class.getDeclaredField(name); f.setAccessible(true); return f.get(w);
    }
    static void set(Object w, String name, Object value) throws Exception {
        Field f = WorkerNode.class.getDeclaredField(name); f.setAccessible(true); f.set(w, value);
    }
    static Object call(Object w, String name, Class<?>[] types, Object... args) throws Exception {
        Method m = WorkerNode.class.getDeclaredMethod(name, types); m.setAccessible(true); return m.invoke(w, args);
    }
    public static void main(String[] args) throws Exception {
        WorkerNode worker = new WorkerNode(1, "localhost", 1, 6001);
        try (EventLogger log = new EventLogger("queue-test.txt")) {
            set(worker, "log", log);
            set(worker, "nextLoadCheck", Long.MAX_VALUE);
            set(worker, "processingEnabled", true);
            for (int i = 1; i <= 8; i++) {
                call(worker, "receiveTask", new Class<?>[]{Task.class},
                        new Task(String.format("%04d", i), "abcd", 1, 0, i == 8, 0));
            }
            Task first = (Task) call(worker, "takeTask", new Class<?>[]{});
            if (!first.id.equals("0008")) throw new AssertionError("Retry order changed");
            synchronized (field(worker, "queueLock")) {
                for (int i = 9; i <= 11; i++) call(worker, "addTaskLocked",
                        new Class<?>[]{Task.class, boolean.class, String.class},
                        new Task(String.format("%04d", i), "abcd", 1, 0, false, 0), false, "P2P 수신");
                List<Task> batch = new ArrayList<>();
                for (int i = 0; i < 3; i++) batch.add((Task) call(worker, "removeTaskLocked",
                        new Class<?>[]{boolean.class, String.class}, true, "P2P 송신"));
                set(worker, "reservedTransferSlots", 3);
                for (int i = 2; i >= 0; i--) call(worker, "addTaskLocked",
                        new Class<?>[]{Task.class, boolean.class, String.class}, batch.get(i), false, "P2P 실패 복원");
                set(worker, "reservedTransferSlots", 0);
                call(worker, "clearQueueLocked", new Class<?>[]{String.class}, "종료 정리");
                call(worker, "removeTaskLocked", new Class<?>[]{boolean.class, String.class}, false, "empty");
            }
        }
        List<String> warns = Files.readAllLines(Path.of("queue-test.txt")).stream()
                .filter(s -> s.contains("| QUEUE | WARN")).toList();
        String[] transitions = {"7->8", "8->7", "7->8", "8->9", "9->10", "10->9", "9->8", "8->7",
                "7->8", "8->9", "9->10", "10->9", "9->8", "8->7"};
        if (warns.size() != transitions.length) throw new AssertionError("WARN count: " + warns.size());
        for (int i = 0; i < transitions.length; i++)
            if (!warns.get(i).contains("크기=" + transitions[i] + "/10")) throw new AssertionError(warns.get(i));
        System.out.println("PASS Queue WARN boundaries, retry order, batch send/receive, restore, clear, empty removal");
    }
}
