import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

public class TransferStatusTest {
    static Object field(Object w, String name) throws Exception {
        Field f = WorkerNode.class.getDeclaredField(name); f.setAccessible(true); return f.get(w);
    }
    static void set(Object w, String name, Object v) throws Exception {
        Field f = WorkerNode.class.getDeclaredField(name); f.setAccessible(true); f.set(w, v);
    }
    static Object call(Object w, String name, Class<?>[] types, Object... args) throws Exception {
        Method m = WorkerNode.class.getDeclaredMethod(name, types); m.setAccessible(true); return m.invoke(w, args);
    }
    static String exchange(int port, String message) throws Exception {
        try (Socket s = new Socket("127.0.0.1", port)) {
            s.setSoTimeout(3000);
            new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true).println(message);
            return new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8)).readLine();
        }
    }
    static void check(boolean value) { if (!value) throw new AssertionError(); }
    public static void main(String[] args) throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        WorkerNode sender = new WorkerNode(1, "localhost", 1, 6001);
        WorkerNode receiver = new WorkerNode(2, "localhost", 1, port);
        try (EventLogger a = new EventLogger("sender.txt"); EventLogger b = new EventLogger("receiver.txt")) {
            set(sender, "log", a); set(receiver, "log", b);
            call(receiver, "startPeerListener", new Class<?>[]{});
            for (int i = 0; i < 100; i++) {
                try { exchange(port, "QUEUE_QUERY|test|1"); break; }
                catch (ConnectException e) { Thread.sleep(10); }
            }
            Task task = new Task("0001", "abcd", 42, 0, false, 0);
            // Proxy delivers both TRANSFER requests but loses both ACKs.
            try (ServerSocket proxy = new ServerSocket(6002)) {
                FutureTask<Void> server = new FutureTask<>(() -> {
                    for (int i = 0; i < 3; i++) {
                        try (Socket client = proxy.accept()) {
                            client.setSoTimeout(3000);
                            String request = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8)).readLine();
                            String response = exchange(port, request);
                            if (i == 2) new PrintWriter(client.getOutputStream(), true).println(response);
                        }
                    }
                    return null;
                });
                new Thread(server).start();
                Object state = call(sender, "transfer", new Class<?>[]{int.class, String.class, List.class}, 2, "1-1", List.of(task));
                check(state.toString().equals("ACCEPTED"));
                server.get(10, TimeUnit.SECONDS);
                check(((Deque<?>)field(receiver, "queue")).size() == 1);
                set(sender, "reservedTransferSlots", 1);
                call(sender, "finishTransfer", new Class<?>[]{int.class, String.class, List.class, state.getClass()}, 2, "1-1", List.of(task), state);
                check(((Deque<?>)field(sender, "queue")).isEmpty());
                check((int)field(sender, "reservedTransferSlots") == 0);
            }
            check(exchange(port, "TRANSFER_STATUS|1-2|1").endsWith("|NOT_FOUND"));
            check(exchange(port, "TRANSFER|1-2|1|" + task.wire()).startsWith("REJECT|"));
            check(exchange(port, "TRANSFER_STATUS|1-2|1").endsWith("|REJECTED"));
            check(((Deque<?>)field(receiver, "queue")).size() == 1);
            // Receiver's committed acceptance survives processing/removal from its queue.
            synchronized (field(receiver, "queueLock")) {
                call(receiver, "removeTaskLocked", new Class<?>[]{boolean.class, String.class}, false, "test processing");
            }
            check(exchange(port, "TRANSFER_STATUS|1-1|1").endsWith("|ACCEPTED"));
            Object unknown = call(sender, "queryTransferStatus", new Class<?>[]{int.class, String.class}, 2, "1-3");
            check(unknown.toString().equals("UNKNOWN"));
            set(sender, "reservedTransferSlots", 1);
            try {
                call(sender, "finishTransfer", new Class<?>[]{int.class, String.class, List.class, unknown.getClass()}, 2, "1-3", List.of(task), unknown);
                throw new AssertionError("UNKNOWN restored");
            } catch (InvocationTargetException e) { check(e.getCause() instanceof IllegalArgumentException); }
            check((int)field(sender, "reservedTransferSlots") == 1);
            check(((Deque<?>)field(sender, "queue")).isEmpty());
            Object rejected = Arrays.stream(unknown.getClass().getEnumConstants())
                    .filter(v -> v.toString().equals("REJECTED")).findFirst().orElseThrow();
            call(sender, "finishTransfer", new Class<?>[]{int.class, String.class, List.class, unknown.getClass()}, 2, "1-3", List.of(task), rejected);
            check((int)field(sender, "reservedTransferSlots") == 0);
            check(((Deque<?>)field(sender, "queue")).size() == 1);
            set(receiver, "stopping", true);
            exchange(port, "QUEUE_QUERY|stop|1");
        }
        System.out.println("PASS lost ACKs / single insertion / no sender restore / NOT_FOUND fence / processed ACCEPTED / UNKNOWN holds reservation");
    }
}
