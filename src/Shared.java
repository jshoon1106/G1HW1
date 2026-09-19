import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

class Task implements Comparable<Task> {
    final String id;
    final String key;
    final int value;
    int attempts;
    final boolean retry;
    long enqueuedAt;

    // 작업 정보 생성
    Task(String id, String key, int value, int attempts, boolean retry, long enqueuedAt) {
        this.id = id;
        this.key = key;
        this.value = value;
        this.attempts = attempts;
        this.retry = retry;
        this.enqueuedAt = enqueuedAt;
    }

    // TCP 전송 문자열 생성
    String wire() {
        return id + "," + key + "," + value + "," + attempts + "," + retry + "," + enqueuedAt;
    }

    // TCP 문자열 작업 복원
    static Task fromWire(String text) {
        String[] p = text.split(",", -1);
        if (p.length != 6) throw new IllegalArgumentException("Task 필드 수 오류");
        if (!p[0].matches("\\d{4}")) throw new IllegalArgumentException("Task ID 형식 오류");
        if (!p[1].matches("[0-9a-fA-F]{4}")) throw new IllegalArgumentException("Key 형식 오류");
        int value = Integer.parseInt(p[2]);
        int attempts = Integer.parseInt(p[3]);
        if (value < 1 || value > 100) throw new IllegalArgumentException("Value 범위 오류");
        if (attempts < 0) throw new IllegalArgumentException("시도 횟수 범위 오류");
        if (!"true".equals(p[4]) && !"false".equals(p[4])) {
            throw new IllegalArgumentException("재시도 플래그 오류");
        }
        long enqueuedAt = Long.parseLong(p[5]);
        if (enqueuedAt < 0) throw new IllegalArgumentException("가상 시각 범위 오류");
        return new Task(p[0], p[1].toLowerCase(Locale.ROOT), value, attempts,
                Boolean.parseBoolean(p[4]), enqueuedAt);
    }

    // 재시도 횟수 기준 우선순위 비교
    @Override
    public int compareTo(Task other) {
        int byAttempt = Integer.compare(other.attempts, attempts);
        return byAttempt != 0 ? byAttempt : id.compareTo(other.id);
    }
}

class VirtualClock {
    private long millis;

    // 가상 시각 증가
    synchronized long advanceSeconds(double seconds) {
        millis += Math.round(seconds * 1000);
        return millis;
    }

    // 현재 가상 시각 반환
    synchronized long now() {
        return millis;
    }

    // 로그용 초 단위 변환
    static String format(long millis) {
        return String.format(Locale.US, "%.2f", millis / 1000.0);
    }
}

class EventLogger implements AutoCloseable {
    private final BufferedWriter writer;

    // 로그 파일 생성
    EventLogger(String filename) throws IOException {
        writer = Files.newBufferedWriter(Path.of(filename), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    // 공통 형식 로그 기록
    synchronized void log(long clock, String node, String event, String status, String message) {
        String line = "[" + VirtualClock.format(clock) + "] " + node + " | " + event
                + " | " + status + " | " + message;
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("로그 기록 실패", e);
        }
        System.out.println(line);
    }

    // 로그 파일 제목 출력
    synchronized void header(String title, String subtitle) {
        try {
            writer.write("=== " + title + " ===");
            writer.newLine();
            writer.write("============================================================");
            writer.newLine();
            writer.write(subtitle);
            writer.newLine();
            writer.write("============================================================");
            writer.newLine();
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // 로그 파일 종료
    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }
}

class WorkerInfo {
    final int id;
    final PrintWriter out;
    int queueSize;
    int success;
    int fail;
    int queueRejects;
    int processed;
    int p2pSent;
    int p2pReceived;
    int p2pSentEvents;
    int p2pReceivedEvents;
    int retryReceived;
    double totalWait;
    final Map<String, Integer> taskAttempts = new HashMap<>();
    boolean connected = true;
    boolean logRequested;
    boolean terminationAcked;

    // Worker 연결 정보 생성
    WorkerInfo(int id, PrintWriter out) {
        this.id = id;
        this.out = out;
    }

    // Worker 메시지 전송
    synchronized void send(String message) {
        out.println(message);
    }
}

// Master 시각 확정을 기다리는 Worker 처리 결과
class PendingResult {
    final Task task;
    final boolean success;
    final double duration;
    final double wait;
    final String reason;

    PendingResult(Task task, boolean success, double duration, double wait, String reason) {
        this.task = task;
        this.success = success;
        this.duration = duration;
        this.wait = wait;
        this.reason = reason;
    }
}
