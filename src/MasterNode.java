import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

class MasterNode {
    private static final int WORKER_COUNT = 4;
    private static final int TASK_COUNT = 5000;
    private static final int QUEUE_LIMIT = 10;
    private static final int CONNECTION_TIMEOUT_MILLIS = 60_000;
    private static final int SOCKET_TIMEOUT_MILLIS = 120_000;
    private static final int TERMINATION_TIMEOUT_SECONDS = 30;
    private static final int COMPLETION_TIMEOUT_MINUTES = 5;

    private final int port;
    private final Object lock = new Object();
    private final VirtualClock clock = new VirtualClock();
    private final Map<Integer, WorkerInfo> workers = new HashMap<>();
    private final Map<String, Integer> kvStore = new HashMap<>();
    private final Map<String, Task> allTasks = new HashMap<>();
    private final ArrayDeque<Task> pending = new ArrayDeque<>();
    private final PriorityQueue<Task> retries = new PriorityQueue<>();
    private final Set<String> completedTaskIds = new HashSet<>();
    private final Set<String> retryQueuedTaskIds = new HashSet<>();
    private final Set<String> processedAttempts = new HashSet<>();
    private final Map<String, Integer> retryExcludedWorkerIds = new HashMap<>();
    private final CountDownLatch completionSignal = new CountDownLatch(1);
    private final CountDownLatch terminationAcks = new CountDownLatch(WORKER_COUNT);
    private EventLogger log;
    private int totalSuccess;
    private int totalFail;
    private int totalQueueRejects;
    private int reassignmentCount;
    private int queueRetryCount;
    private int p2pEvents;
    private int nextProgress = 500;
    private int tieCursor;
    private boolean terminating;

    // Master 포트 설정
    MasterNode(int port) {
        this.port = port;
    }

    // Master 전체 실행
    void run() throws Exception {
        try (EventLogger eventLog = new EventLogger("Master.txt");
             ServerSocket server = new ServerSocket(port)) {
            log = eventLog;
            server.setSoTimeout(CONNECTION_TIMEOUT_MILLIS);
            log.header("Master.txt (Master Node Log)", "Master Node | Distributed KV Store");
            write("INIT", "INFO", "시스템 시계 시작, 포트 " + port + " 연결 대기");
            generateTasks();
            acceptWorkers(server);
            synchronized (lock) {
                write("INIT", "SUCCESS", "워커 4개 연결 완료, 작업 배정 시작");
                dispatchAvailableTasks();
                for (WorkerInfo worker : workers.values()) {
                    worker.send("START|" + clock.now());
                }
            }
            if (!completionSignal.await(COMPLETION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                synchronized (lock) {
                    terminating = true;
                    write("TERMINATE", "FAIL", "전체 처리 제한시간 초과, 강제 종료 신호 전송");
                    for (WorkerInfo worker : workers.values()) {
                        if (worker.connected) worker.send(terminationMessage());
                    }
                }
            }
            if (!terminationAcks.await(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                synchronized (lock) {
                    write("TERMINATE", "WARN", "종료 ACK 제한시간 초과, 수신="
                            + (WORKER_COUNT - terminationAcks.getCount()) + "/" + WORKER_COUNT);
                }
            }
            synchronized (lock) {
                writeFinalStatistics();
                write("TERMINATE", "SUCCESS", "정상 종료 완료");
                sendMasterLog();
            }
        }
    }

    // 고유 KV 작업 5,000개 생성
    private void generateTasks() {
        Set<String> usedKeys = new HashSet<>();
        for (int number = 1; number <= TASK_COUNT; number++) {
            String key;
            do {
                key = String.format("%04x", ThreadLocalRandom.current().nextInt(0x10000));
            } while (!usedKeys.add(key));
            Task task = new Task(String.format("%04d", number), key,
                    ThreadLocalRandom.current().nextInt(1, 101), 0, false, 0);
            pending.addLast(task);
            allTasks.put(task.id, task);
        }
        write("INIT", "SUCCESS", "고유 KV 작업 5,000개 생성 완료");
    }

    // Worker 4개 연결 수락
    private void acceptWorkers(ServerSocket server) throws IOException {
        while (workers.size() < WORKER_COUNT) {
            try {
                Socket socket = server.accept();
                socket.setKeepAlive(true);
                socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(
                        socket.getOutputStream(), StandardCharsets.UTF_8), true);
                try {
                    String hello = in.readLine();
                    String[] p = hello == null ? new String[0] : hello.split("\\|", -1);
                    if (p.length != 3 || !"HELLO".equals(p[0])) {
                        out.println("REJECT|HELLO 메시지 형식 오류");
                        socket.close();
                        continue;
                    }
                    int id = Integer.parseInt(p[1]);
                    int peerPort = Integer.parseInt(p[2]);
                    if (id < 1 || id > WORKER_COUNT || workers.containsKey(id)
                            || peerPort < 1 || peerPort > 65_535) {
                        out.println("REJECT|잘못된 워커 정보");
                        socket.close();
                        continue;
                    }
                    WorkerInfo worker = new WorkerInfo(id, out);
                    workers.put(id, worker);
                    worker.send("WELCOME|" + clock.now());
                    write("CONNECT", "SUCCESS", "워커" + id + " 연결, 대기열 초기화 (0/10)");
                    Thread reader = new Thread(() -> readWorker(worker, in),
                            "master-reader-" + id);
                    reader.setDaemon(true);
                    reader.start();
                } catch (IllegalArgumentException e) {
                    out.println("REJECT|HELLO 값 오류");
                    socket.close();
                }
            } catch (SocketTimeoutException e) {
                throw new IOException("워커 연결 제한시간 초과: " + workers.size()
                        + "/" + WORKER_COUNT + " 연결", e);
            }
        }
    }

    // Worker 메시지 반복 수신
    private void readWorker(WorkerInfo worker, BufferedReader in) {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                handleMessage(worker, line);
            }
        } catch (IOException e) {
            synchronized (lock) {
                disconnectWorker(worker, e.getMessage());
            }
        } finally {
            synchronized (lock) {
                disconnectWorker(worker, "연결 종료");
            }
        }
    }

    // Worker 메시지 유형별 처리
    private void handleMessage(WorkerInfo worker, String line) {
        synchronized (lock) {
            try {
                String[] p = line.split("\\|", -1);
                if (p.length == 0 || p[0].isEmpty()) {
                    throw new IllegalArgumentException("빈 메시지");
                }
                switch (p[0]) {
                    case "STATUS" -> {
                        requireLength(p, 2);
                        int size = Integer.parseInt(p[1]);
                        if (size < 0 || size > QUEUE_LIMIT) {
                            throw new IllegalArgumentException("Queue 크기 범위 오류");
                        }
                        worker.queueSize = size;
                    }
                    case "RESULT" -> handleResult(worker, p);
                    case "P2P" -> handleP2p(worker, p);
                    case "LOG_REQUEST" -> {
                        requireLength(p, 1);
                        worker.logRequested = true;
                    }
                    case "TERMINATE_ACK" -> {
                        requireLength(p, 1);
                        if (!worker.terminationAcked) {
                            worker.terminationAcked = true;
                            terminationAcks.countDown();
                        }
                    }
                    default -> write("PROTO", "WARN", "워커" + worker.id
                            + " 알 수 없는 메시지: " + line);
                }
            } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                write("PROTO", "WARN", "워커" + worker.id + " 메시지 거부: "
                        + safeMessageType(line) + " (" + e.getMessage() + ")");
            }
            if (!terminating) {
                dispatchAvailableTasks();
                checkCompletion();
            }
        }
    }

    // 작업 성공·실패 결과 처리
    private void handleResult(WorkerInfo worker, String[] p) {
        requireLength(p, 8);
        Task task = allTasks.get(p[1]);
        if (task == null) throw new IllegalArgumentException("알 수 없는 Task ID");
        int attempt = Integer.parseInt(p[2]);
        String result = p[3];
        double duration = Double.parseDouble(p[4]);
        double wait = Double.parseDouble(p[5]);
        int queueSize = Integer.parseInt(p[6]);
        String reason = p[7];
        if (attempt < 0 || !Double.isFinite(duration) || !Double.isFinite(wait)
                || duration < 0 || duration > 3.0 || wait < 0
                || queueSize < 0 || queueSize > QUEUE_LIMIT
                || (!"SUCCESS".equals(result) && !"FAIL".equals(result))
                || (!("PROCESS".equals(reason) || "QUEUE_FULL".equals(reason)))
                || ("SUCCESS".equals(result) && !"PROCESS".equals(reason))) {
            throw new IllegalArgumentException("RESULT 값 범위 오류");
        }
        worker.queueSize = queueSize;
        worker.taskAttempts.remove(task.id);
        String attemptKey = task.id + ":" + attempt;
        if (!processedAttempts.add(attemptKey)) {
            write("RESULT", "WARN", "KV[" + task.id + "] 중복 결과 무시, 시도=" + attempt);
            worker.send("RESULT_ACK|" + task.id + "|" + attempt + "|" + result
                    + "|" + clock.now());
            return;
        }
        if (completedTaskIds.contains(task.id)) {
            write("RESULT", "WARN", "KV[" + task.id + "] 이미 완료된 결과 무시, 시도=" + attempt);
            worker.send("RESULT_ACK|" + task.id + "|" + attempt + "|" + result
                    + "|" + clock.now());
            return;
        }
        clock.advanceSeconds(duration + 1.0);
        if ("PROCESS".equals(reason)) {
            worker.processed++;
            worker.totalWait += wait;
        }
        if ("SUCCESS".equals(result)) {
            worker.success++;
            totalSuccess++;
            completedTaskIds.add(task.id);
            retryQueuedTaskIds.remove(task.id);
            retryExcludedWorkerIds.remove(task.id);
            kvStore.put(task.key, task.value);
            write("RESULT", "SUCCESS", "KV[" + task.id + "] 워커" + worker.id
                    + " 저장 완료, Key=" + task.key + ", 값=" + task.value
                    + ", 처리=" + p[4] + "초");
            writeProgressIfNeeded();
        } else {
            if ("PROCESS".equals(reason)) {
                worker.fail++;
                totalFail++;
            } else {
                worker.queueRejects++;
                totalQueueRejects++;
            }
            if (retryQueuedTaskIds.add(task.id)) {
                retries.offer(new Task(task.id, task.key, task.value, attempt + 1,
                        true, task.enqueuedAt));
                retryExcludedWorkerIds.put(task.id, worker.id);
                if ("PROCESS".equals(reason)) reassignmentCount++;
                else queueRetryCount++;
                write("RESULT", "FAIL", "KV[" + task.id + "] 워커" + worker.id
                        + ("PROCESS".equals(reason) ? " 처리 실패" : " Queue 초과 거부")
                        + ", 우선 재시도 Queue 등록");
            } else {
                write("RESULT", "WARN", "KV[" + task.id + "] 재시도 Queue 중복 등록 방지");
            }
        }
        worker.send("RESULT_ACK|" + task.id + "|" + attempt + "|" + result
                + "|" + clock.now());
    }

    // P2P 이전 결과 처리
    private void handleP2p(WorkerInfo worker, String[] p) {
        requireLength(p, 6);
        String direction = p[1];
        int peerId = Integer.parseInt(p[2]);
        String transferId = p[3];
        int count = Integer.parseInt(p[4]);
        String taskRefs = p[5];
        Map<String, Integer> transferred = parseTaskRefs(taskRefs);
        if (!("SENT".equals(direction) || "RECEIVED".equals(direction)) || peerId < 1
                || peerId > WORKER_COUNT || transferId.isBlank() || count < 1
                || count > 3 || transferred.size() != count) {
            throw new IllegalArgumentException("P2P 값 오류");
        }
        clock.advanceSeconds(1.0);
        if ("SENT".equals(direction)) {
            worker.p2pSent += count;
            worker.p2pSentEvents++;
            p2pEvents++;
            WorkerInfo target = workers.get(peerId);
            for (Map.Entry<String, Integer> entry : transferred.entrySet()) {
                worker.taskAttempts.remove(entry.getKey());
                if (target != null) {
                    target.taskAttempts.put(entry.getKey(), entry.getValue());
                    if (entry.getValue() > 0) target.retryReceived++;
                }
            }
        } else {
            worker.p2pReceived += count;
            worker.p2pReceivedEvents++;
        }
        String action = "SENT".equals(direction) ? "작업 전송" : "작업 수신";
        write("LB", "SUCCESS", "워커" + worker.id + " " + action
                + ", 대상 워커" + peerId + ", 전송ID=" + transferId
                + ", 수량=" + count + ", 작업=" + taskRefs);
        worker.send("P2P_ACK|" + direction + "|" + peerId + "|" + transferId
                + "|" + count + "|" + taskRefs + "|" + clock.now());
    }

    // 여유 Queue 대상 작업 배정
    private void dispatchAvailableTasks() {
        while (true) {
            Task task = pollNextTask();
            if (task == null) return;
            int excludedWorkerId = task.retry
                    ? retryExcludedWorkerIds.getOrDefault(task.id, 0) : 0;
            WorkerInfo target = selectLeastLoadedWorker(excludedWorkerId);
            if (target == null) {
                restoreUndispatchedTask(task);
                return;
            }
            retryExcludedWorkerIds.remove(task.id);
            task.enqueuedAt = clock.advanceSeconds(1.0);
            target.queueSize++;
            target.taskAttempts.put(task.id, task.attempts);
            if (task.retry) target.retryReceived++;
            target.send("TASK|" + task.wire());
            String priority = task.retry ? "우선 재시도" : "일반";
            write("DISTRIB", "INFO", "KV[" + task.id + "] -> 워커" + target.id
                    + " 배정, 대기열=" + target.queueSize + "/10, " + priority
                    + (excludedWorkerId == 0 ? "" : ", 이전 실패 워커" + excludedWorkerId + " 제외"));
        }
    }

    // 배정 가능한 Worker가 없을 때 선택한 작업을 원래 Queue에 복원
    private void restoreUndispatchedTask(Task task) {
        if (task.retry) {
            retries.offer(task);
            retryQueuedTaskIds.add(task.id);
        } else {
            pending.addFirst(task);
        }
    }

    // 완료되었거나 이미 취소된 재시도 항목을 건너뛰고 다음 작업 선택
    private Task pollNextTask() {
        while (!retries.isEmpty()) {
            Task task = retries.poll();
            if (completedTaskIds.contains(task.id)) {
                retryQueuedTaskIds.remove(task.id);
                continue;
            }
            if (!retryQueuedTaskIds.remove(task.id)) continue;
            return task;
        }
        while (!pending.isEmpty()) {
            Task task = pending.pollFirst();
            if (!completedTaskIds.contains(task.id)) return task;
        }
        return null;
    }

    // 최소 Queue Worker 선택
    private WorkerInfo selectLeastLoadedWorker(int excludedWorkerId) {
        List<WorkerInfo> candidates = new ArrayList<>();
        int smallest = Integer.MAX_VALUE;
        for (WorkerInfo worker : workers.values()) {
            if (!worker.connected || worker.id == excludedWorkerId
                    || worker.queueSize >= QUEUE_LIMIT) continue;
            if (worker.queueSize < smallest) {
                smallest = worker.queueSize;
                candidates.clear();
                candidates.add(worker);
            } else if (worker.queueSize == smallest) {
                candidates.add(worker);
            }
        }
        if (candidates.isEmpty()) return null;
        candidates.sort(Comparator.comparingInt(w -> w.id));
        WorkerInfo selected = candidates.get(tieCursor % candidates.size());
        tieCursor++;
        return selected;
    }

    // 전체 작업 완료 조건 확인
    private void checkCompletion() {
        if (completedTaskIds.size() != TASK_COUNT || kvStore.size() != TASK_COUNT) return;
        terminating = true;
        pending.clear();
        retries.clear();
        retryQueuedTaskIds.clear();
        retryExcludedWorkerIds.clear();
        write("DISTRIB", "SUCCESS", "KV 작업 5,000개 처리 완료, 종료 신호 전송");
        for (WorkerInfo worker : workers.values()) worker.send(terminationMessage());
        completionSignal.countDown();
    }

    // Master 최종 통계 기록
    private void writeFinalStatistics() {
        write("KV", "INFO", "=== KV 저장소 전체 (Key 오름차순) ===");
        for (Map.Entry<String, Integer> entry : new java.util.TreeMap<>(kvStore).entrySet()) {
            write("KV", "SUCCESS", "Key=" + entry.getKey() + ", Value=" + entry.getValue());
        }
        write("STAT", "INFO", "=== 최종 통계 ===");
        write("STAT", "INFO", "KV 처리 완료 수: " + kvStore.size());
        int totalAttempts = totalSuccess + totalFail;
        double successRate = totalAttempts == 0 ? 0.0 : totalSuccess * 100.0 / totalAttempts;
        double failRate = totalAttempts == 0 ? 0.0 : totalFail * 100.0 / totalAttempts;
        write("STAT", "SUCCESS", String.format("총 성공: %,d (%.1f%%)", totalSuccess, successRate));
        write("STAT", "FAIL", String.format("총 실패(재시도 전): %,d (%.1f%%)", totalFail, failRate));
        write("STAT", "WARN", "Queue 초과 거부 수: " + totalQueueRejects);
        write("STAT", "INFO", "장애 재할당 수: " + reassignmentCount);
        write("STAT", "INFO", "Queue 초과 재시도 수: " + queueRetryCount);
        write("STAT", "INFO", "P2P 부하 분산 수: " + p2pEvents);
        for (WorkerInfo worker : workers.values()) {
            double averageWait = worker.processed == 0 ? 0.0
                    : worker.totalWait / worker.processed;
            write("STAT", "INFO", "워커" + worker.id + ": 처리=" + worker.processed
                    + ", 성공=" + worker.success + ", 실패=" + worker.fail
                    + ", 장애재할당발생=" + worker.fail
                    + ", 평균대기=" + String.format(java.util.Locale.US, "%.2f", averageWait) + "초"
                    + ", 재시도수신=" + worker.retryReceived
                    + ", Queue거부=" + worker.queueRejects
                    + ", P2P 이벤트 전송=" + worker.p2pSentEvents
                    + ", 수신=" + worker.p2pReceivedEvents
                    + ", P2P 작업 전송=" + worker.p2pSent + ", 수신=" + worker.p2pReceived);
        }
        write("STAT", "INFO", "전체 수행 시간: " + VirtualClock.format(clock.now()) + "초");
    }

    // 연결이 끊긴 Worker가 보유하던 미완료 작업을 우선 재시도 Queue로 복구
    private void disconnectWorker(WorkerInfo worker, String reason) {
        if (!worker.connected) return;
        worker.connected = false;
        if (terminating) return;
        write("CONNECT", "FAIL", "워커" + worker.id + " 연결 해제"
                + (reason == null || reason.isBlank() ? "" : ": " + reason));
        for (Map.Entry<String, Integer> entry : new HashMap<>(worker.taskAttempts).entrySet()) {
            Task original = allTasks.get(entry.getKey());
            if (original == null || completedTaskIds.contains(original.id)
                    || !retryQueuedTaskIds.add(original.id)) continue;
            retries.offer(new Task(original.id, original.key, original.value,
                    entry.getValue() + 1, true, original.enqueuedAt));
            retryExcludedWorkerIds.put(original.id, worker.id);
            reassignmentCount++;
            write("RESULT", "WARN", "KV[" + original.id + "] 워커 연결 해제로 재할당");
        }
        worker.taskAttempts.clear();
        worker.queueSize = 0;
        dispatchAvailableTasks();
    }

    // Worker1에 Master 로그 전송
    private void sendMasterLog() {
        WorkerInfo receiver = workers.get(1);
        if (receiver == null || !receiver.connected || !receiver.logRequested) return;
        try {
            byte[] content = Files.readAllBytes(Path.of("Master.txt"));
            receiver.send("MASTER_LOG_BEGIN|" + content.length);
            for (int offset = 0; offset < content.length; offset += 4096) {
                int size = Math.min(4096, content.length - offset);
                byte[] chunk = java.util.Arrays.copyOfRange(content, offset, offset + size);
                receiver.send("MASTER_LOG_CHUNK|" + java.util.Base64.getEncoder().encodeToString(chunk));
            }
            receiver.send("MASTER_LOG_END");
        } catch (IOException e) {
            // 로그 전송 실패는 Master 실행 로그에 기록하지 않는다.
        }
    }

    // Master 로그 출력
    private void write(String event, String status, String message) {
        log.log(clock.now(), "MASTER", event, status, message);
    }

    // 처리 완료 진행률 기록
    private void writeProgressIfNeeded() {
        int completed = completedTaskIds.size();
        if (completed < nextProgress && completed != TASK_COUNT) return;
        double rate = completed * 100.0 / TASK_COUNT;
        write("DISTRIB", "INFO", String.format("진행률: %d / %d (%.1f%%)",
                completed, TASK_COUNT, rate));
        while (nextProgress <= completed) nextProgress += 500;
    }

    // Worker 최종 통계에 필요한 Master 전체 지표를 포함한 종료 메시지
    private String terminationMessage() {
        return "TERMINATE|" + clock.now() + "|" + reassignmentCount + "|" + p2pEvents;
    }

    private static void requireLength(String[] values, int expected) {
        if (values.length != expected) {
            throw new IllegalArgumentException("필드 수 오류: " + values.length + "/" + expected);
        }
    }

    private static String safeMessageType(String line) {
        if (line == null || line.isBlank()) return "<empty>";
        int separator = line.indexOf('|');
        String type = separator < 0 ? line : line.substring(0, separator);
        return type.length() > 32 ? type.substring(0, 32) : type;
    }

    private static Map<String, Integer> parseTaskRefs(String text) {
        Map<String, Integer> result = new HashMap<>();
        if (text == null || text.isBlank()) return result;
        for (String item : text.split(",")) {
            String[] pair = item.split(":", -1);
            if (pair.length != 2 || !pair[0].matches("\\d{4}")) {
                throw new IllegalArgumentException("P2P 작업 참조 형식 오류");
            }
            int attempt = Integer.parseInt(pair[1]);
            if (attempt < 0 || result.put(pair[0], attempt) != null) {
                throw new IllegalArgumentException("P2P 작업 참조 값 오류");
            }
        }
        return result;
    }
}
