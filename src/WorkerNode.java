import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

class WorkerNode implements Runnable {
    private static final int QUEUE_LIMIT = 10;
    private static final int CONNECT_TIMEOUT_MILLIS = 10_000;
    private static final int SOCKET_TIMEOUT_MILLIS = 120_000;
    private static final int P2P_TIMEOUT_MILLIS = 5_000;
    private static final int TERMINATION_CLOCK_TIMEOUT_SECONDS = 30;
    private final int id;
    private final String masterHost;
    private final int masterPort;
    private final int peerPort;
    private final Object queueLock = new Object();
    private final Object loadBalanceLock = new Object();
    private final Set<String> unresolvedTransfers = ConcurrentHashMap.newKeySet();
    private final Set<String> rejectedTransferIds = ConcurrentHashMap.newKeySet();
    private enum TransferState { ACCEPTED, NOT_FOUND, REJECTED, UNKNOWN }
    private final Deque<Task> queue = new ArrayDeque<>();
    private final Map<String, PendingResult> pendingResults = new ConcurrentHashMap<>();
    private final Set<String> acceptedTransferIds = ConcurrentHashMap.newKeySet();
    private final ByteArrayOutputStream masterLogBytes = new ByteArrayOutputStream();
    private final CountDownLatch masterLogReceived = new CountDownLatch(1);
    private final CountDownLatch finalClockReceived = new CountDownLatch(1);
    private volatile boolean stopping;
    private volatile boolean terminationReceived;
    private boolean terminationConfirmed;
    private boolean processingEnabled;
    private volatile long masterClockMillis;
    private long workerAvailableAt;
    private long nextLoadCheck;
    private int reservedTransferSlots;
    private PrintWriter masterOut;
    private EventLogger log;
    private int success;
    private int fail;
    private int queueRejects;
    private int processed;
    private int received;
    private int p2pSent;
    private int p2pReceived;
    private int p2pSentEvents;
    private int p2pReceivedEvents;
    private int retryReceived;
    private int masterReassignments;
    private int masterP2pEvents;
    private double totalWait;
    private long transferSequence;
    private long querySequence;

    // Worker 실행 정보 설정
    WorkerNode(int id, String masterHost, int masterPort, int peerPort) {
        this.id = id;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.peerPort = peerPort;
    }

    // Worker Thread 4개 시작
    static void startFour(String host, int port) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        for (int workerId = 1; workerId <= 4; workerId++) {
            Thread thread = new Thread(new WorkerNode(workerId, host, port, 6000 + workerId),
                    "worker-" + workerId);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) thread.join();
    }

    // Worker 전체 실행
    @Override
    public void run() {
        try (EventLogger eventLog = new EventLogger("Worker" + id + ".txt")) {
            log = eventLog;
            log.header("Worker" + id + ".txt (Worker Node " + id + " Log)",
                    "Worker" + id + " | Thread-based Worker | Ready Queue max=10");
            writeAt(0, "INIT", "INFO", "워커 Thread 시작, 마스터 연결 시도");
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(masterHost, masterPort), CONNECT_TIMEOUT_MILLIS);
                socket.setKeepAlive(true);
                socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
                startPeerListener();
                masterOut = new PrintWriter(new OutputStreamWriter(
                        socket.getOutputStream(), StandardCharsets.UTF_8), true);
                sendMaster("HELLO|" + id + "|" + peerPort);
                Thread receiver = new Thread(() -> receiveMaster(socket),
                        "worker-master-reader-" + id);
                receiver.setDaemon(true);
                receiver.start();
                processLoop();
                if (id == 1 && isRemoteMaster()) sendMaster("LOG_REQUEST");
                sendMaster("TERMINATE_ACK");
                terminationConfirmed = terminationReceived && awaitFinalClock();
                synchronized (EventLogger.CONSOLE_LOCK) {
                    writeFinalStatistics();
                }
                waitForMasterLog();
            }
        } catch (IOException e) {
            writeAt(masterClockMillis, "CONNECT", "FAIL", "마스터 연결 실패: " + e.getMessage());
        }
    }

    // Master 메시지 반복 수신
    private void receiveMaster(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                try {
                    String[] p = line.split("\\|", -1);
                    if (p.length == 0 || p[0].isEmpty()) {
                        throw new IllegalArgumentException("빈 메시지");
                    }
                    switch (p[0]) {
                        case "WELCOME" -> {
                            requireLength(p, 2);
                            updateMasterClock(nonNegativeLong(p[1], "Master 시각"));
                            write("CONNECT", "SUCCESS", "마스터 연결, 대기열 초기화 (0/10)");
                        }
                        case "TASK" -> {
                            if (p.length < 2) throw new IllegalArgumentException("TASK 필드 부족");
                            receiveTask(Task.fromWire(joinFrom(p, 1)));
                        }
                        case "START" -> {
                            requireLength(p, 2);
                            updateMasterClock(nonNegativeLong(p[1], "처리 시작 시각"));
                            synchronized (queueLock) {
                                processingEnabled = true;
                                queueLock.notifyAll();
                            }
                        }
                        case "RESULT_ACK" -> receiveResultAck(p);
                        case "P2P_ACK" -> receiveP2pAck(p);
                        case "FINAL_CLOCK" -> {
                            requireLength(p, 2);
                            updateMasterClock(nonNegativeLong(p[1], "최종 Master 시각"));
                            finalClockReceived.countDown();
                        }
                        case "TERMINATE" -> {
                            requireLength(p, 4);
                            updateMasterClock(nonNegativeLong(p[1], "종료 시각"));
                            masterReassignments = nonNegativeInt(p[2], "장애 재할당 수");
                            masterP2pEvents = nonNegativeInt(p[3], "P2P 이벤트 수");
                            terminationReceived = true;
                            synchronized (queueLock) {
                                stopping = true;
                                clearQueueLocked("종료 정리");
                                queueLock.notifyAll();
                            }
                        }
                        case "MASTER_LOG_BEGIN" -> {
                            requireLength(p, 2);
                            nonNegativeLong(p[1], "Master 로그 크기");
                            masterLogBytes.reset();
                        }
                        case "MASTER_LOG_CHUNK" -> receiveMasterLogChunk(p);
                        case "MASTER_LOG_END" -> {
                            requireLength(p, 1);
                            saveMasterLog();
                        }
                        case "REJECT" -> {
                            if (p.length < 2) throw new IllegalArgumentException("REJECT 사유 누락");
                            write("CONNECT", "FAIL", "마스터 연결 거부: " + joinFrom(p, 1));
                            synchronized (queueLock) {
                                stopping = true;
                                queueLock.notifyAll();
                            }
                        }
                        default -> write("PROTO", "WARN", "알 수 없는 마스터 메시지: "
                                + safeMessageType(line));
                    }
                } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                    write("PROTO", "WARN", "마스터 메시지 거부: " + safeMessageType(line)
                            + " (" + e.getMessage() + ")");
                }
            }
        } catch (IOException e) {
            if (!stopping) write("CONNECT", "FAIL", "마스터 연결 종료: " + e.getMessage());
            synchronized (queueLock) {
                stopping = true;
                clearQueueLocked("연결 오류 정리");
                queueLock.notifyAll();
            }
        }
    }

    // Master 로그 조각 수신
    private void receiveMasterLogChunk(String[] p) {
        requireLength(p, 2);
        try {
            masterLogBytes.write(java.util.Base64.getDecoder().decode(p[1]));
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Master 로그 조각 오류", e);
        }
    }

    // Master 로그를 Worker 실행 폴더에 저장
    private void saveMasterLog() {
        try {
            Files.write(Path.of("Master.txt"), masterLogBytes.toByteArray());
        } catch (IOException ignored) {
        } finally {
            masterLogReceived.countDown();
        }
    }

    // Worker1의 Master 로그 수신 대기
    private void waitForMasterLog() {
        if (id != 1 || !isRemoteMaster()) return;
        try {
            masterLogReceived.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // 모든 Worker의 종료 ACK가 반영된 공통 최종 시각 대기
    private boolean awaitFinalClock() {
        try {
            if (finalClockReceived.await(TERMINATION_CLOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS)) return true;
            write("TERMINATE", "FAIL", "최종 시각 수신 제한시간 초과, 정상 종료 확인 불가");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            write("TERMINATE", "FAIL", "최종 시각 대기 중단, 정상 종료 확인 불가");
        }
        return false;
    }

    // 원격 Master 연결 여부 확인
    private boolean isRemoteMaster() {
        return !"localhost".equalsIgnoreCase(masterHost)
                && !"127.0.0.1".equals(masterHost)
                && !"::1".equals(masterHost);
    }

    // Master 확정 결과 시각 반영
    private void receiveResultAck(String[] p) {
        requireLength(p, 5);
        int attempt = Integer.parseInt(p[2]);
        if (attempt < 0 || (!"SUCCESS".equals(p[3]) && !"FAIL".equals(p[3]))) {
            throw new IllegalArgumentException("RESULT_ACK 값 오류");
        }
        PendingResult result = pendingResults.remove(attemptKey(p[1], attempt));
        if (result == null) return;
        updateMasterClock(nonNegativeLong(p[4], "결과 ACK 시각"));
        if (result.success) {
            write("PROC", "SUCCESS", "KV[" + result.task.id + "] 저장 완료, 처리="
                    + format(result.duration) + "초, 대기=" + format(result.wait) + "초");
        } else {
            write("PROC", "FAIL", "KV[" + result.task.id + "] "
                    + ("QUEUE_FULL".equals(result.reason)
                    ? "Queue 초과로 재할당" : "처리 실패, 20% 규칙"));
        }
    }

    // Master 확정 P2P 시각 반영
    private void receiveP2pAck(String[] p) {
        requireLength(p, 7);
        if (!("SENT".equals(p[1]) || "RECEIVED".equals(p[1]))
                || p[3].isBlank() || Integer.parseInt(p[4]) < 1) {
            throw new IllegalArgumentException("P2P_ACK 값 오류");
        }
        updateMasterClock(nonNegativeLong(p[6], "P2P ACK 시각"));
        String action = "SENT".equals(p[1]) ? "이전 완료" : "수신 완료";
        write("LB", "SUCCESS", "워커" + p[2] + " " + action + ", 전송ID="
                + p[3] + ", 작업=" + p[5]);
    }

    // Master 작업 Queue 삽입
    private void receiveTask(Task task) {
        updateMasterClock(task.enqueuedAt);
        boolean accepted;
        int size;
        synchronized (queueLock) {
            accepted = queue.size() + reservedTransferSlots < QUEUE_LIMIT;
            if (accepted) {
                addTaskLocked(task, task.retry, "Master 수신");
                size = queue.size() + reservedTransferSlots;
                received++;
                if (task.retry) retryReceived++;
            } else {
                size = queue.size() + reservedTransferSlots;
            }
        }
        if (!accepted) {
            pendingResults.put(attemptKey(task.id, task.attempts),
                    new PendingResult(task, false, 0, 0, "QUEUE_FULL"));
            queueRejects++;
            write("QUEUE", "FAIL", "KV[" + task.id + "] Queue 초과 거부 (10/10)");
            sendMaster("RESULT|" + task.id + "|" + task.attempts
                    + "|FAIL|0.00|0.00|" + size + "|QUEUE_FULL");
            return;
        }
        String type = task.retry ? "우선 재시도" : "일반";
        write("RECV", "INFO", "KV[" + task.id + "] " + type + " 작업 수신, 대기열="
                + size + "/10");
        sendMaster("STATUS|" + size);
        checkLoadBalance();
        synchronized (queueLock) {
            queueLock.notifyAll();
        }
    }

    // Queue 작업 처리 반복
    private void processLoop() {
        while (true) {
            Task task = takeTask();
            if (task == null) return;
            long start = Math.max(workerAvailableAt, task.enqueuedAt);
            double wait = Math.max(0, (start - task.enqueuedAt) / 1000.0);
            double duration = ThreadLocalRandom.current().nextDouble(1.0, 3.0);
            synchronized (queueLock) {
                workerAvailableAt = start + Math.round(duration * 1000.0);
            }
            boolean ok = ThreadLocalRandom.current().nextInt(100) < 80;
            int size = queueSize();
            processed++;
            totalWait += wait;
            if (ok) success++;
            else fail++;
            pendingResults.put(attemptKey(task.id, task.attempts),
                    new PendingResult(task, ok, duration, wait, "PROCESS"));
            sendMaster("RESULT|" + task.id + "|" + task.attempts + "|"
                    + (ok ? "SUCCESS" : "FAIL") + "|"
                    + format(duration) + "|" + format(wait) + "|" + size + "|PROCESS");
            sendMaster("STATUS|" + size);
            checkLoadBalance();
        }
    }

    // 다음 작업 대기·반환
    private Task takeTask() {
        synchronized (queueLock) {
            while ((!processingEnabled || queue.isEmpty()) && !stopping) {
                try {
                    queueLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            if (stopping) return null;
            return removeTaskLocked(false, "처리 시작");
        }
    }

    // P2P 부하 분산 조건 확인
    private void checkLoadBalance() {
        synchronized (loadBalanceLock) { checkLoadBalanceLocked(); }
    }

    private void checkLoadBalanceLocked() {
        long now = masterClockMillis;
        if (now < nextLoadCheck) return;
        nextLoadCheck = now + ThreadLocalRandom.current().nextLong(1_000, 3_001);

        int sourceQueueSize;
        synchronized (queueLock) {
            sourceQueueSize = queue.size();
        }
        double estimatedWait = sourceQueueSize * 2.0;
        if (estimatedWait <= 15.0) return;

        String requestId = "Q" + id + "-" + (++querySequence);
        int[] targetLoad = findLeastLoadedPeer(requestId);
        if (targetLoad == null) {
            write("LB", "WARN", "예상 대기=" + format(estimatedWait)
                    + "초, 응답 가능한 P2P 대상 없음");
            return;
        }

        List<Task> batch = new ArrayList<>();
        synchronized (queueLock) {
            estimatedWait = queue.size() * 2.0;
            if (estimatedWait <= 15.0) return;
            int transferable = Math.min(3,
                    Math.min(queue.size(), QUEUE_LIMIT - targetLoad[1]));
            for (int index = 0; index < transferable; index++) batch.add(removeTaskLocked(true, "P2P 송신"));
            reservedTransferSlots += batch.size();
        }
        if (batch.isEmpty()) return;

        int target = targetLoad[0];
        String transferId = id + "-" + (++transferSequence);
        TransferState state = transfer(target, transferId, batch);
        if (state == TransferState.UNKNOWN) {
            unresolvedTransfers.add(transferId);
            write("LB", "WARN", "전송ID=" + transferId + " 소유권 확인 보류, 예약 유지·복원 금지");
            Thread resolver = new Thread(() -> {
                while (!stopping) {
                    TransferState confirmed = queryTransferStatus(target, transferId);
                    if (confirmed != TransferState.UNKNOWN) {
                        synchronized (loadBalanceLock) {
                            if (!stopping) {
                                finishTransfer(target, transferId, batch, confirmed);
                                unresolvedTransfers.remove(transferId);
                            }
                        }
                        return;
                    }
                    // Real backoff for unavailable peer, not simulated processing time.
                    try { new CountDownLatch(1).await(1, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                }
            }, "worker-transfer-status-" + transferId);
            resolver.setDaemon(true);
            resolver.start();
        } else {
            finishTransfer(target, transferId, batch, state);
        }
    }

    private void finishTransfer(int target, String transferId, List<Task> batch, TransferState state) {
        if (state == TransferState.UNKNOWN) throw new IllegalArgumentException("확인되지 않은 이전");
        if (state == TransferState.ACCEPTED) {
            synchronized (queueLock) { reservedTransferSlots -= batch.size(); }
            p2pSent += batch.size();
            p2pSentEvents++;
            sendMaster("P2P|SENT|" + target + "|" + transferId + "|" + batch.size() + "|" + taskRefs(batch));
            write("LB", "INFO", "전송ID=" + transferId + " ACCEPTED 확인, 복원 없이 이전 완료");
        } else {
            synchronized (queueLock) {
                for (int index = batch.size() - 1; index >= 0; index--)
                    addTaskLocked(batch.get(index), false, "P2P 실패 복원");
                reservedTransferSlots -= batch.size();
                queueLock.notifyAll();
            }
            write("LB", "INFO", "전송ID=" + transferId + " " + state + " 확인, 작업 복원");
        }
        sendMaster("STATUS|" + queueSize());
    }

    // 다른 Worker의 Queue를 조회해 가장 여유 있는 대상을 선택
    private int[] findLeastLoadedPeer(String requestId) {
        int targetId = 0;
        int smallestQueue = Integer.MAX_VALUE;
        for (int candidateId = 1; candidateId <= 4; candidateId++) {
            if (candidateId == id) continue;
            int candidateQueue = queryPeerQueue(candidateId, requestId);
            if (candidateQueue < 0 || candidateQueue >= QUEUE_LIMIT) continue;
            if (candidateQueue < smallestQueue
                    || (candidateQueue == smallestQueue && candidateId < targetId)) {
                targetId = candidateId;
                smallestQueue = candidateQueue;
            }
        }
        return targetId == 0 ? null : new int[] {targetId, smallestQueue};
    }

    // P2P 소켓으로 대상 Worker의 현재 Queue 크기 조회
    private int queryPeerQueue(int target, String requestId) {
        try (Socket peer = new Socket()) {
            peer.connect(new InetSocketAddress("127.0.0.1", 6000 + target),
                    P2P_TIMEOUT_MILLIS);
            peer.setSoTimeout(P2P_TIMEOUT_MILLIS);
            try (BufferedReader in = new BufferedReader(new InputStreamReader(
                         peer.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(
                         peer.getOutputStream(), StandardCharsets.UTF_8), true)) {
                reportPeerMessage("QUEUE_QUERY", id, target);
                out.println("QUEUE_QUERY|" + requestId + "|" + id);
                String response = in.readLine();
                if (response != null) {
                    reportPeerMessage("QUEUE_STATUS", target, id);
                }
                String[] p = response == null ? new String[0] : response.split("\\|", -1);
                requireLength(p, 4);
                if (!"QUEUE_STATUS".equals(p[0]) || !requestId.equals(p[1])
                        || Integer.parseInt(p[2]) != target) {
                    throw new IllegalArgumentException("QUEUE_STATUS 값 오류");
                }
                int size = Integer.parseInt(p[3]);
                return size >= 0 && size <= QUEUE_LIMIT ? size : -1;
            }
        } catch (IOException | IllegalArgumentException e) {
            return -1;
        }
    }

    // 인접 Worker 작업 이전 요청
    private TransferState transfer(int target, String transferId, List<Task> batch) {
        if (batch.isEmpty()) return TransferState.REJECTED;
        StringBuilder data = new StringBuilder();
        for (Task task : batch) {
            if (data.length() > 0) data.append(';');
            data.append(task.wire());
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            try (Socket peer = new Socket()) {
                peer.connect(new InetSocketAddress("127.0.0.1", 6000 + target),
                        P2P_TIMEOUT_MILLIS);
                peer.setSoTimeout(P2P_TIMEOUT_MILLIS);
                try (BufferedReader in = new BufferedReader(new InputStreamReader(
                             peer.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(
                             peer.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    reportPeerMessage("TRANSFER", id, target);
                    out.println("TRANSFER|" + transferId + "|" + id + "|" + data);
                    String response = in.readLine();
                    if (response != null) {
                        String responseType = response.startsWith("ACK|")
                                ? "TRANSFER_ACK" : "TRANSFER_REJECT";
                        reportPeerMessage(responseType, target, id);
                    }
                    if (("ACK|" + transferId).equals(response)) return TransferState.ACCEPTED;
                    if (response != null && response.startsWith("REJECT|")) break;
                }
            } catch (IOException e) {
                if (attempt == 1) {
                    write("LB", "WARN", "전송ID=" + transferId + " P2P ACK 제한시간 초과");
                }
            }
        }
        return queryTransferStatus(target, transferId);
    }

    // NOT_FOUND is a cancellation decision: receiver fences any later TRANSFER with this ID.
    private TransferState queryTransferStatus(int target, String transferId) {
        try (Socket peer = new Socket()) {
            peer.connect(new InetSocketAddress("127.0.0.1", 6000 + target), P2P_TIMEOUT_MILLIS);
            peer.setSoTimeout(P2P_TIMEOUT_MILLIS);
            try (BufferedReader in = new BufferedReader(new InputStreamReader(peer.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(peer.getOutputStream(), StandardCharsets.UTF_8), true)) {
                reportPeerMessage("TRANSFER_STATUS", id, target);
                out.println("TRANSFER_STATUS|" + transferId + "|" + id);
                String response = in.readLine();
                if (response == null) return TransferState.UNKNOWN;
                reportPeerMessage("TRANSFER_STATE", target, id);
                String[] p = response.split("\\|", -1);
                if (p.length != 3 || !"TRANSFER_STATE".equals(p[0]) || !transferId.equals(p[1])) return TransferState.UNKNOWN;
                return TransferState.valueOf(p[2]);
            }
        } catch (IOException | IllegalArgumentException e) { return TransferState.UNKNOWN; }
    }

    // P2P 작업 수신 서버 시작
    private void startPeerListener() {
        Thread peerThread = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(peerPort)) {
                server.setSoTimeout(1_000);
                while (!stopping) {
                    try (Socket peer = server.accept()) {
                        peer.setSoTimeout(P2P_TIMEOUT_MILLIS);
                        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                                     peer.getInputStream(), StandardCharsets.UTF_8));
                             PrintWriter out = new PrintWriter(new OutputStreamWriter(
                                     peer.getOutputStream(), StandardCharsets.UTF_8), true)) {
                        String line = in.readLine();
                        if (line == null) continue;
                        try {
                            String[] p = line.split("\\|", -1);
                            if (p.length == 3 && "QUEUE_QUERY".equals(p[0])) {
                                if (p[1].isBlank()) {
                                    throw new IllegalArgumentException("QUEUE_QUERY ID 오류");
                                }
                                int sourceId = Integer.parseInt(p[2]);
                                if (sourceId < 1 || sourceId > 4 || sourceId == id) {
                                    throw new IllegalArgumentException("조회 Worker 번호 오류");
                                }
                                out.println("QUEUE_STATUS|" + p[1] + "|" + id
                                        + "|" + queueSize());
                                continue;
                            }
                            if (p.length == 3 && "TRANSFER_STATUS".equals(p[0])) {
                                int source = Integer.parseInt(p[2]);
                                validateTransferId(p[1], source);
                                TransferState state;
                                synchronized (queueLock) {
                                    if (acceptedTransferIds.contains(p[1])) state = TransferState.ACCEPTED;
                                    else if (rejectedTransferIds.contains(p[1])) state = TransferState.REJECTED;
                                    else {
                                        rejectedTransferIds.add(p[1]);
                                        state = TransferState.NOT_FOUND;
                                    }
                                }
                                out.println("TRANSFER_STATE|" + p[1] + "|" + state);
                                continue;
                            }
                            requireLength(p, 4);
                            if (!"TRANSFER".equals(p[0]) || p[1].isBlank()) {
                                throw new IllegalArgumentException("TRANSFER 형식 오류");
                            }
                            String transferId = p[1];
                            int sourceId = Integer.parseInt(p[2]);
                            if (sourceId < 1 || sourceId > 4 || sourceId == id) {
                                throw new IllegalArgumentException("송신 Worker 번호 오류");
                            }
                            validateTransferId(transferId, sourceId);
                            if (acceptedTransferIds.contains(transferId)) {
                                out.println("ACK|" + transferId);
                                continue;
                            }
                            if (rejectedTransferIds.contains(transferId)) {
                                out.println("REJECT|이전 취소 확정");
                                continue;
                            }
                            String[] encoded = p[3].split(";");
                            if (encoded.length < 1 || encoded.length > 3) {
                                throw new IllegalArgumentException("P2P 작업 수 오류");
                            }
                            List<Task> tasks = new ArrayList<>();
                            for (String item : encoded) tasks.add(Task.fromWire(item));
                            boolean accepted;
                            synchronized (queueLock) {
                                accepted = queue.size() + reservedTransferSlots + tasks.size()
                                        <= QUEUE_LIMIT
                                        && tasks.stream().noneMatch(this::containsTaskLocked);
                                if (accepted) {
                                    for (Task task : tasks) addTaskLocked(task, false, "P2P 수신");
                                    acceptedTransferIds.add(transferId);
                                    received += tasks.size();
                                    for (Task task : tasks) {
                                        if (task.retry) retryReceived++;
                                    }
                                    queueLock.notifyAll();
                                } else {
                                    rejectedTransferIds.add(transferId);
                                }
                            }
                            if (accepted) {
                                // 수락 여부를 먼저 확정해 송신자가 불필요하게 작업을 복원하지 않게 한다.
                                out.println("ACK|" + transferId);
                                p2pReceived += tasks.size();
                                p2pReceivedEvents++;
                                sendMaster("P2P|RECEIVED|" + sourceId + "|" + transferId
                                        + "|" + tasks.size() + "|" + taskRefs(tasks));
                                int size = queueSize();
                                sendMaster("STATUS|" + size);
                            } else {
                                out.println("REJECT|Queue 여유 부족 또는 중복 작업");
                            }
                        } catch (IllegalArgumentException e) {
                            write("PROTO", "WARN", "P2P 메시지 거부: " + e.getMessage());
                            out.println("REJECT|메시지 형식 오류");
                        }
                        }
                    } catch (SocketTimeoutException ignored) {
                        // stopping 상태를 주기적으로 확인한다.
                    } catch (IOException e) {
                        if (!stopping) write("LB", "WARN", "P2P 연결 처리 실패: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                if (!stopping) write("CONNECT", "FAIL", "P2P 포트 시작 실패: " + e.getMessage());
            }
        }, "worker-peer-listener-" + id);
        peerThread.setDaemon(true);
        peerThread.start();
    }

    private void validateTransferId(String transferId, int source) {
        if (source < 1 || source > 4 || source == id || !transferId.matches(source + "-[1-9][0-9]*"))
            throw new IllegalArgumentException("이전 ID/송신 Worker 오류");
    }

    // 현재 Queue 크기 반환
    private int queueSize() {
        synchronized (queueLock) {
            return queue.size() + reservedTransferSlots;
        }
    }

    // Queue 변경과 전후 크기 기록은 같은 잠금 안에서 수행한다.
    // 예약 슬롯은 용량 제한에만 사용하며 WARN은 실제 대기 작업 수 기준이다.
    private void addTaskLocked(Task task, boolean first, String reason) {
        if (!Thread.holdsLock(queueLock)) throw new IllegalStateException("Queue lock required");
        int before = queue.size();
        if (first) queue.addFirst(task);
        else queue.addLast(task);
        warnQueueChangeLocked(task, before, reason);
    }

    private Task removeTaskLocked(boolean last, String reason) {
        if (!Thread.holdsLock(queueLock)) throw new IllegalStateException("Queue lock required");
        int before = queue.size();
        Task task = last ? queue.pollLast() : queue.pollFirst();
        if (task != null) warnQueueChangeLocked(task, before, reason);
        return task;
    }

    private void clearQueueLocked(String reason) {
        while (!queue.isEmpty()) removeTaskLocked(false, reason);
    }

    private void warnQueueChangeLocked(Task task, int before, String reason) {
        int after = queue.size();
        if (before > 7 || after > 7) {
            write("QUEUE", "WARN", "대기열 70% 초과 구간 변경, KV[" + task.id
                    + "], 사유=" + reason + ", 크기=" + before + "->" + after + "/10");
        }
    }

    // Master 메시지 전송
    private synchronized void sendMaster(String message) {
        if (masterOut != null) masterOut.println(message);
    }

    // Worker 간 단방향 메시지 한 건을 Master에 알린다.
    private void reportPeerMessage(String messageType, int sourceId, int targetId) {
        sendMaster("NET|" + messageType + "|" + sourceId + "|" + targetId);
    }

    // Master 단일 가상 시각 반영
    private synchronized void updateMasterClock(long value) {
        masterClockMillis = Math.max(masterClockMillis, value);
    }

    // Worker 최종 통계 기록
    private void writeFinalStatistics() {
        write("STAT", "INFO", "=== WORKER" + id + " 최종 통계 ===");
        write("STAT", "INFO", "작업 처리량: " + success);
        write("STAT", "SUCCESS", "성공 횟수: " + success);
        write("STAT", "FAIL", "실패 횟수: " + fail);
        write("STAT", "INFO", "평균 대기 시간: "
                + format(processed == 0 ? 0 : totalWait / processed) + "초");
        write("STAT", "INFO", "P2P 부하 분산 이벤트 횟수: "
                + (p2pSentEvents + p2pReceivedEvents));
        write("STAT", "INFO", "장애 재할당 횟수: " + fail);
        write("STAT", "INFO", "전체 수행 시간: " + VirtualClock.format(masterClockMillis) + "초");
        write("TERMINATE", terminationConfirmed && unresolvedTransfers.isEmpty() ? "SUCCESS" : "FAIL",
                "워커" + id + (terminationConfirmed && unresolvedTransfers.isEmpty() ? " 정상 연결 해제" : " 불완전 종료: 정상 종료 또는 P2P 소유권 확인 실패"));
    }

    // Worker 로그 출력
    private void write(String event, String status, String message) {
        writeAt(masterClockMillis, event, status, message);
    }

    // 지정 Master 시각 로그 출력
    private void writeAt(long time, String event, String status, String message) {
        if (log != null) log.log(time, "WORKER" + id, event, status, message);
    }

    // P2P 작업 ID와 처리 시도 번호 문자열 생성
    private static String taskRefs(List<Task> tasks) {
        StringBuilder ids = new StringBuilder();
        for (Task task : tasks) {
            if (ids.length() > 0) ids.append(',');
            ids.append(task.id).append(':').append(task.attempts);
        }
        return ids.toString();
    }

    private boolean containsTaskLocked(Task candidate) {
        if (pendingResults.containsKey(attemptKey(candidate.id, candidate.attempts))) return true;
        for (Task task : queue) {
            if (task.id.equals(candidate.id) && task.attempts == candidate.attempts) return true;
        }
        return false;
    }

    // 분리 문자열 재결합
    private static String joinFrom(String[] values, int start) {
        StringBuilder result = new StringBuilder();
        for (int index = start; index < values.length; index++) {
            if (index > start) result.append('|');
            result.append(values[index]);
        }
        return result.toString();
    }

    private static String attemptKey(String taskId, int attempt) {
        return taskId + ":" + attempt;
    }

    private static void requireLength(String[] values, int expected) {
        if (values.length != expected) {
            throw new IllegalArgumentException("필드 수 오류: " + values.length + "/" + expected);
        }
    }

    private static long nonNegativeLong(String value, String label) {
        long parsed = Long.parseLong(value);
        if (parsed < 0) throw new IllegalArgumentException(label + " 범위 오류");
        return parsed;
    }

    private static int nonNegativeInt(String value, String label) {
        int parsed = Integer.parseInt(value);
        if (parsed < 0) throw new IllegalArgumentException(label + " 범위 오류");
        return parsed;
    }

    private static String safeMessageType(String line) {
        if (line == null || line.isBlank()) return "<empty>";
        int separator = line.indexOf('|');
        String type = separator < 0 ? line : line.substring(0, separator);
        return type.length() > 32 ? type.substring(0, 32) : type;
    }

    // 소수점 둘째 자리 형식화
    private static String format(double value) {
        return String.format(java.util.Locale.US, "%.2f", value);
    }
}
