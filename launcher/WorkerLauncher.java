import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

/** Shared launcher only: the distributed application and its protocol are unchanged. */
public class WorkerLauncher {
    static final String[] LOGS = {"Master.txt", "Worker1.txt", "Worker2.txt", "Worker3.txt", "Worker4.txt"};
    static final String[] METRICS = {"작업 처리량", "성공 횟수", "실패 횟수", "평균 대기 시간",
            "P2P 부하 분산 이벤트 횟수", "장애 재할당 횟수", "전체 수행 시간"};
    static final Pattern ERROR = Pattern.compile("Exception|Caused by:|\\| CONNECT \\| FAIL|\\| TERMINATE \\| (?:FAIL|WARN)");
    static volatile Process child;
    record Line(int stream, String text) {}
    record Result(int code, boolean errors, double seconds) {}

    public static void main(String[] args) {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopChild()));
        int code = 1;
        try {
            if (args.length > 2) throw new IllegalArgumentException("사용법: [Master_IP [포트]]");
            String host = args.length > 0 ? args[0] : "32.236.94.251";
            int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
            if (host.isBlank() || host.startsWith("-") || host.matches(".*\\s.*") || port < 1 || port > 65535)
                throw new IllegalArgumentException("Master 주소와 1~65535 포트를 확인하세요.");
            Path root = Path.of(WorkerLauncher.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath().getParent();
            code = run(root, host, port);
        } catch (Exception e) {
            System.err.println("[오류] " + e.getMessage());
        }
        System.exit(code);
    }

    static int run(Path root, String host, int port) throws Exception {
        Path jar = root.resolve("distributed-kv.jar");
        System.out.println("[1/5] 실행 JAR 확인");
        if (!Files.isRegularFile(jar)) throw new IOException("distributed-kv.jar가 없습니다: " + jar);
        System.out.println("[2/5] P2P 포트 6001~6004 확인");
        for (int p = 6001; p <= 6004; p++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.bind(new InetSocketAddress("127.0.0.1", p));
            } catch (IOException e) { throw new IOException("P2P 포트 " + p + "를 사용할 수 없습니다.", e); }
        }
        System.out.println("[3/5] Master " + host + ":" + port + " 연결 확인 (최대 30초)");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30), notice = 0;
        while (true) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress(host, port), 1000);
                probe.setSoTimeout(1000);
                // Empty HELLO is rejected without reserving a Worker ID; drain before close.
                probe.getOutputStream().write('\n');
                try { probe.getInputStream().read(new byte[1024]); } catch (IOException ignored) {}
                break;
            } catch (IOException e) {
                if (System.nanoTime() >= deadline) throw new IOException("Master 연결 실패: 주소·서버·방화벽을 확인하세요.", e);
                if (System.nanoTime() >= notice) {
                    System.out.println("      연결 대기 중..."); notice = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                }
                Thread.sleep(500); // Launcher connection retry only; never simulation time.
            }
        }
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        boolean detailed;
        while (true) {
            System.out.print("상세 로그를 화면에 표시할까요? [y/N] ");
            String answer = input.readLine();
            if (answer == null) throw new IOException("상세 출력 선택 입력이 종료되었습니다.");
            answer = answer.trim();
            if (answer.isEmpty() || answer.equalsIgnoreCase("n")) { detailed = false; break; }
            if (answer.equalsIgnoreCase("y")) { detailed = true; break; }
            System.out.println("y 또는 n을 입력해주세요.");
        }
        String id = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path destination = root.resolve("logs").resolve(id);
        System.out.println("[4/5] 기존 로그 보관");
        copyLogs(root, root.resolve("logs/before-" + id), true);
        System.out.println("[5/5] Worker 실행: " + (detailed ? "상세 출력" : "진행 상태 + 최종 요약"));
        Result result = new Result(1, true, 0);
        String status;
        try { result = console(root, host, port, detailed, destination); }
        finally {
            stopChild();
            copyLogs(root, destination, false);
            status = summary(destination, result);
        }
        return status.equals("FAILED") ? 1 : 0;
    }

    static void copyLogs(Path root, Path destination, boolean remove) throws IOException {
        for (String name : LOGS) {
            Path source = root.resolve(name);
            if (Files.isRegularFile(source)) {
                Files.createDirectories(destination);
                Files.copy(source, destination.resolve(name), StandardCopyOption.REPLACE_EXISTING);
                if (remove) Files.delete(source);
            }
        }
    }

    static void stopChild() {
        Process p = child;
        if (p != null && p.isAlive()) {
            p.destroy();
            try { if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); p.waitFor(); } }
            catch (InterruptedException e) { p.destroyForcibly(); Thread.currentThread().interrupt(); }
        }
    }

    static Result console(Path root, String host, int port, boolean detailed, Path destination) throws Exception {
        Files.createDirectories(destination);
        String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        String java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        long start = System.nanoTime();
        child = new ProcessBuilder(java, "-Dfile.encoding=UTF-8", "-Dsun.stdout.encoding=UTF-8", "-Dsun.stderr.encoding=UTF-8",
                "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "-jar", root.resolve("distributed-kv.jar").toString(),
                "workers", host, Integer.toString(port)).directory(root.toFile()).start();
        BlockingQueue<Line> lines = new ArrayBlockingQueue<>(2048);
        InputStream[] streams = {child.getInputStream(), child.getErrorStream()};
        for (int i = 0; i < 2; i++) {
            final int index = i;
            Thread reader = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(streams[index], StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) lines.put(new Line(index, line));
                } catch (Exception e) {
                    try { lines.put(new Line(1, "Exception: 출력 수집 실패: " + e)); } catch (InterruptedException ignored) {}
                } finally {
                    try { lines.put(new Line(index, null)); } catch (InterruptedException ignored) {}
                }
            });
            reader.setDaemon(true); reader.start();
        }
        Set<String> connections = new HashSet<>(), successes = new HashSet<>();
        boolean errors = false;
        int ended = 0;
        long next = 0;
        String last = "";
        try (BufferedWriter capture = Files.newBufferedWriter(destination.resolve("console.txt"), StandardCharsets.UTF_8)) {
            while (ended < 2) {
                Line line = lines.poll(100, TimeUnit.MILLISECONDS);
                if (line != null) {
                    if (line.text == null) { ended++; continue; }
                    capture.write(line.text); capture.newLine();
                    connections.addAll(matches(line.text, "WORKER(\\d+) \\| CONNECT \\| SUCCESS"));
                    successes.addAll(matches(line.text, "\\| PROC \\| SUCCESS \\| KV\\[(\\d+)\\]"));
                    boolean error = ERROR.matcher(line.text).find(); errors |= error;
                    if (detailed || line.stream == 1 || error) System.out.println(line.text);
                }
                if (!detailed && System.nanoTime() >= next) {
                    String progress = "Worker 연결: " + connections.size() + "/4 | 성공 처리 확인: " + successes.size() + "/5000";
                    if (!progress.equals(last)) { System.out.println(progress); last = progress; }
                    next = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
                }
            }
        }
        return new Result(child.waitFor(), errors, (System.nanoTime() - start) / 1e9);
    }

    static List<String> matches(String text, String regex) {
        List<String> result = new ArrayList<>();
        Matcher m = Pattern.compile(regex).matcher(text);
        while (m.find()) result.add(m.group(1));
        return result;
    }
    static String metric(String text, String label) {
        List<String> values = matches(text, "\\| STAT \\| (?:INFO|SUCCESS|FAIL|WARN) \\| " + Pattern.quote(label) + ": (\\d+(?:\\.\\d+)?)");
        return values.isEmpty() ? "-" : values.get(0);
    }
    static String read(Path directory, String name) throws IOException {
        Path path = directory.resolve(name);
        return Files.exists(path) ? Files.readString(path, StandardCharsets.UTF_8) : "";
    }
    static long number(String text) { return text.equals("-") ? 0 : (long) Double.parseDouble(text); }
    static boolean statsPresent(String text) { return Arrays.stream(METRICS).allMatch(label -> !metric(text, label).equals("-")); }

    static String summary(Path directory, Result result) throws IOException {
        Set<String> expected = new HashSet<>(), ids = new HashSet<>();
        for (int i = 1; i <= 5000; i++) expected.add(String.format(Locale.ROOT, "%04d", i));
        long success = 0, fail = 0, events = 0;
        boolean workerOk = true, errors = result.errors;
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            String text = read(directory, "Worker" + i + ".txt");
            List<String> found = matches(text, "\\| PROC \\| SUCCESS \\| KV\\[(\\d+)\\]");
            ids.addAll(found); events += found.size();
            workerOk &= statsPresent(text) && text.contains("| TERMINATE | SUCCESS");
            errors |= ERROR.matcher(text).find();
            success += number(metric(text, METRICS[1])); fail += number(metric(text, METRICS[2]));
            rows.add(String.format("Worker%d %8s %6s %12s %8s %8s", i, metric(text, METRICS[1]), metric(text, METRICS[2]),
                    metric(text, METRICS[3]), metric(text, METRICS[4]), metric(text, METRICS[5])));
        }
        workerOk &= success == 5000 && events == 5000 && ids.equals(expected);
        String master = read(directory, "Master.txt");
        List<String> masterIds = matches(master, "\\| RESULT \\| SUCCESS \\| KV\\[(\\d+)\\]");
        Matcher kv = Pattern.compile("\\| KV \\| SUCCESS \\| Key=([0-9a-fA-F]{4}), Value=(\\d+)").matcher(master);
        Set<String> keys = new HashSet<>(); int entries = 0; boolean valid = true;
        while (kv.find()) { entries++; keys.add(kv.group(1).toLowerCase(Locale.ROOT)); int value = Integer.parseInt(kv.group(2)); valid &= value >= 1 && value <= 100; }
        boolean masterOk = masterIds.size() == 5000 && new HashSet<>(masterIds).equals(expected)
                && entries == 5000 && keys.size() == 5000 && valid && statsPresent(master)
                && metric(master, METRICS[1]).equals(Long.toString(success)) && metric(master, METRICS[2]).equals(Long.toString(fail))
                && master.contains("| TERMINATE | SUCCESS");
        errors |= ERROR.matcher(master).find();
        String status = result.code != 0 || errors || !workerOk ? "FAILED" : masterOk ? "COMPLETED" : "PARTIAL";
        List<String> lines = new ArrayList<>(List.of("", "========== 통합 실행 결과 ==========", status + " | " + switch (status) {
            case "COMPLETED" -> "고유 작업 5,000개·KV 출력·통계·종료 기록 확인";
            case "PARTIAL" -> "Worker 완료, Master 로그 누락 또는 검증 불일치";
            default -> "실패 또는 미완료: 오류와 Worker 완료 조건 확인 필요";
        }, "Worker     성공   실패   평균대기(초) P2P이벤트 재할당"));
        lines.addAll(rows);
        lines.addAll(List.of("고유 성공 확인: " + ids.size() + "/5000", "Master P2P 이벤트: " + metric(master, METRICS[4]) + " 회",
                "Master 장애 재할당: " + metric(master, METRICS[5]) + " 회", String.format(Locale.ROOT, "실제 실행 시간: %.2f초", result.seconds),
                "가상 수행 시간: " + metric(master, METRICS[6]) + "초", "※ Worker P2P는 송신+수신 참여 횟수이며 Master 이벤트 수와 구분합니다.", "로그 위치: " + directory));
        if (metric(master, METRICS[4]).equals("0")) lines.add("P2P 미발생: 시연용 동작 확인은 별도로 필요합니다.");
        Files.createDirectories(directory);
        Files.write(directory.resolve("summary.txt"), lines, StandardCharsets.UTF_8);
        lines.forEach(System.out::println);
        return status;
    }
}
