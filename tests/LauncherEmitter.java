public class LauncherEmitter {
    public static void main(String[] args) {
        for (int i = 1; i <= 5000; i++) {
            System.out.printf("[1] WORKER1 | PROC | SUCCESS | KV[%04d] 저장 완료%n", i);
            if (i % 500 == 0) System.err.println("diagnostic " + i);
        }
        if (args[1].equals("error")) {
            System.err.println("java.lang.RuntimeException: test failure"); System.exit(7);
        }
    }
}
