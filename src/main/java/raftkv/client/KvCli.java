package raftkv.client;

import raftkv.kv.Op;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Command-line client.
 * <pre>
 *   raft-kv client put color blue
 *   raft-kv client get color
 *   raft-kv client delete color
 *   raft-kv client status          role/term/commit index of every node
 *   raft-kv client                 interactive shell
 * </pre>
 * Use --cluster to point at nodes other than the default 127.0.0.1:7001-7005.
 */
public final class KvCli {

    public static void main(String[] args) throws IOException {
        String spec = null;
        int start = 0;
        if (args.length >= 2 && args[0].equals("--cluster")) {
            spec = args[1];
            start = 2;
        }
        Map<Integer, InetSocketAddress> cluster = spec != null ? ClusterSpec.parse(spec)
                : ClusterSpec.local(5, ClusterSpec.DEFAULT_BASE_PORT);
        String[] rest = java.util.Arrays.copyOfRange(args, start, args.length);
        try (KvClient client = new KvClient(cluster).withTimeouts(1000, 10_000)) {
            if (rest.length == 0) {
                shell(client);
            } else {
                int code = run(client, rest);
                System.exit(code);
            }
        }
    }

    private static void shell(KvClient client) throws IOException {
        System.out.println("commands: put <key> <value> | get <key> | delete <key> | status | quit");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("> ");
            System.out.flush();
            String line = in.readLine();
            if (line == null || line.trim().equals("quit") || line.trim().equals("exit")) {
                return;
            }
            if (!line.isBlank()) {
                run(client, line.trim().split("\\s+", 3));
            }
        }
    }

    private static int run(KvClient client, String[] cmd) {
        try {
            switch (cmd[0]) {
                case "put" -> {
                    long t0 = System.nanoTime();
                    KvClient.Reply r = client.call(Op.PUT, cmd[1], cmd[2]);
                    System.out.printf("OK (leader n%d, %.1f ms)%n", r.servedBy(), (System.nanoTime() - t0) / 1e6);
                }
                case "get" -> {
                    KvClient.Reply r = client.call(Op.GET, cmd[1], null);
                    System.out.println(r.value() == null ? "(not found)" : r.value());
                }
                case "delete" -> {
                    client.call(Op.DELETE, cmd[1], null);
                    System.out.println("OK");
                }
                case "status" -> {
                    for (Map.Entry<Integer, String> e : client.statusAll().entrySet()) {
                        System.out.println("n" + e.getKey() + ": " + (e.getValue() == null ? "DOWN" : e.getValue()));
                    }
                }
                default -> {
                    System.out.println("unknown command: " + cmd[0]);
                    return 2;
                }
            }
            return 0;
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("missing argument");
            return 2;
        } catch (IOException e) {
            System.out.println("error: " + e.getMessage());
            return 1;
        }
    }
}
