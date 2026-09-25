package raftkv.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TreeMap;

/** Parses and prints cluster membership like "1=127.0.0.1:7001,2=127.0.0.1:7002". */
public final class ClusterSpec {
    public static final int DEFAULT_BASE_PORT = 7001;

    private ClusterSpec() {}

    public static Map<Integer, InetSocketAddress> parse(String spec) {
        Map<Integer, InetSocketAddress> m = new TreeMap<>();
        for (String part : spec.split(",")) {
            String[] kv = part.trim().split("=");
            String[] hp = kv[1].split(":");
            m.put(Integer.parseInt(kv[0]), new InetSocketAddress(hp[0], Integer.parseInt(hp[1])));
        }
        return m;
    }

    public static Map<Integer, InetSocketAddress> local(int nodes, int basePort) {
        Map<Integer, InetSocketAddress> m = new TreeMap<>();
        for (int i = 1; i <= nodes; i++) {
            m.put(i, new InetSocketAddress("127.0.0.1", basePort + i - 1));
        }
        return m;
    }

    public static String format(Map<Integer, InetSocketAddress> cluster) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, InetSocketAddress> e : new TreeMap<>(cluster).entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(e.getKey()).append('=').append(e.getValue().getHostString()).append(':').append(e.getValue().getPort());
        }
        return sb.toString();
    }
}
