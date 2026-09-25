package raftkv.checker;

import raftkv.kv.Op;

/** One client operation: when it was invoked, when it returned (or never), and what it saw. */
public final class Operation {
    public static final long PENDING = Long.MAX_VALUE;

    public final int id;
    public final long clientId;
    public final Op op;
    public final String key;
    public final String input;
    public final long invokeTime;
    private String output;
    private long completeTime = PENDING;

    public Operation(int id, long clientId, Op op, String key, String input, long invokeTime) {
        this.id = id;
        this.clientId = clientId;
        this.op = op;
        this.key = key;
        this.input = input;
        this.invokeTime = invokeTime;
    }

    public void complete(String output, long time) {
        this.output = output;
        this.completeTime = time;
    }

    public boolean isPending() {
        return completeTime == PENDING;
    }

    public String output() {
        return output;
    }

    public long completeTime() {
        return completeTime;
    }

    @Override
    public String toString() {
        String call = switch (op) {
            case PUT -> "put(" + key + ", " + input + ")";
            case GET -> "get(" + key + ")";
            case DELETE -> "delete(" + key + ")";
            case STATUS -> "status";
        };
        String result = isPending() ? "pending" : (op == Op.GET ? "-> " + output : "ok");
        String end = isPending() ? "inf" : String.valueOf(completeTime);
        return "c" + clientId + " " + call + " " + result + " [" + invokeTime + ", " + end + "]";
    }
}
