package raftkv.transport;

/** Sends messages; incoming ones are handed to the node's handler. Simulated or TCP. */
public interface Transport {
    void send(Message message);
}
