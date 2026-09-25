# Benchmark results

Measured 2026-09-25 with `./gradlew bench`: 5 node processes on one machine over localhost TCP, every log append fsynced before replying.

- Machine: Linux amd64, 2 CPUs, Java 21.0.10
- Load: 32 closed-loop clients (each waits for its reply before sending the next), 10 s of writes (100-byte values, 10,000 keys) after a 3 s warm-up, then 5 s of reads
- Reads are linearizable (ReadIndex: the leader confirms it still has a majority before answering)
- Failover: leader killed with SIGKILL while 8 clients write; time until the first write is acknowledged by the new leader (5 trials)

| Configuration | Write throughput | Write p50 | Write p99 | Read throughput | Read p50 | Read p99 | Failover (median) | Failover (max) |
|---|---|---|---|---|---|---|---|---|
| No batching (1 entry per fsync and per AppendEntries) | 1,022 ops/s | 28.22 ms | 79.25 ms | 18,762 ops/s | 1.44 ms | 7.12 ms | 337 ms | 435 ms |
| Batching + pipelining | 4,376 ops/s | 5.73 ms | 26.02 ms | 19,835 ops/s | 1.47 ms | 4.56 ms | 209 ms | 345 ms |

Batching raises write throughput 4.3x. Failover time is dominated by the randomized election timeout (150-300 ms): followers wait that long without a heartbeat before starting an election.
