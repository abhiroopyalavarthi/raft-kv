# Benchmark results

Measured 2026-09-25 with `./gradlew bench`: 5 node processes on one machine over localhost TCP, every log append fsynced before replying.

- Machine: Mac OS X aarch64, 8 CPUs, Java 21.0.12.1
- Load: 32 closed-loop clients (each waits for its reply before sending the next), 10 s of writes (100-byte values, 10,000 keys) after a 3 s warm-up, then 5 s of reads
- Reads are linearizable (ReadIndex: the leader confirms it still has a majority before answering)
- Failover: leader killed with SIGKILL while 8 clients write; time until the first write is acknowledged by the new leader (5 trials)

| Configuration | Write throughput | Write p50 | Write p99 | Read throughput | Read p50 | Read p99 | Failover (median) | Failover (max) |
|---|---|---|---|---|---|---|---|---|
| No batching (1 entry per fsync and per AppendEntries) | 122 ops/s | 259.58 ms | 533.50 ms | 64,353 ops/s | 0.44 ms | 1.37 ms | 339 ms | 524 ms |
| Batching + pipelining | 980 ops/s | 31.52 ms | 58.08 ms | 56,605 ops/s | 0.48 ms | 1.52 ms | 455 ms | 688 ms |

Batching raises write throughput 8.0x. Failover time is dominated by the randomized election timeout (150-300 ms): followers wait that long without a heartbeat before starting an election.
