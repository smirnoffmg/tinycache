# tinycache Benchmarks

Measured on Apple M2 Pro, macOS, Go 1.25, Docker Desktop. Results vary by hardware and system load. Reproduce with `make bench` (micro) or `make bench-compare` (vs Memcached).

---

## Internal Micro-Benchmarks

Core operations measured in isolation (no network). `make bench`.

### Cache Store (256 shards)

| Benchmark                   | ns/op | B/op | allocs/op |
| --------------------------- | ----: | ---: | --------: |
| Store Set (64B value)       |   361 |  165 |         3 |
| Store Set (1KB value)       |   370 |  158 |         3 |
| Store Get hit (64B)         |   150 |   63 |         2 |
| Store Get hit (1KB)         |   150 |   63 |         2 |
| Store Get miss              |    70 |   24 |         2 |
| Store Delete                |   275 |   23 |         1 |
| Store CAS (64B)             |   271 |  127 |         3 |
| Parallel Set (64B, 12 cores)|   175 |  157 |         3 |
| Parallel Get (64B, 12 cores)|    80 |   71 |         2 |
| Parallel Mixed 80/20 (64B)  |   100 |   74 |         2 |

### Protocol (parse + marshal)

| Benchmark           | ns/op | B/op | allocs/op |
| ------------------- | ----: | ---: | --------: |
| Parse `get`         |   525 | 4288 |         5 |
| Parse `set` (64B)   |   607 | 4440 |         7 |
| Parse `set` (1KB)   |   729 | 5512 |         7 |
| Marshal `get`       |   101 |   80 |         4 |
| Marshal `set` (64B) |   233 |  304 |         7 |
| Marshal `set` (1KB) |   427 | 2401 |         7 |

### Consistent Hash Ring (FNV-1a, 150 vnodes/node)

| Benchmark              | ns/op | B/op | allocs/op |
| ---------------------- | ----: | ---: | --------: |
| GetNodes (3 nodes)     |   805 |  282 |         6 |
| IsLocal (3 nodes)      |   176 |   70 |         3 |
| GetNodes (10 nodes)    |   398 |  248 |         5 |

### AOF Persistence (append, fsync=no)

| Benchmark          | ns/op | B/op | allocs/op |
| ------------------ | ----: | ---: | --------: |
| Append (64B value)  |  1556 |  112 |         1 |
| Append (1KB value)  |  2445 | 1152 |         1 |

---

## tinycache vs Memcached (over TCP)

Single-client sequential throughput through Docker. Single-node tinycache (no replication, persistence disabled) vs `memcached:1.6-alpine`. Reproduce with `make bench-compare`.

| Operation       | tinycache | memcached | ratio        |
| --------------- | --------: | --------: | ------------ |
| SET 64B         |    143 µs |    185 µs | 1.30× faster |
| SET 1KB         |    155 µs |    196 µs | 1.26× faster |
| GET 64B (hit)   |    356 µs |    506 µs | 1.42× faster |

tinycache outperforms memcached on single-connection SET and GET for small values. Caveats: measured on Docker Desktop macOS. Memcached typically scales better at very high connection counts; run `make bench-compare` on your target hardware.
