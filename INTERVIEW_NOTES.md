# Questions you should be able to answer about this project

Read the code for each answer; don't memorize these. If you can't explain it, change it until you can.

**Partitioning**
1. Why consistent hashing instead of `hash(key) % N`? *(Adding/removing a node remaps almost every key with modulo; with a ring only ~1/N of keys move. See `test_adding_a_node_moves_only_a_fraction…`.)*
2. Why virtual nodes? *(Even load; a failed node's load spreads over many nodes instead of one neighbour.)*
3. How is the preference list built, and why must the nodes be distinct? *(Walk clockwise, skip repeats; otherwise two "replicas" could live on one machine.)*

**Replication and consistency**
4. With N=3, what do ONE / QUORUM / ALL mean for latency and availability? Why does R + W > N give you the latest write?
5. A QUORUM write returns 503. What state is the cluster in? Should the client retry? *(Partially applied, not rolled back; idempotent via LWW so retry is safe.)*
6. How do you resolve two concurrent writes to the same key? What is lost? Why not vector clocks, and what would you change to use them?
7. Why a hybrid logical clock rather than `time.time()`? *(Clock skew between nodes; HLC never goes backwards and `observe()` pulls a lagging node forward.)*

**Failure handling**
8. A replica is down during a write. Walk through exactly what happens (RPC fails → future callback → hint stored → heartbeat sees node alive → replay → LWW makes it idempotent).
9. Hinted handoff vs read repair: what does each fix and what does neither fix? *(Neither fixes a key that is never read after its hint expired → anti-entropy/Merkle trees.)*
10. Why can't a hint count toward W? What would sloppy quorum change?
11. How do deletes work? What goes wrong if you just remove the key? *(Stale replica resurrects it; hence tombstones and the GC grace period.)*

**Storage**
12. What does the WAL guarantee, and what does `fsync` change? What happens if the process dies in the middle of a write? *(CRC mismatch / missing newline → ignore and truncate the tail.)*
13. Why is `kill -9` survivable without fsync but power loss isn't?
14. What does compaction do, why is it needed, and what does it cost? *(Blocks writes on that node while it runs.)*

**Engineering and scale**
15. Where are the bottlenecks? *(Thread-per-connection HTTP server, JSON, one global store lock, Python GIL.) How would you fix each?*
16. What breaks if you add a sixth node today? What would you build to support it? *(Gossip membership, token handoff / streaming of key ranges.)*
17. How would you test a network partition (not just a crash)? *(Proxy that drops traffic between node subsets.)*

**Be ready to say honestly**
* Where the design deliberately differs from the Dynamo paper (LWW instead of vector clocks, strict instead of sloppy quorum, static membership, no anti-entropy).
* What you would build next and why.
