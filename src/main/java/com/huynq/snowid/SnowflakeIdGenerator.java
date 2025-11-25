package com.huynq.snowid;

import java.util.concurrent.atomic.AtomicLong;

public class SnowflakeIdGenerator {
    private static final long WORKER_ID_BITS = 10;
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    private static final long SEQUENCE_BITS = 12;
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    private final long workerId;
    private final long epoch;

    private final AtomicLong lastTimestamp = new AtomicLong(-1L);
    private final AtomicLong sequence = new AtomicLong(0L);

    public SnowflakeIdGenerator(long workerId, long epoch) {
        this.epoch = epoch;
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException(
                    String.format("Worker ID must be between 0 and %d", MAX_WORKER_ID));
        }
        this.workerId = workerId;
    }

    public long nextId() {
        while (true) {
            long currentTimestamp = System.currentTimeMillis();

            long lastTime = lastTimestamp.get();

            if (currentTimestamp < lastTime) {
                currentTimestamp = waitUntil(lastTime);
            }

            if (currentTimestamp == lastTime) {
                long seq = (sequence.incrementAndGet()) & SEQUENCE_MASK;
                if (seq == 0) {
                    currentTimestamp = waitUntilNextMillis(lastTime);
                    sequence.set(0L);
                }
            } else {
                sequence.set(0L);
            }

            if (lastTimestamp.compareAndSet(lastTime, currentTimestamp)) {
                return (timestampPart(currentTimestamp) << TIMESTAMP_SHIFT)
                        | (workerId << WORKER_ID_SHIFT)
                        | sequence.get();
            }
        }
    }

    private long waitUntil(long targetTime) {
        long now = System.currentTimeMillis();
        while (now < targetTime) {
            now = System.currentTimeMillis();
        }
        return now;
    }

    private long waitUntilNextMillis(long lastTime) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTime) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    private long timestampPart(long timestamp) {
        return timestamp - this.epoch;
    }
}
