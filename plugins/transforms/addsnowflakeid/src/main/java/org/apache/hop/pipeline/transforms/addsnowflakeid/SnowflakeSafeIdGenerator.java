/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.addsnowflakeid;

import java.util.concurrent.ThreadLocalRandom;

/**
 * snowflake id utils
 *
 * @author lance
 * @since 2025/10/16 20:57
 */
public class SnowflakeSafeIdGenerator {
  /** 2020-01-01 */
  private static final long START_EPOCH = 1577836800000L;

  private static final long DATA_CENTER_ID_BITS = 5L;
  private static final long MACHINE_ID_BITS = 5L;
  private static final long SEQUENCE_BITS = 12L;

  private static final long MAX_DATA_CENTER_ID = ~(-1L << DATA_CENTER_ID_BITS);
  private static final long MAX_MACHINE_ID = ~(-1L << MACHINE_ID_BITS);
  private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);

  private static final long MACHINE_ID_SHIFT = SEQUENCE_BITS;
  private static final long DATA_CENTER_ID_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS;
  private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS + DATA_CENTER_ID_BITS;

  private final long dataCenterId;
  private final long machineId;

  /** Maximum tolerable rollback time in milliseconds */
  private final long maxBackwardsMs;

  private long sequence = 0L;
  private long lastTimestamp = -1L;

  /** logical timestamp */
  private long logicalTimestamp = -1L;

  public SnowflakeSafeIdGenerator(long dataCenterId, long machineId) {
    this(dataCenterId, machineId, 10);
  }

  public SnowflakeSafeIdGenerator(long dataCenterId, long machineId, long maxBackwardsMs) {
    if (dataCenterId > MAX_DATA_CENTER_ID || dataCenterId < 0) {
      throw new IllegalArgumentException("dataCenterId out of range");
    }
    if (machineId > MAX_MACHINE_ID || machineId < 0) {
      throw new IllegalArgumentException("machineId out of range");
    }

    this.dataCenterId = dataCenterId;
    this.machineId = machineId;
    this.maxBackwardsMs = maxBackwardsMs;
  }

  /**
   * default SnowflakeSafeIdGenerator
   *
   * @return SnowflakeSafeIdGenerator
   */
  public static SnowflakeSafeIdGenerator createDefault() {
    long dataCenterId = ThreadLocalRandom.current().nextInt(0, 32);
    long machineId = ThreadLocalRandom.current().nextInt(0, 32);
    return new SnowflakeSafeIdGenerator(dataCenterId, machineId, 10);
  }

  public synchronized long nextId() {
    long now = currentTimeMillis();

    // Timestamp rollback detection
    if (now < lastTimestamp) {
      long offset = lastTimestamp - now;
      if (offset <= maxBackwardsMs) {
        // For minor clock rollbacks, continue running using logical time.
        now = lastTimestamp;
      } else {
        throw new IllegalArgumentException(
            "Clock moved backwards beyond tolerance. Refusing for " + offset + "ms");
      }
    }

    if (now == lastTimestamp) {
      sequence = (sequence + 1) & MAX_SEQUENCE;
      if (sequence == 0) {
        now = waitUntilNextMillis(lastTimestamp);
      }
    } else {
      sequence = 0;
    }

    lastTimestamp = now;
    logicalTimestamp = Math.max(now, logicalTimestamp + 1);

    return ((logicalTimestamp - START_EPOCH) << TIMESTAMP_SHIFT)
        | (dataCenterId << DATA_CENTER_ID_SHIFT)
        | (machineId << MACHINE_ID_SHIFT)
        | sequence;
  }

  private long waitUntilNextMillis(long lastTime) {
    long ts = currentTimeMillis();
    while (ts <= lastTime) {
      ts = currentTimeMillis();
    }
    return ts;
  }

  private long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
