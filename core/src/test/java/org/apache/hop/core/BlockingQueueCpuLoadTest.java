/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BlockingQueueCpuLoadTest {

  public static void main(String[] args) throws Exception {
    for (int[][] list : threadCountAndWaitTime()) {
      for (int[] params : list) {
        System.out.printf(
            "Thread count: %d, and waiting for %d milliseconds\n", params[0], params[1]);
        blockingRowSetCpuPerformance(params[0], params[1]);
      }
    }
  }

  /** wait time: 1, 10, 100, 500, 1000 milliseconds */
  private static int[][] createThreadParamGroup(int cpuCors, int i) {
    int threadCount = cpuCors * i;
    return Stream.of(1, 10, 100, 500, 1000)
        .map(integer -> new int[] {threadCount, integer})
        .toArray(int[][]::new);
  }

  private static List<int[][]> threadCountAndWaitTime() {
    int cpuCores = Runtime.getRuntime().availableProcessors();
    return Stream.of(1, 4, 8, 16, 32, 64, 128, 256)
        .map(integer -> createThreadParamGroup(cpuCores, integer))
        .collect(Collectors.toList());
  }

  private static void blockingRowSetCpuPerformance(int threadCount, int waitTime)
      throws InterruptedException {
    long waitTerminalTime = 5;
    CountDownLatch counter = new CountDownLatch(threadCount);
    AtomicBoolean terminal = new AtomicBoolean();

    ScheduledExecutorService executor = Executors.newScheduledThreadPool(threadCount + 1);
    List<Runnable> blockingThreads = new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      blockingThreads.add(blockingThread(counter, terminal, waitTime));
    }
    blockingThreads.forEach(executor::submit);
    executor.schedule(() -> terminal.set(true), waitTerminalTime, TimeUnit.SECONDS);
    counter.await();
    executor.shutdownNow();
  }

  private static Runnable blockingThread(
      CountDownLatch counter, AtomicBoolean terminal, int waitTime) {
    return () -> {
      BlockingRowSet rowSet = new BlockingRowSet(10);
      while (!terminal.get()) {
        Object[] row = rowSet.getRowWait(waitTime, TimeUnit.MILLISECONDS);
        if (row != null) {
          break;
        }
      }
      counter.countDown();
    };
  }
}
