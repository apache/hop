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

package org.apache.hop.core.util;

import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorUtil {
  public static final String SIMPLE_NAME = ExecutorUtil.class.getSimpleName();
  private static final AtomicInteger threadNum = new AtomicInteger(1);
  private static final ExecutorService executor = init();

  private ExecutorUtil() {}

  private static ExecutorService init() {
    return Executors.newCachedThreadPool(
        r -> {
          Thread thread = Executors.defaultThreadFactory().newThread(r);
          thread.setDaemon(true);
          thread.setName(SIMPLE_NAME + " thread " + threadNum.getAndIncrement());
          return thread;
        });
  }

  public static ExecutorService getExecutor() {
    return executor;
  }

  public static void cleanup(Timer timer) {
    cleanup(timer, 0);
  }

  public static void cleanup(Timer timer, long delayMs) {
    Runnable runnable = () -> cleanTimer(timer);
    if (delayMs <= 0) {
      runnable.run();
      return;
    }
    executor.submit(
        () -> {
          try {
            TimeUnit.MILLISECONDS.sleep(delayMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          runnable.run();
        });
  }

  private static void cleanTimer(Timer timer) {
    if (timer != null) {
      timer.cancel();
      timer.purge();
    }
  }
}
