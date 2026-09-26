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

package org.apache.hop.ui.hopgui.file.shared;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExecutionGuiSessionTest {

  @Test
  void stopIfCurrentRunsForTheEngineOnScreen() {
    ExecutionGuiSession session = new ExecutionGuiSession();
    Object engine = new Object();
    ExecutionGuiSession.Snapshot snapshot = session.adopt(engine);
    AtomicBoolean stopped = new AtomicBoolean(false);

    assertTrue(session.stopIfCurrent(engine, () -> stopped.set(true)));
    assertTrue(stopped.get());
    assertTrue(session.isCurrent(snapshot.engine(), snapshot.generation()));
    assertTrue(session.isCurrentEngine(engine));
  }

  @Test
  void stopIfCurrentIgnoresTheEngineThatWasReplaced() {
    ExecutionGuiSession session = new ExecutionGuiSession();
    Object first = new Object();
    Object second = new Object();
    session.adopt(first);
    int secondGeneration = session.adopt(second).generation();
    AtomicBoolean stopped = new AtomicBoolean(false);

    assertFalse(session.stopIfCurrent(first, () -> stopped.set(true)));
    assertFalse(stopped.get());
    assertTrue(session.isCurrent(second, secondGeneration));
    assertFalse(session.runIfCurrent(first, secondGeneration, () -> stopped.set(true)));
    assertFalse(stopped.get());
  }

  @Test
  void adoptFromAnotherThreadWaitsUntilTheStopperReleasesTheLock() throws Exception {
    ExecutionGuiSession session = new ExecutionGuiSession();
    Object first = new Object();
    Object second = new Object();
    Object third = new Object();
    session.adopt(first);

    CountDownLatch holding = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger secondGeneration = new AtomicInteger();
    AtomicBoolean failed = new AtomicBoolean(false);
    Thread stopper =
        new Thread(
            () -> {
              try {
                session.stopIfCurrent(
                    first,
                    () -> {
                      secondGeneration.set(session.adopt(second).generation());
                      holding.countDown();
                      try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                          throw new AssertionError("stopper was not released");
                        }
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                      }
                    });
              } catch (AssertionError e) {
                failed.set(true);
              }
            });
    stopper.start();
    assertTrue(holding.await(5, TimeUnit.SECONDS));

    CountDownLatch reachedAdopt = new CountDownLatch(1);
    CountDownLatch adopted = new CountDownLatch(1);
    AtomicInteger thirdGeneration = new AtomicInteger();
    Thread adopter =
        new Thread(
            () -> {
              reachedAdopt.countDown();
              thirdGeneration.set(session.adopt(third).generation());
              adopted.countDown();
            });
    adopter.start();
    assertTrue(reachedAdopt.await(5, TimeUnit.SECONDS));
    // Do not call into the session here: the stopper still holds its lock and is waiting for
    // release. The adopted latch stays open until that lock is released.
    assertFalse(adopted.await(300, TimeUnit.MILLISECONDS));

    release.countDown();
    assertTrue(adopted.await(5, TimeUnit.SECONDS));
    assertTrue(thirdGeneration.get() > secondGeneration.get());
    assertTrue(session.isCurrent(third, thirdGeneration.get()));
    assertFalse(session.isCurrent(second, secondGeneration.get()));
    stopper.join(5_000);
    assertFalse(stopper.isAlive());
    assertFalse(failed.get());
  }
}
