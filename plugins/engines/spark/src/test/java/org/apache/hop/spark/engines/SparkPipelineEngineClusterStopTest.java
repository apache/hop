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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.spark.submit.SparkClusterRun;
import org.junit.jupiter.api.Test;

/**
 * Stop of a cluster-mode run: in Hop GUI it is called on the display thread, which must not wait
 * for a cluster that does not answer. hop-run still waits for the driver to be killed.
 */
class SparkPipelineEngineClusterStopTest {

  @Test
  void stopKillsTheDriverOnAThreadOfItsOwn() throws Exception {
    CountDownLatch killing = new CountDownLatch(1);
    CountDownLatch answer = new CountDownLatch(1);
    AtomicReference<String> killedOn = new AtomicReference<>();
    AtomicInteger kills = new AtomicInteger();
    SparkPipelineEngine engine = new SparkPipelineEngine();
    engine.setClusterRun(
        new SparkClusterRun(
            LogChannel.GENERAL,
            new Variables(),
            null,
            null,
            "test",
            new SparkPipelineRunConfiguration()) {
          @Override
          public void kill() {
            kills.incrementAndGet();
            killedOn.set(Thread.currentThread().getName());
            killing.countDown();
            // A cluster that does not answer
            try {
              answer.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        });

    engine.stopAll();
    assertTrue(engine.isStopped());
    assertTrue(killing.await(10, TimeUnit.SECONDS), "the driver is killed");
    assertEquals("hop-spark-cluster-stop", killedOn.get());

    // hop-run waits for the run, and the JVM exits after that
    Thread hopRun = new Thread(engine::waitUntilFinished);
    hopRun.start();
    hopRun.join(300);
    assertTrue(hopRun.isAlive(), "the run is not over before the driver is killed");

    // A second Stop, as hop-run sends when the first did not seem to work
    engine.stopAll();
    answer.countDown();
    hopRun.join(10_000);
    assertFalse(hopRun.isAlive());
    assertEquals(1, kills.get(), "one kill per run");
  }
}
