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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.junit.jupiter.api.Test;

class PipelineForceStopTest {

  @Test
  void forceStopInterruptsATransformThreadAfterStopAll() throws Exception {
    Pipeline pipeline = new LocalPipelineEngine();
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean(false);
    Thread worker =
        new Thread(
            () -> {
              started.countDown();
              try {
                Thread.sleep(60_000);
              } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
              }
            },
            "force-stop-test");
    worker.setDaemon(true);
    worker.start();
    assertTrue(started.await(5, TimeUnit.SECONDS));
    pipeline.addTransformThread(worker);

    pipeline.forceStop();

    worker.join(5_000);
    assertFalse(worker.isAlive());
    assertTrue(interrupted.get());
    assertTrue(pipeline.isStopped());
  }
}
