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

package org.apache.hop.ui.hopgui;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The desktop half of the facade: still a background thread, with nothing extra around it. */
class BackgroundThreadFacadeTest {

  @Test
  @DisplayName("the work runs on a thread of its own")
  void startsTheWorkInTheBackground() throws Exception {
    CompletableFuture<Thread> ranOn = new CompletableFuture<>();

    BackgroundThreadFacade.start(() -> ranOn.complete(Thread.currentThread()), "field-lookup");

    Thread worker = ranOn.get(10, SECONDS);
    assertNotSame(Thread.currentThread(), worker, "the caller must not be blocked by the lookup");
    assertEquals("field-lookup", worker.getName());
  }
}
