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

package org.apache.hop.run;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * {@code hop run} must surface pipeline/workflow failure as a non-zero process exit code (#9355). A
 * {@link Runnable} command always exits 0 under picocli; {@link java.util.concurrent.Callable}
 * returns the code.
 */
class HopCommandRunExitCodeTest {

  @Test
  void callReturnsZeroWhenRunSucceeded() {
    assertEquals(0, commandThatFinishes(true).call());
  }

  @Test
  void callReturnsOneWhenRunFailed() {
    assertEquals(1, commandThatFinishes(false).call());
  }

  @Test
  void picocliExecuteUsesCallReturnValue() {
    assertEquals(1, new CommandLine(commandThatFinishes(false)).execute());
    assertEquals(0, new CommandLine(commandThatFinishes(true)).execute());
  }

  private static HopCommandRun commandThatFinishes(boolean withoutError) {
    return new HopCommandRun() {
      @Override
      public void run() {
        setFinishedWithoutError(withoutError);
      }
    };
  }
}
