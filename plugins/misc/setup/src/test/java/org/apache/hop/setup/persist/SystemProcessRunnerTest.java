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

package org.apache.hop.setup.persist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemProcessRunnerTest {

  /**
   * A child process that reads its standard input waits for EOF before exiting. If the runner keeps
   * the write end of the stdin pipe open, the child never terminates and reading its output blocks
   * forever. This is what makes powershell.exe hang when writing Windows user environment
   * variables. {@code cat} reproduces the same behaviour on POSIX systems.
   */
  @Test
  void runDoesNotHangOnChildReadingStandardInput() {
    assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("win"));

    int exit =
        assertTimeoutPreemptively(
            Duration.ofSeconds(10), () -> new SystemProcessRunner().run(List.of("cat")));

    assertEquals(0, exit);
  }
}
