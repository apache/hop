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
 *
 */

package org.apache.hop.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class HopRunShutdownTimeoutOptionTest {

  @Test
  void defaultIs25Seconds() {
    assumeTrue(
        System.getenv("HOP_RUN_SHUTDOWN_TIMEOUT") == null,
        "HOP_RUN_SHUTDOWN_TIMEOUT is set in this environment");

    assertEquals(25L, parse().getShutdownTimeout());
  }

  @Test
  void longOption() {
    assertEquals(60L, parse("--shutdown-timeout=60").getShutdownTimeout());
  }

  @Test
  void shortOption() {
    assertEquals(5L, parse("-swt", "5").getShutdownTimeout());
  }

  @Test
  void helpDocumentsTheOption() {
    StringWriter writer = new StringWriter();
    new CommandLine(new HopRun()).usage(new PrintWriter(writer, true));
    String usage = writer.toString().replaceAll("\\s+", " ");

    assertTrue(usage.contains("-swt, --shutdown-timeout"), usage);
    assertTrue(usage.contains("HOP_RUN_SHUTDOWN_TIMEOUT"), usage);
  }

  private static HopRun parse(String... args) {
    HopRun hopRun = new HopRun();
    new CommandLine(hopRun).parseArgs(args);
    return hopRun;
  }
}
