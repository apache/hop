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
package org.apache.hop.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.logging.FileLoggingEventListener;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class HopRunLogFileOptionTest {

  private static final String EARLIER_RUN = "earlier-hop-run";
  private static final String SECOND_RUN = "second-hop-run";

  @TempDir Path tempDir;

  @Test
  void logfileReplacesExistingContentByDefault() throws Exception {
    Path logFile = tempDir.resolve("replace.log");
    Files.writeString(logFile, EARLIER_RUN + System.lineSeparator());

    HopRun hopRun = parse("-lf", logFile.toString());

    assertFalse(hopRun.isAppendLogFile());
    String content = writeSecondRun(hopRun);
    assertFalse(content.contains(EARLIER_RUN), content);
    assertTrue(content.contains(SECOND_RUN), content);
  }

  @Test
  void logfileAppendKeepsExistingContent() throws Exception {
    Path logFile = tempDir.resolve("append.log");
    Files.writeString(logFile, EARLIER_RUN + System.lineSeparator());

    HopRun hopRun = parse("--logfile", logFile.toString(), "--logfile-append");

    assertTrue(hopRun.isAppendLogFile());
    String content = writeSecondRun(hopRun);
    assertTrue(content.contains(EARLIER_RUN), content);
    assertTrue(content.contains(SECOND_RUN), content);
    assertTrue(content.indexOf(EARLIER_RUN) < content.indexOf(SECOND_RUN), content);
  }

  @Test
  void shortAppendOptionIsNotALogLevel() {
    HopRun hopRun = parse("-l", "BASIC", "-lf", "run.log", "-lfa");

    assertEquals("BASIC", hopRun.getLevel());
    assertEquals("run.log", hopRun.getLogFile());
    assertTrue(hopRun.isAppendLogFile());
  }

  @Test
  void appendWithoutLogfileStaysUnset() {
    HopRun hopRun = parse("--logfile-append");

    assertTrue(hopRun.isAppendLogFile());
    assertTrue(hopRun.getLogFile() == null || hopRun.getLogFile().isEmpty());
  }

  @Test
  void helpDocumentsAppendOption() {
    StringWriter writer = new StringWriter();
    new CommandLine(new HopRun()).usage(new PrintWriter(writer, true));
    String usage = writer.toString().replaceAll("\\s+", " ");

    assertTrue(usage.contains("-lfa, --logfile-append"), usage);
    assertTrue(usage.contains("instead of replacing"), usage);
  }

  private static HopRun parse(String... args) {
    HopRun hopRun = new HopRun();
    new CommandLine(hopRun).parseArgs(args);
    return hopRun;
  }

  private static String writeSecondRun(HopRun hopRun) throws Exception {
    FileLoggingEventListener listener = hopRun.createFileLoggingEventListener();
    try {
      listener.eventAdded(
          new HopLoggingEvent(
              new LogMessage(SECOND_RUN, "channel", LogLevel.BASIC),
              System.currentTimeMillis(),
              LogLevel.BASIC));
      assertNull(listener.getException());
    } finally {
      listener.close();
    }
    return Files.readString(Path.of(hopRun.getLogFile()));
  }
}
