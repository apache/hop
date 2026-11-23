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

package org.apache.hop.imp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * {@code hop import} must exit 1 when it stops before importing anything (#9441).
 *
 * <p>{@code hop import} is {@code Hop.main} calling picocli {@code execute()}. {@link HopImport}
 * implements {@link java.util.concurrent.Callable} so that return value is the exit code. A {@link
 * Runnable} that returns is always exit code 0.
 */
class HopImportExitCodeTest {

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @Test
  void noInputOrOutputFolderShouldExitOne() {
    assertImportExitsOne();
  }

  @Test
  void inputWithoutOutputFolderShouldExitOne() {
    assertImportExitsOne("-i", "hop-issue-9441/input");
  }

  @Test
  void unknownPluginTypeShouldExitOne() {
    assertImportExitsOne(
        "-t", "nosuchtype", "-i", "hop-issue-9441/input", "-o", "hop-issue-9441/output");
  }

  /**
   * Same early return {@code hop import} hits, compared with the code {@code hop-import.bat} would
   * use.
   */
  private static void assertImportExitsOne(String... args) {
    HopImport hopImport = new HopImport();
    CommandLine cmd = new CommandLine(hopImport);
    hopImport.setCmd(cmd);

    int hopCommandExit = cmd.execute(args);
    int hopImportBatExit = hopImport.isFinishedWithoutError() ? 0 : 1;

    assertFalse(hopImport.isFinishedWithoutError());
    assertEquals(1, hopImportBatExit);
    assertEquals(
        hopImportBatExit,
        hopCommandExit,
        "hop import exited "
            + hopCommandExit
            + " but hop-import.bat would exit "
            + hopImportBatExit
            + " (issue #9441)");
  }
}
