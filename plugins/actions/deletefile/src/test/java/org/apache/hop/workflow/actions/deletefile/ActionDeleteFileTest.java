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

package org.apache.hop.workflow.actions.deletefile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ActionDeleteFileTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @Test
  void executeDeletesTheFile() throws Exception {
    Path file = Files.createFile(tempDir.resolve("delete-me.txt"));
    ActionDeleteFile action = new ActionDeleteFile();
    action.setFilename(file.toString());

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertFalse(Files.exists(file));
  }

  @Test
  void executeFailsWhenTheFileCannotBeDeleted() throws Exception {
    Path folder = Files.createDirectory(tempDir.resolve("not-empty"));
    Files.createFile(folder.resolve("child.txt"));
    ActionDeleteFile action = new ActionDeleteFile();
    action.setFilename(folder.toString());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    assertTrue(Files.exists(folder));
  }
}
