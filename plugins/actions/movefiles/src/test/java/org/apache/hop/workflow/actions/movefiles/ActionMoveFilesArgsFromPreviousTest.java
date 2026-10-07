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

package org.apache.hop.workflow.actions.movefiles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Moving files with "Copy previous results to args". See https://github.com/apache/hop/issues/4045
 */
class ActionMoveFilesArgsFromPreviousTest {

  @TempDir Path testFolder;

  @BeforeAll
  static void init() {
    HopLogStore.init();
  }

  private static RowMetaAndData row(String src, String dst, String wildcard) {
    RowMetaAndData r = new RowMetaAndData();
    r.addValue(new ValueMetaString("src_path"), src);
    r.addValue(new ValueMetaString("dst_path"), dst);
    r.addValue(new ValueMetaString("wildcard"), wildcard);
    return r;
  }

  @Test
  void renameFileInSameFolderFromPreviousRows() throws Exception {
    Path src = Files.createFile(testFolder.resolve("expIraiser_2024_06_07.csv"));
    Path dst = testFolder.resolve("_deleteme_expIraiser_2024_06_07.csv");

    ActionMoveFiles action = MoveFilesActionHelper.defaultAction();
    action.setLogLevel(LogLevel.DETAILED);
    action.setArgFromPrevious(true);
    action.setDestinationIsAFile(true);

    Result previous = new Result();
    previous.getRows().add(row(src.toString(), dst.toString(), null));

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult(), "move from previous rows must succeed");
    assertFalse(Files.exists(src));
    assertTrue(Files.exists(dst));
  }

  /** Detailed logging of a skipped row used to read the (empty) file grid instead of the row. */
  @Test
  void rowWithEmptyDestinationIsIgnoredWithoutGridEntries() throws Exception {
    ActionMoveFiles action = MoveFilesActionHelper.defaultAction();
    action.setLogLevel(LogLevel.DETAILED);
    action.setArgFromPrevious(true);

    Result previous = new Result();
    previous.getRows().add(row(testFolder.resolve("a.csv").toString(), null, null));

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult(), "an ignored row is not an error");
  }
}
