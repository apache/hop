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

package org.apache.hop.workflow.actions.mssqlbulkload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ActionMssqlBulkLoadCheckTest {
  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void checkReadsFileNameAndTableName() throws Exception {
    Path dataFile = Files.writeString(tempDir.resolve("customers.csv"), "a\n");
    ActionMssqlBulkLoad action = new ActionMssqlBulkLoad();
    action.setFileName(dataFile.toAbsolutePath().toString());
    action.setTableName("customers");

    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, new Variables(), null);

    assertTrue(errors(remarks).isEmpty(), () -> errors(remarks).toString());
  }

  @Test
  void checkReportsEmptySpecificCodepage() throws Exception {
    assertEquals(1, errors(checkSpecificCodePage("", new Variables())).size());
  }

  @Test
  void checkReportsSpecificCodepageThatIsNotACodepage() throws Exception {
    assertEquals(1, errors(checkSpecificCodePage("65001'", new Variables())).size());
  }

  @Test
  void checkAcceptsSpecificCodepageNumberOrVariable() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("CP", "1252");

    assertTrue(errors(checkSpecificCodePage("65001", variables)).isEmpty());
    assertTrue(errors(checkSpecificCodePage("${CP}", variables)).isEmpty());
    // Only known at run time
    assertTrue(errors(checkSpecificCodePage("${NOT_SET_YET}", variables)).isEmpty());
  }

  private List<ICheckResult> checkSpecificCodePage(String specificCodePage, Variables variables)
      throws Exception {
    Path dataFile = Files.writeString(tempDir.resolve("customers.csv"), "a\n");
    ActionMssqlBulkLoad action = new ActionMssqlBulkLoad();
    action.setFileName(dataFile.toAbsolutePath().toString());
    action.setTableName("customers");
    action.setCodePage("Specific");
    action.setSpecificCodePage(specificCodePage);

    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, variables, null);
    return remarks;
  }

  private static List<String> errors(List<ICheckResult> remarks) {
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
