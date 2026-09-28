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

package org.apache.hop.workflow.actions.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.junit.jupiter.api.Test;

/** SQL comes from the script field or from a file. Checking the unused one is a false positive. */
class ActionSqlCheckTest {

  @Test
  void sqlFromAFileDoesNotRequireTheScript() {
    ActionSql action = new ActionSql();
    action.setSqlFromFile(true);
    action.setSqlFilename("${PROJECT_HOME}/create-staging-views.sql");

    assertTrue(errors(action).isEmpty());
  }

  @Test
  void aTypedScriptDoesNotRequireAFile() {
    ActionSql action = new ActionSql();
    action.setSql("drop table staging_customer");

    assertTrue(errors(action).isEmpty());
  }

  @Test
  void aMissingFileIsReportedWhenSqlComesFromAFile() {
    ActionSql action = new ActionSql();
    action.setSqlFromFile(true);

    List<String> errors = errors(action);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("SQL filename"));
  }

  @Test
  void aMissingScriptIsReportedWhenSqlIsTyped() {
    ActionSql action = new ActionSql();

    List<String> errors = errors(action);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("SQL to execute"));
  }

  private static List<String> errors(ActionSql action) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, null, null);
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
