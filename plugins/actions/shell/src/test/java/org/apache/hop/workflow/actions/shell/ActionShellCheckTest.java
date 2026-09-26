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

package org.apache.hop.workflow.actions.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.junit.jupiter.api.Test;

/** A shell action runs a script file or the text on the Script tab, not both. */
class ActionShellCheckTest {

  @Test
  void aScriptOnTheScriptTabDoesNotRequireAFilename() {
    ActionShell action = shellWithWorkingDirectory();
    action.setInsertScript(true);
    action.setScript("echo hello");

    assertTrue(errors(action).isEmpty());
  }

  @Test
  void aScriptFileDoesNotRequireScriptText() {
    ActionShell action = shellWithWorkingDirectory();
    action.setFilename("${PROJECT_HOME}/bootstrap-retail-work.py");

    assertTrue(errors(action).isEmpty());
  }

  @Test
  void aMissingScriptIsReportedWhenTheScriptTabIsUsed() {
    ActionShell action = shellWithWorkingDirectory();
    action.setInsertScript(true);

    List<String> errors = errors(action);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("script to execute"));
  }

  @Test
  void aMissingFilenameIsReportedWhenAFileIsUsed() {
    ActionShell action = shellWithWorkingDirectory();

    List<String> errors = errors(action);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("filename"));
  }

  private static ActionShell shellWithWorkingDirectory() {
    ActionShell action = new ActionShell();
    // The working directory is checked on its own. Give it a value so these tests see only the
    // script-or-file result.
    action.setWorkDirectory(".");
    return action;
  }

  private static List<String> errors(ActionShell action) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, action, null);
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
