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

package org.apache.hop.workflow.actions.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class ActionPipelineCheckTest {

  @Test
  void aMissingFileIsReportedOnFilename() {
    List<String> errors = errors(new ActionPipeline());

    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("filename"));
    assertTrue(errors.stream().noneMatch(text -> text.contains("pipeline-name")));
    assertTrue(errors.stream().noneMatch(text -> text.contains("directory")));
  }

  @Test
  void aSetFilenamePasses() {
    ActionPipeline action = new ActionPipeline();
    action.setFileName("pipelines/load.hpl");

    assertTrue(errors(action).isEmpty());
  }

  private static List<String> errors(ActionPipeline action) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, new Variables(), null);
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
