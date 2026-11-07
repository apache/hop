/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.documentation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.Result;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class ActionDocTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path projectHome;
  @TempDir Path target;

  private ActionDoc newAction(String targetFolder) {
    ActionDoc action = new ActionDoc();
    action.setName("doc");
    action.setMetadataProvider(new MemoryMetadataProvider());
    action.setVariable(DocBuilder.VAR_PROJECT_NAME, "test");
    action.setVariable(DocBuilder.VAR_PROJECT_HOME, projectHome.toString());
    action.setTargetParentFolder(targetFolder);
    action.setIncludingMetadata(false);
    return action;
  }

  @Test
  void writtenDocumentationIsTrueAfterAFalseAction() throws Exception {
    Files.writeString(projectHome.resolve("readme.txt"), "not documented");

    // The previous action was false, for example on an unconditional or a failure hop
    Result previous = new Result();
    previous.setResult(false);

    Result result = newAction(target.toString()).execute(previous, 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertTrue(Files.exists(target.resolve(DocBuilder.INDEX_MD)));
  }

  @Test
  void targetThatIsAFileIsFalse() throws Exception {
    Path file = Files.writeString(target.resolve("a-file"), "x");

    Result result = newAction(file.toString()).execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }
}
