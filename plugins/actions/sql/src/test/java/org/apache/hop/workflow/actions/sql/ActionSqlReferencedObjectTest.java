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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ActionSqlReferencedObjectTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void noReferencesWhenSqlIsInline() {
    ActionSql action = new ActionSql();
    action.setSqlFromFile(false);
    action.setSqlFilename("/some/query.sql");

    assertNull(action.getReferencedObjectDescriptions());
    assertNull(action.isReferencedObjectEnabled());
  }

  @Test
  void referencedObjectDisabledWhenFilenameEmpty() {
    ActionSql action = new ActionSql();
    action.setSqlFromFile(true);
    action.setSqlFilename("");

    assertEquals(1, action.getReferencedObjectDescriptions().length);
    assertFalse(action.isReferencedObjectEnabled()[0]);
  }

  @Test
  void referencedObjectEnabledWhenFilenameSet() throws HopException {
    ActionSql action = new ActionSql();
    action.setSqlFromFile(true);
    action.setSqlFilename("${PROJECT_HOME}/queries/setup.sql");

    assertEquals(1, action.getReferencedObjectDescriptions().length);
    assertTrue(action.isReferencedObjectEnabled()[0]);

    IHasFilename reference =
        action.loadReferencedObject(0, new MemoryMetadataProvider(), new Variables());
    assertNotNull(reference);
    assertEquals("${PROJECT_HOME}/queries/setup.sql", reference.getFilename());
  }

  @Test
  void loadReferencedObjectRejectsInvalidIndex() {
    ActionSql action = new ActionSql();
    action.setSqlFromFile(true);
    action.setSqlFilename("/queries/setup.sql");

    assertThrows(
        HopException.class,
        () -> action.loadReferencedObject(1, new MemoryMetadataProvider(), new Variables()));
  }
}
