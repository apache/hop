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

package org.apache.hop.pipeline.transforms.tableoutput;

import java.util.HashSet;
import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The rows per statement can be set by metadata injection. */
class TableOutputMetaInjectionTest extends BaseMetadataInjectionTestJunit5<TableOutputMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    setup(new TableOutputMeta());
  }

  @Test
  void injectsTheRowsPerStatement() throws Exception {
    check("ROWS_PER_STATEMENT", () -> meta.getRowsPerStatement());
    check("COMMIT_SIZE", () -> meta.getCommitSize());
    check("USE_BATCH_UPDATE", () -> meta.isUseBatchUpdate());
    // The other options predate this test and are not part of it.
    for (String property : new HashSet<>(nonTestedProperties)) {
      skipPropertyTest(property);
    }
  }
}
