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

package org.apache.hop.pipeline.transforms.mergeintotable;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Every option of Merge into table can be set by metadata injection. */
class MergeIntoTableMetaInjectionTest extends BaseMetadataInjectionTestJunit5<MergeIntoTableMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    setup(new MergeIntoTableMeta());
  }

  @Test
  void injectsEveryOption() throws Exception {
    check("CONNECTION_NAME", () -> meta.getConnection());
    check("SCHEMA_NAME", () -> meta.getSchemaName());
    check("TABLE_NAME", () -> meta.getTableName());
    check("COMMIT_SIZE", () -> meta.getCommitSize());
    check("USE_BATCH", () -> meta.isUseBatchUpdate());
    check("UPSERT_FORM", () -> meta.getUpsertForm(), MergeUpsertForm.class);
    check("UPDATE_EXISTING", () -> meta.isUpdateExisting());
    check("UPDATE_ONLY_CHANGED", () -> meta.isUpdateOnlyChanged());
    check("KEY_STREAM_FIELD", () -> meta.getKeyFields().get(0).getStreamField());
    check("KEY_TABLE_COLUMN", () -> meta.getKeyFields().get(0).getTableColumn());
    check("VALUE_STREAM_FIELD", () -> meta.getValueFields().get(0).getStreamField());
    check("VALUE_TABLE_COLUMN", () -> meta.getValueFields().get(0).getTableColumn());
    check("VALUE_UPDATE", () -> meta.getValueFields().get(0).isUpdate());
  }
}
