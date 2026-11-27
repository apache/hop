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

package org.apache.hop.pipeline.transforms.insertupdate;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Metadata injection of the Insert/Update options. */
class InsertUpdateMetaInjectionTest extends BaseMetadataInjectionTestJunit5<InsertUpdateMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    setup(new InsertUpdateMeta());
  }

  @Test
  void test() throws Exception {
    check("CONNECTIONNAME", () -> meta.getConnection());
    check("COMMIT_SIZE", () -> meta.getCommitSize());
    check("DO_NOT", () -> meta.isUpdateBypassed());
    check("BATCH_UPDATE", () -> meta.isUseBatchUpdate());
    check("SCHEMA_NAME", () -> meta.getInsertUpdateLookupField().getSchemaName());
    check("TABLE_NAME", () -> meta.getInsertUpdateLookupField().getTableName());
    check(
        "KEY_STREAM",
        () -> meta.getInsertUpdateLookupField().getLookupKeys().get(0).getKeyStream());
    check(
        "KEY_LOOKUP",
        () -> meta.getInsertUpdateLookupField().getLookupKeys().get(0).getKeyLookup());
    check(
        "KEY_CONDITION",
        () -> meta.getInsertUpdateLookupField().getLookupKeys().get(0).getKeyCondition());
    check(
        "KEY_STREAM2",
        () -> meta.getInsertUpdateLookupField().getLookupKeys().get(0).getKeyStream2());
    check(
        "UPDATE_LOOKUP",
        () -> meta.getInsertUpdateLookupField().getValueFields().get(0).getUpdateLookup());
    check(
        "UPDATE_STREAM",
        () -> meta.getInsertUpdateLookupField().getValueFields().get(0).getUpdateStream());
    check(
        "UPDATE_FLAG", () -> meta.getInsertUpdateLookupField().getValueFields().get(0).isUpdate());
  }
}
