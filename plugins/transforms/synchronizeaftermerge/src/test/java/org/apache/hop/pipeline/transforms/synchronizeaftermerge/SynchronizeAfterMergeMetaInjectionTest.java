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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SynchronizeAfterMergeMetaInjectionTest
    extends BaseMetadataInjectionTestJunit5<SynchronizeAfterMergeMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    super.setup(new SynchronizeAfterMergeMeta());
  }

  @Test
  void test() throws Exception {
    check("SHEMA_NAME", (IStringGetter) () -> meta.getSchemaName());
    check("TABLE_NAME", (IStringGetter) () -> meta.getTableName());
    check("TABLE_FIELD", (IStringGetter) () -> meta.getKeyLookup()[0]);
    check("STREAM_FIELD1", (IStringGetter) () -> meta.getKeyStream()[0]);
    check("STREAM_FIELD2", (IStringGetter) () -> meta.getKeyStream2()[0]);
    check("COMPARATOR", (IStringGetter) () -> meta.getKeyCondition()[0]);

    check("UPDATE_TABLE_FIELD", (IStringGetter) () -> meta.getUpdateLookup()[0]);
    check("STREAM_FIELD", (IStringGetter) () -> meta.getUpdateStream()[0]);
    check("UPDATE", (IBooleanGetter) () -> meta.getUpdate()[0]);

    check("COMMIT_SIZE", (IStringGetter) () -> meta.getCommitSize());
    check("TABLE_NAME_IN_FIELD", (IBooleanGetter) () -> meta.isTableNameInField());
    check("TABLE_NAME_FIELD", (IStringGetter) () -> meta.getTableNameField());
    check("OPERATION_ORDER_FIELD", (IStringGetter) () -> meta.getOperationOrderField());
    check("USE_BATCH_UPDATE", (IBooleanGetter) () -> meta.useBatchUpdate());
    check("PERFORM_LOOKUP", (IBooleanGetter) () -> meta.isPerformLookup());
    check("ORDER_INSERT", (IStringGetter) () -> meta.getOrderInsert());
    check("ORDER_UPDATE", (IStringGetter) () -> meta.getOrderUpdate());
    check("ORDER_DELETE", (IStringGetter) () -> meta.getOrderDelete());
    check("CONNECTION_NAME", (IStringGetter) () -> "My Connection", "My Connection");
  }

  @Test
  void getXml() throws HopException {
    skipProperties(
        "CONNECTION_NAME",
        "TABLE_NAME",
        "STREAM_FIELD2",
        "PERFORM_LOOKUP",
        "COMPARATOR",
        "OPERATION_ORDER_FIELD",
        "ORDER_DELETE",
        "SHEMA_NAME",
        "TABLE_NAME_IN_FIELD",
        "ORDER_UPDATE",
        "ORDER_INSERT",
        "USE_BATCH_UPDATE",
        "STREAM_FIELD",
        "TABLE_FIELD",
        "COMMIT_SIZE",
        "TABLE_NAME_FIELD");
    meta.setDefault();
    check("STREAM_FIELD1", (IStringGetter) () -> meta.getKeyStream()[0]);
    check("UPDATE_TABLE_FIELD", (IStringGetter) () -> meta.getUpdateLookup()[0]);
    check("UPDATE", (IBooleanGetter) () -> meta.getUpdate()[0]);

    meta.getXml();

    String[] actualKeyLookup = meta.getKeyLookup();
    assertNotNull(actualKeyLookup);
    assertEquals(1, actualKeyLookup.length);

    String[] actualKeyCondition = meta.getKeyCondition();
    assertNotNull(actualKeyCondition);
    assertEquals(1, actualKeyCondition.length);

    String[] actualKeyStream2 = meta.getKeyCondition();
    assertNotNull(actualKeyStream2);
    assertEquals(1, actualKeyStream2.length);

    String[] actualUpdateStream = meta.getUpdateStream();
    assertNotNull(actualUpdateStream);
    assertEquals(1, actualUpdateStream.length);
  }

  private void skipProperties(String... propertyName) {
    for (String property : propertyName) {
      skipPropertyTest(property);
    }
  }
}
