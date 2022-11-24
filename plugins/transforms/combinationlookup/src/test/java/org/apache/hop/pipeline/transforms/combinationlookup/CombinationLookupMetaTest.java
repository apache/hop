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

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class CombinationLookupMetaTest {
  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  public void testSerialization() throws Exception {
    TransformSerializationTestUtil.testSerialization(
        "/combination-lookup-transform.xml", CombinationLookupMeta.class);
  }

  @Test
  public void testProvidesModelerMeta() throws Exception {
    final IVariables variables = new Variables();
    final RowMeta rowMeta = Mockito.mock(RowMeta.class);
    final CombinationLookupMeta meta =
        new CombinationLookupMeta() {
          @Override
          Database createDatabaseObject(IVariables variables) {
            return Mockito.mock(Database.class);
          }

          @Override
          protected IRowMeta getDatabaseTableFields(
              Database db, String schemaName, String tableName) throws HopDatabaseException {
            assertEquals("aSchema", schemaName);
            assertEquals("aDimTable", tableName);
            return rowMeta;
          }
        };
    meta.getFields().getKeyFields().add(new KeyField("f1", "s4"));
    meta.getFields().getKeyFields().add(new KeyField("f2", "s5"));
    meta.getFields().getKeyFields().add(new KeyField("f3", "s6"));
    meta.setSchemaName("aSchema");
    meta.setTableName("aDimTable");

    final CombinationLookupData data = new CombinationLookupData();
    assertEquals(rowMeta, meta.getRowMeta(variables, data));
    assertEquals(3, meta.getDatabaseFields().size());
    assertEquals("f1", meta.getDatabaseFields().get(0));
    assertEquals("f2", meta.getDatabaseFields().get(1));
    assertEquals("f3", meta.getDatabaseFields().get(2));
    assertEquals(3, meta.getStreamFields().size());
    assertEquals("s4", meta.getStreamFields().get(0));
    assertEquals("s5", meta.getStreamFields().get(1));
    assertEquals("s6", meta.getStreamFields().get(2));
  }
}
