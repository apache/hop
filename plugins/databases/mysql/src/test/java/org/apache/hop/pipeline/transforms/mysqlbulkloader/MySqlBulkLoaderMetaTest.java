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

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.validation.ReferencedDatabaseConnectionChecker;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MySqlBulkLoaderMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testCheckWithMissingConnectionDoesNotAbortPipelineCheck() throws Exception {
    // Regression for #9386: a connection name that is not in the metadata used to throw a
    // NullPointerException from check(), which failed the whole pipeline check.
    HopEnvironment.init();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("missing-connection");
    pipelineMeta.setMetadataProvider(metadataProvider);

    MySqlBulkLoaderMeta meta = new MySqlBulkLoaderMeta();
    meta.setConnection("mysql");
    meta.setTableName("target");
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    pipelineMeta.addTransform(new TransformMeta(pluginId, "loader", meta));

    List<ICheckResult> remarks = new ArrayList<>();
    assertDoesNotThrow(
        () ->
            pipelineMeta.checkTransforms(remarks, false, null, new Variables(), metadataProvider));

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST.equals(
                        r.getErrorCode())));
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && BaseMessages.getString(
                                MySqlBulkLoaderMeta.class,
                                "MySqlBulkLoaderMeta.CheckResult.InvalidConnection")
                            .equals(r.getText())));
  }

  @Test
  void testMissingFieldFormatDefaultsToOk() {
    // Regression: a field with no/empty <field_format_ok> (legacy, injection-generated or
    // hand-edited fields) must behave as OK, not null. Null caused a dialog NPE on open and changed
    // the bytes written to the bulk-load file at runtime.
    MySqlBulkLoaderMeta.Field field = new MySqlBulkLoaderMeta.Field();
    assertEquals(MySqlBulkLoaderMeta.FieldFormatType.OK, field.getFieldFormatType());
  }

  @Test
  void testNewSerialization() throws Exception {
    MySqlBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/mysql-bulkloader-transform.xml", MySqlBulkLoaderMeta.class);

    assertEquals("testTable", meta.getTableName());
    assertEquals("mysql", meta.getConnection());
    assertEquals("\\", meta.getEscapeChar());
    assertEquals("\"", meta.getEnclosure());
    assertEquals("\t", meta.getDelimiter());
    assertEquals("/tmp/fifo", meta.getFifoFileName());
    assertEquals(2, meta.getFields().size());
    assertFalse(meta.isIgnoringErrors());
    assertFalse(meta.isReplacingData());
    assertTrue(meta.isLocalFile());
  }

  @Test
  void testClone() throws Exception {
    MySqlBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/mysql-bulkloader-transform.xml", MySqlBulkLoaderMeta.class);

    MySqlBulkLoaderMeta clone = (MySqlBulkLoaderMeta) meta.clone();

    assertEquals(clone.getTableName(), meta.getTableName());
    assertEquals(clone.getConnection(), meta.getConnection());
    assertEquals(clone.getEscapeChar(), meta.getEscapeChar());
    assertEquals(clone.getEnclosure(), meta.getEnclosure());
    assertEquals(clone.getDelimiter(), meta.getDelimiter());
    assertEquals(clone.getFifoFileName(), meta.getFifoFileName());
    assertEquals(clone.getFields().size(), meta.getFields().size());
    assertEquals(clone.isIgnoringErrors(), meta.isIgnoringErrors());
    assertEquals(clone.isReplacingData(), meta.isReplacingData());
    assertEquals(clone.isLocalFile(), meta.isLocalFile());
  }
}
